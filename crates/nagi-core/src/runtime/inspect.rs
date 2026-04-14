#[cfg(feature = "bigquery")]
pub mod bigquery;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InspectError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid asset name for path: {0}")]
    InvalidAssetName(String),

    #[error("connection error: {0}")]
    Connection(String),
}

/// A single condition's evaluation result snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConditionSnapshot {
    pub name: String,
    pub status: crate::runtime::evaluate::ConditionStatus,
    pub detail: Option<serde_json::Value>,
}

/// Physical object state snapshot at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PhysicalObjectState {
    pub object_type: String,
    pub metrics: HashMap<String, serde_json::Value>,
}

/// A job executed on the destination during a sync.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DestinationJob {
    pub job_id: String,
    pub statement_type: Option<String>,
    pub details: HashMap<String, serde_json::Value>,
}

/// Snapshot of an Asset's state at a point in time (before or after sync).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncSnapshot {
    pub evaluations: Vec<ConditionSnapshot>,
    pub physical_object: Option<PhysicalObjectState>,
}

/// The inspection record for a single sync execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncInspection {
    pub schema_version: u32,
    pub execution_id: String,
    pub asset_name: String,
    pub before_sync: SyncSnapshot,
    pub after_sync: SyncSnapshot,
    pub destination_jobs: Vec<DestinationJob>,
}

impl SyncInspection {
    pub const CURRENT_SCHEMA_VERSION: u32 = 1;

    pub fn new(execution_id: String, asset_name: String) -> Self {
        Self {
            schema_version: Self::CURRENT_SCHEMA_VERSION,
            execution_id,
            asset_name,
            before_sync: SyncSnapshot {
                evaluations: Vec::new(),
                physical_object: None,
            },
            after_sync: SyncSnapshot {
                evaluations: Vec::new(),
                physical_object: None,
            },
            destination_jobs: Vec::new(),
        }
    }
}

/// Manages inspection file storage under `<nagi_dir>/inspections/`.
pub struct InspectionStore {
    base_dir: PathBuf,
}

impl InspectionStore {
    pub fn new(nagi_dir: &Path) -> Self {
        Self {
            base_dir: nagi_dir.join("inspections"),
        }
    }

    /// Writes an inspection to disk. Creates directories as needed.
    /// If the file already exists, it is overwritten.
    ///
    /// Not atomic: a concurrent reader may see a partial file. Callers must
    /// ensure single-writer access per (asset_name, execution_id) pair.
    pub fn write(&self, inspection: &SyncInspection) -> Result<PathBuf, InspectError> {
        let asset_dir = self.asset_dir(&inspection.asset_name)?;
        std::fs::create_dir_all(&asset_dir)?;
        let path = asset_dir.join(format!("{}.json", inspection.execution_id));
        let json = serde_json::to_string_pretty(inspection)?;
        std::fs::write(&path, json)?;
        Ok(path)
    }

    /// Reads an inspection by asset name and execution_id.
    #[allow(dead_code)]
    pub(crate) fn read(
        &self,
        asset_name: &str,
        execution_id: &str,
    ) -> Result<SyncInspection, InspectError> {
        let path = self
            .asset_dir(asset_name)?
            .join(format!("{execution_id}.json"));
        let content = std::fs::read_to_string(path)?;
        let inspection: SyncInspection = serde_json::from_str(&content)?;
        Ok(inspection)
    }

    /// Lists inspections for an asset, returning up to `limit` most recent
    /// entries. Ordering relies on lexicographic sort of filenames
    /// (`<execution_id>.json`). This works because `generate_uuid()` produces
    /// IDs with a timestamp prefix, so lexicographic order matches chronological
    /// order.
    pub fn list(
        &self,
        asset_name: &str,
        limit: usize,
    ) -> Result<Vec<SyncInspection>, InspectError> {
        let asset_dir = self.asset_dir(asset_name)?;
        if !asset_dir.exists() {
            return Ok(Vec::new());
        }
        let mut entries: Vec<_> = std::fs::read_dir(&asset_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        let recent: Vec<_> = entries.into_iter().rev().take(limit).rev().collect();
        let mut inspections = Vec::with_capacity(recent.len());
        for entry in recent {
            let content = std::fs::read_to_string(entry.path())?;
            let inspection: SyncInspection = serde_json::from_str(&content)?;
            inspections.push(inspection);
        }
        Ok(inspections)
    }

    fn asset_dir(&self, asset_name: &str) -> Result<PathBuf, InspectError> {
        if asset_name.is_empty()
            || asset_name.contains('/')
            || asset_name.contains('\\')
            || asset_name == "."
            || asset_name == ".."
            || asset_name.contains("..")
        {
            return Err(InspectError::InvalidAssetName(asset_name.to_string()));
        }
        Ok(self.base_dir.join(asset_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::evaluate::ConditionStatus;

    fn sample_inspection() -> SyncInspection {
        let mut inspection = SyncInspection::new("exec-001".to_string(), "daily-sales".to_string());
        inspection.before_sync = SyncSnapshot {
            evaluations: vec![ConditionSnapshot {
                name: "freshness-24h".to_string(),
                status: ConditionStatus::Drifted {
                    reason: "age 30h exceeds max 24h".to_string(),
                },
                detail: Some(serde_json::json!({"age_hours": 30})),
            }],
            physical_object: Some(PhysicalObjectState {
                object_type: "BASE TABLE".to_string(),
                metrics: HashMap::from([("row_count".to_string(), serde_json::json!(1000))]),
            }),
        };
        inspection.after_sync = SyncSnapshot {
            evaluations: vec![ConditionSnapshot {
                name: "freshness-24h".to_string(),
                status: ConditionStatus::Ready,
                detail: Some(serde_json::json!({"age_hours": 0})),
            }],
            physical_object: Some(PhysicalObjectState {
                object_type: "BASE TABLE".to_string(),
                metrics: HashMap::from([("row_count".to_string(), serde_json::json!(1500))]),
            }),
        };
        inspection
    }

    #[test]
    fn write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let inspection = sample_inspection();
        store.write(&inspection).unwrap();
        let loaded = store.read("daily-sales", "exec-001").unwrap();
        assert_eq!(inspection, loaded);
    }

    #[test]
    fn schema_version_is_serialized() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let inspection = sample_inspection();
        let path = store.write(&inspection).unwrap();
        let raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(
            raw["schema_version"],
            SyncInspection::CURRENT_SCHEMA_VERSION
        );
    }

    #[test]
    fn list_returns_most_recent() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        for i in 1..=5 {
            let mut inspection =
                SyncInspection::new(format!("exec-{i:03}"), "my-asset".to_string());
            inspection.before_sync.evaluations = vec![];
            store.write(&inspection).unwrap();
        }
        let recent = store.list("my-asset", 3).unwrap();
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].execution_id, "exec-003");
        assert_eq!(recent[1].execution_id, "exec-004");
        assert_eq!(recent[2].execution_id, "exec-005");
    }

    #[test]
    fn list_empty_asset_returns_empty_vec() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let result = store.list("nonexistent", 10).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn write_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let mut inspection = sample_inspection();
        store.write(&inspection).unwrap();

        inspection.destination_jobs = vec![DestinationJob {
            job_id: "bqjob_123".to_string(),
            statement_type: Some("MERGE".to_string()),
            details: HashMap::new(),
        }];
        store.write(&inspection).unwrap();

        let loaded = store.read("daily-sales", "exec-001").unwrap();
        assert_eq!(loaded.destination_jobs.len(), 1);
        assert_eq!(loaded.destination_jobs[0].job_id, "bqjob_123");
    }

    #[test]
    fn invalid_asset_name_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let inspection = SyncInspection::new("exec-001".to_string(), "../evil".to_string());
        assert!(store.write(&inspection).is_err());
    }

    #[test]
    fn json_output_matches_file_content() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let inspection = sample_inspection();
        let path = store.write(&inspection).unwrap();
        let file_content = std::fs::read_to_string(path).unwrap();
        let json_output = serde_json::to_string_pretty(&inspection).unwrap();
        assert_eq!(file_content, json_output);
    }

    #[test]
    fn physical_object_state_with_custom_metrics() {
        let state = PhysicalObjectState {
            object_type: "VECTOR INDEX".to_string(),
            metrics: HashMap::from([
                ("indexed_row_count".to_string(), serde_json::json!(50000)),
                ("coverage".to_string(), serde_json::json!(0.95)),
                ("status".to_string(), serde_json::json!("ACTIVE")),
            ]),
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: PhysicalObjectState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, deserialized);
    }
}

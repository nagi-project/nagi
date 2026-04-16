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

    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),
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

/// Cached inspection record for a single sync execution.
/// This is derived data, not source of truth. See `InspectionStore` for details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncInspection {
    pub schema_version: u32,
    pub execution_id: String,
    pub asset_name: String,
    /// Sync completion timestamp in RFC3339 format.
    pub finished_at: String,
    pub before_sync: SyncSnapshot,
    pub after_sync: SyncSnapshot,
    pub destination_jobs: Vec<DestinationJob>,
}

impl SyncInspection {
    pub const CURRENT_SCHEMA_VERSION: u32 = 1;

    pub fn new(execution_id: String, asset_name: String, finished_at: String) -> Self {
        Self {
            schema_version: Self::CURRENT_SCHEMA_VERSION,
            execution_id,
            asset_name,
            finished_at,
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

    /// Returns true if `before_sync` differs from `after_sync`.
    /// Used to determine the `changed` / `nochange` flag in the filename.
    pub fn has_changes(&self) -> bool {
        self.before_sync != self.after_sync
    }
}

/// Converts an RFC3339 timestamp to ISO8601 basic format with millisecond
/// precision, suitable for use as a filename prefix.
/// Example: `2026-04-16T09:30:00.123+09:00` → `20260416T003000.123Z`.
///
/// The resulting string sorts lexicographically in chronological order.
/// Normalizes to UTC so that files from different timezones sort correctly.
fn timestamp_to_basic_format(rfc3339: &str) -> Result<String, InspectError> {
    let dt = chrono::DateTime::parse_from_rfc3339(rfc3339)
        .map_err(|e| InspectError::InvalidTimestamp(format!("{rfc3339}: {e}")))?
        .with_timezone(&chrono::Utc);
    Ok(dt.format("%Y%m%dT%H%M%S%.3fZ").to_string())
}

/// Builds the inspection filename.
/// Format: `<finished_at_basic>_<changed|nochange>.<execution_id>.json`
fn build_filename(inspection: &SyncInspection) -> Result<String, InspectError> {
    let ts = timestamp_to_basic_format(&inspection.finished_at)?;
    let flag = if inspection.has_changes() {
        "changed"
    } else {
        "nochange"
    };
    Ok(format!("{ts}_{flag}.{}.json", inspection.execution_id))
}

/// Checks whether a filename represents a "changed" inspection.
/// Expected format: `<timestamp>_<changed|nochange>.<execution_id>.json`
fn is_changed_filename(name: &str) -> bool {
    // Look for `_changed.` as a substring. This is unambiguous because
    // the timestamp part has no underscore and execution_id has no dot
    // before `.json`.
    name.contains("_changed.")
}

/// Cache for inspection data under `<nagi_dir>/inspections/`.
///
/// This is a cache, not a persistent store. The source of truth is:
/// - `evaluate_logs` in logs.db (for condition evaluation results)
/// - `INFORMATION_SCHEMA.TABLES` + `SELECT COUNT(*)` (for physical object state)
/// - `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (for jobs executed during sync)
///
/// Deleted files are not restored. New cache files are created from subsequent
/// sync executions.
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
    ///
    /// Removes any existing file for the same `execution_id` before writing,
    /// so that the `changed` / `nochange` flag in the filename stays consistent
    /// when an inspection is updated (e.g. after destination jobs are backfilled).
    ///
    /// Not atomic: a concurrent reader may see a partial file. Callers must
    /// ensure single-writer access per (asset_name, execution_id) pair.
    pub fn write(&self, inspection: &SyncInspection) -> Result<PathBuf, InspectError> {
        let asset_dir = self.asset_dir(&inspection.asset_name)?;
        std::fs::create_dir_all(&asset_dir)?;

        // Remove existing files for this execution_id (filename flag may change
        // between writes, though not currently — backfill preserves flag).
        self.remove_existing_for(&asset_dir, &inspection.execution_id)?;

        let filename = build_filename(inspection)?;
        let path = asset_dir.join(filename);
        let json = serde_json::to_string_pretty(inspection)?;
        std::fs::write(&path, json)?;
        Ok(path)
    }

    fn remove_existing_for(
        &self,
        asset_dir: &Path,
        execution_id: &str,
    ) -> Result<(), InspectError> {
        if !asset_dir.exists() {
            return Ok(());
        }
        let suffix = format!(".{execution_id}.json");
        for entry in std::fs::read_dir(asset_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().ends_with(&suffix) {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    /// Reads an inspection by asset name and execution_id.
    #[allow(dead_code)]
    pub(crate) fn read(
        &self,
        asset_name: &str,
        execution_id: &str,
    ) -> Result<SyncInspection, InspectError> {
        let asset_dir = self.asset_dir(asset_name)?;
        let suffix = format!(".{execution_id}.json");
        let entry = std::fs::read_dir(&asset_dir)?
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().ends_with(&suffix))
            .ok_or_else(|| {
                InspectError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("inspection not found: {asset_name}/{execution_id}"),
                ))
            })?;
        let content = std::fs::read_to_string(entry.path())?;
        let inspection: SyncInspection = serde_json::from_str(&content)?;
        Ok(inspection)
    }

    /// Lists inspections for an asset, returning up to `limit` most recent
    /// entries. Ordering relies on lexicographic sort of filenames. Filenames
    /// start with the finished_at timestamp in ISO8601 basic format (UTC),
    /// so lexicographic order matches chronological order.
    pub fn list(
        &self,
        asset_name: &str,
        limit: usize,
    ) -> Result<Vec<SyncInspection>, InspectError> {
        self.list_filtered(asset_name, limit, |_| true)
    }

    /// Lists inspections where `before_sync` differs from `after_sync`.
    /// Filtering uses filename alone (no file read required); only matching
    /// files are opened.
    pub fn list_changed(
        &self,
        asset_name: &str,
        limit: usize,
    ) -> Result<Vec<SyncInspection>, InspectError> {
        self.list_filtered(asset_name, limit, is_changed_filename)
    }

    fn list_filtered<F>(
        &self,
        asset_name: &str,
        limit: usize,
        name_filter: F,
    ) -> Result<Vec<SyncInspection>, InspectError>
    where
        F: Fn(&str) -> bool,
    {
        let asset_dir = self.asset_dir(asset_name)?;
        if !asset_dir.exists() {
            return Ok(Vec::new());
        }
        let mut entries: Vec<_> = std::fs::read_dir(&asset_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let name_str = name.to_string_lossy();
                name_str.ends_with(".json") && name_filter(&name_str)
            })
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

    const SAMPLE_FINISHED_AT: &str = "2026-04-16T09:30:00.123Z";

    fn sample_inspection() -> SyncInspection {
        let mut inspection = SyncInspection::new(
            "exec-001".to_string(),
            "daily-sales".to_string(),
            SAMPLE_FINISHED_AT.to_string(),
        );
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
            let inspection = SyncInspection::new(
                format!("exec-{i:03}"),
                "my-asset".to_string(),
                format!("2026-04-16T09:30:0{i}.000Z"),
            );
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
        let inspection = SyncInspection::new(
            "exec-001".to_string(),
            "../evil".to_string(),
            SAMPLE_FINISHED_AT.to_string(),
        );
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

    // ── has_changes / filename ────────────────────────────────────

    #[test]
    fn has_changes_detects_diff_in_before_and_after() {
        let inspection = sample_inspection();
        assert!(inspection.has_changes());
    }

    #[test]
    fn has_changes_returns_false_when_before_equals_after() {
        let mut inspection = SyncInspection::new(
            "exec-001".to_string(),
            "a".to_string(),
            SAMPLE_FINISHED_AT.to_string(),
        );
        inspection.after_sync = inspection.before_sync.clone();
        assert!(!inspection.has_changes());
    }

    #[test]
    fn filename_contains_timestamp_flag_and_execution_id() {
        let inspection = sample_inspection();
        let name = build_filename(&inspection).unwrap();
        assert!(name.starts_with("20260416T093000.123Z_changed."));
        assert!(name.ends_with(".exec-001.json"));
    }

    #[test]
    fn filename_uses_nochange_when_before_equals_after() {
        let mut inspection = SyncInspection::new(
            "exec-001".to_string(),
            "a".to_string(),
            SAMPLE_FINISHED_AT.to_string(),
        );
        inspection.after_sync = inspection.before_sync.clone();
        let name = build_filename(&inspection).unwrap();
        assert!(name.contains("_nochange."));
    }

    #[test]
    fn timestamp_normalized_to_utc() {
        let inspection = SyncInspection::new(
            "exec-001".to_string(),
            "a".to_string(),
            "2026-04-16T18:30:00.123+09:00".to_string(),
        );
        let name = build_filename(&inspection).unwrap();
        assert!(name.starts_with("20260416T093000.123Z"));
    }

    // ── list_changed ───────────────────────────────────────────────

    #[test]
    fn list_changed_returns_only_changed() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());

        // Write a mix of changed and nochange inspections.
        for i in 1..=4 {
            let mut inspection = SyncInspection::new(
                format!("exec-{i:03}"),
                "my-asset".to_string(),
                format!("2026-04-16T09:30:0{i}.000Z"),
            );
            // Odd: changed (from sample), Even: nochange
            if i % 2 == 0 {
                // leave before_sync == after_sync (both empty)
            } else {
                inspection.after_sync.evaluations = vec![ConditionSnapshot {
                    name: "c".to_string(),
                    status: ConditionStatus::Ready,
                    detail: None,
                }];
            }
            store.write(&inspection).unwrap();
        }

        let changed = store.list_changed("my-asset", 10).unwrap();
        assert_eq!(changed.len(), 2);
        assert!(changed.iter().all(|i| i.has_changes()));
        assert_eq!(changed[0].execution_id, "exec-001");
        assert_eq!(changed[1].execution_id, "exec-003");
    }

    #[test]
    fn list_changed_respects_limit() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        for i in 1..=5 {
            let mut inspection = SyncInspection::new(
                format!("exec-{i:03}"),
                "my-asset".to_string(),
                format!("2026-04-16T09:30:0{i}.000Z"),
            );
            inspection.after_sync.evaluations = vec![ConditionSnapshot {
                name: "c".to_string(),
                status: ConditionStatus::Ready,
                detail: None,
            }];
            store.write(&inspection).unwrap();
        }
        let changed = store.list_changed("my-asset", 2).unwrap();
        assert_eq!(changed.len(), 2);
        assert_eq!(changed[0].execution_id, "exec-004");
        assert_eq!(changed[1].execution_id, "exec-005");
    }

    #[test]
    fn write_removes_previous_file_for_same_execution_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let inspection = sample_inspection();
        store.write(&inspection).unwrap();
        // Write again (same execution_id, same content → same filename)
        store.write(&inspection).unwrap();

        let asset_dir = dir.path().join("inspections/daily-sales");
        let files: Vec<_> = std::fs::read_dir(&asset_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(files.len(), 1, "only one file should remain");
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

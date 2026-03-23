use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;

use crate::compile::{self, CompileError};
use crate::evaluate::AssetEvalResult;
use crate::log::{LogError, LogStore, SyncLogEntry};
use crate::serve::SuspendedInfo;
use crate::storage::local::{LocalCache, LocalSuspendedStore};
use crate::storage::{Cache, StorageError, SuspendedStore};

#[derive(Debug, Error)]
pub enum StatusError {
    #[error("{0}")]
    Compile(#[from] CompileError),
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("{0}")]
    Log(#[from] LogError),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetStatus {
    pub asset: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<AssetEvalResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sync: Option<Vec<SyncLogEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspended: Option<SuspendedInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusResult {
    pub assets: Vec<AssetStatus>,
}

/// Collects convergence status for compiled assets: cached evaluation + latest sync log + suspended state.
pub fn asset_status(
    target_dir: &Path,
    selectors: &[&str],
    cache_dir: Option<&Path>,
    db_path: &Path,
    logs_dir: &Path,
    suspended_dir: Option<&Path>,
) -> Result<StatusResult, StatusError> {
    let asset_names = compile::resolve_compiled_asset_names(target_dir, selectors)?;

    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| crate::config::default_nagi_dir().join("cache"));
    let cache = LocalCache::new(cache_path);

    let store = if db_path.exists() {
        Some(LogStore::open(db_path, logs_dir)?)
    } else {
        None
    };

    let suspended_store = suspended_dir.map(|d| LocalSuspendedStore::new(d.to_path_buf()));

    let mut assets = Vec::with_capacity(asset_names.len());
    for name in asset_names {
        let evaluation = cache.read(&name)?;

        let last_sync = match &store {
            Some(s) => {
                let entries = s.latest_sync_log(&name)?;
                if entries.is_empty() {
                    None
                } else {
                    Some(entries)
                }
            }
            None => None,
        };

        let suspended = match &suspended_store {
            Some(ss) => ss.read(&name)?,
            None => None,
        };

        assets.push(AssetStatus {
            asset: name,
            evaluation,
            last_sync,
            suspended,
        });
    }

    Ok(StatusResult { assets })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluate::{ConditionResult, ConditionStatus};
    use crate::sync::{Stage, StageResult, SyncExecutionResult, SyncType};

    fn setup_compiled_target(dir: &Path, asset_names: &[&str]) {
        let assets_dir = dir.join("assets");
        std::fs::create_dir_all(&assets_dir).unwrap();

        // Minimal graph.json
        let mut nodes = std::collections::HashMap::new();
        for name in asset_names {
            nodes.insert(name.to_string(), serde_json::json!({"dependencies": []}));
        }
        let graph = serde_json::json!(nodes);
        std::fs::write(dir.join("graph.json"), graph.to_string()).unwrap();

        // Minimal asset YAML files
        for name in asset_names {
            let yaml = format!(
                "apiVersion: nagi/v1alpha1\nkind: Asset\nmetadata:\n  name: {name}\nspec:\n  desiredSet: []\n  sync:\n    run:\n      command: echo ok\n"
            );
            std::fs::write(assets_dir.join(format!("{name}.yaml")), yaml).unwrap();
        }
    }

    fn sample_eval(name: &str) -> AssetEvalResult {
        AssetEvalResult {
            asset_name: name.to_string(),
            ready: true,
            conditions: vec![ConditionResult {
                condition_name: "check".to_string(),
                condition_type: "SQL".to_string(),
                status: ConditionStatus::Ready,
            }],
            evaluation_id: None,
        }
    }

    fn sample_sync_result(name: &str) -> SyncExecutionResult {
        SyncExecutionResult {
            execution_id: format!("exec-{name}"),
            asset_name: name.to_string(),
            sync_type: SyncType::Sync,
            stages: vec![StageResult {
                stage: Stage::Run,
                exit_code: 0,
                stdout: "ok".to_string(),
                stderr: "".to_string(),
                started_at: "2026-03-16T10:00:00+09:00".to_string(),
                finished_at: "2026-03-16T10:00:01+09:00".to_string(),
                args: vec!["echo".to_string()],
            }],
            success: true,
        }
    }

    #[test]
    fn returns_empty_when_no_assets() {
        let dir = tempfile::tempdir().unwrap();
        setup_compiled_target(dir.path(), &[]);

        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");

        let result = asset_status(dir.path(), &[], None, &db_path, &logs_dir, None).unwrap();
        assert!(result.assets.is_empty());
    }

    #[test]
    fn includes_cached_evaluation() {
        let dir = tempfile::tempdir().unwrap();
        setup_compiled_target(dir.path(), &["asset-a"]);

        let cache_dir = dir.path().join("cache");
        let cache = LocalCache::new(cache_dir.clone());
        cache.write(&sample_eval("asset-a")).unwrap();

        let db_path = dir.path().join("nonexistent.db");
        let logs_dir = dir.path().join("logs");

        let result =
            asset_status(dir.path(), &[], Some(&cache_dir), &db_path, &logs_dir, None).unwrap();
        assert_eq!(result.assets.len(), 1);
        assert_eq!(result.assets[0].asset, "asset-a");
        assert!(result.assets[0].evaluation.is_some());
        assert!(result.assets[0].last_sync.is_none());
    }

    #[test]
    fn includes_latest_sync_log() {
        let dir = tempfile::tempdir().unwrap();
        setup_compiled_target(dir.path(), &["asset-b"]);

        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();
        store
            .write_sync_log(&sample_sync_result("asset-b"))
            .unwrap();
        drop(store);

        let result = asset_status(dir.path(), &[], None, &db_path, &logs_dir, None).unwrap();
        assert_eq!(result.assets.len(), 1);
        assert!(result.assets[0].evaluation.is_none());
        assert!(result.assets[0].last_sync.is_some());
        assert_eq!(result.assets[0].last_sync.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn combines_evaluation_and_sync_log() {
        let dir = tempfile::tempdir().unwrap();
        setup_compiled_target(dir.path(), &["asset-c"]);

        let cache_dir = dir.path().join("cache");
        let cache = LocalCache::new(cache_dir.clone());
        cache.write(&sample_eval("asset-c")).unwrap();

        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();
        store
            .write_sync_log(&sample_sync_result("asset-c"))
            .unwrap();
        drop(store);

        let result =
            asset_status(dir.path(), &[], Some(&cache_dir), &db_path, &logs_dir, None).unwrap();
        assert_eq!(result.assets.len(), 1);
        assert!(result.assets[0].evaluation.is_some());
        assert!(result.assets[0].last_sync.is_some());
    }

    #[test]
    fn no_db_file_skips_sync_log() {
        let dir = tempfile::tempdir().unwrap();
        setup_compiled_target(dir.path(), &["asset-d"]);

        let db_path = dir.path().join("nonexistent.db");
        let logs_dir = dir.path().join("logs");

        let result = asset_status(dir.path(), &[], None, &db_path, &logs_dir, None).unwrap();
        assert_eq!(result.assets.len(), 1);
        assert!(result.assets[0].last_sync.is_none());
    }

    #[test]
    fn includes_suspended_info() {
        let dir = tempfile::tempdir().unwrap();
        setup_compiled_target(dir.path(), &["asset-e"]);

        let suspended_dir = dir.path().join("suspended");
        let ss = LocalSuspendedStore::new(suspended_dir.clone());
        ss.write(&SuspendedInfo {
            asset_name: "asset-e".to_string(),
            reason: "3 consecutive sync failures".to_string(),
            suspended_at: "2026-03-20T15:00:00Z".to_string(),
            execution_id: Some("exec-123".to_string()),
        })
        .unwrap();

        let db_path = dir.path().join("nonexistent.db");
        let logs_dir = dir.path().join("logs");

        let result = asset_status(
            dir.path(),
            &[],
            None,
            &db_path,
            &logs_dir,
            Some(&suspended_dir),
        )
        .unwrap();
        assert_eq!(result.assets.len(), 1);
        let status = &result.assets[0];
        assert!(status.suspended.is_some());
        let info = status.suspended.as_ref().unwrap();
        assert_eq!(info.asset_name, "asset-e");
        assert_eq!(info.reason, "3 consecutive sync failures");
    }

    #[test]
    fn no_suspended_dir_skips_suspended() {
        let dir = tempfile::tempdir().unwrap();
        setup_compiled_target(dir.path(), &["asset-f"]);

        let db_path = dir.path().join("nonexistent.db");
        let logs_dir = dir.path().join("logs");

        let result = asset_status(dir.path(), &[], None, &db_path, &logs_dir, None).unwrap();
        assert_eq!(result.assets.len(), 1);
        assert!(result.assets[0].suspended.is_none());
    }
}

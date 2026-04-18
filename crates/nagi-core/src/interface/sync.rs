use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::runtime::compile::CompiledAsset;
use crate::runtime::evaluate::AssetEvalResult;
use crate::runtime::log::LogStore;
use crate::runtime::sync::{
    dry_run_sync, evaluate_and_cache, generate_uuid, parse_sync_type, resolve_sync_spec,
    run_sync_workflow, DryRunStage, Stage, SyncError, SyncType, SyncWorkflowParams,
};

pub(crate) struct ProposeSyncParams<'a> {
    pub target_dir: &'a Path,
    pub selectors: &'a [&'a str],
    pub excludes: &'a [&'a str],
    pub sync_type: &'a str,
    pub stages: Option<&'a str>,
    pub cache_dir: Option<&'a Path>,
    pub db_path: Option<&'a Path>,
    pub logs_dir: Option<&'a Path>,
}

/// Builds sync proposals for compiled assets matching the selectors.
///
/// Evaluation or dry-run failures are not fatal — each proposal will omit
/// whichever part failed.
pub(crate) async fn propose_sync(
    params: ProposeSyncParams<'_>,
) -> Result<Vec<SyncProposal>, SyncError> {
    let assets = crate::runtime::compile::load_compiled_assets(
        params.target_dir,
        params.selectors,
        params.excludes,
    )?;
    let st = parse_sync_type(params.sync_type)?;
    let log_store = open_log_store(params.db_path, params.logs_dir)?;
    let default_timeout = crate::runtime::config::resolve_default_timeout();
    let mut proposals = Vec::with_capacity(assets.len());

    for (name, yaml) in &assets {
        let compiled: CompiledAsset =
            serde_yaml::from_str(yaml).map_err(|e| SyncError::Parse(e.to_string()))?;

        let evaluation = evaluate_for_proposal(
            &compiled,
            params.cache_dir,
            log_store.as_ref(),
            default_timeout,
        )
        .await
        .ok();

        let dry_run_stages = match resolve_sync_spec(&compiled) {
            Ok(sync_spec) => {
                let parsed_stages = params.stages.map(Stage::parse_list).transpose()?;
                let dr = dry_run_sync(name, &sync_spec, st, parsed_stages.as_deref());
                Some(dr.stages)
            }
            Err(_) => None,
        };

        proposals.push(SyncProposal {
            asset: name.clone(),
            yaml_content: yaml.clone(),
            sync_type: st,
            evaluation,
            stages: dry_run_stages,
        });
    }

    Ok(proposals)
}

async fn evaluate_for_proposal(
    compiled: &CompiledAsset,
    cache_dir: Option<&Path>,
    log_store: Option<&LogStore>,
    default_timeout: std::time::Duration,
) -> Result<SyncProposalEvaluation, SyncError> {
    let conn = compiled
        .connection
        .as_ref()
        .map(|c| {
            c.connect(default_timeout)
                .map_err(|e| SyncError::Connection(e.to_string()))
        })
        .transpose()?;
    let result = evaluate_and_cache(
        compiled,
        conn.as_deref(),
        log_store,
        cache_dir,
        default_timeout,
    )
    .await?;
    Ok(eval_result_to_proposal(&result))
}

fn eval_result_to_proposal(result: &AssetEvalResult) -> SyncProposalEvaluation {
    let conditions = result
        .conditions
        .iter()
        .map(|c| serde_json::to_value(c).unwrap_or_default())
        .collect();
    SyncProposalEvaluation {
        ready: result.ready,
        conditions,
        evaluation_id: result.evaluation_id.clone(),
    }
}

/// Result of `propose_sync`: evaluation + dry-run stages for user confirmation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncProposal {
    pub asset: String,
    #[serde(skip)]
    pub yaml_content: String,
    pub sync_type: SyncType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<SyncProposalEvaluation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stages: Option<Vec<DryRunStage>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncProposalEvaluation {
    pub ready: bool,
    pub conditions: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_id: Option<String>,
}

fn open_log_store(
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
) -> Result<Option<LogStore>, SyncError> {
    match (db_path, logs_dir) {
        (Some(db), Some(logs)) => Ok(Some(LogStore::open(db, logs)?)),
        _ => Ok(None),
    }
}

fn serialize<T: Serialize>(value: &T) -> Result<String, SyncError> {
    serde_json::to_string(value).map_err(|e| SyncError::Serialize(e.to_string()))
}

/// Parameters for `sync_from_compiled`.
pub(crate) struct SyncFromCompiledParams<'a> {
    pub yaml: &'a str,
    pub sync_type: &'a str,
    pub stages: Option<&'a str>,
    pub db_path: Option<&'a Path>,
    pub logs_dir: Option<&'a Path>,
    pub cache_dir: Option<&'a Path>,
    pub dry_run: bool,
    pub force: bool,
    pub evaluation_id: Option<&'a str>,
    pub default_timeout: std::time::Duration,
    /// When set, loads config from this directory to build a sync lock.
    pub project_dir: Option<&'a Path>,
}

/// Deserializes compiled YAML, delegates to runtime, and serializes the result.
pub(crate) async fn sync_from_compiled(
    params: SyncFromCompiledParams<'_>,
) -> Result<String, SyncError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(params.yaml).map_err(|e| SyncError::Parse(e.to_string()))?;
    let st = parse_sync_type(params.sync_type)?;
    let parsed_stages = params.stages.map(Stage::parse_list).transpose()?;

    if params.dry_run {
        let sync_spec = resolve_sync_spec(&compiled)?;
        let result = dry_run_sync(
            &compiled.metadata.name,
            &sync_spec,
            st,
            parsed_stages.as_deref(),
        );
        return serialize(&result);
    }

    // Acquire lock if project_dir is provided.
    let lock_state = params
        .project_dir
        .map(crate::runtime::storage::build_sync_lock_from_project)
        .transpose()
        .map_err(|e| SyncError::LockStorage(e.to_string()))?;
    if let Some((ref lock, ttl)) = lock_state {
        let execution_id = generate_uuid();
        let acquired = lock
            .acquire(&compiled.metadata.name, ttl, &execution_id)
            .map_err(|e| SyncError::LockStorage(e.to_string()))?;
        if !acquired {
            return Err(SyncError::LockAcquireFailed {
                asset_name: compiled.metadata.name.clone(),
            });
        }
    }

    let log_store = open_log_store(params.db_path, params.logs_dir)?;
    let result = run_sync_workflow(SyncWorkflowParams {
        compiled: &compiled,
        sync_type: st,
        stages: parsed_stages.as_deref(),
        force: params.force,
        evaluation_id: params.evaluation_id,
        log_store: log_store.as_ref(),
        cache_dir: params.cache_dir,
        default_timeout: params.default_timeout,
    })
    .await;

    if let Some((ref lock, _)) = lock_state {
        if let Err(e) = lock.release(&compiled.metadata.name) {
            tracing::warn!(asset_name = %compiled.metadata.name, error = %e, "failed to release sync lock");
        }
    }

    serialize(&result?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::evaluate::{AssetEvalResult, ConditionResult, ConditionStatus};
    use crate::runtime::storage::local::LocalSyncLock;
    use crate::runtime::storage::SyncLock;

    // ── eval_result_to_proposal ──────────────────────────────────────────

    #[test]
    fn eval_result_to_proposal_ready_with_no_conditions() {
        let result = AssetEvalResult {
            asset_name: "a".to_string(),
            ready: true,
            conditions: vec![],
            evaluation_id: None,
        };
        let proposal = eval_result_to_proposal(&result);
        assert!(proposal.ready);
        assert!(proposal.conditions.is_empty());
        assert!(proposal.evaluation_id.is_none());
    }

    #[test]
    fn eval_result_to_proposal_preserves_conditions_and_id() {
        let result = AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![ConditionResult {
                condition_name: "check".to_string(),
                condition_type: "SQL".to_string(),
                status: ConditionStatus::Drifted {
                    reason: "stale".to_string(),
                },
            }],
            evaluation_id: Some("eval-123".to_string()),
        };
        let proposal = eval_result_to_proposal(&result);
        assert!(!proposal.ready);
        assert_eq!(proposal.conditions.len(), 1);
        assert_eq!(proposal.evaluation_id.as_deref(), Some("eval-123"));
    }

    // ── open_log_store ───────────────────────────────────────────────────

    #[test]
    fn open_log_store_none_none_returns_ok_none() {
        let result = open_log_store(None, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn open_log_store_some_some_returns_ok_some() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("logs.db");
        let logs = tmp.path().join("logs");
        std::fs::create_dir_all(&logs).unwrap();
        let result = open_log_store(Some(&db), Some(&logs)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn open_log_store_partial_args_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("logs.db");
        // Only db_path provided, logs_dir is None.
        let result = open_log_store(Some(&db), None).unwrap();
        assert!(result.is_none());
    }

    // ── sync_from_compiled dry_run ───────────────────────────────────────

    const ASSET_WITH_SYNC_YAML: &str = "\
apiVersion: nagi/v1alpha1
metadata:
  name: test-asset
spec:
  onDrift:
    - conditions: []
      conditionsRef: test-cond
      sync:
        run:
          type: Command
          args: [\"echo\", \"ok\"]
      syncRefName: test-sync
  autoSync: true
";

    #[tokio::test]
    async fn sync_from_compiled_dry_run_returns_json() {
        let params = SyncFromCompiledParams {
            yaml: ASSET_WITH_SYNC_YAML,
            sync_type: "sync",
            stages: None,
            db_path: None,
            logs_dir: None,
            cache_dir: None,
            dry_run: true,
            force: false,
            evaluation_id: None,
            default_timeout: std::time::Duration::from_secs(3600),
            project_dir: None,
        };
        let json = sync_from_compiled(params).await.unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["assetName"], "test-asset");
        assert!(value["stages"].is_array());
    }

    #[tokio::test]
    async fn sync_from_compiled_invalid_yaml_returns_parse_error() {
        let params = SyncFromCompiledParams {
            yaml: "{{invalid",
            sync_type: "sync",
            stages: None,
            db_path: None,
            logs_dir: None,
            cache_dir: None,
            dry_run: true,
            force: false,
            evaluation_id: None,
            default_timeout: std::time::Duration::from_secs(3600),
            project_dir: None,
        };
        let result = sync_from_compiled(params).await;
        assert!(matches!(result, Err(SyncError::Parse(_))));
    }

    #[tokio::test]
    async fn sync_from_compiled_invalid_sync_type_returns_error() {
        let params = SyncFromCompiledParams {
            yaml: ASSET_WITH_SYNC_YAML,
            sync_type: "invalid_type",
            stages: None,
            db_path: None,
            logs_dir: None,
            cache_dir: None,
            dry_run: true,
            force: false,
            evaluation_id: None,
            default_timeout: std::time::Duration::from_secs(3600),
            project_dir: None,
        };
        let result = sync_from_compiled(params).await;
        assert!(result.is_err());
    }

    // ── sync_from_compiled lock ─────────────────────────────────────────

    /// Creates a temp project dir with nagi.yaml pointing nagiDir to a subdirectory.
    /// Returns (tempdir, nagi_dir_path) so the caller can inspect lock files.
    fn setup_project_with_lock() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let nagi_dir = tmp.path().join(".nagi");
        let yaml = format!("nagiDir: {}", nagi_dir.display());
        std::fs::write(tmp.path().join("nagi.yaml"), yaml).unwrap();
        (tmp, nagi_dir)
    }

    #[tokio::test]
    async fn sync_from_compiled_with_lock_executes_sync() {
        let (project, nagi_dir) = setup_project_with_lock();
        let locks_dir = nagi_dir.join("locks");
        let params = SyncFromCompiledParams {
            yaml: ASSET_WITH_SYNC_YAML,
            sync_type: "sync",
            stages: None,
            db_path: None,
            logs_dir: None,
            cache_dir: None,
            dry_run: false,
            force: false,
            evaluation_id: None,
            default_timeout: std::time::Duration::from_secs(3600),
            project_dir: Some(project.path()),
        };
        let result = sync_from_compiled(params).await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result);
        // Lock file should be released after sync.
        assert!(!locks_dir.join("test-asset.lock").exists());
    }

    #[tokio::test]
    async fn sync_from_compiled_lock_held_returns_error() {
        let (project, nagi_dir) = setup_project_with_lock();
        let locks_dir = nagi_dir.join("locks");
        // Pre-acquire the lock so sync_from_compiled cannot get it.
        let lock = LocalSyncLock::new(locks_dir);
        assert!(lock
            .acquire(
                "test-asset",
                std::time::Duration::from_secs(3600),
                "other-exec"
            )
            .unwrap());

        let params = SyncFromCompiledParams {
            yaml: ASSET_WITH_SYNC_YAML,
            sync_type: "sync",
            stages: None,
            db_path: None,
            logs_dir: None,
            cache_dir: None,
            dry_run: false,
            force: false,
            evaluation_id: None,
            default_timeout: std::time::Duration::from_secs(3600),
            project_dir: Some(project.path()),
        };
        let result = sync_from_compiled(params).await;
        assert!(
            matches!(result, Err(SyncError::LockAcquireFailed { .. })),
            "expected LockAcquireFailed, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn sync_from_compiled_dry_run_skips_lock() {
        let (project, nagi_dir) = setup_project_with_lock();
        let locks_dir = nagi_dir.join("locks");
        // Pre-acquire the lock.
        let lock = LocalSyncLock::new(locks_dir);
        assert!(lock
            .acquire(
                "test-asset",
                std::time::Duration::from_secs(3600),
                "other-exec"
            )
            .unwrap());

        // dry_run should succeed despite the lock being held.
        let params = SyncFromCompiledParams {
            yaml: ASSET_WITH_SYNC_YAML,
            sync_type: "sync",
            stages: None,
            db_path: None,
            logs_dir: None,
            cache_dir: None,
            dry_run: true,
            force: false,
            evaluation_id: None,
            default_timeout: std::time::Duration::from_secs(3600),
            project_dir: Some(project.path()),
        };
        let result = sync_from_compiled(params).await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result);
    }
}

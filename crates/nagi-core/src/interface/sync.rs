use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::runtime::compile::CompiledAsset;
use crate::runtime::log::LogStore;
use crate::runtime::sync::{
    dry_run_sync, execute_sync, parse_sync_type, resolve_sync_spec, DryRunStage, Stage, SyncError,
    SyncExecutionResult, SyncType,
};

/// Builds sync proposals for all compiled assets matching the selectors.
///
/// Evaluation or dry-run failures are not fatal — each proposal will omit
/// whichever part failed.
pub async fn propose_sync_all(
    target_dir: &Path,
    selectors: &[&str],
    sync_type: &str,
    stages: Option<&str>,
    cache_dir: Option<&Path>,
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
) -> Result<Vec<SyncProposal>, SyncError> {
    let assets = crate::interface::compile::load_compiled_assets(target_dir, selectors)?;
    let st = parse_sync_type(sync_type)?;
    let mut proposals = Vec::with_capacity(assets.len());

    for (name, yaml) in &assets {
        let evaluation = match crate::interface::evaluate::evaluate_from_compiled(
            yaml, cache_dir, db_path, logs_dir,
        )
        .await
        {
            Ok(json) => serde_json::from_str(&json).ok(),
            Err(_) => None,
        };

        let compiled: CompiledAsset =
            serde_yaml::from_str(yaml).map_err(|e| SyncError::Parse(e.to_string()))?;
        let dry_run_stages = match resolve_sync_spec(&compiled) {
            Ok(sync_spec) => {
                let parsed_stages = stages.map(Stage::parse_list).transpose()?;
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

/// Result of `propose_sync`: evaluation + dry-run stages for user confirmation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncProposal {
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
pub struct SyncProposalEvaluation {
    pub ready: bool,
    pub conditions: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_id: Option<String>,
}

/// Checks dbt Cloud for running jobs. Returns an error if any are found.
///
/// Only called when the Connection is `Dbt`.
async fn check_dbt_cloud_preflight(
    asset_name: &str,
    cred_path: &str,
    job_ids: &std::collections::HashSet<i64>,
) -> Result<(), SyncError> {
    let jobs = crate::runtime::kind::origin::dbt::cloud::check_running_jobs_for_asset(
        Path::new(cred_path),
        job_ids,
    )
    .await
    .map_err(|e| SyncError::DbtCloud(e.to_string()))?;

    if !jobs.is_empty() {
        let details: Vec<String> = jobs
            .iter()
            .map(|j| format!("  job-{} ({})", j.job_id, j.status_humanized))
            .collect();
        return Err(SyncError::DbtCloud(format!(
            "dbt Cloud has running jobs that include asset '{}':\n{}\nUse --force to override.",
            asset_name,
            details.join("\n")
        )));
    }
    Ok(())
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

/// Executes sync from compiled asset YAML.
///
/// Parameters for `sync_from_compiled`.
pub struct SyncFromCompiledParams<'a> {
    pub yaml: &'a str,
    pub sync_type: &'a str,
    pub stages: Option<&'a str>,
    pub db_path: Option<&'a Path>,
    pub logs_dir: Option<&'a Path>,
    pub cache_dir: Option<&'a Path>,
    pub dry_run: bool,
    pub force: bool,
    pub evaluation_id: Option<&'a str>,
}

/// Handles sync type/spec resolution, dry-run, dbt Cloud pre-flight check,
/// logging, evaluation linking, and post-sync re-evaluation.
pub async fn sync_from_compiled(params: SyncFromCompiledParams<'_>) -> Result<String, SyncError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(params.yaml).map_err(|e| SyncError::Parse(e.to_string()))?;
    let st = parse_sync_type(params.sync_type)?;
    let sync_spec = resolve_sync_spec(&compiled)?;
    let parsed_stages = params.stages.map(Stage::parse_list).transpose()?;

    if params.dry_run {
        let result = dry_run_sync(
            &compiled.metadata.name,
            &sync_spec,
            st,
            parsed_stages.as_deref(),
        );
        return serialize(&result);
    }

    if !params.force {
        if let (
            Some(crate::runtime::kind::connection::ResolvedConnection::Dbt {
                dbt_cloud_credentials_file: Some(cred_path),
                ..
            }),
            Some(job_ids),
        ) = (&compiled.connection, &compiled.spec.dbt_cloud_job_ids)
        {
            check_dbt_cloud_preflight(&compiled.metadata.name, cred_path, job_ids).await?;
        }
    }

    let log_store = open_log_store(params.db_path, params.logs_dir)?;
    let result = execute_sync(
        &compiled.metadata.name,
        &sync_spec,
        st,
        parsed_stages.as_deref(),
        log_store.as_ref(),
    )
    .await?;

    // Link pre-sync evaluation to this execution.
    if let (Some(store), Some(eval_id)) = (log_store.as_ref(), params.evaluation_id) {
        let _ = store.write_sync_evaluation(&result.execution_id, eval_id);
    }

    // Re-evaluate after sync (only when no stage filter).
    if params.stages.is_none() {
        let _ = post_sync_re_evaluate(
            params.yaml,
            params.cache_dir,
            params.db_path,
            params.logs_dir,
            &result,
        )
        .await;
    }

    serialize(&result)
}

/// Re-evaluates the asset after sync and links the new evaluation to the execution.
async fn post_sync_re_evaluate(
    yaml: &str,
    cache_dir: Option<&Path>,
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
    sync_result: &SyncExecutionResult,
) -> Result<(), SyncError> {
    let eval_json =
        crate::interface::evaluate::evaluate_from_compiled(yaml, cache_dir, db_path, logs_dir)
            .await
            .map_err(|e| SyncError::Parse(e.to_string()))?;

    if let (Some(db), Some(logs)) = (db_path, logs_dir) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&eval_json) {
            if let Some(eval_id) = val.get("evaluationId").and_then(|v| v.as_str()) {
                let store = LogStore::open(db, logs)?;
                let _ = store.write_sync_evaluation(&sync_result.execution_id, eval_id);
            }
        }
    }
    Ok(())
}

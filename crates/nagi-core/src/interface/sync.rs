use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::runtime::compile::CompiledAsset;
use crate::runtime::evaluate::AssetEvalResult;
use crate::runtime::log::LogStore;
use crate::runtime::sync::{
    dry_run_sync, evaluate_and_cache, parse_sync_type, resolve_sync_spec, run_sync_workflow,
    DryRunStage, Stage, SyncError, SyncType, SyncWorkflowParams,
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
    let mut proposals = Vec::with_capacity(assets.len());

    for (name, yaml) in &assets {
        let compiled: CompiledAsset =
            serde_yaml::from_str(yaml).map_err(|e| SyncError::Parse(e.to_string()))?;

        let evaluation = evaluate_for_proposal(&compiled, params.cache_dir, log_store.as_ref())
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
) -> Result<SyncProposalEvaluation, SyncError> {
    let conn = compiled
        .connection
        .as_ref()
        .map(|c| {
            c.connect()
                .map_err(|e| SyncError::Connection(e.to_string()))
        })
        .transpose()?;
    let result = evaluate_and_cache(compiled, conn.as_deref(), log_store, cache_dir).await?;
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

    let log_store = open_log_store(params.db_path, params.logs_dir)?;
    let result = run_sync_workflow(SyncWorkflowParams {
        compiled: &compiled,
        sync_type: st,
        stages: parsed_stages.as_deref(),
        force: params.force,
        evaluation_id: params.evaluation_id,
        log_store: log_store.as_ref(),
        cache_dir: params.cache_dir,
    })
    .await?;

    serialize(&result)
}

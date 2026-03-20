use std::path::{Path, PathBuf};

use crate::compile::CompiledAsset;
use crate::evaluate::{AssetEvalResult, EvaluateError};
use crate::storage::local::LocalCache;
use crate::storage::Cache;
use crate::sync::SyncError;

/// Evaluates a single compiled asset and writes the result to the local cache.
///
/// This is the "stateless reconciler": it takes all inputs by value so the
/// returned future is `Send` and can be spawned on a `JoinSet`.
/// (`evaluate_from_compiled` cannot be used here because `LogStore` is `!Send`.)
pub async fn evaluate_and_cache(
    yaml: &str,
    cache_dir: Option<&Path>,
) -> Result<AssetEvalResult, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let spec = crate::evaluate::compiled_to_asset_spec(&compiled);
    let conn = compiled
        .connection
        .as_ref()
        .map(crate::evaluate::resolve_connection)
        .transpose()?;
    let result =
        crate::evaluate::evaluate_asset_no_log(&compiled.metadata.name, &spec, conn.as_deref())
            .await?;

    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(LocalCache::default_dir);
    let cache = LocalCache::new(cache_path);
    cache
        .write(&result)
        .map_err(|e| EvaluateError::Cache(e.to_string()))?;

    Ok(result)
}

/// Spawn wrapper: pairs the asset name with the evaluation result so the
/// Controller can identify which asset completed.
pub async fn spawn_evaluate(
    asset_name: String,
    yaml: String,
    cache_dir: Option<PathBuf>,
) -> (String, Result<AssetEvalResult, EvaluateError>) {
    let result = evaluate_and_cache(&yaml, cache_dir.as_deref()).await;
    (asset_name, result)
}

/// Executes sync for a compiled asset. Called via `JoinSet::spawn` so all
/// inputs are owned to produce a `Send` future.
///
/// Uses `execute_sync_core` directly (not `sync_from_compiled`) to avoid
/// `LogStore` (!Send) and `post_sync_re_evaluate` — the Controller handles
/// re-evaluation itself via `handle_sync_result`.
pub async fn spawn_sync(
    asset_name: String,
    yaml: String,
) -> (String, Result<crate::sync::SyncExecutionResult, SyncError>) {
    let result = resolve_and_sync(&yaml).await;
    (asset_name, result)
}

async fn resolve_and_sync(yaml: &str) -> Result<crate::sync::SyncExecutionResult, SyncError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| SyncError::Parse(e.to_string()))?;
    let sync_spec = compiled
        .spec
        .sync
        .as_ref()
        .ok_or_else(|| SyncError::NoSyncSpec {
            asset_name: compiled.metadata.name.clone(),
        })?;
    crate::sync::execute_sync_core(
        &compiled.metadata.name,
        sync_spec,
        crate::sync::SyncType::Sync,
        None,
    )
    .await
}

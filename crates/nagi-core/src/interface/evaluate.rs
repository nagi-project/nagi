use std::path::{Path, PathBuf};

use crate::runtime::compile::CompiledAsset;
use crate::runtime::evaluate::{dry_run_asset, evaluate_asset, EvaluateError};
use crate::runtime::log::LogStore;
use crate::runtime::storage::local::LocalCache;
use crate::runtime::storage::Cache;

/// Evaluates an asset from its compiled YAML.
///
/// Handles connection resolution, logging, and cache — callers pass only paths.
pub async fn evaluate_from_compiled(
    yaml: &str,
    cache_dir: Option<&Path>,
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
) -> Result<String, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let asset_name = &compiled.metadata.name;

    let log_store = match (db_path, logs_dir) {
        (Some(db), Some(logs)) => Some(LogStore::open(db, logs)?),
        _ => None,
    };

    let conn = compiled
        .connection
        .as_ref()
        .map(|c| c.connect().map_err(EvaluateError::Connection))
        .transpose()?;

    let conn_ref = conn.as_deref();
    let result = evaluate_asset(
        asset_name,
        &compiled.spec.on_drift,
        conn_ref,
        log_store.as_ref(),
    )
    .await?;

    let cache_path = cache_dir.map(PathBuf::from).unwrap_or_else(|| {
        crate::runtime::config::resolve_nagi_dir(std::path::Path::new(".")).evaluate_cache_dir()
    });
    let cache = LocalCache::new(cache_path);
    cache
        .write(&result)
        .map_err(|e| EvaluateError::Cache(e.to_string()))?;

    serde_json::to_string(&result).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

/// Evaluates all compiled assets matching the selectors.
/// Returns a JSON array of evaluation results.
pub async fn evaluate_all(
    target_dir: &Path,
    selectors: &[&str],
    cache_dir: Option<&Path>,
    dry_run: bool,
) -> Result<String, EvaluateError> {
    let assets = crate::interface::compile::load_compiled_assets(target_dir, selectors)?;
    let mut results: Vec<serde_json::Value> = Vec::with_capacity(assets.len());

    for (_name, yaml) in &assets {
        if dry_run {
            let dr = dry_run_from_compiled(yaml)?;
            results.push(
                serde_json::from_str(&dr).map_err(|e| EvaluateError::Serialize(e.to_string()))?,
            );
        } else {
            let r = evaluate_from_compiled(yaml, cache_dir, None, None).await?;
            results.push(
                serde_json::from_str(&r).map_err(|e| EvaluateError::Serialize(e.to_string()))?,
            );
        }
    }

    serde_json::to_string(&results).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

/// Dry-run from compiled YAML.
pub fn dry_run_from_compiled(yaml: &str) -> Result<String, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let result = dry_run_asset(&compiled.metadata.name, &compiled.spec.on_drift);
    serde_json::to_string(&result).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

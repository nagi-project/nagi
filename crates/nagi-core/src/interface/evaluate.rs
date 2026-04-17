use std::path::{Path, PathBuf};

use crate::runtime::compile::CompiledAsset;
use crate::runtime::evaluate::{dry_run_asset, evaluate_asset, EvaluateError};
use crate::runtime::log::LogStore;
use crate::runtime::storage::local::LocalCache;
use crate::runtime::storage::Cache;

/// Evaluates an asset from its compiled YAML.
///
/// Handles connection resolution, logging, and cache — callers pass only paths.
async fn evaluate_from_compiled(
    yaml: &str,
    cache_dir: Option<&Path>,
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
    default_timeout: std::time::Duration,
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
        .map(|c| {
            c.connect(default_timeout)
                .map_err(EvaluateError::Connection)
        })
        .transpose()?;

    let conn_ref = conn.as_deref();
    let result = evaluate_asset(
        asset_name,
        &compiled.spec.on_drift,
        conn_ref,
        log_store.as_ref(),
        default_timeout,
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
pub(crate) async fn evaluate_all(
    target_dir: &Path,
    selectors: &[&str],
    excludes: &[&str],
    cache_dir: Option<&Path>,
    dry_run: bool,
) -> Result<String, EvaluateError> {
    let assets = crate::runtime::compile::load_compiled_assets(target_dir, selectors, excludes)?;

    let values = if dry_run {
        dry_run_assets(&assets)?
    } else {
        evaluate_assets(&assets, cache_dir).await?
    };

    serde_json::to_string(&values).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

fn dry_run_assets(assets: &[(String, String)]) -> Result<Vec<serde_json::Value>, EvaluateError> {
    assets
        .iter()
        .map(|(_name, yaml)| {
            let json = dry_run_from_compiled(yaml)?;
            serde_json::from_str(&json).map_err(|e| EvaluateError::Serialize(e.to_string()))
        })
        .collect()
}

async fn evaluate_assets(
    assets: &[(String, String)],
    cache_dir: Option<&Path>,
) -> Result<Vec<serde_json::Value>, EvaluateError> {
    let default_timeout = crate::runtime::config::resolve_default_timeout();
    let handles: Vec<_> = assets
        .iter()
        .map(|(name, yaml)| {
            let name = name.clone();
            let yaml = yaml.clone();
            let cache = cache_dir.map(PathBuf::from);
            tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                let json = rt.block_on(evaluate_from_compiled(
                    &yaml,
                    cache.as_deref(),
                    None,
                    None,
                    default_timeout,
                ))?;
                let value: serde_json::Value = serde_json::from_str(&json)
                    .map_err(|e| EvaluateError::Serialize(e.to_string()))?;
                Ok::<(String, serde_json::Value), EvaluateError>((name, value))
            })
        })
        .collect();

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        let pair = handle
            .await
            .map_err(|e| EvaluateError::Serialize(format!("task join error: {e}")))??;
        results.push(pair);
    }
    results.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(results.into_iter().map(|(_, v)| v).collect())
}

/// Dry-run from compiled YAML.
fn dry_run_from_compiled(yaml: &str) -> Result<String, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let result = dry_run_asset(&compiled.metadata.name, &compiled.spec.on_drift);
    serde_json::to_string(&result).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_ASSET_YAML: &str = "\
apiVersion: nagi/v1alpha1
metadata:
  name: test-asset
spec:
  onDrift: []
  autoSync: true
";

    const ASSET_WITH_ONDRIFT_YAML: &str = "\
apiVersion: nagi/v1alpha1
metadata:
  name: test-asset-with-drift
spec:
  onDrift:
    - conditions:
        - type: Command
          name: check
          run: [\"true\"]
      conditionsRef: test-cond
      sync:
        run:
          type: Command
          args: [\"echo\", \"ok\"]
      syncRefName: test-sync
  autoSync: true
";

    // -- dry_run_from_compiled: parameterized success cases --

    macro_rules! dry_run_success_tests {
        ($($name:ident: $yaml:expr, $expected_asset_name:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let json_str = dry_run_from_compiled($yaml).unwrap();
                    let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();
                    assert_eq!(value["assetName"], $expected_asset_name);
                    assert!(value["conditions"].is_array());
                }
            )*
        };
    }

    dry_run_success_tests! {
        dry_run_minimal_asset: MINIMAL_ASSET_YAML, "test-asset";
        dry_run_asset_with_ondrift: ASSET_WITH_ONDRIFT_YAML, "test-asset-with-drift";
    }

    #[test]
    fn dry_run_from_compiled_invalid_yaml_returns_parse_error() {
        let result = dry_run_from_compiled("not: valid: yaml: {{{}}}");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, EvaluateError::Parse(_)),
            "expected Parse error, got: {err:?}"
        );
    }

    #[test]
    fn dry_run_from_compiled_missing_fields_returns_parse_error() {
        let yaml = "apiVersion: nagi/v1alpha1\nmetadata:\n  name: x\n";
        let result = dry_run_from_compiled(yaml);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), EvaluateError::Parse(_)),
            "expected Parse error for incomplete YAML"
        );
    }

    #[test]
    fn dry_run_assets_returns_correct_count() {
        let assets = vec![
            ("a".to_string(), MINIMAL_ASSET_YAML.to_string()),
            ("b".to_string(), ASSET_WITH_ONDRIFT_YAML.to_string()),
        ];
        let results = dry_run_assets(&assets).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn dry_run_assets_empty_input_returns_empty() {
        let assets: Vec<(String, String)> = vec![];
        let results = dry_run_assets(&assets).unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn evaluate_all_dry_run_with_temp_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let assets_dir = tmp.path().join("assets");
        std::fs::create_dir_all(&assets_dir).unwrap();

        std::fs::write(assets_dir.join("test-asset.yaml"), MINIMAL_ASSET_YAML).unwrap();
        // graph.json is read unconditionally by load_compiled_assets
        std::fs::write(tmp.path().join("graph.json"), "{}").unwrap();

        let result = evaluate_all(tmp.path(), &[], &[], None, true)
            .await
            .unwrap();
        let values: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["assetName"], "test-asset");
    }
}

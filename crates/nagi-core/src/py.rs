use std::path::PathBuf;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::db::bigquery::{BigQueryConfig, BigQueryConnection};
use crate::db::Connection;
use crate::dbt_profile::DbtProfilesFile;
use crate::evaluate;
use crate::kind::{self, NagiKind};
use crate::storage::local::LocalCache;
use crate::storage::Cache;

fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Parses a YAML string and returns a JSON representation of the parsed resources.
#[pyfunction]
pub fn parse_yaml(yaml: &str) -> PyResult<String> {
    let kinds = kind::parse_kinds(yaml).map_err(to_py_err)?;
    serde_json::to_string(&kinds).map_err(to_py_err)
}

/// Loads ~/.dbt/profiles.yml and returns profile information as JSON.
/// Returns: `{"profiles": [{"name": "...", "default_target": "...", "targets": ["...", ...]}]}`
#[pyfunction]
pub fn load_dbt_profiles() -> PyResult<String> {
    let f = DbtProfilesFile::load_default().map_err(to_py_err)?;
    profiles_to_json(&f)
}

/// Loads profiles.yml from a specific path.
#[pyfunction]
pub fn load_dbt_profiles_from(path: &str) -> PyResult<String> {
    let f = DbtProfilesFile::load(std::path::Path::new(path)).map_err(to_py_err)?;
    profiles_to_json(&f)
}

fn profiles_to_json(f: &DbtProfilesFile) -> PyResult<String> {
    let info = f.profiles_info();
    let profiles: Vec<serde_json::Value> = info
        .iter()
        .map(|(name, default_target, targets)| {
            serde_json::json!({
                "name": name,
                "defaultTarget": default_target,
                "targets": targets,
            })
        })
        .collect();
    serde_json::to_string(&serde_json::json!({ "profiles": profiles })).map_err(to_py_err)
}

/// Tests a BigQuery connection using the given profile and target from ~/.dbt/profiles.yml.
/// Returns a JSON string with connection details on success.
#[pyfunction]
pub fn test_connection(profile: &str, target: Option<&str>) -> PyResult<String> {
    let f = DbtProfilesFile::load_default().map_err(to_py_err)?;
    let output = f.resolve(profile, target).map_err(to_py_err)?;
    let config = BigQueryConfig::from_output(output).map_err(to_py_err)?;

    let rt = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    rt.block_on(async {
        let conn = BigQueryConnection::new(config.clone());
        // Run a simple query to verify connectivity.
        conn.query_scalar("SELECT 1").await.map_err(to_py_err)?;
        Ok(serde_json::to_string(&serde_json::json!({
            "status": "ok",
            "adapter": "bigquery",
            "project": config.project,
            "dataset": config.dataset,
        }))
        .map_err(to_py_err)?)
    })
}

/// Evaluates an asset's desired conditions and writes the result to cache.
/// `yaml` is the full YAML string for the asset resource.
/// `profile` and `target` identify the dbt profile for the DB connection.
/// `cache_dir` is optional; defaults to `~/.nagi/cache/`.
/// Returns the evaluation result as JSON.
#[pyfunction]
#[pyo3(signature = (yaml, profile, target=None, cache_dir=None))]
pub fn evaluate_asset(
    yaml: &str,
    profile: &str,
    target: Option<&str>,
    cache_dir: Option<&str>,
) -> PyResult<String> {
    let kinds = kind::parse_kinds(yaml).map_err(to_py_err)?;
    let (asset_name, asset_spec) = kinds
        .iter()
        .find_map(|k| match k {
            NagiKind::Asset { metadata, spec } => Some((metadata.name.clone(), spec.clone())),
            _ => None,
        })
        .ok_or_else(|| PyRuntimeError::new_err("no Asset resource found in YAML"))?;

    // Resolve connection from the first Connection resource in the YAML,
    // or fall back to the provided profile/target.
    let f = DbtProfilesFile::load_default().map_err(to_py_err)?;
    let output = f.resolve(profile, target).map_err(to_py_err)?;
    let config = BigQueryConfig::from_output(output).map_err(to_py_err)?;

    let rt = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    let result = rt.block_on(async {
        let conn = BigQueryConnection::new(config);
        evaluate::evaluate_asset(&asset_name, &asset_spec, &conn)
            .await
            .map_err(to_py_err)
    })?;

    // Write to cache.
    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(LocalCache::default_dir);
    let cache = LocalCache::new(cache_path);
    cache.write(&result).map_err(to_py_err)?;

    serde_json::to_string(&result).map_err(to_py_err)
}

/// Reads cached evaluation result for a single asset.
/// Returns JSON or None if no cache exists.
#[pyfunction]
#[pyo3(signature = (asset_name, cache_dir=None))]
pub fn read_cache(asset_name: &str, cache_dir: Option<&str>) -> PyResult<Option<String>> {
    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(LocalCache::default_dir);
    let cache = LocalCache::new(cache_path);
    match cache.read(asset_name).map_err(to_py_err)? {
        Some(result) => Ok(Some(serde_json::to_string(&result).map_err(to_py_err)?)),
        None => Ok(None),
    }
}

/// Returns a dry-run summary of what evaluate would execute, without running anything.
/// `yaml` is the full YAML string for the asset resource.
#[pyfunction]
pub fn dry_run_asset(yaml: &str) -> PyResult<String> {
    let kinds = kind::parse_kinds(yaml).map_err(to_py_err)?;
    let (asset_name, asset_spec) = kinds
        .iter()
        .find_map(|k| match k {
            NagiKind::Asset { metadata, spec } => Some((metadata.name.clone(), spec.clone())),
            _ => None,
        })
        .ok_or_else(|| PyRuntimeError::new_err("no Asset resource found in YAML"))?;

    let result = evaluate::dry_run_asset(&asset_name, &asset_spec);
    serde_json::to_string(&result).map_err(to_py_err)
}

/// Compiles assets from `assets_dir` into `target_dir`.
/// Returns the dependency graph as a JSON string.
#[pyfunction]
pub fn compile_assets(assets_dir: &str, target_dir: &str) -> PyResult<String> {
    let assets_path = std::path::Path::new(assets_dir);
    let target_path = std::path::Path::new(target_dir);
    let output = crate::compile::compile(assets_path, target_path).map_err(to_py_err)?;
    serde_json::to_string(&output.graph).map_err(to_py_err)
}

/// Selects asset names from a dependency graph JSON using dbt-compatible selector expressions.
/// Returns a JSON array of selected asset names.
#[pyfunction]
pub fn select_assets(graph_json: &str, selectors: Vec<String>) -> PyResult<String> {
    let graph: crate::compile::DependencyGraph =
        serde_json::from_str(graph_json).map_err(to_py_err)?;
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    let selected = crate::select::select_assets(&graph, &selector_refs).map_err(to_py_err)?;
    serde_json::to_string(&selected).map_err(to_py_err)
}

/// Lists all cached evaluation results.
/// Returns JSON array.
#[pyfunction]
#[pyo3(signature = (cache_dir=None))]
pub fn list_cache(cache_dir: Option<&str>) -> PyResult<String> {
    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(LocalCache::default_dir);
    let cache = LocalCache::new(cache_path);
    let results = cache.list().map_err(to_py_err)?;
    serde_json::to_string(&results).map_err(to_py_err)
}

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::dbt::profile::DbtProfilesFile;

fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Loads ~/.dbt/profiles.yml and returns profile information as JSON.
/// Returns: `{"profiles": [{"name": "...", "default_target": "...", "targets": ["...", ...]}]}`
#[pyfunction]
pub fn load_dbt_profiles() -> PyResult<String> {
    let f = DbtProfilesFile::load_default().map_err(to_py_err)?;
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

/// Evaluates all compiled assets matching selectors.
/// Returns JSON array of evaluation results.
#[pyfunction]
#[pyo3(signature = (target_dir, selectors, cache_dir=None, dry_run=false))]
pub fn evaluate_all(
    target_dir: &str,
    selectors: Vec<String>,
    cache_dir: Option<&str>,
    dry_run: bool,
) -> PyResult<String> {
    let rt = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    rt.block_on(crate::evaluate::evaluate_all(
        std::path::Path::new(target_dir),
        &selector_refs,
        cache_dir.map(std::path::Path::new),
        dry_run,
    ))
    .map_err(to_py_err)
}

/// Compiles assets and returns a summary JSON.
/// Returns: `{"nodes": N, "edges": N, "target": "..."}`
#[pyfunction]
pub fn compile_assets(assets_dir: &str, target_dir: &str) -> PyResult<String> {
    let assets_path = std::path::Path::new(assets_dir);
    let target_path = std::path::Path::new(target_dir);
    let output = crate::compile::compile(assets_path, target_path).map_err(to_py_err)?;
    let summary = serde_json::json!({
        "nodes": output.graph.nodes.len(),
        "edges": output.graph.edges.len(),
        "target": target_dir,
    });
    serde_json::to_string(&summary).map_err(to_py_err)
}

/// Lists dbt Origin project directories found in `assets_dir`.
/// Returns a comma-separated string of directories, or empty string if none.
#[pyfunction]
pub fn list_dbt_origin_dirs(assets_dir: &str) -> PyResult<String> {
    let assets_path = std::path::Path::new(assets_dir);
    let origins = crate::compile::list_dbt_origin_dirs(assets_path).map_err(to_py_err)?;
    let dirs: Vec<&str> = origins.iter().map(|(_, dir)| dir.as_str()).collect();
    Ok(dirs.join(", "))
}

// ── Sync / Resync ────────────────────────────────────────────────────────────

/// Builds sync proposals for all compiled assets matching selectors.
/// Returns JSON array of proposals. Each proposal contains an opaque `_index`
/// field used by `execute_sync_proposal`.
#[pyfunction]
#[pyo3(signature = (target_dir, selectors, sync_type, stages=None, cache_dir=None))]
pub fn propose_sync(
    target_dir: &str,
    selectors: Vec<String>,
    sync_type: &str,
    stages: Option<&str>,
    cache_dir: Option<&str>,
) -> PyResult<String> {
    let rt = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    let proposals = rt
        .block_on(crate::sync::propose_sync_all(
            std::path::Path::new(target_dir),
            &selector_refs,
            sync_type,
            stages,
            cache_dir.map(std::path::Path::new),
            None,
            None,
        ))
        .map_err(to_py_err)?;

    // Serialize proposals with yaml_content included (under `_yaml` key) so
    // execute_sync_proposal can use it without re-reading files.
    let json: Vec<serde_json::Value> = proposals
        .iter()
        .map(|p| {
            let mut v = serde_json::to_value(p).unwrap_or_default();
            v["_yaml"] = serde_json::Value::String(p.yaml_content.clone());
            v
        })
        .collect();
    serde_json::to_string(&json).map_err(to_py_err)
}

/// Executes sync for a single proposal returned by `propose_sync`.
#[pyfunction]
#[pyo3(signature = (proposal_json, sync_type, stages=None, cache_dir=None, force=false))]
pub fn execute_sync_proposal(
    proposal_json: &str,
    sync_type: &str,
    stages: Option<&str>,
    cache_dir: Option<&str>,
    force: bool,
) -> PyResult<String> {
    let v: serde_json::Value = serde_json::from_str(proposal_json).map_err(to_py_err)?;
    let yaml = v["_yaml"]
        .as_str()
        .ok_or_else(|| PyRuntimeError::new_err("proposal missing _yaml field"))?;
    let evaluation_id = v
        .get("evaluation")
        .and_then(|e| e.get("evaluationId"))
        .and_then(|id| id.as_str());

    let rt = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    rt.block_on(crate::sync::sync_from_compiled(
        yaml,
        sync_type,
        stages,
        None,
        None,
        cache_dir.map(std::path::Path::new),
        false,
        force,
        evaluation_id,
    ))
    .map_err(to_py_err)
}

// ── Status ───────────────────────────────────────────────────────────────────

/// Returns convergence status (cached evaluation + latest sync log) as JSON.
#[pyfunction]
#[pyo3(signature = (target_dir, selectors, cache_dir=None, db_path=None, logs_dir=None))]
pub fn asset_status(
    target_dir: &str,
    selectors: Vec<String>,
    cache_dir: Option<&str>,
    db_path: Option<&str>,
    logs_dir: Option<&str>,
) -> PyResult<String> {
    let db = db_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(crate::init::default_db_path);
    let logs = logs_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(crate::init::default_logs_dir);
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    let result = crate::status::asset_status(
        std::path::Path::new(target_dir),
        &selector_refs,
        cache_dir.map(std::path::Path::new),
        &db,
        &logs,
    )
    .map_err(to_py_err)?;
    serde_json::to_string(&result).map_err(to_py_err)
}

// ── Init ─────────────────────────────────────────────────────────────────────

#[pyfunction]
#[pyo3(signature = (base_dir=".", db_path=None, logs_dir=None))]
pub fn init_workspace(
    base_dir: &str,
    db_path: Option<&str>,
    logs_dir: Option<&str>,
) -> PyResult<()> {
    let db = db_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(crate::init::default_db_path);
    let logs = logs_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(crate::init::default_logs_dir);
    crate::init::init_workspace(std::path::Path::new(base_dir), &db, &logs).map_err(to_py_err)
}

#[pyfunction]
pub fn run_dbt_debug(project_dir: &str, profile: &str, target: Option<&str>) -> PyResult<()> {
    crate::dbt::run_dbt_debug(std::path::Path::new(project_dir), profile, target).map_err(to_py_err)
}

/// Generates and writes connection.yaml and origin.yaml from dbt project entries.
/// `entries` is a JSON array of `[{"projectDir": "...", "profile": "...", "target": "..."}]`.
/// Returns JSON with paths of written files.
#[pyfunction]
#[pyo3(signature = (base_dir, entries_json))]
pub fn write_init_dbt_files(base_dir: &str, entries_json: &str) -> PyResult<String> {
    let raw: Vec<serde_json::Value> =
        serde_json::from_str(entries_json).map_err(to_py_err)?;
    let entries: Vec<crate::init::DbtProjectEntry> = raw
        .iter()
        .map(|v| crate::init::DbtProjectEntry {
            project_dir: v["projectDir"].as_str().unwrap_or_default().to_string(),
            profile: v["profile"].as_str().unwrap_or_default().to_string(),
            target: v["target"].as_str().map(String::from),
        })
        .collect();
    let result =
        crate::init::write_init_dbt_files(std::path::Path::new(base_dir), &entries)
            .map_err(to_py_err)?;
    let json = serde_json::json!({
        "connectionPath": result.connection_path.map(|p| p.to_string_lossy().into_owned()),
        "originPath": result.origin_path.map(|p| p.to_string_lossy().into_owned()),
    });
    serde_json::to_string(&json).map_err(to_py_err)
}


use std::sync::LazyLock;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::dbt::profile::DbtProfilesFile;

static TOKIO_RT: LazyLock<tokio::runtime::Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().expect("failed to create tokio runtime"));

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
    let nagi_dir = crate::config::resolve_nagi_dir(std::path::Path::new("."));
    let resolved_cache = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| nagi_dir.cache_dir());
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    rt.block_on(crate::evaluate::evaluate_all(
        std::path::Path::new(target_dir),
        &selector_refs,
        Some(resolved_cache.as_path()),
        dry_run,
    ))
    .map_err(to_py_err)
}

/// Compiles resources and returns a summary JSON.
/// Returns: `{"nodes": N, "edges": N, "target": "..."}`
#[pyfunction]
#[pyo3(signature = (resources_dir, target_dir, project_dir=None))]
pub fn compile_assets(
    resources_dir: &str,
    target_dir: &str,
    project_dir: Option<&str>,
) -> PyResult<String> {
    let resources_path = std::path::Path::new(resources_dir);
    let target_path = std::path::Path::new(target_dir);
    let config = project_dir
        .map(|d| crate::config::load_config(std::path::Path::new(d)))
        .transpose()
        .map_err(to_py_err)?;
    let export_config = config.as_ref().and_then(|c| c.export.as_ref());
    let output =
        crate::compile::compile(resources_path, target_path, export_config).map_err(to_py_err)?;
    let summary = serde_json::json!({
        "nodes": output.graph.nodes.len(),
        "edges": output.graph.edges.len(),
        "target": target_dir,
    });
    serde_json::to_string(&summary).map_err(to_py_err)
}

/// Lists dbt Origin project directories found in `resources_dir`.
/// Returns a comma-separated string of directories, or empty string if none.
#[pyfunction]
pub fn list_dbt_origin_dirs(resources_dir: &str) -> PyResult<String> {
    let resources_path = std::path::Path::new(resources_dir);
    let origins = crate::compile::list_dbt_origin_dirs(resources_path).map_err(to_py_err)?;
    let dirs: Vec<&str> = origins.iter().map(|(_, dir)| dir.as_str()).collect();
    Ok(dirs.join(", "))
}

/// Lists all compiled resources in target/ as JSON.
#[pyfunction]
#[pyo3(signature = (target_dir))]
pub fn list_resources(target_dir: &str) -> PyResult<String> {
    let output = crate::ls::ls(std::path::Path::new(target_dir)).map_err(to_py_err)?;
    serde_json::to_string(&output).map_err(to_py_err)
}

// ── Sync ─────────────────────────────────────────────────────────────────────

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
            let mut v = serde_json::to_value(p).map_err(to_py_err)?;
            v["_yaml"] = serde_json::Value::String(p.yaml_content.clone());
            Ok(v)
        })
        .collect::<PyResult<Vec<_>>>()?;
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

    TOKIO_RT
        .block_on(crate::sync::sync_from_compiled(
            crate::sync::SyncFromCompiledParams {
                yaml,
                sync_type,
                stages,
                db_path: None,
                logs_dir: None,
                cache_dir: cache_dir.map(std::path::Path::new),
                dry_run: false,
                force,
                evaluation_id,
            },
        ))
        .map_err(to_py_err)
}

// ── Status ───────────────────────────────────────────────────────────────────

/// Returns convergence status (cached evaluation + latest sync log + suspended state) as JSON.
#[pyfunction]
#[pyo3(signature = (target_dir, selectors, cache_dir=None, db_path=None, logs_dir=None, suspended_dir=None))]
pub fn asset_status(
    target_dir: &str,
    selectors: Vec<String>,
    cache_dir: Option<&str>,
    db_path: Option<&str>,
    logs_dir: Option<&str>,
    suspended_dir: Option<&str>,
) -> PyResult<String> {
    let config = crate::config::load_config(std::path::Path::new(".")).map_err(to_py_err)?;
    let db = db_path
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| config.nagi_dir.db_path());
    let logs = logs_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| config.nagi_dir.logs_dir());
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    let result = crate::status::asset_status(
        std::path::Path::new(target_dir),
        &selector_refs,
        Some(
            cache_dir
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| config.nagi_dir.cache_dir())
                .as_path(),
        ),
        &db,
        &logs,
        Some(
            suspended_dir
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| config.nagi_dir.suspended_dir())
                .as_path(),
        ),
    )
    .map_err(to_py_err)?;
    serde_json::to_string(&result).map_err(to_py_err)
}

// ── Export ────────────────────────────────────────────────────────────────────

/// Runs export dry-run and returns JSON array of results.
#[pyfunction]
#[pyo3(signature = (select=None))]
pub fn export_dry_run(select: Option<&str>) -> PyResult<String> {
    let config = crate::config::load_config(std::path::Path::new(".")).map_err(to_py_err)?;
    let results = crate::export::dry_run_for_config(&config, select).map_err(to_py_err)?;
    serde_json::to_string(&results).map_err(to_py_err)
}

/// Runs full export and returns JSON array of results.
#[pyfunction]
#[pyo3(signature = (select=None, resources_dir="resources"))]
pub fn export_logs(select: Option<&str>, resources_dir: &str) -> PyResult<String> {
    let rt = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    let config = crate::config::load_config(std::path::Path::new(".")).map_err(to_py_err)?;
    let results = rt
        .block_on(crate::export::export_for_config(
            &config,
            std::path::Path::new(resources_dir),
            select,
        ))
        .map_err(to_py_err)?;
    serde_json::to_string(&results).map_err(to_py_err)
}

/// Attempts export if configured and interval has elapsed.
/// Failures are logged as warnings and do not affect the caller.
#[pyfunction]
#[pyo3(signature = (resources_dir="resources", project_dir="."))]
pub fn try_export(resources_dir: &str, project_dir: &str) {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return,
    };
    rt.block_on(crate::export::try_export(
        std::path::Path::new(resources_dir),
        std::path::Path::new(project_dir),
    ));
}

// ── Init ─────────────────────────────────────────────────────────────────────

#[pyfunction]
#[pyo3(signature = (base_dir=".", nagi_dir=None))]
pub fn init_workspace(base_dir: &str, nagi_dir: Option<&str>) -> PyResult<()> {
    let nd = crate::config::NagiDir::new(
        nagi_dir
            .map(std::path::PathBuf::from)
            .unwrap_or_else(crate::config::default_nagi_dir),
    );
    crate::init::init_workspace(std::path::Path::new(base_dir), &nd).map_err(to_py_err)
}

#[pyfunction]
pub fn run_dbt_debug(project_dir: &str, profile: &str, target: Option<&str>) -> PyResult<()> {
    crate::dbt::run_dbt_debug(std::path::Path::new(project_dir), profile, target).map_err(to_py_err)
}

// ── Serve ───────────────────────────────────────────────────────────────────

/// Compiles resources and starts the reconciliation loop.
/// Blocks until Ctrl-C is received.
#[pyfunction]
#[pyo3(signature = (resources_dir, target_dir, selectors, cache_dir=None, project_dir=None))]
pub fn serve(
    resources_dir: &str,
    target_dir: &str,
    selectors: Vec<String>,
    cache_dir: Option<&str>,
    project_dir: Option<&str>,
) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new().map_err(to_py_err)?;
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    rt.block_on(crate::serve::serve(
        std::path::Path::new(resources_dir),
        std::path::Path::new(target_dir),
        &selector_refs,
        cache_dir.map(std::path::Path::new),
        project_dir.map(std::path::Path::new),
    ))
    .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (selectors))]
pub fn serve_resume(selectors: Vec<String>) -> PyResult<String> {
    let nagi_dir = crate::config::resolve_nagi_dir(std::path::Path::new("."));
    let selector_refs: Vec<&str> = selectors.iter().map(|s| s.as_str()).collect();
    let result = crate::serve::resume(&selector_refs, &nagi_dir).map_err(to_py_err)?;
    serde_json::to_string(&result).map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (target_dir, reason=None))]
pub fn serve_halt(target_dir: &str, reason: Option<&str>) -> PyResult<String> {
    let nagi_dir = crate::config::resolve_nagi_dir(std::path::Path::new("."));
    let r = reason.unwrap_or("manual halt");
    let result =
        crate::serve::halt(std::path::Path::new(target_dir), r, &nagi_dir).map_err(to_py_err)?;
    serde_json::to_string(&result).map_err(to_py_err)
}

/// Generates and writes connection.yaml and origin.yaml from dbt project entries.
/// `entries` is a JSON array of `[{"projectDir": "...", "profile": "...", "target": "..."}]`.
/// Returns JSON with paths of written files.
#[pyfunction]
#[pyo3(signature = (base_dir, entries_json))]
pub fn write_init_dbt_files(base_dir: &str, entries_json: &str) -> PyResult<String> {
    let entries: Vec<crate::init::DbtProjectEntry> =
        serde_json::from_str(entries_json).map_err(to_py_err)?;
    let result = crate::init::write_init_dbt_files(std::path::Path::new(base_dir), &entries)
        .map_err(to_py_err)?;
    let json = serde_json::json!({
        "connectionPath": result.connection_path.map(|p| p.to_string_lossy().into_owned()),
        "originPath": result.origin_path.map(|p| p.to_string_lossy().into_owned()),
    });
    serde_json::to_string(&json).map_err(to_py_err)
}

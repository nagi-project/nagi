//! Reconciliation loop for continuous evaluation.
//!
//! Architecture (inspired by k8s controller-runtime):
//!
//! - **Controller** ([`run_controller`]) — one per connected component of the
//!   dependency graph.  Runs a `tokio::select!` loop that reacts to three
//!   events: timer fire, task completion, and shutdown signal.
//! - **ServeState** ([`state::ServeState`]) — all mutable in-memory state
//!   lives here.  Sub-states: [`state::WorkQueue`], [`state::SchedulerState`],
//!   [`state::ReadinessState`], plus `in_flight` tracking and the downstream
//!   propagation map.
//! - **Reconciler** ([`reconciler::evaluate_and_cache`]) — stateless async
//!   function that evaluates a single asset and writes the result to the
//!   local cache.
//!
//! The top-level [`serve`] function loads compiled assets, partitions them into
//! connected components, and spawns one Controller per component.

mod controller;
mod graph;
pub mod guardrail;
pub mod queue;
mod reconciler;
pub mod scheduler;
pub mod state;
pub mod suspended;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

use tokio::sync::watch;

use crate::runtime::compile::DependencyGraph;
use crate::runtime::log::LogStore;

pub use suspended::SuspendedInfo;

use controller::{
    await_controller_shutdown, build_controller_inputs, build_notifier, run_controller,
    BackendStores, ConcurrencyLimits,
};
use suspended::{list_suspended, remove_suspended, suspended_path};

use crate::runtime::storage::local::{LocalReadinessStore, LocalSuspendedStore, LocalSyncLock};
use crate::runtime::storage::{ReadinessStore, SuspendedStore};

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("compile error: {0}")]
    Compile(#[from] crate::runtime::compile::CompileError),
    #[error("evaluate error: {0}")]
    Evaluate(#[from] crate::runtime::evaluate::EvaluateError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sync error: {0}")]
    Sync(#[from] crate::runtime::sync::SyncError),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Storage(crate::runtime::storage::StorageError),
}

// ── Entry Point ──────────────────────────────────────────────────────────────

/// Entry point for `nagi serve`.
///
/// 1. Compiles resources from `resources_dir` into `target_dir`.
/// 2. Loads compiled assets and the dependency graph from `target_dir`.
/// 3. Partitions assets into connected components.
/// 4. Spawns one [`run_controller`] per component.
/// 5. Waits for Ctrl-C, then signals all Controllers to shut down.
pub async fn serve(
    resources_dir: &Path,
    target_dir: &Path,
    selectors: &[&str],
    excludes: &[&str],
    cache_dir: Option<&Path>,
    project_dir: Option<&Path>,
) -> Result<(), ServeError> {
    let config = crate::runtime::config::load_config(project_dir.unwrap_or(Path::new(".")))
        .map_err(|e| ServeError::Parse(format!("failed to load config: {e}")))?;

    tracing::info!("compiling resources...");
    let output =
        crate::runtime::compile::compile(resources_dir, target_dir, config.export.as_ref())?;
    tracing::info!(
        nodes = output.graph.nodes.len(),
        edges = output.graph.edges.len(),
        "compiled"
    );

    let inputs = load_controller_inputs(target_dir, selectors, excludes, &config, cache_dir)?;
    let (shutdown_tx, handles) = spawn_controllers(inputs, &config, project_dir)?;

    tracing::info!(
        controllers = handles.len(),
        "started controllers, press Ctrl-C to stop"
    );

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("received Ctrl-C, shutting down...");
    shutdown_tx.send(true).ok();

    let grace_period = config
        .termination_grace_period_seconds
        .map(StdDuration::from_secs);

    await_controller_shutdown(handles, grace_period).await;

    run_final_export(&config, resources_dir).await;

    Ok(())
}

/// Loads compiled assets and the dependency graph, then partitions into controller inputs.
fn load_controller_inputs(
    target_dir: &Path,
    selectors: &[&str],
    excludes: &[&str],
    config: &crate::runtime::config::NagiConfig,
    cache_dir: Option<&Path>,
) -> Result<Vec<controller::ControllerInput>, ServeError> {
    let assets = crate::runtime::compile::load_compiled_assets(target_dir, selectors, excludes)?;

    let graph: DependencyGraph = crate::runtime::compile::load_graph(target_dir)?;

    let asset_map: HashMap<String, String> = assets.into_iter().collect();
    let mut inputs = build_controller_inputs(&graph, &asset_map)?;

    if let Some(max) = config.max_controllers {
        if inputs.len() > max {
            return Err(ServeError::Parse(format!(
                "dependency graph has {} connected components, but maxControllers is set to {}. \
                 Reduce the number of independent asset groups or increase maxControllers in nagi.yaml.",
                inputs.len(),
                max
            )));
        }
    }

    let resolved_cache = Some(
        cache_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| config.nagi_dir.evaluate_cache_dir()),
    );
    for input in &mut inputs {
        input.cache_dir = resolved_cache.clone();
    }

    Ok(inputs)
}

type ControllerHandle = tokio::task::JoinHandle<Result<(), ServeError>>;

/// Spawns one controller task per input, returning the shutdown channel and task handles.
fn spawn_controllers(
    inputs: Vec<controller::ControllerInput>,
    config: &crate::runtime::config::NagiConfig,
    project_dir: Option<&Path>,
) -> Result<(watch::Sender<bool>, Vec<ControllerHandle>), ServeError> {
    let notifier = build_notifier(project_dir);
    let base_backend = build_backend_stores(config)?;
    let db_path = config.nagi_dir.db_path();
    let logs_dir = config.nagi_dir.logs_dir();

    let lc = reconciler::LockConfig {
        ttl_seconds: config.lock_ttl_seconds,
        retry_interval_seconds: config.lock_retry_interval_seconds,
        retry_max_attempts: config.lock_retry_max_attempts,
    };

    let concurrency = ConcurrencyLimits {
        max_evaluate: config.max_evaluate_concurrency,
        max_sync: config.max_sync_concurrency,
    };

    let (shutdown_tx, _) = watch::channel(false);

    let mut handles = Vec::new();
    for input in inputs {
        let rx = shutdown_tx.subscribe();
        let n = notifier.clone();
        let store = LogStore::open(&db_path, &logs_dir)
            .map_err(|e| ServeError::Parse(format!("failed to open log store: {e}")))?;
        let backend = base_backend.clone();
        handles.push(tokio::spawn(run_controller(
            input,
            backend,
            n,
            Some(store),
            lc,
            concurrency,
            rx,
        )));
    }

    Ok((shutdown_tx, handles))
}

/// Runs a final export of all log tables during graceful shutdown.
/// Failures are logged as warnings and do not propagate.
async fn run_final_export(config: &crate::runtime::config::NagiConfig, resources_dir: &Path) {
    let export_config = match config.export {
        Some(ref c) => c,
        None => return,
    };

    tracing::info!("running final export...");
    let db_path = config.nagi_dir.db_path();
    let logs_dir = config.nagi_dir.logs_dir();
    let wm_dir = config.nagi_dir.watermarks_dir();

    let log_store = match crate::runtime::log::LogStore::open(&db_path, &logs_dir) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(%e, "failed to open log store for export");
            return;
        }
    };

    let conn = match crate::runtime::export::resolve_export_connection(
        resources_dir,
        &export_config.connection,
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(%e, "export connection resolution failed");
            return;
        }
    };

    let remote_store = crate::runtime::storage::remote::create_remote_store(&config.backend).ok();
    let results = crate::runtime::export::export_all(
        &log_store,
        conn.as_ref(),
        remote_store.as_ref(),
        export_config,
        &wm_dir,
        &[],
    )
    .await;

    for r in &results {
        tracing::info!(table = %r.table, rows = r.rows_exported, "export complete");
    }
}

/// Creates [`BackendStores`] from the backend config.
fn build_backend_stores(
    config: &crate::runtime::config::NagiConfig,
) -> Result<BackendStores, ServeError> {
    use crate::runtime::storage::remote::create_remote_store;
    use std::sync::Arc;

    match config.backend.r#type.as_str() {
        "local" => {
            let sync_lock: Arc<dyn crate::runtime::storage::SyncLock> =
                Arc::new(LocalSyncLock::new(config.nagi_dir.locks_dir()));
            let suspended_store: Arc<dyn SuspendedStore> =
                Arc::new(LocalSuspendedStore::new(config.nagi_dir.suspended_dir()));
            let readiness_store: Arc<dyn ReadinessStore> =
                Arc::new(LocalReadinessStore::new(config.nagi_dir.readiness_dir()));
            Ok(BackendStores {
                sync_lock,
                suspended_store,
                readiness_store,
            })
        }
        "gcs" | "s3" => {
            let remote =
                Arc::new(create_remote_store(&config.backend).map_err(ServeError::Storage)?);
            Ok(BackendStores {
                sync_lock: remote.clone(),
                suspended_store: remote.clone(),
                readiness_store: remote,
            })
        }
        t => Err(ServeError::Parse(format!("unknown backend type: {t}"))),
    }
}

/// Resumes suspended assets by removing their flag files.
///
/// If `selectors` is empty, lists suspended assets without removing.
/// If `selectors` is non-empty, removes the suspended flag for each matching asset.
pub fn resume(
    selectors: &[&str],
    nagi_dir: &crate::runtime::config::NagiDir,
) -> Result<Vec<String>, std::io::Error> {
    let dir = nagi_dir.suspended_dir();
    if selectors.is_empty() {
        let items = list_suspended(&dir)?;
        return Ok(items.into_iter().map(|i| i.asset_name).collect());
    }
    let mut resumed = Vec::new();
    for &sel in selectors {
        if suspended_path(&dir, sel)?.exists() {
            remove_suspended(&dir, sel)?;
            resumed.push(sel.to_string());
        }
    }
    Ok(resumed)
}

/// Halts all compiled assets by writing suspended flags for each.
///
/// Returns the list of asset names that were halted (newly suspended).
/// Assets already suspended are skipped.
pub fn halt(
    target_dir: &Path,
    reason: &str,
    nagi_dir: &crate::runtime::config::NagiDir,
) -> Result<Vec<String>, ServeError> {
    use crate::runtime::storage::local::LocalSuspendedStore;
    use crate::runtime::storage::SuspendedStore;

    let asset_names = crate::runtime::compile::resolve_compiled_asset_names(target_dir, &[], &[])?;
    let store = LocalSuspendedStore::new(nagi_dir.suspended_dir());
    let now = chrono::Utc::now().to_rfc3339();

    let mut halted = Vec::new();
    for name in asset_names {
        if store.exists(&name).map_err(ServeError::Storage)? {
            continue;
        }
        store
            .write(&SuspendedInfo {
                asset_name: name.clone(),
                reason: reason.to_string(),
                suspended_at: now.clone(),
                execution_id: None,
            })
            .map_err(ServeError::Storage)?;
        halted.push(name);
    }
    Ok(halted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::compile::{DependencyGraph, GraphEdge, GraphNode};

    fn setup_target(dir: &Path, asset_names: &[&str], edges: &[(&str, &str)]) {
        let assets_dir = dir.join("assets");
        std::fs::create_dir_all(&assets_dir).unwrap();

        let graph = DependencyGraph {
            nodes: asset_names
                .iter()
                .map(|n| GraphNode {
                    name: n.to_string(),
                    kind: "Asset".to_string(),
                    tags: vec![],
                })
                .collect(),
            edges: edges
                .iter()
                .map(|(f, t)| GraphEdge {
                    from: f.to_string(),
                    to: t.to_string(),
                })
                .collect(),
        };
        std::fs::write(
            dir.join("graph.json"),
            serde_json::to_string(&graph).unwrap(),
        )
        .unwrap();

        for name in asset_names {
            let yaml = format!(
                "apiVersion: nagi.io/v1alpha1\nmetadata:\n  name: {name}\nspec:\n  upstreams: []\n  onDrift: []\n  autoSync: false\n  tags: []\n"
            );
            std::fs::write(assets_dir.join(format!("{name}.yaml")), yaml).unwrap();
        }
    }

    #[test]
    fn build_backend_stores_rejects_unknown_type() {
        let config = crate::runtime::config::NagiConfig {
            backend: crate::runtime::config::BackendConfig {
                r#type: "redis".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(build_backend_stores(&config).is_err());
    }

    #[test]
    fn load_controller_inputs_returns_components() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        setup_target(&target, &["a", "b"], &[]);

        let config = crate::runtime::config::NagiConfig::default();
        let inputs = load_controller_inputs(&target, &[], &[], &config, None).unwrap();
        // Two independent assets → two components
        assert_eq!(inputs.len(), 2);
        assert!(inputs[0].cache_dir.is_some());
    }

    #[test]
    fn load_controller_inputs_rejects_exceeding_max_controllers() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        setup_target(&target, &["a", "b", "c"], &[]);

        let config = crate::runtime::config::NagiConfig {
            max_controllers: Some(2),
            ..Default::default()
        };
        let err = load_controller_inputs(&target, &[], &[], &config, None).unwrap_err();
        assert!(err.to_string().contains("3 connected components"));
    }

    #[test]
    fn load_controller_inputs_respects_cache_dir_override() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        setup_target(&target, &["a"], &[]);

        let config = crate::runtime::config::NagiConfig::default();
        let custom = dir.path().join("custom-cache");
        let inputs =
            load_controller_inputs(&target, &[], &[], &config, Some(custom.as_path())).unwrap();
        assert_eq!(inputs[0].cache_dir, Some(custom));
    }
}

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

use crate::compile::DependencyGraph;
use crate::log::LogStore;

pub use graph::connected_components;
pub use suspended::SuspendedInfo;

use controller::{
    await_controller_shutdown, build_controller_inputs, build_notifier, run_controller,
    BackendStores,
};
use suspended::{list_suspended, remove_suspended, suspended_dir, suspended_path};

use crate::storage::local::{LocalSuspendedStore, LocalSyncLock};
use crate::storage::SuspendedStore;

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("compile error: {0}")]
    Compile(#[from] crate::compile::CompileError),
    #[error("evaluate error: {0}")]
    Evaluate(#[from] crate::evaluate::EvaluateError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sync error: {0}")]
    Sync(#[from] crate::sync::SyncError),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Storage(crate::storage::StorageError),
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
    cache_dir: Option<&Path>,
    project_dir: Option<&Path>,
) -> Result<(), ServeError> {
    tracing::info!("compiling resources...");
    let output = crate::compile::compile(resources_dir, target_dir)?;
    tracing::info!(
        nodes = output.graph.nodes.len(),
        edges = output.graph.edges.len(),
        "compiled"
    );

    let assets = crate::compile::load_compiled_assets(target_dir, selectors)?;

    let graph: DependencyGraph = crate::compile::load_graph(target_dir)?;

    let config = crate::config::load_config(project_dir.unwrap_or(Path::new(".")))
        .map_err(|e| ServeError::Parse(format!("failed to load config: {e}")))?;

    let notifier = build_notifier(project_dir);

    let db_path = config.db_path();
    let logs_dir = config.logs_dir();

    let asset_map: HashMap<String, String> = assets.into_iter().collect();
    let inputs = build_controller_inputs(&graph, &asset_map)?;

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

    // Build storage backends from config.
    let base_backend = build_backend_stores(&config)?;

    let (shutdown_tx, _) = watch::channel(false);

    let lc = reconciler::LockConfig {
        ttl_seconds: config.lock_ttl_seconds,
        retry_interval_seconds: config.lock_retry_interval_seconds,
        retry_max_attempts: config.lock_retry_max_attempts,
    };

    let mut handles = Vec::new();
    for input in inputs {
        let rx = shutdown_tx.subscribe();
        let cd = cache_dir.map(PathBuf::from);
        let n = notifier.clone();
        // LogStore uses rusqlite::Connection which is !Send, so each controller
        // needs its own instance.
        let store = LogStore::open(&db_path, &logs_dir)
            .map_err(|e| ServeError::Parse(format!("failed to open log store: {e}")))?;
        let backend = base_backend.clone();
        handles.push(tokio::spawn(run_controller(
            input,
            backend,
            cd,
            n,
            Some(store),
            lc,
            rx,
        )));
    }

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

    Ok(())
}

/// Creates [`BackendStores`] from the backend config.
fn build_backend_stores(config: &crate::config::NagiConfig) -> Result<BackendStores, ServeError> {
    use crate::storage::remote::create_remote_store;
    use std::sync::Arc;

    match config.backend.r#type.as_str() {
        "local" => {
            let sync_lock: Arc<dyn crate::storage::SyncLock> =
                Arc::new(LocalSyncLock::new(config.locks_dir()));
            let suspended_store: Arc<dyn SuspendedStore> =
                Arc::new(LocalSuspendedStore::new(config.suspended_dir()));
            Ok(BackendStores {
                sync_lock,
                suspended_store,
            })
        }
        "gcs" | "s3" => {
            let remote =
                Arc::new(create_remote_store(&config.backend).map_err(ServeError::Storage)?);
            Ok(BackendStores {
                sync_lock: remote.clone(),
                suspended_store: remote,
            })
        }
        t => Err(ServeError::Parse(format!("unknown backend type: {t}"))),
    }
}

/// Lists all currently suspended assets.
pub fn list_suspended_assets(nagi_dir: &Path) -> Result<Vec<SuspendedInfo>, std::io::Error> {
    list_suspended(&suspended_dir(nagi_dir))
}

/// Resumes suspended assets by removing their flag files.
///
/// If `selectors` is empty, lists suspended assets without removing.
/// If `selectors` is non-empty, removes the suspended flag for each matching asset.
pub fn resume(selectors: &[&str], nagi_dir: &Path) -> Result<Vec<String>, std::io::Error> {
    let dir = suspended_dir(nagi_dir);
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
pub fn halt(target_dir: &Path, reason: &str, nagi_dir: &Path) -> Result<Vec<String>, ServeError> {
    use crate::storage::local::LocalSuspendedStore;
    use crate::storage::SuspendedStore;

    let asset_names = crate::compile::resolve_compiled_asset_names(target_dir, &[])?;
    let store = LocalSuspendedStore::new(suspended_dir(nagi_dir));
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

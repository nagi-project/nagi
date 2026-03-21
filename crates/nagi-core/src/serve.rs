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

mod graph;
pub mod guardrail;
pub mod queue;
mod reconciler;
pub mod scheduler;
pub mod state;
pub mod suspended;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::compile::{CompiledAsset, DependencyGraph, GraphEdge};
use crate::evaluate::EvaluateError;
use crate::kind::asset::DesiredSetEntry;
use crate::log::LogStore;
use crate::notify::{Notifier, NotifyEvent};
use crate::sync::SyncError;

pub use graph::connected_components;
pub use suspended::SuspendedInfo;

use state::{AssetEntry, ServeState};
use suspended::{list_suspended, remove_suspended, suspended_dir, suspended_path};

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("compile error: {0}")]
    Compile(#[from] crate::compile::CompileError),
    #[error("evaluate error: {0}")]
    Evaluate(#[from] EvaluateError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sync error: {0}")]
    Sync(#[from] SyncError),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Storage(crate::storage::StorageError),
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Builds a notifier from project config, if configured.
/// Returns `None` if no project dir, no config, or no Slack config.
fn build_notifier(project_dir: Option<&Path>) -> Option<Arc<dyn Notifier>> {
    let dir = project_dir?;
    let config = crate::config::load_config(dir).ok()?;
    let slack = config.notify.slack?;
    Some(Arc::new(crate::notify::slack::SlackNotifier::new(slack.channel)) as Arc<dyn Notifier>)
}

/// Waits for all in-flight sync tasks to complete before returning.
/// Sync tasks have side effects and must not be aborted mid-execution.
async fn drain_sync_tasks(
    tasks: &mut JoinSet<(String, Result<crate::sync::SyncExecutionResult, SyncError>)>,
) {
    if tasks.is_empty() {
        return;
    }
    eprintln!(
        "[serve] waiting for {} in-flight sync task(s) to finish",
        tasks.len()
    );
    while let Some(result) = tasks.join_next().await {
        if let Ok((name, Err(e))) = result {
            eprintln!("[serve] sync for {name} failed during shutdown: {e}");
        }
    }
}

/// Waits for all controller tasks to finish, optionally with a timeout.
async fn await_controller_shutdown(
    handles: Vec<tokio::task::JoinHandle<Result<(), ServeError>>>,
    grace_period: Option<StdDuration>,
) {
    for h in handles {
        let result = match grace_period {
            Some(timeout) => match tokio::time::timeout(timeout, h).await {
                Ok(r) => Some(r),
                Err(_) => {
                    eprintln!("[serve] controller did not shut down within {timeout:?}, aborting");
                    None
                }
            },
            None => Some(h.await),
        };
        if let Some(r) = result {
            match r {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("[serve] controller error: {e}"),
                Err(e) => eprintln!("[serve] controller task panicked: {e}"),
            }
        }
    }
}

/// Sends a notification asynchronously without blocking the serve loop.
fn fire_notify(notifier: &Option<Arc<dyn Notifier>>, event: NotifyEvent) {
    let Some(n) = notifier.clone() else {
        return;
    };
    tokio::spawn(async move {
        if let Err(e) = n.notify(&event).await {
            eprintln!("[serve] notification failed: {e}");
        }
    });
}

/// Processes eval outcome: writes to log store and returns the result for state update.
/// Returns a tuple of (name, eval_result) and an optional notification event.
fn process_eval_outcome(
    name: String,
    outcome: reconciler::EvalOutcome,
    log_store: &Option<LogStore>,
) -> (
    (
        String,
        Result<crate::evaluate::AssetEvalResult, EvaluateError>,
    ),
    Option<NotifyEvent>,
) {
    if let (Some(store), Ok(ref eval)) = (log_store, &outcome.result) {
        let eval_id = crate::sync::generate_uuid();
        if let Err(e) =
            store.write_evaluate_log(&eval_id, eval, &outcome.started_at, &outcome.finished_at)
        {
            eprintln!("[serve] warning: failed to log evaluation for {name}: {e}");
        }
    }
    let event = if let Err(ref e) = outcome.result {
        Some(NotifyEvent::EvalFailed {
            asset_name: name.clone(),
            error: e.to_string(),
        })
    } else {
        None
    };
    ((name, outcome.result), event)
}

/// Writes sync result to log store. Errors are logged but not propagated.
fn log_sync_result(name: &str, result: &crate::sync::SyncExecutionResult, log_store: &LogStore) {
    if let Err(e) = log_store.write_sync_log(result) {
        eprintln!("[serve] warning: failed to log sync for {name}: {e}");
    }
}

/// Computes the minimum interval across all inline conditions of a compiled asset.
fn compute_min_interval(compiled: &CompiledAsset) -> Option<StdDuration> {
    compiled
        .spec
        .desired_sets
        .iter()
        .filter_map(|entry| match entry {
            DesiredSetEntry::Inline(cond) => cond.interval().map(|d| d.as_std()),
            _ => None,
        })
        .min()
}

// ── Controller ───────────────────────────────────────────────────────────────

/// Runs the reconciliation loop for one connected component.
///
/// The loop reacts to four events via `tokio::select!`:
/// 1. **Timer** — a scheduled asset becomes due → enqueue it.
/// 2. **Eval completion** — a spawned evaluation finishes → update state,
///    propagate to downstream assets if newly Ready, request sync if Not Ready.
/// 3. **Sync completion** — a sync finishes → enqueue re-evaluation.
/// 4. **Shutdown** — break and return.
///
/// Sync is serialized: at most one sync runs at a time per Controller.
async fn run_controller(
    assets: Vec<AssetEntry>,
    edges: Vec<GraphEdge>,
    cache_dir: Option<PathBuf>,
    notifier: Option<Arc<dyn Notifier>>,
    log_store: Option<LogStore>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServeError> {
    let mut state = ServeState::new(&edges, suspended_dir()?);
    let mut eval_tasks: JoinSet<(String, reconciler::EvalOutcome)> = JoinSet::new();
    let mut sync_tasks: JoinSet<(String, Result<crate::sync::SyncExecutionResult, SyncError>)> =
        JoinSet::new();

    state.init(&assets);

    let yaml_map: HashMap<&str, &str> = assets
        .iter()
        .map(|a| (a.name.as_str(), a.yaml.as_str()))
        .collect();

    // ── Main reconciliation loop ──────────────────────────────────────
    //
    // Each iteration:
    //   1. Spawn: drain work_queue into eval JoinSet (concurrent).
    //   2. Spawn: start syncs from sync_queue (serialized per sync ref).
    //   3. Wait:  select! on the first event that fires:
    //      a) Timer        — interval elapsed → enqueue the due asset.
    //      b) Eval done    — update readiness; if Ready, propagate to
    //                        downstreams; if Not Ready, request sync.
    //      c) Sync done    — enqueue re-evaluation to verify convergence.
    //      d) Shutdown      — break out of the loop.
    //
    // Evaluations run concurrently (read-only).
    // Syncs sharing the same sync ref are serialized; different refs may
    // run concurrently.

    loop {
        // (1) Spawn pending evaluations — multiple may run concurrently.
        while let Some(name) = state.next_spawnable() {
            if let Some(&yaml) = yaml_map.get(name.as_str()) {
                eval_tasks.spawn(reconciler::spawn_evaluate(
                    name,
                    yaml.to_string(),
                    cache_dir.clone(),
                    None,
                ));
            }
        }

        // (2) Spawn syncs whose sync ref is not currently in use.
        while let Some(name) = state.next_syncable() {
            if let Some(&yaml) = yaml_map.get(name.as_str()) {
                eprintln!("[serve] starting sync for {name}");
                sync_tasks.spawn(reconciler::spawn_sync(name, yaml.to_string()));
            }
        }

        // (3) Wait for the next event.
        let sleep_until = state.scheduler.next_due().map(|(_, instant)| instant);

        tokio::select! {
            // (a) Timer: a scheduled asset's interval has elapsed.
            _ = async {
                match sleep_until {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            } => {
                state.enqueue_due();
            }

            // (b) Eval complete: log, update state, propagate or request sync.
            join_result = eval_tasks.join_next(), if !eval_tasks.is_empty() => {
                let eval_result = join_result.map(|r| r.map(|(name, outcome)| {
                    let (name_and_result, event) = process_eval_outcome(name, outcome, &log_store);
                    if let Some(ev) = event { fire_notify(&notifier, ev); }
                    name_and_result
                }));
                if let Some(event) = state.on_eval_complete(eval_result) {
                    fire_notify(&notifier, NotifyEvent::Suspended {
                        asset_name: event.asset_name,
                        reason: event.reason,
                    });
                }
            }

            // (c) Sync complete: log, enqueue re-evaluation to verify convergence.
            result = sync_tasks.join_next(), if !sync_tasks.is_empty() => {
                if let Some(Ok((ref name, Ok(ref sync_result)))) = result {
                    if let Some(ref store) = log_store {
                        log_sync_result(name, sync_result, store);
                    }
                }
                if let Some(event) = state.on_sync_complete(result) {
                    fire_notify(&notifier, NotifyEvent::Suspended {
                        asset_name: event.asset_name,
                        reason: event.reason,
                    });
                }
            }

            // (d) Shutdown signal received.
            _ = shutdown.changed() => {
                eprintln!("[serve] shutting down controller");
                break;
            }
        }
    }

    // Eval tasks are read-only and safe to abort.
    drop(eval_tasks);
    drain_sync_tasks(&mut sync_tasks).await;

    Ok(())
}

// ── Entry Point ──────────────────────────────────────────────────────────────

/// Input for one Controller: assets with their YAML and intervals, plus edges.
struct ControllerInput {
    assets: Vec<AssetEntry>,
    edges: Vec<GraphEdge>,
}

/// Builds per-component [`ControllerInput`]s from the graph and compiled assets.
fn build_controller_inputs(
    graph: &DependencyGraph,
    asset_map: &HashMap<String, String>,
) -> Result<Vec<ControllerInput>, ServeError> {
    let components = connected_components(graph);
    let mut inputs = Vec::new();

    for component in components {
        // Set of asset names in this component, used to filter edges below.
        let component_set: HashSet<&str> = component.iter().map(|s| s.as_str()).collect();

        // Parse each compiled YAML to extract interval and sync config.
        let assets: Vec<_> = component
            .iter()
            .filter_map(|name| {
                let yaml = asset_map.get(name)?;
                let compiled: CompiledAsset = match serde_yaml::from_str(yaml) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("[serve] warning: skipping asset {name}: {e}");
                        return None;
                    }
                };
                let min_interval = compute_min_interval(&compiled);
                Some(AssetEntry {
                    name: name.clone(),
                    yaml: yaml.clone(),
                    min_interval,
                    auto_sync: compiled.spec.auto_sync,
                    has_sync: compiled.spec.sync.is_some(),
                    sync_ref_name: compiled.spec.sync_ref_name,
                })
            })
            .collect();

        if assets.is_empty() {
            continue;
        }

        // Collect edges where either endpoint belongs to this component.
        // Includes Source → Asset edges so downstream_map covers them.
        let edges: Vec<GraphEdge> = graph
            .edges
            .iter()
            .filter(|e| {
                component_set.contains(e.from.as_str()) || component_set.contains(e.to.as_str())
            })
            .cloned()
            .collect();

        inputs.push(ControllerInput { assets, edges });
    }

    Ok(inputs)
}

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
    eprintln!("[serve] compiling resources...");
    let output = crate::compile::compile(resources_dir, target_dir)?;
    eprintln!(
        "[serve] compiled {} node(s), {} edge(s)",
        output.graph.nodes.len(),
        output.graph.edges.len()
    );

    let assets = crate::compile::load_compiled_assets(target_dir, selectors)?;

    let graph_path = target_dir.join("graph.json");
    let graph_json = std::fs::read_to_string(&graph_path)?;
    let graph: DependencyGraph =
        serde_json::from_str(&graph_json).map_err(|e| ServeError::Parse(e.to_string()))?;

    let config = crate::config::load_config(project_dir.unwrap_or(Path::new(".")))
        .map_err(|e| ServeError::Parse(format!("failed to load config: {e}")))?;

    let notifier = build_notifier(project_dir);

    let db_path = crate::init::default_db_path();
    let logs_dir = crate::init::default_logs_dir();

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

    let (shutdown_tx, _) = watch::channel(false);

    let mut handles = Vec::new();
    for input in inputs {
        let rx = shutdown_tx.subscribe();
        let cd = cache_dir.map(PathBuf::from);
        let n = notifier.clone();
        // LogStore uses rusqlite::Connection which is !Send, so each controller
        // needs its own instance.
        let store = LogStore::open(&db_path, &logs_dir)
            .map_err(|e| ServeError::Parse(format!("failed to open log store: {e}")))?;
        handles.push(tokio::spawn(run_controller(
            input.assets,
            input.edges,
            cd,
            n,
            Some(store),
            rx,
        )));
    }

    eprintln!(
        "[serve] started {} controller(s). Press Ctrl-C to stop.",
        handles.len()
    );

    tokio::signal::ctrl_c().await.ok();
    eprintln!("[serve] received Ctrl-C, shutting down...");
    shutdown_tx.send(true).ok();

    let grace_period = config
        .termination_grace_period_seconds
        .map(StdDuration::from_secs);

    await_controller_shutdown(handles, grace_period).await;

    Ok(())
}

/// Lists all currently suspended assets.
pub fn list_suspended_assets() -> Result<Vec<SuspendedInfo>, std::io::Error> {
    list_suspended(&suspended_dir()?)
}

/// Resumes suspended assets by removing their flag files.
///
/// If `selectors` is empty, lists suspended assets without removing.
/// If `selectors` is non-empty, removes the suspended flag for each matching asset.
pub fn resume(selectors: &[&str]) -> Result<Vec<String>, std::io::Error> {
    let dir = suspended_dir()?;
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
pub fn halt(target_dir: &Path, reason: &str) -> Result<Vec<String>, ServeError> {
    use crate::storage::local::LocalSuspendedStore;
    use crate::storage::SuspendedStore;

    let asset_names = crate::compile::resolve_compiled_asset_names(target_dir, &[])?;
    let store = LocalSuspendedStore::new(suspended_dir()?);
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{GraphEdge, GraphNode};
    use crate::notify::NotifyError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn asset_node(name: &str) -> GraphNode {
        GraphNode {
            name: name.to_string(),
            kind: "Asset".to_string(),
            tags: vec![],
        }
    }

    fn source_node(name: &str) -> GraphNode {
        GraphNode {
            name: name.to_string(),
            kind: "Source".to_string(),
            tags: vec![],
        }
    }

    fn edge(from: &str, to: &str) -> GraphEdge {
        GraphEdge {
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    struct MockNotifier {
        call_count: AtomicUsize,
    }

    impl MockNotifier {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::notify::Notifier for MockNotifier {
        async fn notify(&self, _event: &NotifyEvent) -> Result<(), NotifyError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn fire_notify_skips_when_none() {
        // Should not panic.
        fire_notify(
            &None,
            NotifyEvent::Suspended {
                asset_name: "a".to_string(),
                reason: "test".to_string(),
            },
        );
    }

    #[tokio::test]
    async fn fire_notify_calls_notifier_when_some() {
        let mock = Arc::new(MockNotifier::new());
        let notifier: Option<Arc<dyn Notifier>> = Some(mock.clone());

        fire_notify(
            &notifier,
            NotifyEvent::Suspended {
                asset_name: "a".to_string(),
                reason: "test".to_string(),
            },
        );

        // fire_notify spawns a task; yield to let it run.
        tokio::task::yield_now().await;

        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn build_notifier_returns_none_when_no_project_dir() {
        assert!(build_notifier(None).is_none());
    }

    #[test]
    fn build_notifier_returns_none_when_no_config() {
        let dir = tempfile::tempdir().unwrap();
        assert!(build_notifier(Some(dir.path())).is_none());
    }

    #[test]
    fn build_notifier_returns_none_when_no_slack_config() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "backend:\n  type: local\n").unwrap();
        assert!(build_notifier(Some(dir.path())).is_none());
    }

    #[test]
    fn build_notifier_returns_some_when_slack_configured() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("nagi.yaml"),
            "notify:\n  slack:\n    channel: \"#test\"\n",
        )
        .unwrap();
        assert!(build_notifier(Some(dir.path())).is_some());
    }

    #[test]
    fn build_controller_inputs_splits_components() {
        let graph = DependencyGraph {
            nodes: vec![
                source_node("s1"),
                asset_node("a1"),
                source_node("s2"),
                asset_node("a2"),
            ],
            edges: vec![edge("s1", "a1"), edge("s2", "a2")],
        };
        let yaml = |name: &str| {
            format!("apiVersion: v1\nmetadata:\n  name: {name}\nspec:\n  desiredSets: []\n")
        };
        let asset_map: HashMap<String, String> = [
            ("a1".to_string(), yaml("a1")),
            ("a2".to_string(), yaml("a2")),
        ]
        .into();

        let inputs = build_controller_inputs(&graph, &asset_map).unwrap();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].assets.len(), 1);
        assert_eq!(inputs[1].assets.len(), 1);
    }

    // ── drain_sync_tasks tests ──────────────────────────────────────────

    #[tokio::test]
    async fn drain_sync_tasks_empty_returns_immediately() {
        let mut tasks = JoinSet::new();
        drain_sync_tasks(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn drain_sync_tasks_waits_for_completion() {
        use crate::sync::{SyncExecutionResult, SyncType};
        use std::sync::atomic::{AtomicBool, Ordering};

        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            completed_clone.store(true, Ordering::SeqCst);
            let result = SyncExecutionResult {
                execution_id: "test".to_string(),
                asset_name: "a".to_string(),
                sync_type: SyncType::Sync,
                stages: vec![],
                success: true,
            };
            ("a".to_string(), Ok(result))
        });

        drain_sync_tasks(&mut tasks).await;
        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn drain_sync_tasks_handles_errors() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            let err = SyncError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test error"));
            ("a".to_string(), Err(err))
        });

        // Should not panic.
        drain_sync_tasks(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    // ── process_eval_outcome tests ────────────────────────────────────────

    fn sample_eval_result(name: &str, ready: bool) -> crate::evaluate::AssetEvalResult {
        crate::evaluate::AssetEvalResult {
            asset_name: name.to_string(),
            ready,
            conditions: vec![],
            evaluation_id: None,
        }
    }

    fn sample_outcome_ok(name: &str) -> reconciler::EvalOutcome {
        reconciler::EvalOutcome {
            result: Ok(sample_eval_result(name, true)),
            started_at: "2025-01-01T00:00:00Z".to_string(),
            finished_at: "2025-01-01T00:00:01Z".to_string(),
        }
    }

    fn sample_outcome_err() -> reconciler::EvalOutcome {
        reconciler::EvalOutcome {
            result: Err(EvaluateError::Parse("test".to_string())),
            started_at: "2025-01-01T00:00:00Z".to_string(),
            finished_at: "2025-01-01T00:00:01Z".to_string(),
        }
    }

    #[test]
    fn process_eval_outcome_ok_returns_result_no_event() {
        let (name_and_result, event) =
            process_eval_outcome("a".to_string(), sample_outcome_ok("a"), &None);
        assert_eq!(name_and_result.0, "a");
        assert!(name_and_result.1.is_ok());
        assert!(event.is_none());
    }

    #[test]
    fn process_eval_outcome_err_returns_eval_failed_event() {
        let (name_and_result, event) =
            process_eval_outcome("a".to_string(), sample_outcome_err(), &None);
        assert_eq!(name_and_result.0, "a");
        assert!(name_and_result.1.is_err());
        assert!(matches!(event, Some(NotifyEvent::EvalFailed { .. })));
    }

    #[test]
    fn process_eval_outcome_writes_log_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();

        let (name_and_result, _) =
            process_eval_outcome("a".to_string(), sample_outcome_ok("a"), &Some(store));
        assert!(name_and_result.1.is_ok());
        // Verify a log was written by checking the db is non-empty.
        assert!(db_path.metadata().unwrap().len() > 0);
    }

    #[test]
    fn process_eval_outcome_skips_log_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();
        let initial_size = db_path.metadata().unwrap().len();

        let (_name_and_result, event) =
            process_eval_outcome("a".to_string(), sample_outcome_err(), &Some(store));
        assert!(event.is_some());
        // DB size unchanged (no eval log written on error).
        assert_eq!(db_path.metadata().unwrap().len(), initial_size);
    }

    // ── log_sync_result tests ─────────────────────────────────────────────

    #[test]
    fn log_sync_result_writes_to_store() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();

        let result = crate::sync::SyncExecutionResult {
            execution_id: "exec-1".to_string(),
            asset_name: "a".to_string(),
            sync_type: crate::sync::SyncType::Sync,
            stages: vec![crate::sync::StageResult {
                stage: crate::sync::Stage::Run,
                started_at: "2025-01-01T00:00:00Z".to_string(),
                finished_at: "2025-01-01T00:00:01Z".to_string(),
                exit_code: 0,
                stdout: "ok".to_string(),
                stderr: String::new(),
                args: vec![],
            }],
            success: true,
        };
        log_sync_result("a", &result, &store);

        // Verify log files were written.
        assert!(logs_dir.join("a").exists());
    }

    // ── await_controller_shutdown tests ─────────────────────────────────

    #[tokio::test]
    async fn shutdown_no_timeout_waits_for_completion() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            completed_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        await_controller_shutdown(vec![handle], None).await;
        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_with_sufficient_timeout_waits_for_completion() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            completed_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        let timeout = Some(StdDuration::from_secs(5));
        await_controller_shutdown(vec![handle], timeout).await;
        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_with_expired_timeout_aborts() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            completed_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        let timeout = Some(StdDuration::from_millis(10));
        await_controller_shutdown(vec![handle], timeout).await;
        assert!(!completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_empty_handles() {
        await_controller_shutdown(vec![], None).await;
        await_controller_shutdown(vec![], Some(StdDuration::from_secs(1))).await;
    }
}

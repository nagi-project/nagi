//! Controller logic for the reconciliation loop.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::runtime::compile::ResolvedOnDriftEntry;
use crate::runtime::compile::{CompiledAsset, DependencyGraph, GraphEdge};
use crate::runtime::evaluate::EvaluateError;
use crate::runtime::log::LogStore;
use crate::runtime::notify::{Notifier, NotifyEvent};
use crate::runtime::sync::SyncError;

use crate::runtime::storage::SyncLock;

use super::graph::connected_components;
use super::reconciler;
use super::state::{AssetEntry, ServeState};
use super::ServeError;

/// Shared storage backends passed to each Controller.
#[derive(Clone)]
pub(super) struct BackendStores {
    pub sync_lock: Arc<dyn SyncLock>,
    pub suspended_store: Arc<dyn crate::runtime::storage::SuspendedStore>,
    pub readiness_store: Arc<dyn crate::runtime::storage::ReadinessStore>,
}

/// Builds a notifier from project config, if configured.
/// Returns `None` if no project dir, no config, or no Slack config.
pub(super) fn build_notifier(project_dir: Option<&Path>) -> Option<Arc<dyn Notifier>> {
    let dir = project_dir?;
    let config = crate::runtime::config::load_config(dir).ok()?;
    let slack = config.notify.slack?;
    Some(Arc::new(crate::runtime::notify::slack::SlackNotifier::new(
        slack.channel,
    )) as Arc<dyn Notifier>)
}

/// Waits for all in-flight sync tasks to complete before returning.
/// Sync tasks have side effects and must not be aborted mid-execution.
async fn drain_sync_tasks(
    tasks: &mut JoinSet<(
        String,
        Result<crate::runtime::sync::SyncExecutionResult, SyncError>,
    )>,
) {
    if tasks.is_empty() {
        return;
    }
    tracing::info!(
        count = tasks.len(),
        "waiting for in-flight sync tasks to finish"
    );
    while let Some(result) = tasks.join_next().await {
        if let Ok((name, Err(e))) = result {
            tracing::error!(asset = %name, error = %e, "sync failed during shutdown");
        }
    }
}

/// Waits for all controller tasks to finish, optionally with a timeout.
pub(super) async fn await_controller_shutdown(
    handles: Vec<tokio::task::JoinHandle<Result<(), ServeError>>>,
    grace_period: Option<StdDuration>,
) {
    for h in handles {
        let result = match grace_period {
            Some(timeout) => match tokio::time::timeout(timeout, h).await {
                Ok(r) => Some(r),
                Err(_) => {
                    tracing::warn!(
                        ?timeout,
                        "controller did not shut down within timeout, aborting"
                    );
                    None
                }
            },
            None => Some(h.await),
        };
        if let Some(r) = result {
            match r {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::error!(error = %e, "controller error"),
                Err(e) => tracing::error!(error = %e, "controller task panicked"),
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
            tracing::warn!(error = %e, "notification failed");
        }
    });
}

/// Processes evaluate outcome: writes to log store and returns the result for state update.
/// Returns a tuple of (name, evaluate_result) and an optional notification event.
fn process_evaluate_outcome(
    name: String,
    outcome: reconciler::EvaluateOutcome,
    log_store: &Option<LogStore>,
) -> (
    (
        String,
        Result<crate::runtime::evaluate::AssetEvalResult, EvaluateError>,
    ),
    Option<NotifyEvent>,
) {
    if let (Some(store), Ok(ref result)) = (log_store, &outcome.result) {
        let evaluate_id = crate::runtime::sync::generate_uuid();
        if let Err(e) = store.write_evaluate_log(
            &evaluate_id,
            result,
            &outcome.started_at,
            &outcome.finished_at,
        ) {
            tracing::warn!(asset = %name, error = %e, "failed to log evaluation");
        }
    }
    let event = if let Err(ref e) = outcome.result {
        Some(NotifyEvent::EvaluateFailed {
            asset_name: name.clone(),
            error: e.to_string(),
        })
    } else {
        None
    };
    ((name, outcome.result), event)
}

/// Writes sync result to log store. Errors are logged but not propagated.
fn log_sync_result(
    name: &str,
    result: &crate::runtime::sync::SyncExecutionResult,
    log_store: &LogStore,
) {
    if let Err(e) = log_store.write_sync_log(result) {
        tracing::warn!(asset = %name, error = %e, "failed to log sync");
    }
}

/// Computes the minimum interval across all conditions of a compiled asset.
fn compute_min_interval(on_drift: &[ResolvedOnDriftEntry]) -> Option<StdDuration> {
    on_drift
        .iter()
        .flat_map(|entry| &entry.conditions)
        .filter_map(|cond| cond.interval().map(|d| d.as_std()))
        .min()
}

/// Concurrency limits for evaluate and sync tasks within a Controller.
#[derive(Debug, Clone, Copy)]
pub(super) struct ConcurrencyLimits {
    pub max_evaluate: Option<usize>,
    pub max_sync: Option<usize>,
}

/// Drains the evaluate queue and spawns evaluate tasks, respecting the concurrency limit.
fn spawn_evaluates(
    state: &mut ServeState,
    evaluate_tasks: &mut JoinSet<(String, reconciler::EvaluateOutcome)>,
    yaml_map: &HashMap<&str, &str>,
    cache_dir: &Option<PathBuf>,
    max_evaluate: Option<usize>,
    conn_semaphores: &ConnectionSemaphores,
) {
    while let Some(name) = state.next_spawnable(max_evaluate) {
        if let Some(&yaml) = yaml_map.get(name.as_str()) {
            let skip_cache = state.is_awaiting_post_sync_evaluate(&name);
            let sem = conn_semaphores.get(&name).cloned();
            let cache = cache_dir.clone();
            let yaml_owned = yaml.to_string();
            evaluate_tasks.spawn(async move {
                let _permit = match &sem {
                    Some(s) => Some(s.acquire().await.expect("semaphore closed")),
                    None => None,
                };
                reconciler::spawn_evaluate(name, yaml_owned, cache, skip_cache).await
            });
        }
    }
}

/// Context for spawning sync tasks.
struct SyncSpawnContext<'a> {
    yaml_map: &'a HashMap<&'a str, &'a str>,
    sync_lock: &'a Arc<dyn SyncLock>,
    lock_config: reconciler::LockConfig,
    notifier: &'a Option<Arc<dyn Notifier>>,
    max_sync: Option<usize>,
    conn_semaphores: &'a ConnectionSemaphores,
}

/// Drains the sync queue and spawns sync tasks, respecting the concurrency limit.
fn spawn_syncs(
    state: &mut ServeState,
    sync_tasks: &mut JoinSet<(
        String,
        Result<crate::runtime::sync::SyncExecutionResult, SyncError>,
    )>,
    ctx: &SyncSpawnContext<'_>,
) {
    while let Some(name) = state.next_syncable(ctx.max_sync) {
        if let Some(&yaml) = ctx.yaml_map.get(name.as_str()) {
            tracing::info!(asset = %name, "starting sync");
            let sem = ctx.conn_semaphores.get(&name).cloned();
            let sync_lock = ctx.sync_lock.clone();
            let notifier = ctx.notifier.clone();
            let lock_config = ctx.lock_config;
            let yaml_owned = yaml.to_string();
            sync_tasks.spawn(async move {
                let _permit = match &sem {
                    Some(s) => Some(s.acquire().await.expect("semaphore closed")),
                    None => None,
                };
                reconciler::spawn_sync(name, yaml_owned, sync_lock, lock_config, notifier).await
            });
        }
    }
}

/// Processes an evaluate JoinSet result: logs, notifies, and updates state.
fn handle_evaluate_completion(
    state: &mut ServeState,
    join_result: Option<Result<(String, reconciler::EvaluateOutcome), tokio::task::JoinError>>,
    log_store: &Option<LogStore>,
    notifier: &Option<Arc<dyn Notifier>>,
) {
    let mapped = join_result.map(|r| {
        r.map(|(name, outcome)| {
            let (name_and_result, event) = process_evaluate_outcome(name, outcome, log_store);
            if let Some(ev) = event {
                fire_notify(notifier, ev);
            }
            name_and_result
        })
    });
    if let Some(event) = state.on_evaluate_complete(mapped) {
        fire_notify(
            notifier,
            NotifyEvent::Suspended {
                asset_name: event.asset_name,
                reason: event.reason,
            },
        );
    }
}

/// Processes a sync JoinSet result: logs, notifies, and updates state.
fn handle_sync_completion(
    state: &mut ServeState,
    result: super::state::SyncJoinResult,
    log_store: &Option<LogStore>,
    notifier: &Option<Arc<dyn Notifier>>,
) {
    if let Some(Ok((ref name, Ok(ref sync_result)))) = result {
        if let Some(ref store) = log_store {
            log_sync_result(name, sync_result, store);
        }
    }
    if let Some(event) = state.on_sync_complete(result) {
        fire_notify(
            notifier,
            NotifyEvent::Suspended {
                asset_name: event.asset_name,
                reason: event.reason,
            },
        );
    }
}

/// Runs the reconciliation loop for one connected component.
pub(super) async fn run_controller(
    input: ControllerInput,
    backend: BackendStores,
    notifier: Option<Arc<dyn Notifier>>,
    log_store: Option<LogStore>,
    lock_config: reconciler::LockConfig,
    concurrency: ConcurrencyLimits,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServeError> {
    let ControllerInput {
        assets,
        edges,
        cache_dir,
    } = input;
    let BackendStores {
        sync_lock,
        suspended_store,
        readiness_store,
    } = backend;
    let mut state = ServeState::new(&edges, suspended_store);
    let mut evaluate_tasks: JoinSet<(String, reconciler::EvaluateOutcome)> = JoinSet::new();
    let mut sync_tasks: JoinSet<(
        String,
        Result<crate::runtime::sync::SyncExecutionResult, SyncError>,
    )> = JoinSet::new();

    // Restore persisted readiness from the previous run.
    match readiness_store.read_all() {
        Ok(persisted) => {
            if !persisted.is_empty() {
                let ready_count = persisted.values().filter(|&&r| r).count();
                state.restore_readiness(persisted);
                tracing::info!(count = ready_count, "restored readiness from previous run");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to restore readiness, starting fresh");
        }
    }

    state.register_assets(&assets);

    let yaml_map: HashMap<&str, &str> = assets
        .iter()
        .map(|a| (a.name.as_str(), a.yaml.as_str()))
        .collect();

    // Build per-connection semaphores for concurrency control.
    let conn_semaphores = build_connection_semaphores(&yaml_map);

    loop {
        spawn_evaluates(
            &mut state,
            &mut evaluate_tasks,
            &yaml_map,
            &cache_dir,
            concurrency.max_evaluate,
            &conn_semaphores,
        );
        let sync_ctx = SyncSpawnContext {
            yaml_map: &yaml_map,
            sync_lock: &sync_lock,
            lock_config,
            notifier: &notifier,
            max_sync: concurrency.max_sync,
            conn_semaphores: &conn_semaphores,
        };
        spawn_syncs(&mut state, &mut sync_tasks, &sync_ctx);

        let sleep_until = state.scheduler.next_due().map(|(_, instant)| instant);

        tokio::select! {
            _ = async {
                match sleep_until {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            } => {
                state.enqueue_due();
            }

            join_result = evaluate_tasks.join_next(), if !evaluate_tasks.is_empty() => {
                handle_evaluate_completion(&mut state, join_result, &log_store, &notifier);
            }

            result = sync_tasks.join_next(), if !sync_tasks.is_empty() => {
                handle_sync_completion(&mut state, result, &log_store, &notifier);
            }

            _ = shutdown.changed() => {
                tracing::info!("shutting down controller");
                break;
            }
        }
    }

    drop(evaluate_tasks);
    drain_sync_tasks(&mut sync_tasks).await;

    // Persist readiness for the next startup.
    if let Err(e) = readiness_store.write_all(&state.readiness.ready) {
        tracing::warn!(error = %e, "failed to persist readiness");
    }

    Ok(())
}

/// Input for one Controller: assets with their YAML and intervals, plus edges.
#[derive(Debug)]
pub(super) struct ControllerInput {
    pub(super) assets: Vec<AssetEntry>,
    pub(super) edges: Vec<GraphEdge>,
    pub(super) cache_dir: Option<PathBuf>,
}

/// Asset name → connection semaphore mapping.
/// Assets sharing the same connection share the same semaphore.
type ConnectionSemaphores = HashMap<String, Arc<tokio::sync::Semaphore>>;

/// Parses compiled asset YAMLs and builds per-connection semaphores.
/// Connections with `max_concurrency` limits get a bounded semaphore.
/// Assets without a connection or with unlimited connections get no entry.
fn build_connection_semaphores(yaml_map: &HashMap<&str, &str>) -> ConnectionSemaphores {
    use crate::runtime::kind::connection::ResolvedConnection;

    let mut conn_limits: HashMap<String, usize> = HashMap::new();
    let mut asset_to_conn: HashMap<String, String> = HashMap::new();

    for (&asset_name, &yaml) in yaml_map {
        if let Ok(compiled) = serde_yaml::from_str::<CompiledAsset>(yaml) {
            if let Some(ref conn) = compiled.connection {
                let conn_name = conn.name().to_string();
                // Determine concurrency limit from the connection type.
                // For dbt connections, resolve the adapter to check if
                // the underlying database has concurrency constraints.
                let limit = match conn {
                    ResolvedConnection::DuckDb { .. } => Some(1),
                    ResolvedConnection::Dbt { .. } => {
                        conn.connect().ok().and_then(|c| c.max_concurrency())
                    }
                    _ => None,
                };
                if let Some(n) = limit {
                    conn_limits.entry(conn_name.clone()).or_insert(n);
                }
                asset_to_conn.insert(asset_name.to_string(), conn_name);
            }
        }
    }

    let semaphores: HashMap<String, Arc<tokio::sync::Semaphore>> = conn_limits
        .into_iter()
        .map(|(name, limit)| (name, Arc::new(tokio::sync::Semaphore::new(limit))))
        .collect();

    let mut result = HashMap::new();
    for (asset_name, conn_name) in asset_to_conn {
        if let Some(sem) = semaphores.get(&conn_name) {
            result.insert(asset_name, Arc::clone(sem));
        }
    }
    result
}

/// Builds per-component [`ControllerInput`]s from the graph and compiled assets.
pub(super) fn build_controller_inputs(
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
                        tracing::warn!(asset = %name, error = %e, "skipping asset");
                        return None;
                    }
                };
                let min_interval = compute_min_interval(&compiled.spec.on_drift);
                let first_on_drift = compiled.spec.on_drift.first();
                Some(AssetEntry {
                    name: name.clone(),
                    yaml: yaml.clone(),
                    min_interval,
                    auto_sync: compiled.spec.auto_sync,
                    has_sync: first_on_drift.is_some(),
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

        inputs.push(ControllerInput {
            assets,
            edges,
            cache_dir: None,
        });
    }

    Ok(inputs)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::compile::GraphNode;
    use crate::runtime::notify::NotifyError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn asset_node(name: &str) -> GraphNode {
        GraphNode {
            name: name.to_string(),
            kind: "Asset".to_string(),
            tags: vec![],
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
    impl crate::runtime::notify::Notifier for MockNotifier {
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
            nodes: vec![asset_node("a1"), asset_node("a2")],
            edges: vec![],
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
        use crate::runtime::sync::{SyncExecutionResult, SyncType};
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
            let err = SyncError::Io(std::io::Error::other("test error"));
            ("a".to_string(), Err(err))
        });

        // Should not panic.
        drain_sync_tasks(&mut tasks).await;
        assert!(tasks.is_empty());
    }

    // ── process_evaluate_outcome tests ────────────────────────────────────────

    fn sample_evaluate_result(
        name: &str,
        ready: bool,
    ) -> crate::runtime::evaluate::AssetEvalResult {
        crate::runtime::evaluate::AssetEvalResult {
            asset_name: name.to_string(),
            ready,
            conditions: vec![],
            evaluation_id: None,
        }
    }

    fn sample_outcome_ok(name: &str) -> reconciler::EvaluateOutcome {
        reconciler::EvaluateOutcome {
            result: Ok(sample_evaluate_result(name, true)),
            started_at: "2025-01-01T00:00:00Z".to_string(),
            finished_at: "2025-01-01T00:00:01Z".to_string(),
        }
    }

    fn sample_outcome_err() -> reconciler::EvaluateOutcome {
        reconciler::EvaluateOutcome {
            result: Err(EvaluateError::Parse("test".to_string())),
            started_at: "2025-01-01T00:00:00Z".to_string(),
            finished_at: "2025-01-01T00:00:01Z".to_string(),
        }
    }

    #[test]
    fn process_evaluate_outcome_ok_returns_result_no_event() {
        let (name_and_result, event) =
            process_evaluate_outcome("a".to_string(), sample_outcome_ok("a"), &None);
        assert_eq!(name_and_result.0, "a");
        assert!(name_and_result.1.is_ok());
        assert!(event.is_none());
    }

    #[test]
    fn process_evaluate_outcome_err_returns_eval_failed_event() {
        let (name_and_result, event) =
            process_evaluate_outcome("a".to_string(), sample_outcome_err(), &None);
        assert_eq!(name_and_result.0, "a");
        assert!(name_and_result.1.is_err());
        assert!(matches!(event, Some(NotifyEvent::EvaluateFailed { .. })));
    }

    #[test]
    fn process_evaluate_outcome_writes_log_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();

        let (name_and_result, _) =
            process_evaluate_outcome("a".to_string(), sample_outcome_ok("a"), &Some(store));
        assert!(name_and_result.1.is_ok());
        // Verify a log was written by checking the db is non-empty.
        assert!(db_path.metadata().unwrap().len() > 0);
    }

    #[test]
    fn process_evaluate_outcome_skips_log_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();
        let initial_size = db_path.metadata().unwrap().len();

        let (_name_and_result, event) =
            process_evaluate_outcome("a".to_string(), sample_outcome_err(), &Some(store));
        assert!(event.is_some());
        // DB size unchanged (no evaluate log written on error).
        assert_eq!(db_path.metadata().unwrap().len(), initial_size);
    }

    // ── log_sync_result tests ─────────────────────────────────────────────

    #[test]
    fn log_sync_result_writes_to_store() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        let store = LogStore::open(&db_path, &logs_dir).unwrap();

        let result = crate::runtime::sync::SyncExecutionResult {
            execution_id: "exec-1".to_string(),
            asset_name: "a".to_string(),
            sync_type: crate::runtime::sync::SyncType::Sync,
            stages: vec![crate::runtime::sync::StageResult {
                stage: crate::runtime::sync::Stage::Run,
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

    // ── handle_evaluate_completion tests ──────────────────────────────────────

    fn make_state() -> ServeState {
        use crate::runtime::storage::StorageError;

        #[derive(Debug, Default)]
        struct MemSuspendedStore {
            inner: std::sync::Mutex<
                std::collections::HashMap<String, crate::runtime::serve::suspended::SuspendedInfo>,
            >,
        }

        impl crate::runtime::storage::SuspendedStore for MemSuspendedStore {
            fn write(
                &self,
                info: &crate::runtime::serve::suspended::SuspendedInfo,
            ) -> Result<(), StorageError> {
                self.inner
                    .lock()
                    .unwrap()
                    .insert(info.asset_name.clone(), info.clone());
                Ok(())
            }
            fn read(
                &self,
                name: &str,
            ) -> Result<Option<crate::runtime::serve::suspended::SuspendedInfo>, StorageError>
            {
                Ok(self.inner.lock().unwrap().get(name).cloned())
            }
            fn remove(&self, name: &str) -> Result<(), StorageError> {
                self.inner.lock().unwrap().remove(name);
                Ok(())
            }
            fn exists(&self, name: &str) -> Result<bool, StorageError> {
                Ok(self.inner.lock().unwrap().contains_key(name))
            }
        }

        let store: Arc<dyn crate::runtime::storage::SuspendedStore> =
            Arc::new(MemSuspendedStore::default());
        ServeState::new(&[], store)
    }

    #[test]
    fn handle_evaluate_completion_updates_state_on_success() {
        let mut state = make_state();
        state.register_assets(&[AssetEntry {
            name: "a".to_string(),
            yaml: String::new(),
            min_interval: None,
            auto_sync: false,
            has_sync: false,
        }]);
        state.in_flight.insert("a".to_string());

        let evaluate_result = Ok(sample_evaluate_result("a", true));
        let join_result: crate::runtime::serve::state::EvaluateJoinResult =
            Some(Ok(("a".to_string(), evaluate_result)));
        state.on_evaluate_complete(join_result);

        assert!(!state.in_flight.contains("a"));
        assert_eq!(state.readiness.ready.get("a"), Some(&true));
    }

    #[test]
    fn handle_sync_completion_clears_syncing_slot() {
        let mut state = make_state();
        state.register_assets(&[AssetEntry {
            name: "a".to_string(),
            yaml: String::new(),
            min_interval: None,
            auto_sync: true,
            has_sync: true,
        }]);
        state.syncing.insert("a".to_string());

        let sync_result = crate::runtime::sync::SyncExecutionResult {
            execution_id: "exec-1".to_string(),
            asset_name: "a".to_string(),
            sync_type: crate::runtime::sync::SyncType::Sync,
            stages: vec![],
            success: true,
        };
        let join_result = Some(Ok(("a".to_string(), Ok(sync_result))));
        state.on_sync_complete(join_result);

        assert!(!state.syncing.contains("a"));
        // Re-evaluate enqueued after sync completion.
        assert_eq!(state.evaluate_queue.dequeue(), Some("a".to_string()));
    }
}

//! Reconciliation loop for continuous evaluation.
//!
//! Architecture (inspired by k8s controller-runtime):
//!
//! - **Controller** ([`run_controller`]) — one per connected component of the
//!   dependency graph.  Runs a `tokio::select!` loop that reacts to three
//!   events: timer fire, task completion, and shutdown signal.
//! - **ServeState** — all mutable in-memory state lives here.  Sub-states:
//!   [`WorkQueue`], [`SchedulerState`], [`ReadinessState`], plus `in_flight`
//!   tracking and the downstream propagation map.
//! - **Reconciler** ([`evaluate_and_cache`]) — stateless async function that
//!   evaluates a single asset and writes the result to the local cache.
//!
//! The top-level [`serve`] function loads compiled assets, partitions them into
//! connected components, and spawns one Controller per component.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::compile::{CompiledAsset, DependencyGraph, GraphEdge};
use crate::evaluate::{AssetEvalResult, EvaluateError};
use crate::kind::asset::DesiredSetEntry;
use crate::storage::local::LocalCache;
use crate::storage::Cache;

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("compile error: {0}")]
    Compile(#[from] crate::compile::CompileError),
    #[error("evaluate error: {0}")]
    Evaluate(#[from] EvaluateError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse error: {0}")]
    Parse(String),
}

// ── Connected Components ─────────────────────────────────────────────────────

/// Detects connected components in the dependency graph using Union-Find.
/// Returns groups of Asset names (Source nodes are excluded from output).
pub fn connected_components(graph: &DependencyGraph) -> Vec<Vec<String>> {
    let mut name_to_id: HashMap<&str, usize> = HashMap::new();
    for (i, node) in graph.nodes.iter().enumerate() {
        name_to_id.insert(&node.name, i);
    }

    let n = graph.nodes.len();
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }

    fn union(parent: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent[rb] = ra;
        }
    }

    for edge in &graph.edges {
        if let (Some(&a), Some(&b)) = (
            name_to_id.get(edge.from.as_str()),
            name_to_id.get(edge.to.as_str()),
        ) {
            union(&mut parent, a, b);
        }
    }

    let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, node) in graph.nodes.iter().enumerate() {
        if node.kind == "Asset" {
            let root = find(&mut parent, i);
            groups.entry(root).or_default().push(node.name.clone());
        }
    }

    let mut result: Vec<Vec<String>> = groups.into_values().collect();
    result.sort_by(|a, b| a[0].cmp(&b[0]));
    for group in &mut result {
        group.sort();
    }
    result
}

// ── AssetEntry ───────────────────────────────────────────────────────────────

/// A compiled asset prepared for the Controller: name, raw YAML, and
/// the shortest evaluation interval derived from its conditions.
#[derive(Debug, Clone)]
struct AssetEntry {
    name: String,
    yaml: String,
    min_interval: Option<StdDuration>,
}

// ── WorkQueue ────────────────────────────────────────────────────────────────

/// FIFO queue with deduplication. An asset that is already queued will not be
/// added again until it is dequeued.
#[derive(Debug)]
struct WorkQueue {
    queue: VecDeque<String>,
    pending: HashSet<String>,
}

impl WorkQueue {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pending: HashSet::new(),
        }
    }

    /// Enqueues an asset. Returns false if already queued.
    fn enqueue(&mut self, name: String) -> bool {
        if self.pending.contains(&name) {
            return false;
        }
        self.pending.insert(name.clone());
        self.queue.push_back(name);
        true
    }

    fn dequeue(&mut self) -> Option<String> {
        let name = self.queue.pop_front()?;
        self.pending.remove(&name);
        Some(name)
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

// ── SchedulerState ───────────────────────────────────────────────────────────

/// Tracks per-asset evaluation intervals and computes the next due time.
#[derive(Debug)]
struct SchedulerState {
    intervals: HashMap<String, StdDuration>,
    next_eval_at: HashMap<String, Instant>,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            intervals: HashMap::new(),
            next_eval_at: HashMap::new(),
        }
    }

    fn register(&mut self, asset_name: String, interval: StdDuration) {
        self.next_eval_at
            .insert(asset_name.clone(), Instant::now() + interval);
        self.intervals.insert(asset_name, interval);
    }

    /// Returns the asset due soonest and its scheduled time, or None.
    fn next_due(&self) -> Option<(&str, Instant)> {
        self.next_eval_at
            .iter()
            .min_by_key(|(_, instant)| *instant)
            .map(|(name, instant)| (name.as_str(), *instant))
    }

    /// Resets the timer for an asset to `now + interval`.
    fn reschedule(&mut self, asset_name: &str) {
        if let Some(interval) = self.intervals.get(asset_name) {
            self.next_eval_at
                .insert(asset_name.to_string(), Instant::now() + *interval);
        }
    }
}

// ── ReadinessState ────────────────────────────────────────────────────────────

/// Tracks the Ready / Not Ready state of each asset.
/// Detects the Not Ready → Ready transition, which triggers downstream
/// propagation.
#[derive(Debug)]
struct ReadinessState {
    ready: HashMap<String, bool>,
}

impl ReadinessState {
    fn new() -> Self {
        Self {
            ready: HashMap::new(),
        }
    }

    /// Records the latest readiness. Returns `true` only when the asset
    /// transitions from Not Ready to Ready (i.e. became_ready).
    fn record(&mut self, asset_name: &str, ready: bool) -> bool {
        let was_ready = self.ready.get(asset_name).copied().unwrap_or(false);
        self.ready.insert(asset_name.to_string(), ready);
        !was_ready && ready
    }
}

// ── ServeState ───────────────────────────────────────────────────────────────

/// All mutable in-memory state for one Controller.
///
/// Sub-states are intentionally kept as flat fields rather than nested behind
/// traits so that each method can be unit-tested with plain assertions.
#[derive(Debug)]
struct ServeState {
    scheduler: SchedulerState,
    work_queue: WorkQueue,
    readiness: ReadinessState,
    /// Assets currently being evaluated in the JoinSet.
    in_flight: HashSet<String>,
    /// asset name → list of downstream asset names.
    downstream_map: HashMap<String, Vec<String>>,
}

impl ServeState {
    fn new(edges: &[GraphEdge]) -> Self {
        Self {
            scheduler: SchedulerState::new(),
            work_queue: WorkQueue::new(),
            readiness: ReadinessState::new(),
            in_flight: HashSet::new(),
            downstream_map: build_downstream_map(edges),
        }
    }

    /// Registers all assets: enqueue for initial evaluation + register intervals.
    fn init(&mut self, assets: &[AssetEntry]) {
        for asset in assets {
            self.work_queue.enqueue(asset.name.clone());
            if let Some(dur) = asset.min_interval {
                self.scheduler.register(asset.name.clone(), dur);
            }
        }
    }

    /// Enqueues the next due asset (if any) when its timer fires.
    fn enqueue_due(&mut self) {
        if let Some((name, _)) = self.scheduler.next_due() {
            let name = name.to_string();
            if !self.in_flight.contains(&name) {
                self.work_queue.enqueue(name);
            }
        }
    }

    /// Dequeues the next asset to spawn, skipping those already in flight.
    fn next_spawnable(&mut self) -> Option<String> {
        while let Some(name) = self.work_queue.dequeue() {
            if !self.in_flight.contains(&name) {
                self.in_flight.insert(name.clone());
                return Some(name);
            }
        }
        None
    }

    /// Processes an evaluation result: updates scheduler, readiness, and
    /// in-flight tracking.  Returns names of downstream assets that were
    /// enqueued due to a Not Ready → Ready transition.
    fn handle_eval_result(
        &mut self,
        asset_name: &str,
        result: &Result<AssetEvalResult, EvaluateError>,
    ) -> Vec<String> {
        self.in_flight.remove(asset_name);
        self.scheduler.reschedule(asset_name);

        let ready = match result {
            Ok(r) => r.ready,
            Err(_) => false,
        };

        let mut propagated = Vec::new();
        if self.readiness.record(asset_name, ready) {
            if let Some(downstreams) = self.downstream_map.get(asset_name) {
                for ds in downstreams {
                    if !self.in_flight.contains(ds) && self.work_queue.enqueue(ds.clone()) {
                        propagated.push(ds.clone());
                    }
                }
            }
        }
        propagated
    }
}

// ── Reconciler ───────────────────────────────────────────────────────────────

/// Evaluates a single compiled asset and writes the result to the local cache.
///
/// This is the "stateless reconciler": it takes all inputs by value so the
/// returned future is `Send` and can be spawned on a `JoinSet`.
/// (`evaluate_from_compiled` cannot be used here because `LogStore` is `!Send`.)
async fn evaluate_and_cache(
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
async fn spawn_evaluate(
    asset_name: String,
    yaml: String,
    cache_dir: Option<PathBuf>,
) -> (String, Result<AssetEvalResult, EvaluateError>) {
    let result = evaluate_and_cache(&yaml, cache_dir.as_deref()).await;
    (asset_name, result)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn build_downstream_map(edges: &[GraphEdge]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges {
        map.entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
    }
    map
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
/// The loop reacts to three events via `tokio::select!`:
/// 1. **Timer** — a scheduled asset becomes due → enqueue it.
/// 2. **Task completion** — a spawned evaluation finishes → update state,
///    propagate to downstream assets if newly Ready.
/// 3. **Shutdown** — break and return.
async fn run_controller(
    assets: Vec<AssetEntry>,
    edges: Vec<GraphEdge>,
    cache_dir: Option<PathBuf>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServeError> {
    let mut state = ServeState::new(&edges);
    let mut tasks: JoinSet<(String, Result<AssetEvalResult, EvaluateError>)> = JoinSet::new();

    state.init(&assets);

    let yaml_map: HashMap<&str, &str> = assets
        .iter()
        .map(|a| (a.name.as_str(), a.yaml.as_str()))
        .collect();

    loop {
        while let Some(name) = state.next_spawnable() {
            if let Some(&yaml) = yaml_map.get(name.as_str()) {
                tasks.spawn(spawn_evaluate(name, yaml.to_string(), cache_dir.clone()));
            }
        }

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

            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Ok((asset_name, eval_result))) = result {
                    match &eval_result {
                        Ok(r) => eprintln!("[serve] evaluated {}: ready={}", r.asset_name, r.ready),
                        Err(e) => eprintln!("[serve] evaluation failed for {asset_name}: {e}"),
                    }
                    let propagated = state.handle_eval_result(&asset_name, &eval_result);
                    for ds in &propagated {
                        eprintln!("[serve] propagating to downstream: {ds}");
                    }
                }
            }

            _ = shutdown.changed() => {
                eprintln!("[serve] shutting down controller");
                break;
            }
        }
    }

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
        let component_set: HashSet<&str> = component.iter().map(|s| s.as_str()).collect();

        let assets: Vec<_> = component
            .iter()
            .filter_map(|name| {
                let yaml = asset_map.get(name)?;
                let compiled: CompiledAsset = serde_yaml::from_str(yaml)
                    .map_err(|e| ServeError::Parse(e.to_string()))
                    .ok()?;
                let min_interval = compute_min_interval(&compiled);
                Some(AssetEntry {
                    name: name.clone(),
                    yaml: yaml.clone(),
                    min_interval,
                })
            })
            .collect();

        if assets.is_empty() {
            continue;
        }

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
/// 1. Loads compiled assets and the dependency graph from `target_dir`.
/// 2. Partitions assets into connected components.
/// 3. Spawns one [`run_controller`] per component.
/// 4. Waits for Ctrl-C, then signals all Controllers to shut down.
pub async fn serve(
    target_dir: &Path,
    selectors: &[&str],
    cache_dir: Option<&Path>,
) -> Result<(), ServeError> {
    let assets = crate::compile::load_compiled_assets(target_dir, selectors)?;

    let graph_path = target_dir.join("graph.json");
    let graph_json = std::fs::read_to_string(&graph_path)?;
    let graph: DependencyGraph =
        serde_json::from_str(&graph_json).map_err(|e| ServeError::Parse(e.to_string()))?;

    let asset_map: HashMap<String, String> = assets.into_iter().collect();
    let inputs = build_controller_inputs(&graph, &asset_map)?;

    let (shutdown_tx, _) = watch::channel(false);

    let mut handles = Vec::new();
    for input in inputs {
        let rx = shutdown_tx.subscribe();
        let cd = cache_dir.map(PathBuf::from);
        handles.push(tokio::spawn(run_controller(
            input.assets,
            input.edges,
            cd,
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

    for h in handles {
        if let Err(e) = h.await {
            eprintln!("[serve] controller error: {e}");
        }
    }

    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{GraphEdge, GraphNode};

    // ── connected_components tests ───────────────────────────────────────

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

    macro_rules! connected_components_test {
        ($($name:ident: $graph:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let result = connected_components(&$graph);
                    assert_eq!(result, $expected);
                }
            )*
        };
    }

    connected_components_test! {
        single_asset_no_edges: DependencyGraph {
            nodes: vec![asset_node("a")],
            edges: vec![],
        } => vec![vec!["a".to_string()]];

        two_independent_assets: DependencyGraph {
            nodes: vec![asset_node("a"), asset_node("b")],
            edges: vec![],
        } => vec![vec!["a".to_string()], vec!["b".to_string()]];

        chain_via_source: DependencyGraph {
            nodes: vec![source_node("s"), asset_node("a"), asset_node("b")],
            edges: vec![edge("s", "a"), edge("s", "b")],
        } => vec![vec!["a".to_string(), "b".to_string()]];

        two_separate_chains: DependencyGraph {
            nodes: vec![
                source_node("s1"), asset_node("a1"),
                source_node("s2"), asset_node("a2"),
            ],
            edges: vec![edge("s1", "a1"), edge("s2", "a2")],
        } => vec![vec!["a1".to_string()], vec!["a2".to_string()]];

        three_assets_one_component: DependencyGraph {
            nodes: vec![
                source_node("raw"), asset_node("daily"), asset_node("monthly"), asset_node("raw-asset"),
            ],
            edges: vec![edge("raw", "daily"), edge("raw", "monthly"), edge("raw", "raw-asset")],
        } => vec![vec!["daily".to_string(), "monthly".to_string(), "raw-asset".to_string()]];

        empty_graph: DependencyGraph {
            nodes: vec![],
            edges: vec![],
        } => Vec::<Vec<String>>::new();
    }

    // ── WorkQueue tests ─────────────────────────────────────────────────

    #[test]
    fn work_queue_enqueue_dequeue() {
        let mut q = WorkQueue::new();
        assert!(q.is_empty());

        assert!(q.enqueue("a".to_string()));
        assert!(q.enqueue("b".to_string()));
        assert!(!q.is_empty());

        assert_eq!(q.dequeue(), Some("a".to_string()));
        assert_eq!(q.dequeue(), Some("b".to_string()));
        assert_eq!(q.dequeue(), None);
        assert!(q.is_empty());
    }

    #[test]
    fn work_queue_dedup() {
        let mut q = WorkQueue::new();
        assert!(q.enqueue("a".to_string()));
        assert!(!q.enqueue("a".to_string())); // duplicate rejected
        assert_eq!(q.dequeue(), Some("a".to_string()));

        // After dequeue, can enqueue again
        assert!(q.enqueue("a".to_string()));
        assert_eq!(q.dequeue(), Some("a".to_string()));
    }

    // ── SchedulerState tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn scheduler_register_and_next_due() {
        tokio::time::pause();

        let mut s = SchedulerState::new();
        assert!(s.next_due().is_none());

        s.register("a".to_string(), StdDuration::from_secs(60));
        s.register("b".to_string(), StdDuration::from_secs(30));

        // "b" is due sooner (30s vs 60s)
        let (name, _) = s.next_due().unwrap();
        assert_eq!(name, "b");
    }

    #[tokio::test]
    async fn scheduler_reschedule_resets_timer() {
        tokio::time::pause();

        let mut s = SchedulerState::new();
        s.register("a".to_string(), StdDuration::from_secs(60));

        let (_, first_due) = s.next_due().unwrap();

        // Advance time by 60s so "a" is due
        tokio::time::advance(StdDuration::from_secs(60)).await;

        s.reschedule("a");
        let (_, second_due) = s.next_due().unwrap();

        // After reschedule, next_due should be ~60s from now (later than first)
        assert!(second_due > first_due);
    }

    // ── ReadinessState tests ────────────────────────────────────────────

    #[test]
    fn readiness_initial_not_ready_to_ready() {
        let mut r = ReadinessState::new();
        assert!(r.record("a", true));
    }

    #[test]
    fn readiness_stays_ready_no_transition() {
        let mut r = ReadinessState::new();
        r.record("a", true);
        assert!(!r.record("a", true));
    }

    #[test]
    fn readiness_not_ready_no_transition() {
        let mut r = ReadinessState::new();
        assert!(!r.record("a", false));
    }

    #[test]
    fn readiness_ready_to_not_ready_to_ready() {
        let mut r = ReadinessState::new();
        assert!(r.record("a", true));
        assert!(!r.record("a", false));
        assert!(r.record("a", true));
    }

    // ── build_downstream_map tests ──────────────────────────────────────

    #[test]
    fn downstream_map_basic() {
        let edges = vec![edge("a", "b"), edge("a", "c"), edge("b", "c")];
        let map = build_downstream_map(&edges);
        assert_eq!(
            map.get("a").unwrap(),
            &vec!["b".to_string(), "c".to_string()]
        );
        assert_eq!(map.get("b").unwrap(), &vec!["c".to_string()]);
        assert!(map.get("c").is_none());
    }

    #[test]
    fn downstream_map_empty() {
        let map = build_downstream_map(&[]);
        assert!(map.is_empty());
    }

    // ── ServeState tests ────────────────────────────────────────────────

    fn asset_entry(name: &str, interval: Option<StdDuration>) -> AssetEntry {
        AssetEntry {
            name: name.to_string(),
            yaml: String::new(),
            min_interval: interval,
        }
    }

    fn eval_ok(name: &str, ready: bool) -> Result<AssetEvalResult, EvaluateError> {
        Ok(AssetEvalResult {
            asset_name: name.to_string(),
            ready,
            conditions: vec![],
            evaluation_id: None,
        })
    }

    #[test]
    fn serve_state_init_enqueues_and_registers() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges);
        let assets = vec![
            asset_entry("a", Some(StdDuration::from_secs(60))),
            asset_entry("b", None),
        ];
        state.init(&assets);

        assert_eq!(state.work_queue.dequeue(), Some("a".to_string()));
        assert_eq!(state.work_queue.dequeue(), Some("b".to_string()));
        assert_eq!(state.work_queue.dequeue(), None);
        assert!(state.scheduler.intervals.contains_key("a"));
        assert!(!state.scheduler.intervals.contains_key("b"));
    }

    #[test]
    fn next_spawnable_skips_in_flight() {
        let mut state = ServeState::new(&[]);
        state.work_queue.enqueue("a".to_string());
        state.work_queue.enqueue("b".to_string());
        state.in_flight.insert("a".to_string());

        assert_eq!(state.next_spawnable(), Some("b".to_string()));
        assert_eq!(state.next_spawnable(), None);
        assert!(state.in_flight.contains("b"));
    }

    #[test]
    fn handle_eval_result_success_propagates_downstream() {
        let edges = vec![edge("a", "b"), edge("a", "c")];
        let mut state = ServeState::new(&edges);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", true);
        let propagated = state.handle_eval_result("a", &result);

        assert_eq!(propagated, vec!["b".to_string(), "c".to_string()]);
        assert!(!state.in_flight.contains("a"));
    }

    #[test]
    fn handle_eval_result_error_marks_not_ready() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges);
        state.in_flight.insert("a".to_string());

        // First make "a" ready
        state.handle_eval_result("a", &eval_ok("a", true));

        // Now error — readiness becomes false, no propagation
        state.in_flight.insert("a".to_string());
        let err: Result<AssetEvalResult, EvaluateError> =
            Err(EvaluateError::Parse("test error".to_string()));
        let propagated = state.handle_eval_result("a", &err);

        assert!(propagated.is_empty());
        assert!(!state.readiness.ready.get("a").copied().unwrap_or(false));
    }

    #[test]
    fn handle_eval_result_no_propagation_when_stays_ready() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", true);
        state.handle_eval_result("a", &result);

        // Second evaluation still ready — no propagation
        state.in_flight.insert("a".to_string());
        let propagated = state.handle_eval_result("a", &result);
        assert!(propagated.is_empty());
    }

    #[test]
    fn handle_eval_result_skips_in_flight_downstream() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges);
        state.in_flight.insert("a".to_string());
        state.in_flight.insert("b".to_string()); // b already running

        let result = eval_ok("a", true);
        let propagated = state.handle_eval_result("a", &result);

        assert!(propagated.is_empty());
    }

    // ── build_controller_inputs tests ───────────────────────────────────

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
}

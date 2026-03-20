use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::compile::{CompiledAsset, DependencyGraph};
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

// ── QueueState ───────────────────────────────────────────────────────────────

#[derive(Debug)]
struct QueueState {
    queue: VecDeque<String>,
    in_queue: HashSet<String>,
}

impl QueueState {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            in_queue: HashSet::new(),
        }
    }

    /// Enqueues an asset. Returns false if already in queue.
    fn enqueue(&mut self, name: String) -> bool {
        if self.in_queue.contains(&name) {
            return false;
        }
        self.in_queue.insert(name.clone());
        self.queue.push_back(name);
        true
    }

    fn dequeue(&mut self) -> Option<String> {
        let name = self.queue.pop_front()?;
        self.in_queue.remove(&name);
        Some(name)
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

// ── SchedulerState ───────────────────────────────────────────────────────────

#[derive(Debug)]
struct SchedulerState {
    intervals: HashMap<String, StdDuration>,
    next_eval: HashMap<String, Instant>,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            intervals: HashMap::new(),
            next_eval: HashMap::new(),
        }
    }

    fn register(&mut self, asset_name: String, interval: StdDuration) {
        self.next_eval
            .insert(asset_name.clone(), Instant::now() + interval);
        self.intervals.insert(asset_name, interval);
    }

    /// Returns the asset due soonest and its scheduled time, or None.
    fn next_due(&self) -> Option<(&str, Instant)> {
        self.next_eval
            .iter()
            .min_by_key(|(_, instant)| *instant)
            .map(|(name, instant)| (name.as_str(), *instant))
    }

    fn mark_evaluated(&mut self, asset_name: &str) {
        if let Some(interval) = self.intervals.get(asset_name) {
            self.next_eval
                .insert(asset_name.to_string(), Instant::now() + *interval);
        }
    }
}

// ── ServeState ───────────────────────────────────────────────────────────────

#[derive(Debug)]
struct ServeState {
    scheduler: SchedulerState,
    queue: QueueState,
}

impl ServeState {
    fn new() -> Self {
        Self {
            scheduler: SchedulerState::new(),
            queue: QueueState::new(),
        }
    }
}

// ── Reconciler ───────────────────────────────────────────────────────────────

/// Stateless reconciler: evaluates an asset and writes cache.
/// Avoids `evaluate_from_compiled` because `LogStore` is `!Send`.
async fn reconcile_evaluate(
    asset_name: String,
    yaml: String,
    cache_dir: Option<PathBuf>,
) -> (String, Result<AssetEvalResult, EvaluateError>) {
    let result = reconcile_evaluate_inner(&yaml, cache_dir.as_deref()).await;
    (asset_name, result)
}

async fn reconcile_evaluate_inner(
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
    // Use evaluate_asset_no_log to produce a Send future (LogStore is !Send).
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

// ── Controller ───────────────────────────────────────────────────────────────

/// Runs the reconciliation loop for one connected component.
pub async fn run_controller(
    assets: Vec<(String, String, Option<StdDuration>)>,
    cache_dir: Option<PathBuf>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServeError> {
    let mut state = ServeState::new();
    let mut tasks: JoinSet<(String, Result<AssetEvalResult, EvaluateError>)> = JoinSet::new();

    // Initial setup: enqueue all assets + register intervals
    for (name, _yaml, interval) in &assets {
        state.queue.enqueue(name.clone());
        if let Some(dur) = interval {
            state.scheduler.register(name.clone(), *dur);
        }
    }

    let asset_map: HashMap<String, String> = assets
        .into_iter()
        .map(|(name, yaml, _)| (name, yaml))
        .collect();

    loop {
        // Drain queue into JoinSet
        while let Some(asset_name) = state.queue.dequeue() {
            if let Some(yaml) = asset_map.get(&asset_name) {
                let yaml = yaml.clone();
                let cd = cache_dir.clone();
                let name = asset_name.clone();
                tasks.spawn(reconcile_evaluate(name, yaml, cd));
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
                // Timer fired: enqueue the due asset
                if let Some((name, _)) = state.scheduler.next_due() {
                    let name = name.to_string();
                    state.queue.enqueue(name);
                }
            }

            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Ok((asset_name, eval_result))) = result {
                    match eval_result {
                        Ok(r) => {
                            eprintln!("[serve] evaluated {}: ready={}", r.asset_name, r.ready);
                            state.scheduler.mark_evaluated(&asset_name);
                        }
                        Err(e) => {
                            eprintln!("[serve] evaluation failed for {asset_name}: {e}");
                        }
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

/// Computes the minimum interval across all conditions of a compiled asset.
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

/// Entry point for `nagi serve`.
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

    let components = connected_components(&graph);

    let asset_map: HashMap<String, String> = assets.into_iter().collect();

    let (shutdown_tx, _) = watch::channel(false);

    let mut handles = Vec::new();
    for component in components {
        let mut component_assets = Vec::new();
        for name in component {
            if let Some(yaml) = asset_map.get(&name) {
                let compiled: CompiledAsset =
                    serde_yaml::from_str(yaml).map_err(|e| ServeError::Parse(e.to_string()))?;
                let min_interval = compute_min_interval(&compiled);
                component_assets.push((name, yaml.clone(), min_interval));
            }
        }
        if component_assets.is_empty() {
            continue;
        }
        let rx = shutdown_tx.subscribe();
        let cd = cache_dir.map(PathBuf::from);
        handles.push(tokio::spawn(run_controller(component_assets, cd, rx)));
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

    // ── QueueState tests ─────────────────────────────────────────────────

    #[test]
    fn queue_enqueue_dequeue() {
        let mut q = QueueState::new();
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
    fn queue_dedup() {
        let mut q = QueueState::new();
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
    async fn scheduler_mark_evaluated_resets_timer() {
        tokio::time::pause();

        let mut s = SchedulerState::new();
        s.register("a".to_string(), StdDuration::from_secs(60));

        let (_, first_due) = s.next_due().unwrap();

        // Advance time by 60s so "a" is due
        tokio::time::advance(StdDuration::from_secs(60)).await;

        s.mark_evaluated("a");
        let (_, second_due) = s.next_due().unwrap();

        // After mark_evaluated, next_due should be ~60s from now (later than first)
        assert!(second_due > first_due);
    }
}

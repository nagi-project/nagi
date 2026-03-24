use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use crate::compile::GraphEdge;
use crate::evaluate::{AssetEvalResult, EvaluateError};
use crate::storage::SuspendedStore;
use crate::sync::SyncError;

use super::graph::build_edge_maps;
use super::guardrail::{GuardrailState, MAX_CONSECUTIVE_FAILURES};
use super::queue::WorkQueue;
use super::scheduler::SchedulerState;
use super::suspended::SuspendedInfo;

/// Emitted when an asset is suspended, allowing the controller to
/// trigger notifications without coupling ServeState to the notifier.
#[derive(Debug, Clone)]
pub struct SuspendedEvent {
    pub asset_name: String,
    pub reason: String,
}

// ── Type aliases for JoinSet results ─────────────────────────────────────────

pub type EvalJoinResult =
    Option<Result<(String, Result<AssetEvalResult, EvaluateError>), tokio::task::JoinError>>;
pub type SyncJoinResult = Option<
    Result<(String, Result<crate::sync::SyncExecutionResult, SyncError>), tokio::task::JoinError>,
>;

// ── ReadinessState ───────────────────────────────────────────────────────────

/// Tracks the Ready / Not Ready state of each asset.
/// Detects the Not Ready → Ready transition, which triggers downstream
/// propagation.
#[derive(Debug, Default)]
pub struct ReadinessState {
    pub ready: HashMap<String, bool>,
}

impl ReadinessState {
    pub fn new() -> Self {
        Self {
            ready: HashMap::new(),
        }
    }

    /// Records the latest readiness. Returns `true` only when the asset
    /// transitions from Not Ready to Ready (i.e. became_ready).
    pub fn record(&mut self, asset_name: &str, ready: bool) -> bool {
        let was_ready = self.ready.get(asset_name).copied().unwrap_or(false);
        self.ready.insert(asset_name.to_string(), ready);
        !was_ready && ready
    }
}

// ── AssetEntry / AssetSyncConfig ─────────────────────────────────────────────

/// A compiled asset prepared for the Controller: name, raw YAML, evaluation
/// interval, and sync configuration.
#[derive(Debug, Clone)]
pub struct AssetEntry {
    pub name: String,
    pub yaml: String,
    pub min_interval: Option<StdDuration>,
    /// When true and sync spec exists, the Controller will automatically
    /// trigger sync after a Not Ready evaluation.
    pub auto_sync: bool,
    /// Whether this asset has a sync spec defined.
    pub has_sync: bool,
    /// The sync ref name from the original asset spec.
    pub sync_ref_name: Option<String>,
}

/// Per-asset sync configuration derived from the compiled asset.
#[derive(Debug, Clone)]
struct AssetSyncConfig {
    auto_sync: bool,
    has_sync: bool,
    #[allow(dead_code)]
    sync_ref_name: Option<String>,
}

// ── ServeState ───────────────────────────────────────────────────────────────

/// All mutable in-memory state for one Controller.
///
/// Sub-states are intentionally kept as flat fields rather than nested behind
/// traits so that each method can be unit-tested with plain assertions.
#[derive(Debug)]
pub struct ServeState {
    pub scheduler: SchedulerState,
    pub work_queue: WorkQueue,
    pub readiness: ReadinessState,
    /// Assets currently being evaluated in the JoinSet.
    pub in_flight: HashSet<String>,
    /// asset name → list of downstream asset names.
    downstream_map: HashMap<String, Vec<String>>,
    /// asset name → list of upstream asset names.
    upstream_map: HashMap<String, Vec<String>>,
    /// Per-asset sync configuration.
    sync_configs: HashMap<String, AssetSyncConfig>,
    /// FIFO queue of assets waiting for sync execution.
    pub sync_queue: WorkQueue,
    /// Asset names currently being synced (for dedup and completion tracking).
    pub syncing: HashSet<String>,
    /// Tracks consecutive sync failures and exponential backoff.
    pub guardrail: GuardrailState,
    /// Store for suspended flag files.
    suspended_store: Arc<dyn SuspendedStore>,
    /// Ready condition count from the last evaluation, per asset.
    last_ready_count: HashMap<String, usize>,
    /// Assets awaiting post-sync re-evaluation for degradation detection.
    awaiting_post_sync_eval: HashSet<String>,
}

impl ServeState {
    pub fn new(edges: &[GraphEdge], suspended_store: Arc<dyn SuspendedStore>) -> Self {
        let edge_maps = build_edge_maps(edges);
        Self {
            scheduler: SchedulerState::new(),
            work_queue: WorkQueue::new(),
            readiness: ReadinessState::new(),
            in_flight: HashSet::new(),
            downstream_map: edge_maps.downstream,
            upstream_map: edge_maps.upstream,
            sync_configs: HashMap::new(),
            sync_queue: WorkQueue::new(),
            syncing: HashSet::new(),
            guardrail: GuardrailState::new(),
            suspended_store,
            last_ready_count: HashMap::new(),
            awaiting_post_sync_eval: HashSet::new(),
        }
    }

    /// Registers all assets: enqueue for initial evaluation + register intervals
    /// + store sync configuration.
    pub fn register_assets(&mut self, assets: &[AssetEntry]) {
        for asset in assets {
            if self.all_upstreams_ready(&asset.name) {
                self.work_queue.enqueue(asset.name.clone());
            }
            if let Some(dur) = asset.min_interval {
                self.scheduler.register(asset.name.clone(), dur);
            }
            self.sync_configs.insert(
                asset.name.clone(),
                AssetSyncConfig {
                    auto_sync: asset.auto_sync,
                    has_sync: asset.has_sync,
                    sync_ref_name: asset.sync_ref_name.clone(),
                },
            );
        }
    }

    /// Returns true if the asset is awaiting post-sync re-evaluation.
    /// Used to skip the condition TTL cache for re-evaluations after sync.
    pub fn is_awaiting_post_sync_eval(&self, asset_name: &str) -> bool {
        self.awaiting_post_sync_eval.contains(asset_name)
    }

    /// Returns true if all upstream assets are Ready (or there are no upstreams).
    fn all_upstreams_ready(&self, asset_name: &str) -> bool {
        let Some(upstreams) = self.upstream_map.get(asset_name) else {
            return true;
        };
        upstreams
            .iter()
            .all(|u| self.readiness.ready.get(u).copied().unwrap_or(false))
    }

    /// Enqueues the next due asset (if any) when its timer fires.
    /// Skips if any upstream is not Ready.
    pub fn enqueue_due(&mut self) {
        let Some((name, _)) = self.scheduler.next_due() else {
            return;
        };
        let name = name.to_string();
        if self.in_flight.contains(&name) {
            return;
        }
        if !self.all_upstreams_ready(&name) {
            tracing::debug!(asset = %name, "evaluate skipped: upstream not ready");
            return;
        }
        self.work_queue.enqueue(name);
    }

    /// Dequeues the next asset to spawn, skipping those already in flight.
    pub fn next_spawnable(&mut self) -> Option<String> {
        while let Some(name) = self.work_queue.dequeue() {
            if !self.in_flight.contains(&name) {
                self.in_flight.insert(name.clone());
                return Some(name);
            }
        }
        None
    }

    /// Requests a sync for an asset. Only enqueues if all of these hold:
    /// - `auto_sync` is true and a sync spec exists
    /// - the asset is not already queued or syncing
    /// - the asset is not suspended
    /// - the asset is not in a backoff period
    pub fn request_sync(&mut self, asset_name: &str) -> bool {
        let Some(config) = self.sync_configs.get(asset_name) else {
            return false;
        };
        // Sync is allowed only when the asset opts in (auto_sync + has_sync),
        // is not already syncing, not suspended, and not in backoff.
        let is_suspended = self.suspended_store.exists(asset_name).unwrap_or(false);
        let eligible = config.auto_sync
            && config.has_sync
            && !self.syncing.contains(asset_name)
            && !is_suspended
            && !self.guardrail.is_backoff_active(asset_name);
        if !eligible {
            return false;
        }
        self.sync_queue.enqueue(asset_name.to_string())
    }

    /// Removes the suspended flag if the asset is Ready and was previously suspended.
    /// Also resets the guardrail failure counter so sync can resume.
    fn try_auto_unsuspend(&mut self, asset_name: &str) {
        let is_suspended = self.suspended_store.exists(asset_name).unwrap_or(false);
        if !is_suspended {
            return;
        }
        match self.suspended_store.remove(asset_name) {
            Ok(()) => {
                tracing::info!(asset = %asset_name, "asset is Ready, auto-unsuspending");
                self.guardrail.record_sync_success(asset_name);
            }
            Err(e) => {
                tracing::warn!(asset = %asset_name, error = %e, "failed to remove suspended flag");
            }
        }
    }

    /// Returns the next asset that is not currently being synced.
    /// Each asset is serialized individually by asset name.
    pub fn next_syncable(&mut self) -> Option<String> {
        let mut skipped = Vec::new();
        let result = loop {
            let Some(name) = self.sync_queue.dequeue() else {
                break None;
            };
            if self.syncing.contains(&name) {
                skipped.push(name);
            } else {
                self.syncing.insert(name.clone());
                break Some(name);
            }
        };
        for name in skipped {
            self.sync_queue.enqueue(name);
        }
        result
    }

    /// Processes a sync completion: clears the syncing slot, updates guardrail
    /// state, and enqueues the asset for re-evaluation.
    ///
    /// On failure: increments consecutive failure count and applies exponential
    /// backoff. If failures reach the threshold, writes a suspended flag file
    /// to prevent further sync attempts until manually resumed.
    /// Returns the suspension reason if the asset was suspended.
    pub fn handle_sync_result(
        &mut self,
        asset_name: &str,
        success: bool,
        execution_id: Option<&str>,
    ) -> Option<String> {
        self.release_sync_slot(asset_name);

        let suspended_reason = if success {
            self.guardrail.record_sync_success(asset_name);
            None
        } else {
            self.handle_sync_failure(asset_name, execution_id)
        };

        // Re-evaluate after sync to check convergence (and detect degradation).
        self.awaiting_post_sync_eval.insert(asset_name.to_string());
        self.work_queue.enqueue(asset_name.to_string());
        suspended_reason
    }

    fn release_sync_slot(&mut self, asset_name: &str) {
        self.syncing.remove(asset_name);
    }

    fn handle_sync_failure(
        &mut self,
        asset_name: &str,
        execution_id: Option<&str>,
    ) -> Option<String> {
        self.guardrail.record_sync_failure(asset_name);
        if !self.guardrail.should_suspend(asset_name) {
            return None;
        }
        let reason = format!("{MAX_CONSECUTIVE_FAILURES} consecutive sync failures");
        self.suspend_asset(asset_name, &reason, execution_id);
        Some(reason)
    }

    fn suspend_asset(&self, asset_name: &str, reason: &str, execution_id: Option<&str>) {
        let info = SuspendedInfo {
            asset_name: asset_name.to_string(),
            reason: reason.to_string(),
            suspended_at: chrono::Utc::now().to_rfc3339(),
            execution_id: execution_id.map(|s| s.to_string()),
        };
        if let Err(e) = self.suspended_store.write(&info) {
            tracing::warn!(asset = %asset_name, error = %e, "failed to write suspended flag");
        }
    }

    /// If a Not Ready → Ready transition occurred, requests sync on downstream
    /// assets directly (skipping evaluate) and returns their names.
    /// The upstream Drifted → Ready transition means upstream data changed,
    /// which is sufficient grounds to sync downstreams without evaluating first.
    fn propagate_downstream(&mut self, asset_name: &str, ready: bool) -> Vec<String> {
        if !self.readiness.record(asset_name, ready) {
            return Vec::new();
        }
        let Some(downstreams) = self.downstream_map.get(asset_name).cloned() else {
            return Vec::new();
        };
        let mut propagated = Vec::new();
        for ds in &downstreams {
            if self.request_sync(ds) {
                propagated.push(ds.clone());
            }
        }
        propagated
    }

    /// Suspends the asset if the Ready condition count decreased after sync.
    /// Returns the suspension reason if suspended.
    fn check_degradation(&self, asset_name: &str, ready_count: usize) -> Option<String> {
        let &prev_count = self.last_ready_count.get(asset_name)?;
        if ready_count >= prev_count {
            return None;
        }
        let reason =
            format!("degradation after sync: ready conditions {prev_count} → {ready_count}");
        self.suspend_asset(asset_name, &reason, None);
        Some(reason)
    }

    /// Processes an evaluation result: updates scheduler, readiness, and
    /// in-flight tracking.  Returns names of downstream assets that were
    /// enqueued due to a Not Ready → Ready transition.
    /// Returns (propagated downstream names, optional suspension reason).
    pub fn handle_eval_result(
        &mut self,
        asset_name: &str,
        result: &Result<AssetEvalResult, EvaluateError>,
    ) -> (Vec<String>, Option<SuspendedEvent>) {
        self.in_flight.remove(asset_name);
        self.scheduler.reschedule(asset_name);

        let (ready, ready_count) = match result {
            Ok(r) => {
                let count = r
                    .conditions
                    .iter()
                    .filter(|c| c.status == crate::evaluate::ConditionStatus::Ready)
                    .count();
                (r.ready, count)
            }
            Err(_) => (false, 0),
        };

        // Degradation detection: if this is a post-sync re-evaluation and
        // the number of Ready conditions decreased, suspend the asset.
        let suspended = self
            .awaiting_post_sync_eval
            .remove(asset_name)
            .then(|| self.check_degradation(asset_name, ready_count))
            .flatten()
            .map(|reason| SuspendedEvent {
                asset_name: asset_name.to_string(),
                reason,
            });
        self.last_ready_count
            .insert(asset_name.to_string(), ready_count);

        if ready {
            self.try_auto_unsuspend(asset_name);
        } else {
            self.request_sync(asset_name);
        }

        (self.propagate_downstream(asset_name, ready), suspended)
    }

    /// Handles a JoinSet result from an eval task.
    /// Logs the outcome, updates state, and returns downstream propagations.
    pub fn on_eval_complete(&mut self, join_result: EvalJoinResult) -> Option<SuspendedEvent> {
        let Some(Ok((asset_name, eval_result))) = join_result else {
            return None;
        };
        match &eval_result {
            Ok(r) => tracing::info!(asset = %r.asset_name, ready = r.ready, "evaluated"),
            Err(e) => tracing::error!(asset = %asset_name, error = %e, "evaluation failed"),
        }
        let (propagated, suspended) = self.handle_eval_result(&asset_name, &eval_result);
        for ds in &propagated {
            tracing::debug!(downstream = %ds, "propagating to downstream");
        }
        suspended
    }

    /// Handles a JoinSet result from a sync task.
    /// Logs the outcome and updates guardrail / suspended state.
    pub fn on_sync_complete(&mut self, join_result: SyncJoinResult) -> Option<SuspendedEvent> {
        let Some(Ok((asset_name, sync_result))) = join_result else {
            return None;
        };
        match &sync_result {
            Ok(r) => tracing::info!(asset = %asset_name, success = r.success, "sync completed"),
            Err(e) => tracing::error!(asset = %asset_name, error = %e, "sync failed"),
        }
        let (success, execution_id) = match &sync_result {
            Ok(r) => (r.success, Some(r.execution_id.as_str())),
            Err(_) => (false, None),
        };
        self.handle_sync_result(&asset_name, success, execution_id)
            .map(|reason| SuspendedEvent { asset_name, reason })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::GraphEdge;
    use crate::storage::StorageError;

    /// In-memory SuspendedStore for testing.
    #[derive(Debug, Default)]
    struct MemSuspendedStore {
        inner: std::sync::Mutex<std::collections::HashMap<String, SuspendedInfo>>,
    }

    impl crate::storage::SuspendedStore for MemSuspendedStore {
        fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError> {
            self.inner
                .lock()
                .unwrap()
                .insert(info.asset_name.clone(), info.clone());
            Ok(())
        }
        fn read(&self, name: &str) -> Result<Option<SuspendedInfo>, StorageError> {
            Ok(self.inner.lock().unwrap().get(name).cloned())
        }
        fn remove(&self, name: &str) -> Result<(), StorageError> {
            self.inner.lock().unwrap().remove(name);
            Ok(())
        }
        fn exists(&self, name: &str) -> Result<bool, StorageError> {
            Ok(self.inner.lock().unwrap().contains_key(name))
        }
        fn list(&self) -> Result<Vec<SuspendedInfo>, StorageError> {
            Ok(self.inner.lock().unwrap().values().cloned().collect())
        }
    }

    fn mem_suspended_store() -> Arc<dyn crate::storage::SuspendedStore> {
        Arc::new(MemSuspendedStore::default())
    }

    fn edge(from: &str, to: &str) -> GraphEdge {
        GraphEdge {
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    fn asset_entry(name: &str, interval: Option<StdDuration>) -> AssetEntry {
        AssetEntry {
            name: name.to_string(),
            yaml: String::new(),
            min_interval: interval,
            auto_sync: false,
            has_sync: false,
            sync_ref_name: None,
        }
    }

    fn asset_entry_with_sync(name: &str) -> AssetEntry {
        AssetEntry {
            name: name.to_string(),
            yaml: String::new(),
            min_interval: None,
            auto_sync: true,
            has_sync: true,
            sync_ref_name: None,
        }
    }

    fn asset_entry_with_sync_ref(name: &str, sync_ref: &str) -> AssetEntry {
        AssetEntry {
            name: name.to_string(),
            yaml: String::new(),
            min_interval: None,
            auto_sync: true,
            has_sync: true,
            sync_ref_name: Some(sync_ref.to_string()),
        }
    }

    fn ready_condition(name: &str) -> crate::evaluate::ConditionResult {
        crate::evaluate::ConditionResult {
            condition_name: name.to_string(),
            condition_type: "test".to_string(),
            status: crate::evaluate::ConditionStatus::Ready,
        }
    }

    fn not_ready_condition(name: &str) -> crate::evaluate::ConditionResult {
        crate::evaluate::ConditionResult {
            condition_name: name.to_string(),
            condition_type: "test".to_string(),
            status: crate::evaluate::ConditionStatus::Drifted {
                reason: "test".to_string(),
            },
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

    // ── ServeState tests ────────────────────────────────────────────────

    #[test]
    fn serve_state_init_enqueues_and_registers() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, mem_suspended_store());
        let assets = vec![
            asset_entry("a", Some(StdDuration::from_secs(60))),
            asset_entry("b", None),
        ];
        state.register_assets(&assets);

        // Only root asset "a" is enqueued; "b" is blocked (upstream "a" not ready)
        assert_eq!(state.work_queue.dequeue(), Some("a".to_string()));
        assert_eq!(state.work_queue.dequeue(), None);
        assert!(state.scheduler.intervals.contains_key("a"));
        assert!(!state.scheduler.intervals.contains_key("b"));
    }

    #[test]
    fn next_spawnable_skips_in_flight() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.work_queue.enqueue("a".to_string());
        state.work_queue.enqueue("b".to_string());
        state.in_flight.insert("a".to_string());

        assert_eq!(state.next_spawnable(), Some("b".to_string()));
        assert_eq!(state.next_spawnable(), None);
        assert!(state.in_flight.contains("b"));
    }

    // ── handle_eval_result tests ─────────────────────────────────────────

    #[test]
    fn handle_eval_result_clears_in_flight_and_reschedules() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry("a", Some(StdDuration::from_secs(60)))]);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", true);
        state.handle_eval_result("a", &result);

        assert!(!state.in_flight.contains("a"));
        assert!(state.scheduler.next_eval_at.contains_key("a"));
    }

    #[test]
    fn handle_eval_result_error_marks_not_ready() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.in_flight.insert("a".to_string());

        state.handle_eval_result("a", &eval_ok("a", true));

        state.in_flight.insert("a".to_string());
        let err: Result<AssetEvalResult, EvaluateError> =
            Err(EvaluateError::Parse("test error".to_string()));
        state.handle_eval_result("a", &err);

        assert!(!state.readiness.ready.get("a").copied().unwrap_or(false));
    }

    // ── propagate_downstream tests ───────────────────────────────────────

    #[test]
    fn propagate_downstream_requests_sync_on_transition() {
        let edges = vec![edge("a", "b"), edge("a", "c")];
        let mut state = ServeState::new(&edges, mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("b"), asset_entry_with_sync("c")]);
        state.readiness.record("a", false);

        let propagated = state.propagate_downstream("a", true);
        assert_eq!(propagated, vec!["b".to_string(), "c".to_string()]);
        // Downstreams should be in sync_queue, not work_queue
        assert_eq!(state.sync_queue.dequeue(), Some("b".to_string()));
        assert_eq!(state.sync_queue.dequeue(), Some("c".to_string()));
    }

    #[test]
    fn propagate_downstream_skips_without_transition() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("b")]);
        state.readiness.record("a", false);
        state.propagate_downstream("a", true);

        let propagated = state.propagate_downstream("a", true);
        assert!(propagated.is_empty());
    }

    #[test]
    fn propagate_downstream_skips_when_already_syncing() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("b")]);
        state.readiness.record("a", false);
        state.syncing.insert("b".to_string());

        let propagated = state.propagate_downstream("a", true);
        assert!(propagated.is_empty());
    }

    #[test]
    fn propagate_downstream_no_downstreams() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.readiness.record("a", false);

        let propagated = state.propagate_downstream("a", true);
        assert!(propagated.is_empty());
    }

    #[test]
    fn propagate_downstream_does_not_enqueue_evaluate() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("b")]);
        // Drain initial work_queue entries from init
        while state.work_queue.dequeue().is_some() {}
        state.readiness.record("a", false);

        state.propagate_downstream("a", true);

        // work_queue (evaluate) must remain empty; only sync_queue is used.
        assert!(state.work_queue.dequeue().is_none());
        assert_eq!(state.sync_queue.dequeue(), Some("b".to_string()));
    }

    #[test]
    fn propagate_downstream_diamond_syncs_once_when_concurrent() {
        // A → B → X, A → C → X
        let edges = vec![edge("b", "x"), edge("c", "x")];
        let mut state = ServeState::new(&edges, mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("x")]);
        while state.work_queue.dequeue().is_some() {}

        // B becomes Ready → X sync requested
        state.readiness.record("b", false);
        let propagated = state.propagate_downstream("b", true);
        assert_eq!(propagated, vec!["x".to_string()]);

        // Start X's sync
        assert_eq!(state.next_syncable(), Some("x".to_string()));

        // C becomes Ready while X is syncing → X sync rejected
        state.readiness.record("c", false);
        let propagated = state.propagate_downstream("c", true);
        assert!(propagated.is_empty());
    }

    // ── Sync management tests ─────────────────────────────────────────

    #[test]
    fn request_sync_enqueues_when_auto_sync_enabled() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        assert!(state.request_sync("a"));
        assert_eq!(state.sync_queue.dequeue(), Some("a".to_string()));
    }

    #[test]
    fn request_sync_skips_when_auto_sync_false() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry("a", None)]);
        state.work_queue.dequeue();

        assert!(!state.request_sync("a"));
        assert!(state.sync_queue.is_empty());
    }

    #[test]
    fn request_sync_skips_when_already_syncing() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        state.syncing.insert("a".to_string());
        assert!(!state.request_sync("a"));
    }

    #[test]
    fn request_sync_dedup() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        assert!(state.request_sync("a"));
        assert!(!state.request_sync("a")); // duplicate rejected
    }

    #[test]
    fn next_syncable_allows_different_assets_with_same_sync_ref() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[
            asset_entry_with_sync_ref("a", "dbt-run"),
            asset_entry_with_sync_ref("b", "dbt-run"),
        ]);
        state.request_sync("a");
        state.request_sync("b");

        assert_eq!(state.next_syncable(), Some("a".to_string()));
        // "b" is a different asset, so it can run concurrently.
        assert_eq!(state.next_syncable(), Some("b".to_string()));
    }

    #[test]
    fn handle_sync_result_clears_syncing_and_enqueues_re_eval() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        state.syncing.insert("a".to_string());
        state.handle_sync_result("a", true, None);

        assert!(state.syncing.is_empty());
        assert_eq!(state.work_queue.dequeue(), Some("a".to_string()));
    }

    #[test]
    fn next_syncable_prevents_same_asset_concurrent_sync() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.request_sync("a");

        assert_eq!(state.next_syncable(), Some("a".to_string()));
        // Same asset cannot sync concurrently.
        state.request_sync("a");
        assert_eq!(state.next_syncable(), None);

        state.handle_sync_result("a", true, None);
        state.request_sync("a");
        assert_eq!(state.next_syncable(), Some("a".to_string()));
    }

    #[test]
    fn handle_eval_not_ready_with_auto_sync_requests_sync() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", false);
        state.handle_eval_result("a", &result);

        assert_eq!(state.sync_queue.dequeue(), Some("a".to_string()));
    }

    #[test]
    fn handle_eval_ready_does_not_request_sync() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", true);
        state.handle_eval_result("a", &result);

        assert!(state.sync_queue.is_empty());
    }

    // ── Guardrail integration tests ────────────────────────────────────

    #[test]
    fn handle_sync_failure_applies_backoff() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        state.syncing.insert("a".to_string());
        state.handle_sync_result("a", false, None);

        assert!(!state.request_sync("a"));
    }

    #[test]
    fn handle_sync_failure_suspends_after_max() {
        let susp = Arc::new(MemSuspendedStore::default());
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);

        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            state.syncing.insert("a".to_string());
            state.handle_sync_result("a", false, None);
        }

        assert!(susp.exists("a").unwrap());
        assert!(!state.request_sync("a"));
    }

    #[test]
    fn request_sync_blocked_when_suspended() {
        let susp = Arc::new(MemSuspendedStore::default());
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);

        susp.write(&SuspendedInfo {
            asset_name: "a".to_string(),
            reason: "test".to_string(),
            suspended_at: "2025-01-01T00:00:00Z".to_string(),
            execution_id: None,
        })
        .unwrap();

        assert!(!state.request_sync("a"));
    }

    // ── on_eval_complete / on_sync_complete tests ──────────────────────

    #[test]
    fn on_eval_complete_updates_state() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, mem_suspended_store());
        state.register_assets(&[asset_entry("a", None), asset_entry_with_sync("b")]);
        state.in_flight.insert("a".to_string());
        while state.work_queue.dequeue().is_some() {}

        state.readiness.record("a", false);

        let join_result = Some(Ok(("a".to_string(), eval_ok("a", true))));
        state.on_eval_complete(join_result);

        assert!(!state.in_flight.contains("a"));
        // Downstream "b" is enqueued for sync (not evaluate) on upstream Ready.
        assert_eq!(state.sync_queue.dequeue(), Some("b".to_string()));
    }

    #[test]
    fn on_eval_complete_ignores_none() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry("a", None)]);
        while state.work_queue.dequeue().is_some() {}

        state.on_eval_complete(None);
        assert!(state.work_queue.dequeue().is_none());
    }

    #[test]
    fn on_sync_complete_success_resets_guardrail() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        while state.work_queue.dequeue().is_some() {}

        state.syncing.insert("a".to_string());
        state.handle_sync_result("a", false, None);

        state.syncing.insert("a".to_string());
        let sync_result = Ok(crate::sync::SyncExecutionResult {
            execution_id: "test".to_string(),
            asset_name: "a".to_string(),
            sync_type: crate::sync::SyncType::Sync,
            stages: vec![],
            success: true,
        });
        state.on_sync_complete(Some(Ok(("a".to_string(), sync_result))));

        assert!(state.syncing.is_empty());
        assert!(!state.guardrail.should_suspend("a"));
    }

    #[test]
    fn on_sync_complete_failure_increments_guardrail() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);

        state.syncing.insert("a".to_string());
        let sync_result: Result<crate::sync::SyncExecutionResult, SyncError> =
            Err(SyncError::NoSyncSpec {
                asset_name: "a".to_string(),
            });
        state.on_sync_complete(Some(Ok(("a".to_string(), sync_result))));

        assert!(state.guardrail.is_backoff_active("a"));
    }

    #[test]
    fn on_sync_complete_ignores_none() {
        let mut state = ServeState::new(&[], mem_suspended_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.syncing.insert("a".to_string());

        state.on_sync_complete(None);
        assert!(!state.syncing.is_empty());
    }

    fn susp_store() -> Arc<dyn crate::storage::SuspendedStore> {
        Arc::new(MemSuspendedStore::default())
    }

    fn susp_store_shared() -> Arc<MemSuspendedStore> {
        Arc::new(MemSuspendedStore::default())
    }

    // ── Degradation detection tests ──────────────────────────────────────

    #[test]
    fn degradation_suspends_when_ready_count_decreases() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.in_flight.insert("a".to_string());

        let result = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![ready_condition("c1"), ready_condition("c2")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &result);
        assert_eq!(state.last_ready_count["a"], 2);

        state.syncing.insert("a".to_string());
        state.handle_sync_result("a", true, None);
        assert!(state.awaiting_post_sync_eval.contains("a"));

        state.in_flight.insert("a".to_string());
        let degraded = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![ready_condition("c1"), not_ready_condition("c2")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &degraded);

        assert!(susp.exists("a").unwrap());
    }

    #[test]
    fn no_degradation_when_ready_count_same_or_increases() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.in_flight.insert("a".to_string());

        let result = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![ready_condition("c1"), not_ready_condition("c2")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &result);

        state.syncing.insert("a".to_string());
        state.handle_sync_result("a", true, None);
        state.in_flight.insert("a".to_string());
        let same = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![ready_condition("c1"), not_ready_condition("c2")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &same);

        assert!(!susp.exists("a").unwrap());
    }

    #[test]
    fn auto_unsuspend_when_ready() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);

        susp.write(&SuspendedInfo {
            asset_name: "a".to_string(),
            reason: "test".to_string(),
            suspended_at: "2025-01-01T00:00:00Z".to_string(),
            execution_id: None,
        })
        .unwrap();
        assert!(susp.exists("a").unwrap());

        state.in_flight.insert("a".to_string());
        let result = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: true,
            conditions: vec![ready_condition("c1")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &result);

        assert!(!susp.exists("a").unwrap());
        assert_eq!(state.guardrail.consecutive_failures.get("a").copied(), None);
    }

    #[test]
    fn no_unsuspend_when_not_ready() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);

        susp.write(&SuspendedInfo {
            asset_name: "a".to_string(),
            reason: "test".to_string(),
            suspended_at: "2025-01-01T00:00:00Z".to_string(),
            execution_id: None,
        })
        .unwrap();

        state.in_flight.insert("a".to_string());
        let result = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![not_ready_condition("c1")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &result);

        assert!(susp.exists("a").unwrap());
    }

    // ── release_sync_slot tests ─────────────────────────────────────────

    #[test]
    fn release_sync_slot_clears_syncing() {
        let mut state = ServeState::new(&[], susp_store());
        state.register_assets(&[asset_entry_with_sync("a")]);
        state.syncing.insert("a".to_string());

        state.release_sync_slot("a");

        assert!(!state.syncing.contains("a"));
    }

    #[test]
    fn release_sync_slot_noop_if_not_syncing() {
        let mut state = ServeState::new(&[], susp_store());
        state.register_assets(&[asset_entry_with_sync("a")]);

        state.release_sync_slot("a");
        assert!(!state.syncing.contains("a"));
    }

    // ── handle_sync_failure tests ───────────────────────────────────────

    #[test]
    fn handle_sync_failure_returns_none_below_threshold() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);

        let result = state.handle_sync_failure("a", None);
        assert!(result.is_none());
        assert!(!susp.exists("a").unwrap());
    }

    #[test]
    fn handle_sync_failure_suspends_at_threshold() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);

        for _ in 0..MAX_CONSECUTIVE_FAILURES - 1 {
            assert!(state.handle_sync_failure("a", None).is_none());
        }
        let result = state.handle_sync_failure("a", None);
        assert!(result.is_some());
        assert!(susp.exists("a").unwrap());
    }

    #[test]
    fn handle_sync_failure_records_execution_id() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.register_assets(&[asset_entry_with_sync("a")]);

        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            state.handle_sync_failure("a", Some("exec-123"));
        }
        let info = susp.list().unwrap();
        assert_eq!(info[0].execution_id.as_deref(), Some("exec-123"));
    }

    // ── suspend_asset tests ─────────────────────────────────────────────

    #[test]
    fn suspend_asset_writes_flag() {
        let susp = susp_store_shared();
        let state = ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);

        state.suspend_asset("a", "test reason", Some("exec-1"));

        let info = susp.read("a").unwrap().unwrap();
        assert_eq!(info.asset_name, "a");
        assert_eq!(info.reason, "test reason");
        assert_eq!(info.execution_id.as_deref(), Some("exec-1"));
    }

    #[test]
    fn suspend_asset_without_execution_id() {
        let susp = susp_store_shared();
        let state = ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);

        state.suspend_asset("a", "test reason", None);

        let info = susp.read("a").unwrap().unwrap();
        assert_eq!(info.asset_name, "a");
        assert!(info.execution_id.is_none());
    }

    // ── check_degradation tests ─────────────────────────────────────────

    #[test]
    fn check_degradation_returns_none_when_improved() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.last_ready_count.insert("a".to_string(), 1);

        assert!(state.check_degradation("a", 2).is_none());
        assert!(!susp.exists("a").unwrap());
    }

    #[test]
    fn check_degradation_returns_none_when_unchanged() {
        let mut state = ServeState::new(&[], susp_store());
        state.last_ready_count.insert("a".to_string(), 2);

        assert!(state.check_degradation("a", 2).is_none());
    }

    #[test]
    fn check_degradation_suspends_when_decreased() {
        let susp = susp_store_shared();
        let mut state =
            ServeState::new(&[], susp.clone() as Arc<dyn crate::storage::SuspendedStore>);
        state.last_ready_count.insert("a".to_string(), 3);

        let reason = state.check_degradation("a", 1);
        assert!(reason.is_some());
        assert!(susp.exists("a").unwrap());
    }

    #[test]
    fn check_degradation_returns_none_when_no_previous() {
        let state = ServeState::new(&[], susp_store());

        assert!(state.check_degradation("a", 1).is_none());
    }

    // ── try_auto_unsuspend tests ────────────────────────────────────────

    #[test]
    fn try_auto_unsuspend_noop_when_not_suspended() {
        let mut state = ServeState::new(&[], susp_store());
        state.register_assets(&[asset_entry_with_sync("a")]);

        state.try_auto_unsuspend("a");
    }

    // ── upstream block tests ────────────────────────────────────────────

    #[test]
    fn all_upstreams_ready_no_upstreams() {
        let state = ServeState::new(&[], susp_store());
        assert!(state.all_upstreams_ready("a"));
    }

    #[test]
    fn all_upstreams_ready_single_upstream_ready() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, susp_store());
        state.readiness.record("a", true);
        assert!(state.all_upstreams_ready("b"));
    }

    #[test]
    fn all_upstreams_ready_single_upstream_not_ready() {
        let edges = vec![edge("a", "b")];
        let state = ServeState::new(&edges, susp_store());
        assert!(!state.all_upstreams_ready("b"));
    }

    #[test]
    fn all_upstreams_ready_mixed() {
        let edges = vec![edge("a", "c"), edge("b", "c")];
        let mut state = ServeState::new(&edges, susp_store());
        state.readiness.record("a", true);
        // b is not ready
        assert!(!state.all_upstreams_ready("c"));
    }

    #[test]
    fn all_upstreams_ready_all_ready() {
        let edges = vec![edge("a", "c"), edge("b", "c")];
        let mut state = ServeState::new(&edges, susp_store());
        state.readiness.record("a", true);
        state.readiness.record("b", true);
        assert!(state.all_upstreams_ready("c"));
    }

    #[test]
    fn init_only_enqueues_root_assets() {
        let edges = vec![edge("a", "b"), edge("b", "c")];
        let mut state = ServeState::new(&edges, susp_store());
        state.register_assets(&[
            asset_entry("a", None),
            asset_entry("b", None),
            asset_entry("c", None),
        ]);
        // Only "a" (no upstreams) should be enqueued
        assert_eq!(state.work_queue.dequeue(), Some("a".to_string()));
        assert_eq!(state.work_queue.dequeue(), None);
    }

    #[test]
    fn enqueue_due_skips_when_upstream_not_ready() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, susp_store());
        state.register_assets(&[
            asset_entry("a", None),
            asset_entry("b", Some(StdDuration::from_secs(1))),
        ]);
        while state.work_queue.dequeue().is_some() {}

        // Set b's next_eval_at to the past so it is due
        state
            .scheduler
            .next_eval_at
            .insert("b".to_string(), tokio::time::Instant::now());
        state.enqueue_due();
        // b should not be enqueued because a is not ready
        assert_eq!(state.work_queue.dequeue(), None);
    }

    #[test]
    fn enqueue_due_allows_when_upstream_ready() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, susp_store());
        state.register_assets(&[
            asset_entry("a", None),
            asset_entry("b", Some(StdDuration::from_secs(1))),
        ]);
        while state.work_queue.dequeue().is_some() {}

        state.readiness.record("a", true);
        state
            .scheduler
            .next_eval_at
            .insert("b".to_string(), tokio::time::Instant::now());
        state.enqueue_due();
        assert_eq!(state.work_queue.dequeue(), Some("b".to_string()));
    }
}

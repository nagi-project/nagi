use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::Duration as StdDuration;

use tokio::time::Instant;

use crate::compile::GraphEdge;
use crate::evaluate::{AssetEvalResult, EvaluateError};
use crate::sync::SyncError;

use super::graph::build_downstream_map;
use super::suspended::{suspended_path, write_suspended, SuspendedInfo};

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

// ── WorkQueue ────────────────────────────────────────────────────────────────

/// FIFO queue with deduplication. An asset that is already queued will not be
/// added again until it is dequeued.
#[derive(Debug, Default)]
pub struct WorkQueue {
    queue: VecDeque<String>,
    pending: HashSet<String>,
}

impl WorkQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pending: HashSet::new(),
        }
    }

    /// Enqueues an asset. Returns false if already queued.
    pub fn enqueue(&mut self, name: String) -> bool {
        if self.pending.contains(&name) {
            return false;
        }
        self.pending.insert(name.clone());
        self.queue.push_back(name);
        true
    }

    pub fn dequeue(&mut self) -> Option<String> {
        let name = self.queue.pop_front()?;
        self.pending.remove(&name);
        Some(name)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

// ── SchedulerState ───────────────────────────────────────────────────────────

/// Tracks per-asset evaluation intervals and computes the next due time.
#[derive(Debug, Default)]
pub struct SchedulerState {
    pub intervals: HashMap<String, StdDuration>,
    pub next_eval_at: HashMap<String, Instant>,
}

impl SchedulerState {
    pub fn new() -> Self {
        Self {
            intervals: HashMap::new(),
            next_eval_at: HashMap::new(),
        }
    }

    pub fn register(&mut self, asset_name: String, interval: StdDuration) {
        self.next_eval_at
            .insert(asset_name.clone(), Instant::now() + interval);
        self.intervals.insert(asset_name, interval);
    }

    /// Returns the asset due soonest and its scheduled time, or None.
    pub fn next_due(&self) -> Option<(&str, Instant)> {
        self.next_eval_at
            .iter()
            .min_by_key(|(_, instant)| *instant)
            .map(|(name, instant)| (name.as_str(), *instant))
    }

    /// Resets the timer for an asset to `now + interval`.
    pub fn reschedule(&mut self, asset_name: &str) {
        if let Some(interval) = self.intervals.get(asset_name) {
            self.next_eval_at
                .insert(asset_name.to_string(), Instant::now() + *interval);
        }
    }
}

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

// ── GuardrailState ───────────────────────────────────────────────────────────

pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;
const BACKOFF_BASE_SECS: u64 = 30;
const BACKOFF_MAX_SECS: u64 = 30 * 60; // 30 minutes

/// Tracks consecutive sync failures and backoff timers per asset.
/// When failures reach `MAX_CONSECUTIVE_FAILURES`, the asset should be
/// suspended (sync stopped, evaluate continues).
#[derive(Debug, Default)]
pub struct GuardrailState {
    pub consecutive_failures: HashMap<String, u32>,
    /// Earliest time at which the next sync attempt is allowed.
    next_sync_at: HashMap<String, Instant>,
}

impl GuardrailState {
    pub fn new() -> Self {
        Self {
            consecutive_failures: HashMap::new(),
            next_sync_at: HashMap::new(),
        }
    }

    pub fn record_sync_success(&mut self, asset_name: &str) {
        self.consecutive_failures.remove(asset_name);
        self.next_sync_at.remove(asset_name);
    }

    /// Increments the failure counter and sets the next backoff time.
    /// Returns the new failure count.
    pub fn record_sync_failure(&mut self, asset_name: &str) -> u32 {
        let count = self
            .consecutive_failures
            .entry(asset_name.to_string())
            .or_insert(0);
        *count += 1;
        let current = *count;

        // Exponential backoff: base * 2^(failures-1), capped at max.
        let backoff_secs = (BACKOFF_BASE_SECS * 2u64.saturating_pow(current.saturating_sub(1)))
            .min(BACKOFF_MAX_SECS);
        self.next_sync_at.insert(
            asset_name.to_string(),
            Instant::now() + StdDuration::from_secs(backoff_secs),
        );

        current
    }

    pub fn should_suspend(&self, asset_name: &str) -> bool {
        self.consecutive_failures
            .get(asset_name)
            .copied()
            .unwrap_or(0)
            >= MAX_CONSECUTIVE_FAILURES
    }

    /// Returns true if the asset is in a backoff period (too early to retry).
    pub fn is_backoff_active(&self, asset_name: &str) -> bool {
        self.next_sync_at
            .get(asset_name)
            .map(|t| Instant::now() < *t)
            .unwrap_or(false)
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
    /// The sync ref name from the original asset spec. Assets sharing the same
    /// sync ref are serialized; assets with different refs may sync concurrently.
    pub sync_ref_name: Option<String>,
}

/// Per-asset sync configuration derived from the compiled asset.
#[derive(Debug, Clone)]
struct AssetSyncConfig {
    auto_sync: bool,
    has_sync: bool,
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
    /// Per-asset sync configuration.
    sync_configs: HashMap<String, AssetSyncConfig>,
    /// FIFO queue of assets waiting for sync execution.
    pub sync_queue: WorkQueue,
    /// Sync refs currently being synced. Assets sharing the same sync ref
    /// are serialized; different refs may run concurrently.
    syncing_refs: HashSet<String>,
    /// Asset names currently being synced (for dedup and completion tracking).
    pub syncing: HashSet<String>,
    /// Tracks consecutive sync failures and exponential backoff.
    pub guardrail: GuardrailState,
    /// Directory for suspended flag files.
    suspended_dir: PathBuf,
    /// Ready condition count from the last evaluation, per asset.
    last_ready_count: HashMap<String, usize>,
    /// Assets awaiting post-sync re-evaluation for degradation detection.
    pending_sync_reeval: HashSet<String>,
}

impl ServeState {
    pub fn new(edges: &[GraphEdge], suspended_dir: PathBuf) -> Self {
        Self {
            scheduler: SchedulerState::new(),
            work_queue: WorkQueue::new(),
            readiness: ReadinessState::new(),
            in_flight: HashSet::new(),
            downstream_map: build_downstream_map(edges),
            sync_configs: HashMap::new(),
            sync_queue: WorkQueue::new(),
            syncing_refs: HashSet::new(),
            syncing: HashSet::new(),
            guardrail: GuardrailState::new(),
            suspended_dir,
            last_ready_count: HashMap::new(),
            pending_sync_reeval: HashSet::new(),
        }
    }

    /// Registers all assets: enqueue for initial evaluation + register intervals
    /// + store sync configuration.
    pub fn init(&mut self, assets: &[AssetEntry]) {
        for asset in assets {
            self.work_queue.enqueue(asset.name.clone());
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

    /// Enqueues the next due asset (if any) when its timer fires.
    pub fn enqueue_due(&mut self) {
        if let Some((name, _)) = self.scheduler.next_due() {
            let name = name.to_string();
            if !self.in_flight.contains(&name) {
                self.work_queue.enqueue(name);
            }
        }
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
        let eligible = config.auto_sync
            && config.has_sync
            && !self.syncing.contains(asset_name)
            && !suspended_path(&self.suspended_dir, asset_name)
                .map(|p| p.exists())
                .unwrap_or(false)
            && !self.guardrail.is_backoff_active(asset_name);
        if !eligible {
            return false;
        }
        self.sync_queue.enqueue(asset_name.to_string())
    }

    /// Removes the suspended flag if the asset is Ready and was previously suspended.
    /// Also resets the guardrail failure counter so sync can resume.
    fn try_auto_unsuspend(&mut self, asset_name: &str) {
        let is_suspended = suspended_path(&self.suspended_dir, asset_name)
            .map(|p| p.exists())
            .unwrap_or(false);
        if !is_suspended {
            return;
        }
        match super::suspended::remove_suspended(&self.suspended_dir, asset_name) {
            Ok(()) => {
                eprintln!("[serve] asset {asset_name} is Ready, auto-unsuspending");
                self.guardrail.record_sync_success(asset_name);
            }
            Err(e) => {
                eprintln!("[serve] warning: failed to remove suspended flag for {asset_name}: {e}");
            }
        }
    }

    /// Returns the next asset whose sync ref is not currently in use.
    /// Assets sharing the same sync ref are serialized; different refs may
    /// run concurrently.
    pub fn next_syncable(&mut self) -> Option<String> {
        let mut skipped = Vec::new();
        let result = loop {
            let Some(name) = self.sync_queue.dequeue() else {
                break None;
            };
            let ref_key = self.sync_ref_key(&name);
            if self.syncing_refs.contains(&ref_key) {
                skipped.push(name);
            } else {
                self.syncing_refs.insert(ref_key);
                self.syncing.insert(name.clone());
                break Some(name);
            }
        };
        for name in skipped {
            self.sync_queue.enqueue(name);
        }
        result
    }

    /// Returns the sync ref key for an asset. Falls back to the asset name
    /// when no sync ref is defined, ensuring per-asset serialization.
    fn sync_ref_key(&self, asset_name: &str) -> String {
        self.sync_configs
            .get(asset_name)
            .and_then(|c| c.sync_ref_name.clone())
            .unwrap_or_else(|| asset_name.to_string())
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
        self.pending_sync_reeval.insert(asset_name.to_string());
        self.work_queue.enqueue(asset_name.to_string());
        suspended_reason
    }

    fn release_sync_slot(&mut self, asset_name: &str) {
        if self.syncing.remove(asset_name) {
            let ref_key = self.sync_ref_key(asset_name);
            self.syncing_refs.remove(&ref_key);
        }
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
        if let Err(e) = write_suspended(&self.suspended_dir, &info) {
            eprintln!("[serve] warning: failed to write suspended flag for {asset_name}: {e}");
        }
    }

    /// If a Not Ready → Ready transition occurred, enqueues downstream assets
    /// for re-evaluation and returns their names.
    fn propagate_downstream(&mut self, asset_name: &str, ready: bool) -> Vec<String> {
        if !self.readiness.record(asset_name, ready) {
            return Vec::new();
        }
        let Some(downstreams) = self.downstream_map.get(asset_name) else {
            return Vec::new();
        };
        let mut propagated = Vec::new();
        for ds in downstreams {
            if !self.in_flight.contains(ds) && self.work_queue.enqueue(ds.clone()) {
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
        let suspended = if self.pending_sync_reeval.remove(asset_name) {
            self.check_degradation(asset_name, ready_count)
                .map(|reason| SuspendedEvent {
                    asset_name: asset_name.to_string(),
                    reason,
                })
        } else {
            None
        };
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
            Ok(r) => eprintln!("[serve] evaluated {}: ready={}", r.asset_name, r.ready),
            Err(e) => eprintln!("[serve] evaluation failed for {asset_name}: {e}"),
        }
        let (propagated, suspended) = self.handle_eval_result(&asset_name, &eval_result);
        for ds in &propagated {
            eprintln!("[serve] propagating to downstream: {ds}");
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
            Ok(r) => eprintln!(
                "[serve] sync completed for {asset_name}: success={}",
                r.success
            ),
            Err(e) => eprintln!("[serve] sync failed for {asset_name}: {e}"),
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
            status: crate::evaluate::ConditionStatus::NotReady {
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

    // ── GuardrailState tests ──────────────────────────────────────────

    #[test]
    fn guardrail_success_resets_counter() {
        let mut g = GuardrailState::new();
        g.record_sync_failure("a");
        g.record_sync_failure("a");
        assert_eq!(g.consecutive_failures.get("a").copied(), Some(2));

        g.record_sync_success("a");
        assert_eq!(g.consecutive_failures.get("a"), None);
        assert!(!g.is_backoff_active("a"));
    }

    #[test]
    fn guardrail_suspend_after_max_failures() {
        let mut g = GuardrailState::new();
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            g.record_sync_failure("a");
        }
        assert!(g.should_suspend("a"));
    }

    #[test]
    fn guardrail_no_suspend_below_max() {
        let mut g = GuardrailState::new();
        for _ in 0..MAX_CONSECUTIVE_FAILURES - 1 {
            g.record_sync_failure("a");
        }
        assert!(!g.should_suspend("a"));
    }

    #[tokio::test]
    async fn guardrail_backoff_active_after_failure() {
        tokio::time::pause();
        let mut g = GuardrailState::new();
        g.record_sync_failure("a");
        assert!(g.is_backoff_active("a"));

        // Advance past first backoff (30s)
        tokio::time::advance(StdDuration::from_secs(31)).await;
        assert!(!g.is_backoff_active("a"));
    }

    #[tokio::test]
    async fn guardrail_backoff_increases_exponentially() {
        tokio::time::pause();

        // 1st failure: 30s backoff
        let mut g = GuardrailState::new();
        g.record_sync_failure("a");
        assert!(g.is_backoff_active("a"));
        tokio::time::advance(StdDuration::from_secs(31)).await;
        assert!(!g.is_backoff_active("a"));

        // 2nd failure (consecutive): 60s backoff
        let count = g.record_sync_failure("a");
        assert_eq!(count, 2);
        tokio::time::advance(StdDuration::from_secs(59)).await;
        assert!(g.is_backoff_active("a"));
        tokio::time::advance(StdDuration::from_secs(2)).await;
        assert!(!g.is_backoff_active("a"));
    }

    // ── ServeState tests ────────────────────────────────────────────────

    #[test]
    fn serve_state_init_enqueues_and_registers() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, PathBuf::from("/tmp/nagi-test-suspended"));
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
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
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
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry("a", Some(StdDuration::from_secs(60)))]);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", true);
        state.handle_eval_result("a", &result);

        assert!(!state.in_flight.contains("a"));
        assert!(state.scheduler.next_eval_at.contains_key("a"));
    }

    #[test]
    fn handle_eval_result_error_marks_not_ready() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
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
    fn propagate_downstream_enqueues_on_transition() {
        let edges = vec![edge("a", "b"), edge("a", "c")];
        let mut state = ServeState::new(&edges, PathBuf::from("/tmp/nagi-test-suspended"));
        state.readiness.record("a", false);

        let propagated = state.propagate_downstream("a", true);
        assert_eq!(propagated, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn propagate_downstream_skips_without_transition() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, PathBuf::from("/tmp/nagi-test-suspended"));
        state.readiness.record("a", false);
        state.propagate_downstream("a", true);

        let propagated = state.propagate_downstream("a", true);
        assert!(propagated.is_empty());
    }

    #[test]
    fn propagate_downstream_skips_in_flight() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, PathBuf::from("/tmp/nagi-test-suspended"));
        state.readiness.record("a", false);
        state.in_flight.insert("b".to_string());

        let propagated = state.propagate_downstream("a", true);
        assert!(propagated.is_empty());
    }

    #[test]
    fn propagate_downstream_no_downstreams() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.readiness.record("a", false);

        let propagated = state.propagate_downstream("a", true);
        assert!(propagated.is_empty());
    }

    // ── Sync management tests ─────────────────────────────────────────

    #[test]
    fn request_sync_enqueues_when_auto_sync_enabled() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        assert!(state.request_sync("a"));
        assert_eq!(state.sync_queue.dequeue(), Some("a".to_string()));
    }

    #[test]
    fn request_sync_skips_when_auto_sync_false() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry("a", None)]);
        state.work_queue.dequeue();

        assert!(!state.request_sync("a"));
        assert!(state.sync_queue.is_empty());
    }

    #[test]
    fn request_sync_skips_when_already_syncing() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        state.syncing.insert("a".to_string());
        assert!(!state.request_sync("a"));
    }

    #[test]
    fn request_sync_dedup() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        assert!(state.request_sync("a"));
        assert!(!state.request_sync("a")); // duplicate rejected
    }

    #[test]
    fn next_syncable_serializes_same_sync_ref() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[
            asset_entry_with_sync_ref("a", "dbt-run"),
            asset_entry_with_sync_ref("b", "dbt-run"),
        ]);
        state.request_sync("a");
        state.request_sync("b");

        assert_eq!(state.next_syncable(), Some("a".to_string()));
        // "b" shares the same sync ref, so it must wait.
        assert_eq!(state.next_syncable(), None);
    }

    #[test]
    fn next_syncable_allows_different_sync_refs() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[
            asset_entry_with_sync_ref("a", "dbt-run"),
            asset_entry_with_sync_ref("b", "bq-copy"),
        ]);
        state.request_sync("a");
        state.request_sync("b");

        assert_eq!(state.next_syncable(), Some("a".to_string()));
        // "b" has a different sync ref, so it can run concurrently.
        assert_eq!(state.next_syncable(), Some("b".to_string()));
    }

    #[test]
    fn handle_sync_result_clears_syncing_and_enqueues_re_eval() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        state.syncing.insert("a".to_string());
        state.handle_sync_result("a", true, None);

        assert!(state.syncing.is_empty());
        assert_eq!(state.work_queue.dequeue(), Some("a".to_string()));
    }

    #[test]
    fn handle_sync_result_unblocks_same_sync_ref() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[
            asset_entry_with_sync_ref("a", "dbt-run"),
            asset_entry_with_sync_ref("b", "dbt-run"),
        ]);
        state.request_sync("a");
        state.request_sync("b");

        assert_eq!(state.next_syncable(), Some("a".to_string()));
        assert_eq!(state.next_syncable(), None);

        state.handle_sync_result("a", true, None);
        // Now "b" can proceed since "dbt-run" is no longer in use.
        assert_eq!(state.next_syncable(), Some("b".to_string()));
    }

    #[test]
    fn handle_eval_not_ready_with_auto_sync_requests_sync() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", false);
        state.handle_eval_result("a", &result);

        assert_eq!(state.sync_queue.dequeue(), Some("a".to_string()));
    }

    #[test]
    fn handle_eval_ready_does_not_request_sync() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.in_flight.insert("a".to_string());

        let result = eval_ok("a", true);
        state.handle_eval_result("a", &result);

        assert!(state.sync_queue.is_empty());
    }

    // ── Guardrail integration tests ────────────────────────────────────

    #[test]
    fn handle_sync_failure_applies_backoff() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.work_queue.dequeue();

        state.syncing.insert("a".to_string());
        state.handle_sync_result("a", false, None);

        assert!(!state.request_sync("a"));
    }

    #[test]
    fn handle_sync_failure_suspends_after_max() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            state.syncing.insert("a".to_string());
            state.handle_sync_result("a", false, None);
        }

        assert!(suspended_path(dir.path(), "a").unwrap().exists());
        assert!(!state.request_sync("a"));
    }

    #[test]
    fn request_sync_blocked_when_suspended() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        write_suspended(
            dir.path(),
            &SuspendedInfo {
                asset_name: "a".to_string(),
                reason: "test".to_string(),
                suspended_at: "2025-01-01T00:00:00Z".to_string(),
                execution_id: None,
            },
        )
        .unwrap();

        assert!(!state.request_sync("a"));
    }

    // ── on_eval_complete / on_sync_complete tests ──────────────────────

    #[test]
    fn on_eval_complete_updates_state() {
        let edges = vec![edge("a", "b")];
        let mut state = ServeState::new(&edges, PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry("a", None), asset_entry("b", None)]);
        state.in_flight.insert("a".to_string());
        while state.work_queue.dequeue().is_some() {}

        state.readiness.record("a", false);

        let join_result = Some(Ok(("a".to_string(), eval_ok("a", true))));
        state.on_eval_complete(join_result);

        assert!(!state.in_flight.contains("a"));
        assert_eq!(state.work_queue.dequeue(), Some("b".to_string()));
    }

    #[test]
    fn on_eval_complete_ignores_none() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry("a", None)]);
        while state.work_queue.dequeue().is_some() {}

        state.on_eval_complete(None);
        assert!(state.work_queue.dequeue().is_none());
    }

    #[test]
    fn on_sync_complete_success_resets_guardrail() {
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
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
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);

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
        let mut state = ServeState::new(&[], PathBuf::from("/tmp/nagi-test-suspended"));
        state.init(&[asset_entry_with_sync("a")]);
        state.syncing.insert("a".to_string());

        state.on_sync_complete(None);
        assert!(!state.syncing.is_empty());
    }

    // ── Degradation detection tests ──────────────────────────────────────

    #[test]
    fn degradation_suspends_when_ready_count_decreases() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);
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
        assert!(state.pending_sync_reeval.contains("a"));

        state.in_flight.insert("a".to_string());
        let degraded = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![ready_condition("c1"), not_ready_condition("c2")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &degraded);

        assert!(suspended_path(dir.path(), "a").unwrap().exists());
    }

    #[test]
    fn no_degradation_when_ready_count_same_or_increases() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);
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

        assert!(!suspended_path(dir.path(), "a").unwrap().exists());
    }

    #[test]
    fn auto_unsuspend_when_ready() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        // Manually suspend the asset.
        write_suspended(
            dir.path(),
            &SuspendedInfo {
                asset_name: "a".to_string(),
                reason: "test".to_string(),
                suspended_at: "2025-01-01T00:00:00Z".to_string(),
                execution_id: None,
            },
        )
        .unwrap();
        assert!(suspended_path(dir.path(), "a").unwrap().exists());

        // Evaluate returns Ready.
        state.in_flight.insert("a".to_string());
        let result = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: true,
            conditions: vec![ready_condition("c1")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &result);

        // Suspended flag should be removed.
        assert!(!suspended_path(dir.path(), "a").unwrap().exists());
        // Guardrail failure counter should be reset.
        assert_eq!(state.guardrail.consecutive_failures.get("a").copied(), None);
    }

    #[test]
    fn no_unsuspend_when_not_ready() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        write_suspended(
            dir.path(),
            &SuspendedInfo {
                asset_name: "a".to_string(),
                reason: "test".to_string(),
                suspended_at: "2025-01-01T00:00:00Z".to_string(),
                execution_id: None,
            },
        )
        .unwrap();

        state.in_flight.insert("a".to_string());
        let result = Ok(AssetEvalResult {
            asset_name: "a".to_string(),
            ready: false,
            conditions: vec![not_ready_condition("c1")],
            evaluation_id: None,
        });
        state.handle_eval_result("a", &result);

        // Suspended flag should remain.
        assert!(suspended_path(dir.path(), "a").unwrap().exists());
    }

    // ── release_sync_slot tests ─────────────────────────────────────────

    #[test]
    fn release_sync_slot_clears_syncing_and_ref() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync_ref("a", "dbt-default")]);
        state.syncing.insert("a".to_string());
        state.syncing_refs.insert("dbt-default".to_string());

        state.release_sync_slot("a");

        assert!(!state.syncing.contains("a"));
        assert!(!state.syncing_refs.contains("dbt-default"));
    }

    #[test]
    fn release_sync_slot_noop_if_not_syncing() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        state.release_sync_slot("a"); // should not panic
        assert!(!state.syncing.contains("a"));
    }

    // ── handle_sync_failure tests ───────────────────────────────────────

    #[test]
    fn handle_sync_failure_returns_none_below_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        let result = state.handle_sync_failure("a", None);
        assert!(result.is_none());
        assert!(!suspended_path(dir.path(), "a").unwrap().exists());
    }

    #[test]
    fn handle_sync_failure_suspends_at_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        for _ in 0..MAX_CONSECUTIVE_FAILURES - 1 {
            assert!(state.handle_sync_failure("a", None).is_none());
        }
        let result = state.handle_sync_failure("a", None);
        assert!(result.is_some());
        assert!(suspended_path(dir.path(), "a").unwrap().exists());
    }

    #[test]
    fn handle_sync_failure_records_execution_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            state.handle_sync_failure("a", Some("exec-123"));
        }
        let info = crate::serve::suspended::list_suspended(dir.path()).unwrap();
        assert_eq!(info[0].execution_id.as_deref(), Some("exec-123"));
    }

    // ── suspend_asset tests ─────────────────────────────────────────────

    #[test]
    fn suspend_asset_writes_flag_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = ServeState::new(&[], dir.path().to_path_buf());

        state.suspend_asset("a", "test reason", Some("exec-1"));

        let path = suspended_path(dir.path(), "a").unwrap();
        assert!(path.exists());
        let content = std::fs::read_to_string(path).unwrap();
        let info: SuspendedInfo = serde_json::from_str(&content).unwrap();
        assert_eq!(info.asset_name, "a");
        assert_eq!(info.reason, "test reason");
        assert_eq!(info.execution_id.as_deref(), Some("exec-1"));
    }

    #[test]
    fn suspend_asset_without_execution_id() {
        let dir = tempfile::tempdir().unwrap();
        let state = ServeState::new(&[], dir.path().to_path_buf());

        state.suspend_asset("a", "test reason", None);

        let info = crate::serve::suspended::list_suspended(dir.path()).unwrap();
        assert_eq!(info[0].asset_name, "a");
        assert!(info[0].execution_id.is_none());
    }

    // ── check_degradation tests ─────────────────────────────────────────

    #[test]
    fn check_degradation_returns_none_when_improved() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.last_ready_count.insert("a".to_string(), 1);

        assert!(state.check_degradation("a", 2).is_none());
        assert!(!suspended_path(dir.path(), "a").unwrap().exists());
    }

    #[test]
    fn check_degradation_returns_none_when_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.last_ready_count.insert("a".to_string(), 2);

        assert!(state.check_degradation("a", 2).is_none());
    }

    #[test]
    fn check_degradation_suspends_when_decreased() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.last_ready_count.insert("a".to_string(), 3);

        let reason = state.check_degradation("a", 1);
        assert!(reason.is_some());
        assert!(suspended_path(dir.path(), "a").unwrap().exists());
    }

    #[test]
    fn check_degradation_returns_none_when_no_previous() {
        let dir = tempfile::tempdir().unwrap();
        let state = ServeState::new(&[], dir.path().to_path_buf());

        assert!(state.check_degradation("a", 1).is_none());
    }

    // ── try_auto_unsuspend tests ────────────────────────────────────────

    #[test]
    fn try_auto_unsuspend_noop_when_not_suspended() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServeState::new(&[], dir.path().to_path_buf());
        state.init(&[asset_entry_with_sync("a")]);

        state.try_auto_unsuspend("a"); // should not panic
    }
}

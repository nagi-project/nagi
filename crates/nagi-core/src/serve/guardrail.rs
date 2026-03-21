use std::collections::HashMap;
use std::time::Duration as StdDuration;

use tokio::time::Instant;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}

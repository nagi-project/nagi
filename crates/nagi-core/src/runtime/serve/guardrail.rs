use std::collections::HashMap;
use std::time::Duration as StdDuration;

use tokio::time::Instant;

use crate::runtime::config::{
    default_cooldown_initial_secs, default_cooldown_max_secs, default_max_consecutive_sync_failures,
};

/// Configuration for guardrail thresholds.
#[derive(Debug, Clone)]
pub struct GuardrailConfig {
    pub max_consecutive_failures: u32,
    pub cooldown_initial_secs: u64,
    pub cooldown_max_secs: u64,
}

impl Default for GuardrailConfig {
    fn default() -> Self {
        Self {
            max_consecutive_failures: default_max_consecutive_sync_failures(),
            cooldown_initial_secs: default_cooldown_initial_secs(),
            cooldown_max_secs: default_cooldown_max_secs(),
        }
    }
}

/// Tracks consecutive sync failures and cooldown timers per asset.
/// When failures reach the configured threshold, the asset should be
/// suspended (sync stopped, evaluate continues).
#[derive(Debug)]
pub struct GuardrailState {
    pub config: GuardrailConfig,
    pub consecutive_failures: HashMap<String, u32>,
    /// Earliest time at which the next sync attempt is allowed.
    next_sync_at: HashMap<String, Instant>,
}

impl Default for GuardrailState {
    fn default() -> Self {
        Self::new(GuardrailConfig::default())
    }
}

impl GuardrailState {
    pub fn new(config: GuardrailConfig) -> Self {
        Self {
            config,
            consecutive_failures: HashMap::new(),
            next_sync_at: HashMap::new(),
        }
    }

    pub fn record_sync_success(&mut self, asset_name: &str) {
        self.consecutive_failures.remove(asset_name);
        self.next_sync_at.remove(asset_name);
    }

    /// Increments the failure counter and sets the next cooldown time.
    /// Returns the new failure count.
    pub fn record_sync_failure(&mut self, asset_name: &str) -> u32 {
        let count = self
            .consecutive_failures
            .entry(asset_name.to_string())
            .or_insert(0);
        *count += 1;
        let current = *count;

        // Exponential cooldown: initial * 2^(failures-1), capped at max.
        let cooldown_secs = (self.config.cooldown_initial_secs
            * 2u64.saturating_pow(current.saturating_sub(1)))
        .min(self.config.cooldown_max_secs);
        self.next_sync_at.insert(
            asset_name.to_string(),
            Instant::now() + StdDuration::from_secs(cooldown_secs),
        );

        current
    }

    pub fn should_suspend(&self, asset_name: &str) -> bool {
        self.consecutive_failures
            .get(asset_name)
            .copied()
            .unwrap_or(0)
            >= self.config.max_consecutive_failures
    }

    /// Returns true if the asset is in a cooldown period
    /// (sync initiation is suppressed).
    pub fn is_in_cooldown(&self, asset_name: &str) -> bool {
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
        let mut g = GuardrailState::new(GuardrailConfig::default());
        g.record_sync_failure("a");
        g.record_sync_failure("a");
        assert_eq!(g.consecutive_failures.get("a").copied(), Some(2));

        g.record_sync_success("a");
        assert_eq!(g.consecutive_failures.get("a"), None);
        assert!(!g.is_in_cooldown("a"));
    }

    #[test]
    fn guardrail_suspend_after_max_failures() {
        let mut g = GuardrailState::new(GuardrailConfig::default());
        for _ in 0..g.config.max_consecutive_failures {
            g.record_sync_failure("a");
        }
        assert!(g.should_suspend("a"));
    }

    #[test]
    fn guardrail_no_suspend_below_max() {
        let mut g = GuardrailState::new(GuardrailConfig::default());
        for _ in 0..g.config.max_consecutive_failures - 1 {
            g.record_sync_failure("a");
        }
        assert!(!g.should_suspend("a"));
    }

    #[tokio::test]
    async fn guardrail_cooldown_active_after_failure() {
        tokio::time::pause();
        let mut g = GuardrailState::new(GuardrailConfig::default());
        g.record_sync_failure("a");
        assert!(g.is_in_cooldown("a"));

        // Advance past first cooldown (30s)
        tokio::time::advance(StdDuration::from_secs(31)).await;
        assert!(!g.is_in_cooldown("a"));
    }

    #[tokio::test]
    async fn guardrail_cooldown_increases_exponentially() {
        tokio::time::pause();

        // 1st failure: 30s cooldown
        let mut g = GuardrailState::new(GuardrailConfig::default());
        g.record_sync_failure("a");
        assert!(g.is_in_cooldown("a"));
        tokio::time::advance(StdDuration::from_secs(31)).await;
        assert!(!g.is_in_cooldown("a"));

        // 2nd failure (consecutive): 60s cooldown
        let count = g.record_sync_failure("a");
        assert_eq!(count, 2);
        tokio::time::advance(StdDuration::from_secs(59)).await;
        assert!(g.is_in_cooldown("a"));
        tokio::time::advance(StdDuration::from_secs(2)).await;
        assert!(!g.is_in_cooldown("a"));
    }

    #[test]
    fn guardrail_custom_config() {
        let config = GuardrailConfig {
            max_consecutive_failures: 5,
            cooldown_initial_secs: 60,
            cooldown_max_secs: 3600,
        };
        let mut g = GuardrailState::new(config);

        // 3 failures should not suspend with threshold of 5
        for _ in 0..3 {
            g.record_sync_failure("a");
        }
        assert!(!g.should_suspend("a"));

        // 5 failures should suspend
        for _ in 3..5 {
            g.record_sync_failure("a");
        }
        assert!(g.should_suspend("a"));
    }
}

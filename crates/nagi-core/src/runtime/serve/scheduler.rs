use std::collections::HashMap;
use std::time::Duration as StdDuration;

use tokio::time::Instant;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}

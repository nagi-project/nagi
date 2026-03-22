use serde::{Deserialize, Serialize};

/// Lock metadata written to the lock file. Shared across local and remote backends.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct LockInfo {
    /// Process ID of the lock holder.
    pub pid: u32,
    /// Unix epoch seconds when the lock was acquired.
    pub acquired_at_epoch_secs: u64,
    /// Time-to-live in seconds; the lock expires after this duration.
    pub ttl_secs: u64,
}

impl LockInfo {
    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.acquired_at_epoch_secs + self.ttl_secs
    }
}

pub mod local;
pub mod lock;
pub mod remote;

use std::collections::HashMap;
use std::time::Duration;

use thiserror::Error;

use crate::runtime::evaluate::AssetEvalResult;
use crate::runtime::serve::SuspendedInfo;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize/deserialize: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid filename: {0}")]
    InvalidFilename(String),
}

/// Validates that a string is a safe single-component filename (no path
/// separators, no `.` or `..`, no null bytes).
pub fn validate_filename(name: &str) -> Result<(), StorageError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err(StorageError::InvalidFilename(name.to_string()));
    }
    Ok(())
}

/// Reads and writes evaluate result caches per asset.
pub trait Cache: Send + Sync {
    fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError>;
}

/// Manages per-asset suspension flags.
pub trait SuspendedStore: Send + Sync + std::fmt::Debug {
    fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<SuspendedInfo>, StorageError>;
    fn remove(&self, asset_name: &str) -> Result<(), StorageError>;
    fn exists(&self, asset_name: &str) -> Result<bool, StorageError>;
}

/// Per-condition evaluate result with a timestamp, used for TTL-based caching.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConditionCacheEntry {
    pub result: crate::runtime::evaluate::ConditionResult,
    pub cached_at: String,
}

/// Per-asset map of condition name → cached result with timestamp.
pub type ConditionCacheMap = std::collections::HashMap<String, ConditionCacheEntry>;

/// Caches per-condition evaluate results with timestamps for TTL-based reuse.
/// Each condition is stored as a separate file under `{asset_name}/{condition_name}.json`.
pub trait ConditionCache: Send + Sync {
    fn write_condition(
        &self,
        asset_name: &str,
        condition_name: &str,
        entry: &ConditionCacheEntry,
    ) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<ConditionCacheMap>, StorageError>;
    fn write(&self, asset_name: &str, map: &ConditionCacheMap) -> Result<(), StorageError>;
}

/// Persists per-asset readiness (Ready / Not Ready) across process restarts.
///
/// Each asset is stored as a separate entry. An asset is considered Ready if
/// its entry exists and contains `ready: true`. Missing entries default to
/// Not Ready.
pub trait ReadinessStore: Send + Sync + std::fmt::Debug {
    /// Writes the readiness snapshot for all assets managed by one controller.
    /// Replaces previous entries for these assets.
    fn write_all(&self, readiness: &HashMap<String, bool>) -> Result<(), StorageError>;
    /// Reads all persisted readiness entries.
    fn read_all(&self) -> Result<HashMap<String, bool>, StorageError>;
}

/// Distributed lock for serializing sync execution per asset.
pub trait SyncLock: Send + Sync {
    /// Attempts to acquire the lock. Returns `true` if acquired.
    /// If the lock is held but its TTL has expired, it is stolen.
    /// `execution_id` is recorded in the lock file for correlation with sync logs.
    fn acquire(
        &self,
        sync_ref: &str,
        ttl: Duration,
        execution_id: &str,
    ) -> Result<bool, StorageError>;
    /// Releases the lock. No-op if not held.
    fn release(&self, sync_ref: &str) -> Result<(), StorageError>;
}

/// Creates a [`SyncLock`] implementation matching the configured backend type.
pub fn build_sync_lock(
    config: &crate::runtime::config::NagiConfig,
) -> Result<std::sync::Arc<dyn SyncLock>, StorageError> {
    match config.backend.r#type.as_str() {
        "local" => Ok(std::sync::Arc::new(local::LocalSyncLock::new(
            config.nagi_dir.locks_dir(),
        ))),
        "gcs" | "s3" => Ok(std::sync::Arc::new(remote::create_remote_store(
            &config.backend,
        )?)),
        t => Err(StorageError::Io(std::io::Error::other(format!(
            "unknown backend type: {t}"
        )))),
    }
}

/// Loads project config and builds a [`SyncLock`] with the configured TTL.
pub fn build_sync_lock_from_project(
    project_dir: &std::path::Path,
) -> Result<(std::sync::Arc<dyn SyncLock>, Duration), StorageError> {
    let config = crate::runtime::config::load_config(project_dir)
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
    let lock = build_sync_lock(&config)?;
    let ttl = Duration::from_secs(config.lock_ttl_seconds);
    Ok((lock, ttl))
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! validate_filename_test {
        ($($name:ident: $input:expr => $ok:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(validate_filename($input).is_ok(), $ok);
                }
            )*
        };
    }

    validate_filename_test! {
        valid_simple: "my-sync" => true;
        valid_with_dots: "check.freshness" => true;
        reject_empty: "" => false;
        reject_dot: "." => false;
        reject_dotdot: ".." => false;
        reject_slash: "a/b" => false;
        reject_backslash: "a\\b" => false;
        reject_null: "a\0b" => false;
        reject_traversal: "../etc" => false;
    }
}

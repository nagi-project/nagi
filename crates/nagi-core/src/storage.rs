pub mod local;
pub mod lock;
pub mod remote;

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

use crate::db::TableStats;
use crate::evaluate::AssetEvalResult;
use crate::serve::SuspendedInfo;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize/deserialize: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("cache directory does not exist: {path}")]
    NoCacheDir { path: PathBuf },
    #[error("invalid asset name: {0}")]
    InvalidAssetName(String),
}

/// Validates that the asset name is a safe filename component (no path
/// separators, no `.` or `..`, no null bytes).
pub fn validate_asset_name(asset_name: &str) -> Result<(), StorageError> {
    if asset_name.is_empty()
        || asset_name == "."
        || asset_name == ".."
        || asset_name.contains('/')
        || asset_name.contains('\\')
        || asset_name.contains('\0')
    {
        return Err(StorageError::InvalidAssetName(asset_name.to_string()));
    }
    Ok(())
}

/// Reads and writes evaluate result caches per asset.
pub trait Cache: Send + Sync {
    fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError>;
    fn list(&self) -> Result<Vec<AssetEvalResult>, StorageError>;
}

/// Manages per-asset suspension flags.
pub trait SuspendedStore: Send + Sync + std::fmt::Debug {
    fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<SuspendedInfo>, StorageError>;
    fn remove(&self, asset_name: &str) -> Result<(), StorageError>;
    fn exists(&self, asset_name: &str) -> Result<bool, StorageError>;
    fn list(&self) -> Result<Vec<SuspendedInfo>, StorageError>;
}

/// Caches `TableStats` per source for change detection.
pub trait SourceStatsCache: Send + Sync {
    fn read(&self, source_name: &str) -> Result<Option<TableStats>, StorageError>;
    fn write(&self, source_name: &str, stats: &TableStats) -> Result<(), StorageError>;
}

/// Per-condition evaluate result with a timestamp, used for TTL-based caching.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConditionCacheEntry {
    pub result: crate::evaluate::ConditionResult,
    pub cached_at: String,
}

/// Per-asset map of condition name → cached result with timestamp.
pub type ConditionCacheMap = std::collections::HashMap<String, ConditionCacheEntry>;

/// Caches per-condition evaluate results with timestamps for TTL-based reuse.
/// Each condition is stored as a separate file under `{asset_name}/{condition_name}.json`.
pub trait ConditionCache: Send + Sync {
    fn read_condition(
        &self,
        asset_name: &str,
        condition_name: &str,
    ) -> Result<Option<ConditionCacheEntry>, StorageError>;
    fn write_condition(
        &self,
        asset_name: &str,
        condition_name: &str,
        entry: &ConditionCacheEntry,
    ) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<ConditionCacheMap>, StorageError>;
    fn write(&self, asset_name: &str, map: &ConditionCacheMap) -> Result<(), StorageError>;
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

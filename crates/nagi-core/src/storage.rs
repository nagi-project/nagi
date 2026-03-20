pub mod local;
pub mod lock;

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

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

/// Reads and writes evaluate result caches per asset.
pub trait Cache: Send + Sync {
    fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError>;
    fn list(&self) -> Result<Vec<AssetEvalResult>, StorageError>;
}

/// Manages per-asset suspension flags.
pub trait SuspendedStore: Send + Sync {
    fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<SuspendedInfo>, StorageError>;
    fn remove(&self, asset_name: &str) -> Result<(), StorageError>;
    fn exists(&self, asset_name: &str) -> Result<bool, StorageError>;
    fn list(&self) -> Result<Vec<SuspendedInfo>, StorageError>;
}

/// Distributed lock for serializing sync execution per sync ref.
pub trait SyncLock: Send + Sync {
    /// Attempts to acquire the lock. Returns `true` if acquired.
    /// If the lock is held but its TTL has expired, it is stolen.
    fn acquire(&self, sync_ref: &str, ttl: Duration) -> Result<bool, StorageError>;
    /// Releases the lock. No-op if not held.
    fn release(&self, sync_ref: &str) -> Result<(), StorageError>;
}

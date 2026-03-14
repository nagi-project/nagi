pub mod local;

use std::path::PathBuf;

use thiserror::Error;

use crate::evaluate::AssetEvalResult;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize/deserialize cache: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("cache directory does not exist: {path}")]
    NoCacheDir { path: PathBuf },
}

/// Reads and writes evaluate result caches per asset.
pub trait Cache: Send + Sync {
    fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError>;
    fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError>;
    fn list(&self) -> Result<Vec<AssetEvalResult>, StorageError>;
}

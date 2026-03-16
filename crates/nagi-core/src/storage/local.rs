use std::path::PathBuf;

use crate::evaluate::AssetEvalResult;

use super::{Cache, StorageError};

/// Local file-based cache backend.
/// Stores evaluate results as `{cache_dir}/{asset_name}.json`.
pub struct LocalCache {
    cache_dir: PathBuf,
}

impl LocalCache {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    /// Creates a cache using the default directory: `~/.nagi/cache/`.
    pub fn default_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".nagi")
            .join("cache")
    }

    fn asset_path(&self, asset_name: &str) -> PathBuf {
        self.cache_dir.join(format!("{asset_name}.json"))
    }
}

impl Cache for LocalCache {
    fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError> {
        std::fs::create_dir_all(&self.cache_dir)?;
        let path = self.asset_path(&result.asset_name);
        let json = serde_json::to_string_pretty(result)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError> {
        let path = self.asset_path(asset_name);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(path)?;
        let result: AssetEvalResult = serde_json::from_str(&content)?;
        Ok(Some(result))
    }

    fn list(&self) -> Result<Vec<AssetEvalResult>, StorageError> {
        if !self.cache_dir.exists() {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        for entry in std::fs::read_dir(&self.cache_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let content = std::fs::read_to_string(&path)?;
                let result: AssetEvalResult = serde_json::from_str(&content)?;
                results.push(result);
            }
        }
        results.sort_by(|a, b| a.asset_name.cmp(&b.asset_name));
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::{ConditionResult, ConditionStatus};

    use super::*;

    fn sample_result(name: &str, ready: bool) -> AssetEvalResult {
        AssetEvalResult {
            asset_name: name.to_string(),
            ready,
            conditions: vec![ConditionResult {
                condition_name: "check".to_string(),
                condition_type: "SQL".to_string(),
                status: if ready {
                    ConditionStatus::Ready
                } else {
                    ConditionStatus::NotReady {
                        reason: "query returned false".to_string(),
                    }
                },
            }],
        }
    }

    #[test]
    fn write_and_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(dir.path().to_path_buf());
        let result = sample_result("daily-sales", true);

        cache.write(&result).unwrap();
        let loaded = cache.read("daily-sales").unwrap().unwrap();

        assert_eq!(loaded.asset_name, "daily-sales");
        assert!(loaded.ready);
        assert_eq!(loaded.conditions.len(), 1);
    }

    #[test]
    fn read_returns_none_for_missing_asset() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(dir.path().to_path_buf());

        let result = cache.read("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn list_returns_sorted_results() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(dir.path().to_path_buf());

        cache.write(&sample_result("zebra", true)).unwrap();
        cache.write(&sample_result("alpha", false)).unwrap();
        cache.write(&sample_result("middle", true)).unwrap();

        let results = cache.list().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].asset_name, "alpha");
        assert_eq!(results[1].asset_name, "middle");
        assert_eq!(results[2].asset_name, "zebra");
    }

    #[test]
    fn list_returns_empty_when_no_cache_dir() {
        let cache = LocalCache::new(PathBuf::from("/nonexistent/path/cache"));
        let results = cache.list().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn write_creates_directory_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("nested").join("cache");
        let cache = LocalCache::new(nested.clone());

        cache.write(&sample_result("test", true)).unwrap();
        assert!(nested.join("test.json").exists());
    }

    #[test]
    fn write_overwrites_existing_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(dir.path().to_path_buf());

        cache.write(&sample_result("asset", false)).unwrap();
        let first = cache.read("asset").unwrap().unwrap();
        assert!(!first.ready);

        cache.write(&sample_result("asset", true)).unwrap();
        let second = cache.read("asset").unwrap().unwrap();
        assert!(second.ready);
    }
}

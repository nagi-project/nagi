use std::path::PathBuf;

use crate::evaluate::AssetEvalResult;
use crate::serve::SuspendedInfo;

use super::{Cache, StorageError, SuspendedStore};

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

// ── LocalSuspendedStore ──────────────────────────────────────────────────────

/// Validates that the asset name is a safe filename component.
fn validate_asset_name(asset_name: &str) -> Result<(), StorageError> {
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

/// Local file-based suspended store.
/// Stores suspension flags as `{dir}/{asset_name}.json`.
pub struct LocalSuspendedStore {
    dir: PathBuf,
}

impl LocalSuspendedStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn default_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".nagi")
            .join("suspended")
    }

    fn asset_path(&self, asset_name: &str) -> Result<PathBuf, StorageError> {
        validate_asset_name(asset_name)?;
        Ok(self.dir.join(format!("{asset_name}.json")))
    }
}

impl SuspendedStore for LocalSuspendedStore {
    fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError> {
        validate_asset_name(&info.asset_name)?;
        std::fs::create_dir_all(&self.dir)?;
        let json = serde_json::to_string_pretty(info)?;
        std::fs::write(self.asset_path(&info.asset_name)?, json)?;
        Ok(())
    }

    fn remove(&self, asset_name: &str) -> Result<(), StorageError> {
        let path = self.asset_path(asset_name)?;
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn exists(&self, asset_name: &str) -> Result<bool, StorageError> {
        Ok(self.asset_path(asset_name)?.exists())
    }

    fn list(&self) -> Result<Vec<SuspendedInfo>, StorageError> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut result = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let data = std::fs::read_to_string(&path)?;
                if let Ok(info) = serde_json::from_str::<SuspendedInfo>(&data) {
                    result.push(info);
                }
            }
        }
        result.sort_by(|a, b| a.asset_name.cmp(&b.asset_name));
        Ok(result)
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
            evaluation_id: None,
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

    // ── LocalSuspendedStore tests ────────────────────────────────────────

    fn sample_suspended(name: &str) -> SuspendedInfo {
        SuspendedInfo {
            asset_name: name.to_string(),
            reason: "3 consecutive sync failures".to_string(),
            suspended_at: "2025-06-15T03:12:00Z".to_string(),
            execution_id: Some("exec-001".to_string()),
        }
    }

    #[test]
    fn suspended_write_and_exists() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSuspendedStore::new(dir.path().to_path_buf());

        assert!(!store.exists("a").unwrap());
        store.write(&sample_suspended("a")).unwrap();
        assert!(store.exists("a").unwrap());
    }

    #[test]
    fn suspended_remove() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSuspendedStore::new(dir.path().to_path_buf());

        store.write(&sample_suspended("a")).unwrap();
        store.remove("a").unwrap();
        assert!(!store.exists("a").unwrap());
    }

    #[test]
    fn suspended_remove_nonexistent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSuspendedStore::new(dir.path().to_path_buf());
        store.remove("nonexistent").unwrap();
    }

    #[test]
    fn suspended_list_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSuspendedStore::new(dir.path().to_path_buf());

        store.write(&sample_suspended("z-asset")).unwrap();
        store.write(&sample_suspended("a-asset")).unwrap();

        let list = store.list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].asset_name, "a-asset");
        assert_eq!(list[1].asset_name, "z-asset");
    }

    #[test]
    fn suspended_list_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSuspendedStore::new(dir.path().to_path_buf());
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn suspended_list_nonexistent_dir() {
        let store = LocalSuspendedStore::new(PathBuf::from("/tmp/nonexistent-nagi-test"));
        assert!(store.list().unwrap().is_empty());
    }

    macro_rules! validate_asset_name_test {
        ($($name:ident: $input:expr => $ok:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(validate_asset_name($input).is_ok(), $ok);
                }
            )*
        };
    }

    validate_asset_name_test! {
        valid_simple: "daily-sales" => true;
        valid_with_dots: "my.asset" => true;
        reject_empty: "" => false;
        reject_dot: "." => false;
        reject_dotdot: ".." => false;
        reject_slash: "a/b" => false;
        reject_backslash: "a\\b" => false;
        reject_null: "a\0b" => false;
    }
}

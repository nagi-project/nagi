use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use crate::runtime::evaluate::AssetEvalResult;
use crate::runtime::serve::SuspendedInfo;

use super::lock::LockInfo;
use super::{
    Cache, ConditionCache, ConditionCacheEntry, ConditionCacheMap, ReadinessStore, StorageError,
    SuspendedStore, SyncLock,
};

/// Local file-based cache backend.
/// Stores evaluate results as `{cache_dir}/{asset_name}.json`.
pub struct LocalCache {
    cache_dir: PathBuf,
}

impl LocalCache {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    fn asset_path(&self, asset_name: &str) -> Result<PathBuf, StorageError> {
        super::validate_filename(asset_name)?;
        Ok(self.cache_dir.join(format!("{asset_name}.json")))
    }
}

impl Cache for LocalCache {
    fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError> {
        std::fs::create_dir_all(&self.cache_dir)?;
        let path = self.asset_path(&result.asset_name)?;
        let json = serde_json::to_string_pretty(result)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError> {
        let path = self.asset_path(asset_name)?;
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(path)?;
        let result: AssetEvalResult = serde_json::from_str(&content)?;
        Ok(Some(result))
    }
}

/// Local file-based suspended store.
/// Stores suspension flags as `{dir}/{asset_name}.json`.
#[derive(Debug)]
pub struct LocalSuspendedStore {
    dir: PathBuf,
}

impl LocalSuspendedStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn asset_path(&self, asset_name: &str) -> Result<PathBuf, StorageError> {
        super::validate_filename(asset_name)?;
        Ok(self.dir.join(format!("{asset_name}.json")))
    }
}

impl SuspendedStore for LocalSuspendedStore {
    fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError> {
        super::validate_filename(&info.asset_name)?;
        std::fs::create_dir_all(&self.dir)?;
        let json = serde_json::to_string_pretty(info)?;
        std::fs::write(self.asset_path(&info.asset_name)?, json)?;
        Ok(())
    }

    fn read(&self, asset_name: &str) -> Result<Option<SuspendedInfo>, StorageError> {
        let path = self.asset_path(asset_name)?;
        match std::fs::read_to_string(path) {
            Ok(data) => Ok(Some(serde_json::from_str(&data)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
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
}

/// Local file-based cache for per-condition evaluate results with timestamps.
/// Stores as `{dir}/{asset_name}.json` containing a map of condition name → entry.
pub struct LocalConditionCache {
    dir: PathBuf,
}

impl LocalConditionCache {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn asset_dir(&self, asset_name: &str) -> Result<PathBuf, StorageError> {
        super::validate_filename(asset_name)?;
        Ok(self.dir.join(asset_name))
    }

    fn condition_path(
        &self,
        asset_name: &str,
        condition_name: &str,
    ) -> Result<PathBuf, StorageError> {
        let dir = self.asset_dir(asset_name)?;
        super::validate_filename(condition_name)?;
        Ok(dir.join(format!("{condition_name}.json")))
    }
}

impl ConditionCache for LocalConditionCache {
    fn write_condition(
        &self,
        asset_name: &str,
        condition_name: &str,
        entry: &ConditionCacheEntry,
    ) -> Result<(), StorageError> {
        let dir = self.asset_dir(asset_name)?;
        std::fs::create_dir_all(&dir)?;
        let json = serde_json::to_string_pretty(entry)?;
        std::fs::write(self.condition_path(asset_name, condition_name)?, json)?;
        Ok(())
    }

    fn read(&self, asset_name: &str) -> Result<Option<ConditionCacheMap>, StorageError> {
        let dir = self.asset_dir(asset_name)?;
        if !dir.exists() {
            return Ok(None);
        }
        let mut map = ConditionCacheMap::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "json") {
                let name = path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                let data = std::fs::read_to_string(&path)?;
                let cached: ConditionCacheEntry = serde_json::from_str(&data)?;
                map.insert(name, cached);
            }
        }
        if map.is_empty() {
            Ok(None)
        } else {
            Ok(Some(map))
        }
    }

    fn write(&self, asset_name: &str, map: &ConditionCacheMap) -> Result<(), StorageError> {
        for (condition_name, entry) in map {
            self.write_condition(asset_name, condition_name, entry)?;
        }
        Ok(())
    }
}

/// Local file-based readiness store.
/// Stores each asset's readiness as `{dir}/{asset_name}.json`.
#[derive(Debug)]
pub struct LocalReadinessStore {
    dir: PathBuf,
}

impl LocalReadinessStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn asset_path(&self, asset_name: &str) -> Result<PathBuf, StorageError> {
        super::validate_filename(asset_name)?;
        Ok(self.dir.join(format!("{asset_name}.json")))
    }
}

impl ReadinessStore for LocalReadinessStore {
    fn write_all(&self, readiness: &HashMap<String, bool>) -> Result<(), StorageError> {
        std::fs::create_dir_all(&self.dir)?;
        for (name, &ready) in readiness {
            super::validate_filename(name)?;
            let json = serde_json::to_string(&ready)?;
            std::fs::write(self.asset_path(name)?, json)?;
        }
        Ok(())
    }

    fn read_all(&self) -> Result<HashMap<String, bool>, StorageError> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(e.into()),
        };
        let mut result = HashMap::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let name = path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                let data = std::fs::read_to_string(&path)?;
                if let Ok(ready) = serde_json::from_str::<bool>(&data) {
                    result.insert(name, ready);
                }
            }
        }
        Ok(result)
    }
}

/// Local file-based sync lock.
/// Each asset gets a lock file at `{dir}/{asset_name}.lock`.
/// Uses atomic file creation for mutual exclusion and TTL for deadlock prevention.
pub struct LocalSyncLock {
    dir: PathBuf,
}

impl LocalSyncLock {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn lock_path(&self, sync_ref: &str) -> Result<PathBuf, StorageError> {
        super::validate_filename(sync_ref)?;
        Ok(self.dir.join(format!("{sync_ref}.lock")))
    }
}

/// Result of checking an existing lock file.
enum LockCheck {
    /// No lock file exists or it was cleared — safe to acquire.
    Cleared,
    /// Lock is actively held — cannot acquire.
    Held,
}

/// Checks an existing lock file and returns whether it can be cleared.
/// Removes expired or corrupted lock files.
fn check_existing_lock(path: &std::path::Path) -> Result<LockCheck, StorageError> {
    if !path.exists() {
        return Ok(LockCheck::Cleared);
    }
    match std::fs::read_to_string(path) {
        Ok(content) => {
            if let Ok(info) = serde_json::from_str::<LockInfo>(&content) {
                if info.is_expired() {
                    std::fs::remove_file(path)?;
                    Ok(LockCheck::Cleared)
                } else {
                    Ok(LockCheck::Held)
                }
            } else {
                std::fs::remove_file(path)?;
                Ok(LockCheck::Cleared)
            }
        }
        Err(_) => Ok(LockCheck::Held),
    }
}

impl SyncLock for LocalSyncLock {
    fn acquire(
        &self,
        sync_ref: &str,
        ttl: Duration,
        execution_id: &str,
    ) -> Result<bool, StorageError> {
        std::fs::create_dir_all(&self.dir)?;
        let path = self.lock_path(sync_ref)?;

        if matches!(check_existing_lock(&path)?, LockCheck::Held) {
            return Ok(false);
        }

        let info = LockInfo {
            execution_id: execution_id.to_string(),
            acquired_at_epoch_secs: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ttl_secs: ttl.as_secs(),
        };
        let json = serde_json::to_string_pretty(&info)?;

        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => {
                std::fs::write(&path, json)?;
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn release(&self, sync_ref: &str) -> Result<(), StorageError> {
        let path = self.lock_path(sync_ref)?;
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::evaluate::{ConditionResult, ConditionStatus};

    use super::*;

    impl LocalCache {
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

    impl LocalConditionCache {
        fn read_condition(
            &self,
            asset_name: &str,
            condition_name: &str,
        ) -> Result<Option<ConditionCacheEntry>, StorageError> {
            let path = self.condition_path(asset_name, condition_name)?;
            match std::fs::read_to_string(path) {
                Ok(data) => Ok(Some(serde_json::from_str(&data)?)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
    }

    impl LocalReadinessStore {
        fn read(&self, asset_name: &str) -> Result<Option<bool>, StorageError> {
            let path = self.asset_path(asset_name)?;
            match std::fs::read_to_string(path) {
                Ok(data) => Ok(Some(serde_json::from_str(&data)?)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
    }

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
                    ConditionStatus::Drifted {
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

    fn sample_condition_entry(name: &str, ready: bool) -> (String, ConditionCacheEntry) {
        use crate::runtime::evaluate::{ConditionResult, ConditionStatus};
        (
            name.to_string(),
            ConditionCacheEntry {
                result: ConditionResult {
                    condition_name: name.to_string(),
                    condition_type: "SQL".to_string(),
                    status: if ready {
                        ConditionStatus::Ready
                    } else {
                        ConditionStatus::Drifted {
                            reason: "failed".to_string(),
                        }
                    },
                },
                cached_at: "2025-01-01T00:00:00Z".to_string(),
            },
        )
    }

    #[test]
    fn condition_cache_write_and_read_single() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalConditionCache::new(dir.path().to_path_buf());
        let (name, entry) = sample_condition_entry("freshness", true);

        cache.write_condition("daily-sales", &name, &entry).unwrap();
        let loaded = cache
            .read_condition("daily-sales", "freshness")
            .unwrap()
            .unwrap();

        assert_eq!(loaded.result.condition_name, "freshness");
        assert_eq!(loaded.cached_at, "2025-01-01T00:00:00Z");
    }

    #[test]
    fn condition_cache_read_returns_none_for_missing() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalConditionCache::new(dir.path().to_path_buf());

        assert!(cache
            .read_condition("no-asset", "no-cond")
            .unwrap()
            .is_none());
    }

    #[test]
    fn condition_cache_read_aggregates_all_conditions() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalConditionCache::new(dir.path().to_path_buf());
        let (n1, e1) = sample_condition_entry("freshness", true);
        let (n2, e2) = sample_condition_entry("data-test", false);

        cache.write_condition("asset-a", &n1, &e1).unwrap();
        cache.write_condition("asset-a", &n2, &e2).unwrap();

        let map = cache.read("asset-a").unwrap().unwrap();
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("freshness"));
        assert!(map.contains_key("data-test"));
    }

    #[test]
    fn condition_cache_read_returns_none_for_missing_asset() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalConditionCache::new(dir.path().to_path_buf());

        assert!(cache.read("nonexistent").unwrap().is_none());
    }

    #[test]
    fn condition_cache_write_map_creates_per_condition_files() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalConditionCache::new(dir.path().to_path_buf());
        let map: ConditionCacheMap = [
            sample_condition_entry("check-a", true),
            sample_condition_entry("check-b", false),
        ]
        .into_iter()
        .collect();

        cache.write("my-asset", &map).unwrap();

        assert!(dir.path().join("my-asset").join("check-a.json").exists());
        assert!(dir.path().join("my-asset").join("check-b.json").exists());
    }

    #[test]
    fn condition_cache_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalConditionCache::new(dir.path().to_path_buf());
        let (name, entry1) = sample_condition_entry("check", false);

        cache.write_condition("asset", &name, &entry1).unwrap();
        let loaded = cache.read_condition("asset", "check").unwrap().unwrap();
        assert!(matches!(
            loaded.result.status,
            crate::runtime::evaluate::ConditionStatus::Drifted { .. }
        ));

        let (_, entry2) = sample_condition_entry("check", true);
        cache.write_condition("asset", "check", &entry2).unwrap();
        let loaded = cache.read_condition("asset", "check").unwrap().unwrap();
        assert!(matches!(
            loaded.result.status,
            crate::runtime::evaluate::ConditionStatus::Ready
        ));
    }

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
    fn readiness_write_all_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalReadinessStore::new(dir.path().to_path_buf());
        let mut map = HashMap::new();
        map.insert("asset-a".to_string(), true);
        map.insert("asset-b".to_string(), false);

        store.write_all(&map).unwrap();
        assert_eq!(store.read("asset-a").unwrap(), Some(true));
        assert_eq!(store.read("asset-b").unwrap(), Some(false));
    }

    #[test]
    fn readiness_read_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalReadinessStore::new(dir.path().to_path_buf());
        assert_eq!(store.read("nonexistent").unwrap(), None);
    }

    #[test]
    fn readiness_read_all_returns_map() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalReadinessStore::new(dir.path().to_path_buf());
        let mut map = HashMap::new();
        map.insert("x".to_string(), true);
        map.insert("y".to_string(), false);

        store.write_all(&map).unwrap();
        let loaded = store.read_all().unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded["x"]);
        assert!(!loaded["y"]);
    }

    #[test]
    fn readiness_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalReadinessStore::new(dir.path().to_path_buf());

        let mut map1 = HashMap::new();
        map1.insert("a".to_string(), false);
        store.write_all(&map1).unwrap();

        let mut map2 = HashMap::new();
        map2.insert("a".to_string(), true);
        store.write_all(&map2).unwrap();

        assert_eq!(store.read("a").unwrap(), Some(true));
    }

    #[test]
    fn condition_path_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalConditionCache::new(dir.path().to_path_buf());
        assert!(cache.condition_path("asset", "../etc").is_err());
        assert!(cache.condition_path("asset", "a/b").is_err());
        assert!(cache.condition_path("asset", "a\\b").is_err());
        assert!(cache.condition_path("asset", "valid-name").is_ok());
    }

    #[test]
    fn lock_path_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());
        assert!(lock.lock_path("../etc").is_err());
        assert!(lock.lock_path("a/b").is_err());
        assert!(lock.lock_path("a\\b").is_err());
        assert!(lock.lock_path("valid-ref").is_ok());
    }

    #[test]
    fn acquire_and_release() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());

        assert!(lock
            .acquire("dbt-run", Duration::from_secs(60), "exec-1")
            .unwrap());
        assert!(!lock
            .acquire("dbt-run", Duration::from_secs(60), "exec-1")
            .unwrap());

        lock.release("dbt-run").unwrap();
        assert!(lock
            .acquire("dbt-run", Duration::from_secs(60), "exec-1")
            .unwrap());
    }

    #[test]
    fn different_refs_independent() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());

        assert!(lock
            .acquire("ref-a", Duration::from_secs(60), "exec-1")
            .unwrap());
        assert!(lock
            .acquire("ref-b", Duration::from_secs(60), "exec-2")
            .unwrap());
    }

    #[test]
    fn expired_lock_is_stolen() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());

        std::fs::create_dir_all(dir.path()).unwrap();
        let info = LockInfo {
            execution_id: "exec-stale".to_string(),
            acquired_at_epoch_secs: 0,
            ttl_secs: 1,
        };
        let json = serde_json::to_string(&info).unwrap();
        std::fs::write(dir.path().join("stale.lock"), json).unwrap();

        assert!(lock
            .acquire("stale", Duration::from_secs(60), "exec-1")
            .unwrap());
    }

    #[test]
    fn corrupted_lock_file_is_cleaned() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());

        std::fs::create_dir_all(dir.path()).unwrap();
        std::fs::write(dir.path().join("bad.lock"), "not json").unwrap();

        assert!(lock
            .acquire("bad", Duration::from_secs(60), "exec-1")
            .unwrap());
    }

    #[test]
    fn creates_directory_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("nested").join("locks");
        let lock = LocalSyncLock::new(nested);

        assert!(lock
            .acquire("test", Duration::from_secs(60), "exec-1")
            .unwrap());
    }

    #[test]
    fn check_no_file_returns_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.lock");
        assert!(matches!(
            check_existing_lock(&path).unwrap(),
            LockCheck::Cleared
        ));
    }

    #[test]
    fn check_active_lock_returns_held() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("active.lock");
        let info = LockInfo {
            execution_id: "exec-active".to_string(),
            acquired_at_epoch_secs: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            ttl_secs: 3600,
        };
        std::fs::write(&path, serde_json::to_string(&info).unwrap()).unwrap();
        assert!(matches!(
            check_existing_lock(&path).unwrap(),
            LockCheck::Held
        ));
    }

    #[test]
    fn check_expired_lock_clears_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("expired.lock");
        let info = LockInfo {
            execution_id: "exec-stale".to_string(),
            acquired_at_epoch_secs: 0,
            ttl_secs: 1,
        };
        std::fs::write(&path, serde_json::to_string(&info).unwrap()).unwrap();
        assert!(matches!(
            check_existing_lock(&path).unwrap(),
            LockCheck::Cleared
        ));
        assert!(!path.exists());
    }

    #[test]
    fn check_corrupted_lock_clears_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.lock");
        std::fs::write(&path, "not json").unwrap();
        assert!(matches!(
            check_existing_lock(&path).unwrap(),
            LockCheck::Cleared
        ));
        assert!(!path.exists());
    }
}

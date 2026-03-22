use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use crate::db::TableStats;
use crate::evaluate::AssetEvalResult;
use crate::serve::SuspendedInfo;

use super::lock::LockInfo;
use super::{Cache, SourceStatsCache, StorageError, SuspendedStore, SyncLock};

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

    fn asset_path(&self, asset_name: &str) -> Result<PathBuf, StorageError> {
        super::validate_asset_name(asset_name)?;
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

    pub fn default_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".nagi")
            .join("suspended")
    }

    fn asset_path(&self, asset_name: &str) -> Result<PathBuf, StorageError> {
        super::validate_asset_name(asset_name)?;
        Ok(self.dir.join(format!("{asset_name}.json")))
    }
}

impl SuspendedStore for LocalSuspendedStore {
    fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError> {
        super::validate_asset_name(&info.asset_name)?;
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

// ── LocalSourceStatsCache ──────────────────────────────────────────────────

/// Local file-based cache for Source `TableStats`.
/// Stores stats as `{dir}/{source_name}.json`.
pub struct LocalSourceStatsCache {
    dir: PathBuf,
}

impl LocalSourceStatsCache {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn default_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".nagi")
            .join("source_stats")
    }

    fn source_path(&self, source_name: &str) -> Result<PathBuf, StorageError> {
        super::validate_asset_name(source_name)?;
        Ok(self.dir.join(format!("{source_name}.json")))
    }
}

impl SourceStatsCache for LocalSourceStatsCache {
    fn read(&self, source_name: &str) -> Result<Option<TableStats>, StorageError> {
        let path = self.source_path(source_name)?;
        match std::fs::read_to_string(path) {
            Ok(data) => Ok(Some(serde_json::from_str(&data)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn write(&self, source_name: &str, stats: &TableStats) -> Result<(), StorageError> {
        super::validate_asset_name(source_name)?;
        std::fs::create_dir_all(&self.dir)?;
        let json = serde_json::to_string_pretty(stats)?;
        std::fs::write(self.source_path(source_name)?, json)?;
        Ok(())
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
                    assert_eq!(crate::storage::validate_asset_name($input).is_ok(), $ok);
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

    // ── LocalSourceStatsCache tests ─────────────────────────────────────

    #[test]
    fn source_stats_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalSourceStatsCache::new(dir.path().to_path_buf());
        let stats = TableStats {
            num_rows: 100,
            num_bytes: 2048,
        };

        cache.write("my-source", &stats).unwrap();
        let loaded = cache.read("my-source").unwrap().unwrap();
        assert_eq!(loaded, stats);
    }

    #[test]
    fn source_stats_read_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalSourceStatsCache::new(dir.path().to_path_buf());
        assert!(cache.read("nonexistent").unwrap().is_none());
    }

    #[test]
    fn source_stats_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalSourceStatsCache::new(dir.path().to_path_buf());

        let v1 = TableStats {
            num_rows: 10,
            num_bytes: 100,
        };
        let v2 = TableStats {
            num_rows: 20,
            num_bytes: 200,
        };

        cache.write("src", &v1).unwrap();
        cache.write("src", &v2).unwrap();
        let loaded = cache.read("src").unwrap().unwrap();
        assert_eq!(loaded, v2);
    }
}

// ── LocalSyncLock ────────────────────────────────────────────────────────────

/// Local file-based sync lock.
/// Each sync ref gets a lock file at `{dir}/{sync_ref}.lock`.
/// Uses atomic file creation for mutual exclusion and TTL for deadlock prevention.
pub struct LocalSyncLock {
    dir: PathBuf,
}

impl LocalSyncLock {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn default_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".nagi")
            .join("locks")
    }

    fn lock_path(&self, sync_ref: &str) -> PathBuf {
        self.dir.join(format!("{sync_ref}.lock"))
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
        let path = self.lock_path(sync_ref);

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
        let path = self.lock_path(sync_ref);
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;

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
    fn release_nonexistent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());
        lock.release("nonexistent").unwrap();
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

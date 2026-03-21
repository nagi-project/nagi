use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use super::{StorageError, SyncLock};

/// Lock metadata written to the lock file.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct LockInfo {
    /// Process ID of the lock holder.
    pid: u32,
    /// Unix epoch seconds when the lock was acquired.
    acquired_at_epoch_secs: u64,
    /// Time-to-live in seconds; the lock expires after this duration.
    ttl_secs: u64,
}

impl LockInfo {
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.acquired_at_epoch_secs + self.ttl_secs
    }
}

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
/// Removes expired or corrupted lock files. Returns `Held` if the lock
/// is active or unreadable.
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
                // Corrupted lock file — remove and allow re-acquire.
                std::fs::remove_file(path)?;
                Ok(LockCheck::Cleared)
            }
        }
        Err(_) => {
            // Cannot read — treat as held.
            Ok(LockCheck::Held)
        }
    }
}

impl SyncLock for LocalSyncLock {
    fn acquire(&self, sync_ref: &str, ttl: Duration) -> Result<bool, StorageError> {
        std::fs::create_dir_all(&self.dir)?;
        let path = self.lock_path(sync_ref);

        if matches!(check_existing_lock(&path)?, LockCheck::Held) {
            return Ok(false);
        }

        let info = LockInfo {
            pid: std::process::id(),
            acquired_at_epoch_secs: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ttl_secs: ttl.as_secs(),
        };
        let json = serde_json::to_string_pretty(&info)?;

        // Attempt atomic creation via create_new.
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
mod tests {
    use super::*;

    #[test]
    fn acquire_and_release() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());

        assert!(lock.acquire("dbt-run", Duration::from_secs(60)).unwrap());
        // Second acquire fails — lock is held.
        assert!(!lock.acquire("dbt-run", Duration::from_secs(60)).unwrap());

        lock.release("dbt-run").unwrap();
        // Now it can be re-acquired.
        assert!(lock.acquire("dbt-run", Duration::from_secs(60)).unwrap());
    }

    #[test]
    fn different_refs_independent() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());

        assert!(lock.acquire("ref-a", Duration::from_secs(60)).unwrap());
        assert!(lock.acquire("ref-b", Duration::from_secs(60)).unwrap());
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

        // Write a lock that expired in the past.
        std::fs::create_dir_all(dir.path()).unwrap();
        let info = LockInfo {
            pid: 99999,
            acquired_at_epoch_secs: 0, // epoch — definitely expired
            ttl_secs: 1,
        };
        let json = serde_json::to_string(&info).unwrap();
        std::fs::write(dir.path().join("stale.lock"), json).unwrap();

        // Should steal the expired lock.
        assert!(lock.acquire("stale", Duration::from_secs(60)).unwrap());
    }

    #[test]
    fn corrupted_lock_file_is_cleaned() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalSyncLock::new(dir.path().to_path_buf());

        std::fs::create_dir_all(dir.path()).unwrap();
        std::fs::write(dir.path().join("bad.lock"), "not json").unwrap();

        assert!(lock.acquire("bad", Duration::from_secs(60)).unwrap());
    }

    #[test]
    fn creates_directory_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("nested").join("locks");
        let lock = LocalSyncLock::new(nested);

        assert!(lock.acquire("test", Duration::from_secs(60)).unwrap());
    }

    // ── check_existing_lock unit tests ──

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
            pid: std::process::id(),
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
            pid: 99999,
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

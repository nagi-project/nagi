use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};

use crate::runtime::config::BackendConfig;
use crate::runtime::evaluate::AssetEvalResult;
use crate::runtime::serve::SuspendedInfo;
use crate::runtime::storage::lock::LockInfo;
use crate::runtime::storage::{Cache, ReadinessStore, StorageError, SuspendedStore, SyncLock};

/// Remote storage backend backed by an `ObjectStore` (GCS or S3).
///
/// All operations are performed synchronously by blocking on the async
/// object store via `tokio::task::block_in_place`. This requires the
/// multi-threaded tokio runtime.
pub struct RemoteObjectStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl std::fmt::Debug for RemoteObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteObjectStore")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl RemoteObjectStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> Self {
        Self { store, prefix }
    }

    fn resolve(&self, key: &str) -> OsPath {
        match &self.prefix {
            Some(p) => OsPath::from(format!("{p}/{key}")),
            None => OsPath::from(key),
        }
    }

    fn cache_path(&self, asset_name: &str) -> Result<OsPath, StorageError> {
        super::validate_filename(asset_name)?;
        Ok(self.resolve(&format!("cache/{asset_name}.json")))
    }

    fn suspended_path(&self, asset_name: &str) -> Result<OsPath, StorageError> {
        super::validate_filename(asset_name)?;
        Ok(self.resolve(&format!("suspended/{asset_name}.json")))
    }

    fn readiness_path(&self, asset_name: &str) -> Result<OsPath, StorageError> {
        super::validate_filename(asset_name)?;
        Ok(self.resolve(&format!("readiness/{asset_name}.json")))
    }

    fn lock_path(&self, sync_ref: &str) -> Result<OsPath, StorageError> {
        super::validate_filename(sync_ref)?;
        Ok(self.resolve(&format!("locks/{sync_ref}.lock")))
    }

    /// Uploads a local file to the remote store. Returns the object store URI.
    pub async fn upload_file(
        &self,
        local_path: &std::path::Path,
        remote_path: &str,
    ) -> Result<String, StorageError> {
        let content = std::fs::read(local_path)?;
        let path = self.resolve(remote_path);
        self.store
            .put(&path, content.into())
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("upload failed: {e}"))))?;
        Ok(format!("{}", path))
    }
}

/// Runs a future on the current tokio runtime from a sync context.
///
/// Requires the multi-threaded runtime; panics on single-threaded runtimes.
fn block<F: std::future::Future>(fut: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
}

fn remote_err(e: object_store::Error) -> StorageError {
    StorageError::Io(std::io::Error::other(e.to_string()))
}

fn serde_err(e: serde_json::Error) -> StorageError {
    StorageError::Serde(e)
}

impl Cache for RemoteObjectStore {
    fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError> {
        let path = self.cache_path(&result.asset_name)?;
        let bytes = serde_json::to_vec(result).map_err(serde_err)?;
        block(self.store.put(&path, bytes.into())).map_err(remote_err)?;
        Ok(())
    }

    fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError> {
        let path = self.cache_path(asset_name)?;
        match block(self.store.get(&path)) {
            Ok(result) => {
                let bytes = block(result.bytes()).map_err(remote_err)?;
                let value = serde_json::from_slice(&bytes).map_err(serde_err)?;
                Ok(Some(value))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(remote_err(e)),
        }
    }
}

impl SuspendedStore for RemoteObjectStore {
    fn write(&self, info: &SuspendedInfo) -> Result<(), StorageError> {
        let path = self.suspended_path(&info.asset_name)?;
        let bytes = serde_json::to_vec(info).map_err(serde_err)?;
        block(self.store.put(&path, bytes.into())).map_err(remote_err)?;
        Ok(())
    }

    fn read(&self, asset_name: &str) -> Result<Option<SuspendedInfo>, StorageError> {
        let path = self.suspended_path(asset_name)?;
        match block(self.store.get(&path)) {
            Ok(result) => {
                let bytes = block(result.bytes()).map_err(remote_err)?;
                let value = serde_json::from_slice(&bytes).map_err(serde_err)?;
                Ok(Some(value))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(remote_err(e)),
        }
    }

    fn remove(&self, asset_name: &str) -> Result<(), StorageError> {
        let path = self.suspended_path(asset_name)?;
        match block(self.store.delete(&path)) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(remote_err(e)),
        }
    }

    fn exists(&self, asset_name: &str) -> Result<bool, StorageError> {
        let path = self.suspended_path(asset_name)?;
        match block(self.store.head(&path)) {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(remote_err(e)),
        }
    }
}

impl ReadinessStore for RemoteObjectStore {
    fn write_all(&self, readiness: &HashMap<String, bool>) -> Result<(), StorageError> {
        for (name, &ready) in readiness {
            let path = self.readiness_path(name)?;
            let bytes = serde_json::to_vec(&ready).map_err(serde_err)?;
            block(self.store.put(&path, bytes.into())).map_err(remote_err)?;
        }
        Ok(())
    }

    fn read_all(&self) -> Result<HashMap<String, bool>, StorageError> {
        let prefix = self.resolve("readiness/");
        let objects = block(self.store.list_with_delimiter(Some(&prefix))).map_err(remote_err)?;
        let mut result = HashMap::new();
        for obj in objects.objects {
            let name = obj
                .location
                .filename()
                .unwrap_or_default()
                .strip_suffix(".json")
                .unwrap_or_default()
                .to_string();
            if name.is_empty() {
                continue;
            }
            match block(self.store.get(&obj.location)) {
                Ok(r) => {
                    let bytes = block(r.bytes()).map_err(remote_err)?;
                    if let Ok(ready) = serde_json::from_slice::<bool>(&bytes) {
                        result.insert(name, ready);
                    }
                }
                Err(e) => tracing::warn!(error = %e, "failed to read readiness entry"),
            }
        }
        Ok(result)
    }
}

impl SyncLock for RemoteObjectStore {
    /// Attempts to acquire the lock using a conditional put (`PutMode::Create`).
    ///
    /// If the object already exists, reads it and checks whether the TTL has
    /// expired. If expired, deletes the stale lock and retries once. Returns
    /// `true` if the lock was acquired, `false` otherwise.
    fn acquire(
        &self,
        sync_ref: &str,
        ttl: Duration,
        execution_id: &str,
    ) -> Result<bool, StorageError> {
        let path = self.lock_path(sync_ref)?;
        let info = LockInfo {
            execution_id: execution_id.to_string(),
            acquired_at_epoch_secs: now_epoch_secs(),
            ttl_secs: ttl.as_secs(),
        };
        let bytes = serde_json::to_vec(&info).map_err(serde_err)?;

        if try_create(&self.store, &path, bytes.clone())? {
            return Ok(true);
        }

        steal_expired_lock(&self.store, &path, bytes)
    }

    fn release(&self, sync_ref: &str) -> Result<(), StorageError> {
        let path = self.lock_path(sync_ref)?;
        match block(self.store.delete(&path)) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(remote_err(e)),
        }
    }
}

/// Attempts to steal an expired lock and replace it with `new_bytes`.
///
/// Result of reading an existing lock file.
enum LockReadResult {
    /// Lock is held and not yet expired.
    Held,
    /// Lock has expired; contains the holder's `execution_id` (None if corrupted).
    Expired(Option<String>),
    /// Lock file no longer exists.
    NotFound,
}

/// Reads the lock at `path` and returns whether it is held, expired, or gone.
fn read_lock(store: &Arc<dyn ObjectStore>, path: &OsPath) -> Result<LockReadResult, StorageError> {
    match block(store.get(path)) {
        Ok(r) => {
            let b = block(r.bytes()).map_err(remote_err)?;
            match serde_json::from_slice::<LockInfo>(&b) {
                Ok(lock) if lock.is_expired() => {
                    Ok(LockReadResult::Expired(Some(lock.execution_id)))
                }
                Ok(_) => Ok(LockReadResult::Held),
                Err(_) => Ok(LockReadResult::Expired(None)), // treat corrupted lock as expired
            }
        }
        Err(object_store::Error::NotFound { .. }) => Ok(LockReadResult::NotFound),
        Err(e) => Err(remote_err(e)),
    }
}

/// Deletes the lock at `path` and creates a new one with `new_bytes`, but only
/// if the current holder's `execution_id` matches `expected_id`.
///
/// Returns `Ok(true)` if the new lock was created, `Ok(false)` if the holder
/// changed between reads (another process already stole the lock).
fn replace_if_holder_unchanged(
    store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    expected_id: Option<String>,
    new_bytes: Vec<u8>,
) -> Result<bool, StorageError> {
    let current_holder = match block(store.get(path)) {
        Ok(r) => {
            let b = block(r.bytes()).map_err(remote_err)?;
            serde_json::from_slice::<LockInfo>(&b)
                .map(|l| l.execution_id)
                .ok()
        }
        Err(object_store::Error::NotFound { .. }) => {
            return try_create(store, path, new_bytes);
        }
        Err(e) => return Err(remote_err(e)),
    };
    if current_holder != expected_id {
        return Ok(false);
    }
    if let Err(e) = block(store.delete(path)) {
        tracing::warn!(error = %e, "failed to delete lock before re-creation");
    }
    try_create(store, path, new_bytes)
}

/// Attempts to steal an expired lock and replace it with `new_bytes`.
fn steal_expired_lock(
    store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    new_bytes: Vec<u8>,
) -> Result<bool, StorageError> {
    let holder_id = match read_lock(store, path)? {
        LockReadResult::NotFound => return try_create(store, path, new_bytes),
        LockReadResult::Held => return Ok(false),
        LockReadResult::Expired(id) => id,
    };
    replace_if_holder_unchanged(store, path, holder_id, new_bytes)
}

/// Tries to atomically create an object. Returns `true` if created, `false` if
/// the object already exists.
fn try_create(
    store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    bytes: Vec<u8>,
) -> Result<bool, StorageError> {
    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match block(store.put_opts(path, bytes.into(), opts)) {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(e) => Err(remote_err(e)),
    }
}

fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Creates a `RemoteObjectStore` from the given `BackendConfig`.
///
/// Credentials are resolved from the environment at call time and not stored:
/// - GCS: Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS` or
///   `gcloud auth application-default login`)
/// - S3: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / IAM role
///
/// Returns an error if `bucket` is missing or the backend type is unknown.
pub fn create_remote_store(config: &BackendConfig) -> Result<RemoteObjectStore, StorageError> {
    let bucket = config.bucket.as_deref().ok_or_else(|| {
        StorageError::Io(std::io::Error::other(
            "backend.bucket is required for remote backends",
        ))
    })?;

    let store: Arc<dyn ObjectStore> = match config.r#type.as_str() {
        "gcs" => {
            // Credentials are resolved from the environment (ADC) at build time.
            let store = object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e: object_store::Error| {
                    StorageError::Io(std::io::Error::other(e.to_string()))
                })?;
            Arc::new(store)
        }
        "s3" => {
            let region = config.region.as_deref().ok_or_else(|| {
                StorageError::Io(std::io::Error::other(
                    "backend.region is required for s3 backend",
                ))
            })?;
            // Credentials are resolved from the environment (env vars / IAM).
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_region(region)
                .build()
                .map_err(|e: object_store::Error| {
                    StorageError::Io(std::io::Error::other(e.to_string()))
                })?;
            Arc::new(store)
        }
        t => {
            return Err(StorageError::Io(std::io::Error::other(format!(
                "unknown backend type: {t}"
            ))))
        }
    };

    Ok(RemoteObjectStore::new(store, config.prefix.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RemoteObjectStore {
        fn cache_list(&self) -> Result<Vec<AssetEvalResult>, StorageError> {
            let prefix = self.resolve("cache/");
            let objects =
                block(self.store.list_with_delimiter(Some(&prefix))).map_err(remote_err)?;
            let mut results = Vec::new();
            for obj in objects.objects {
                match block(self.store.get(&obj.location)) {
                    Ok(r) => {
                        let bytes = block(r.bytes()).map_err(remote_err)?;
                        match serde_json::from_slice::<AssetEvalResult>(&bytes) {
                            Ok(v) => results.push(v),
                            Err(e) => tracing::warn!(error = %e, "failed to parse cache entry"),
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "failed to read cache entry"),
                }
            }
            Ok(results)
        }

        fn suspended_list(&self) -> Result<Vec<SuspendedInfo>, StorageError> {
            let prefix = self.resolve("suspended/");
            let objects =
                block(self.store.list_with_delimiter(Some(&prefix))).map_err(remote_err)?;
            let mut results = Vec::new();
            for obj in objects.objects {
                match block(self.store.get(&obj.location)) {
                    Ok(r) => {
                        let bytes = block(r.bytes()).map_err(remote_err)?;
                        match serde_json::from_slice::<SuspendedInfo>(&bytes) {
                            Ok(v) => results.push(v),
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to parse suspended entry")
                            }
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "failed to read suspended entry"),
                }
            }
            Ok(results)
        }

        fn readiness_read(&self, asset_name: &str) -> Result<Option<bool>, StorageError> {
            let path = self.readiness_path(asset_name)?;
            match block(self.store.get(&path)) {
                Ok(result) => {
                    let bytes = block(result.bytes()).map_err(remote_err)?;
                    let value = serde_json::from_slice(&bytes).map_err(serde_err)?;
                    Ok(Some(value))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(remote_err(e)),
            }
        }
    }
    use crate::runtime::evaluate::{ConditionResult, ConditionStatus};
    use crate::runtime::storage::{Cache, SuspendedStore, SyncLock};

    fn in_memory_store(prefix: Option<&str>) -> RemoteObjectStore {
        RemoteObjectStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            prefix.map(str::to_string),
        )
    }

    fn sample_eval(name: &str) -> AssetEvalResult {
        AssetEvalResult {
            asset_name: name.to_string(),
            ready: true,
            conditions: vec![ConditionResult {
                condition_name: "check".to_string(),
                condition_type: "SQL".to_string(),
                status: ConditionStatus::Ready,
            }],
            evaluation_id: Some("eval-001".to_string()),
        }
    }

    fn sample_suspended(name: &str) -> SuspendedInfo {
        SuspendedInfo {
            asset_name: name.to_string(),
            reason: "test".to_string(),
            suspended_at: "2026-03-22T00:00:00Z".to_string(),
            execution_id: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_write_and_read() {
        let store = in_memory_store(None);
        let result = sample_eval("asset-a");
        Cache::write(&store, &result).unwrap();
        let got = Cache::read(&store, "asset-a").unwrap().unwrap();
        assert_eq!(got.asset_name, "asset-a");
        assert!(got.ready);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_read_missing_returns_none() {
        let store = in_memory_store(None);
        assert!(Cache::read(&store, "nonexistent").unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_list_returns_all() {
        let store = in_memory_store(None);
        Cache::write(&store, &sample_eval("a")).unwrap();
        Cache::write(&store, &sample_eval("b")).unwrap();
        let mut names: Vec<_> = store
            .cache_list()
            .unwrap()
            .into_iter()
            .map(|r| r.asset_name)
            .collect();
        names.sort();
        assert_eq!(names, ["a", "b"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_respects_prefix() {
        let store = in_memory_store(Some("proj/nagi"));
        Cache::write(&store, &sample_eval("asset-x")).unwrap();
        let got = Cache::read(&store, "asset-x").unwrap().unwrap();
        assert_eq!(got.asset_name, "asset-x");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn suspended_write_read_remove() {
        let store = in_memory_store(None);
        let info = sample_suspended("asset-b");
        SuspendedStore::write(&store, &info).unwrap();
        assert!(SuspendedStore::exists(&store, "asset-b").unwrap());
        let got = SuspendedStore::read(&store, "asset-b").unwrap().unwrap();
        assert_eq!(got.reason, "test");
        SuspendedStore::remove(&store, "asset-b").unwrap();
        assert!(!SuspendedStore::exists(&store, "asset-b").unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn suspended_remove_nonexistent_is_noop() {
        let store = in_memory_store(None);
        SuspendedStore::remove(&store, "ghost").unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn suspended_list() {
        let store = in_memory_store(None);
        SuspendedStore::write(&store, &sample_suspended("x")).unwrap();
        SuspendedStore::write(&store, &sample_suspended("y")).unwrap();
        let mut names: Vec<_> = store
            .suspended_list()
            .unwrap()
            .into_iter()
            .map(|i| i.asset_name)
            .collect();
        names.sort();
        assert_eq!(names, ["x", "y"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn readiness_write_all_and_read() {
        let store = in_memory_store(None);
        let mut map = HashMap::new();
        map.insert("a".to_string(), true);
        map.insert("b".to_string(), false);
        ReadinessStore::write_all(&store, &map).unwrap();
        assert_eq!(store.readiness_read("a").unwrap(), Some(true));
        assert_eq!(store.readiness_read("b").unwrap(), Some(false));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn readiness_read_missing_returns_none() {
        let store = in_memory_store(None);
        assert_eq!(store.readiness_read("nope").unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn readiness_read_all() {
        let store = in_memory_store(None);
        let mut map = HashMap::new();
        map.insert("x".to_string(), true);
        map.insert("y".to_string(), false);
        ReadinessStore::write_all(&store, &map).unwrap();
        let loaded = ReadinessStore::read_all(&store).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded["x"]);
        assert!(!loaded["y"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn readiness_respects_prefix() {
        let store = in_memory_store(Some("proj/nagi"));
        let mut map = HashMap::new();
        map.insert("z".to_string(), true);
        ReadinessStore::write_all(&store, &map).unwrap();
        assert_eq!(store.readiness_read("z").unwrap(), Some(true));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_lock_acquire_and_release() {
        let store = in_memory_store(None);
        let ttl = Duration::from_secs(3600);
        assert!(store.acquire("ref-1", ttl, "exec-1").unwrap());
        store.release("ref-1").unwrap();
        // After release, can acquire again.
        assert!(store.acquire("ref-1", ttl, "exec-1").unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_lock_second_acquire_returns_false() {
        let store = in_memory_store(None);
        let store2 = RemoteObjectStore::new(store.store.clone(), None);
        let ttl = Duration::from_secs(3600);
        assert!(store.acquire("ref-2", ttl, "exec-1").unwrap());
        assert!(!store2.acquire("ref-2", ttl, "exec-2").unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_lock_acquires_expired_lock() {
        let store = in_memory_store(None);
        let expired_ttl = Duration::from_secs(0); // immediately expired
        assert!(store.acquire("ref-3", expired_ttl, "exec-1").unwrap());
        // Second store can steal the expired lock.
        let store2 = RemoteObjectStore::new(store.store.clone(), None);
        // Sleep 1s to ensure the lock is expired.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(store2
            .acquire("ref-3", Duration::from_secs(3600), "exec-2")
            .unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_lock_release_nonexistent_is_noop() {
        let store = in_memory_store(None);
        store.release("ref-ghost").unwrap();
    }

    fn write_lock_info(store: &RemoteObjectStore, sync_ref: &str, info: &LockInfo) {
        let path = store.lock_path(sync_ref).unwrap();
        let bytes = serde_json::to_vec(info).unwrap();
        block(store.store.put(&path, bytes.into())).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_lock_returns_not_found_when_missing() {
        let store = in_memory_store(None);
        let path = store.lock_path("ref").unwrap();
        assert!(matches!(
            read_lock(&store.store, &path).unwrap(),
            LockReadResult::NotFound
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_lock_returns_held_for_active_lock() {
        let store = in_memory_store(None);
        write_lock_info(
            &store,
            "ref",
            &LockInfo {
                execution_id: "exec-1".to_string(),
                acquired_at_epoch_secs: now_epoch_secs(),
                ttl_secs: 3600,
            },
        );
        let path = store.lock_path("ref").unwrap();
        assert!(matches!(
            read_lock(&store.store, &path).unwrap(),
            LockReadResult::Held
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_lock_returns_expired_with_execution_id() {
        let store = in_memory_store(None);
        write_lock_info(
            &store,
            "ref",
            &LockInfo {
                execution_id: "exec-old".to_string(),
                acquired_at_epoch_secs: 0,
                ttl_secs: 1,
            },
        );
        let path = store.lock_path("ref").unwrap();
        assert!(matches!(
            read_lock(&store.store, &path).unwrap(),
            LockReadResult::Expired(Some(ref id)) if id == "exec-old"
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_lock_returns_expired_none_for_corrupted_content() {
        let store = in_memory_store(None);
        let path = store.lock_path("ref").unwrap();
        block(store.store.put(&path, b"not json".to_vec().into())).unwrap();
        assert!(matches!(
            read_lock(&store.store, &path).unwrap(),
            LockReadResult::Expired(None)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replace_if_holder_unchanged_succeeds_when_id_matches() {
        let store = in_memory_store(None);
        write_lock_info(
            &store,
            "ref",
            &LockInfo {
                execution_id: "exec-old".to_string(),
                acquired_at_epoch_secs: 0,
                ttl_secs: 1,
            },
        );
        let path = store.lock_path("ref").unwrap();
        let new_bytes = serde_json::to_vec(&LockInfo {
            execution_id: "exec-new".to_string(),
            acquired_at_epoch_secs: now_epoch_secs(),
            ttl_secs: 3600,
        })
        .unwrap();
        assert!(replace_if_holder_unchanged(
            &store.store,
            &path,
            Some("exec-old".to_string()),
            new_bytes
        )
        .unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replace_if_holder_unchanged_backs_off_when_id_changed() {
        let store = in_memory_store(None);
        write_lock_info(
            &store,
            "ref",
            &LockInfo {
                execution_id: "exec-current".to_string(),
                acquired_at_epoch_secs: now_epoch_secs(),
                ttl_secs: 3600,
            },
        );
        let path = store.lock_path("ref").unwrap();
        let new_bytes = serde_json::to_vec(&LockInfo {
            execution_id: "exec-new".to_string(),
            acquired_at_epoch_secs: now_epoch_secs(),
            ttl_secs: 3600,
        })
        .unwrap();
        // expected_id is stale — should back off
        assert!(!replace_if_holder_unchanged(
            &store.store,
            &path,
            Some("exec-old".to_string()),
            new_bytes
        )
        .unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replace_if_holder_unchanged_creates_when_not_found() {
        let store = in_memory_store(None);
        let path = store.lock_path("ref").unwrap();
        let new_bytes = serde_json::to_vec(&LockInfo {
            execution_id: "exec-new".to_string(),
            acquired_at_epoch_secs: now_epoch_secs(),
            ttl_secs: 3600,
        })
        .unwrap();
        assert!(replace_if_holder_unchanged(
            &store.store,
            &path,
            Some("exec-old".to_string()),
            new_bytes
        )
        .unwrap());
    }
}

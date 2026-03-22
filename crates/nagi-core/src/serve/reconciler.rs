use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::compile::CompiledAsset;
use crate::evaluate::{AssetEvalResult, EvaluateError};
use crate::init;
use crate::log::LogStore;
use crate::notify::{Notifier, NotifyEvent};
use crate::storage::local::{LocalCache, LocalSourceStatsCache};
use crate::storage::{Cache, SourceStatsCache, StorageError, SyncLock};
use crate::sync::SyncError;

/// Evaluates a single compiled asset and writes the result to the local cache.
///
/// This is the "stateless reconciler": it takes all inputs by value so the
/// returned future is `Send` and can be spawned on a `JoinSet`.
/// (`evaluate_from_compiled` cannot be used here because `LogStore` is `!Send`.)
///
/// When `source_stats_dir` is provided and the asset has sources with a
/// connection, checks `table_stats` for each source. If all source stats are
/// unchanged from the cached values, returns the cached eval result (skipping
/// the actual evaluation queries).
pub async fn evaluate_and_cache(
    yaml: &str,
    cache_dir: Option<&Path>,
    source_stats_dir: Option<&Path>,
) -> Result<AssetEvalResult, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let conn = compiled
        .connection
        .as_ref()
        .map(crate::evaluate::resolve_connection)
        .transpose()?;

    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(LocalCache::default_dir);
    let eval_cache = LocalCache::new(cache_path);

    let has_sources = conn.is_some() && !compiled.spec.sources.is_empty();
    let stats_cache = if has_sources {
        Some(LocalSourceStatsCache::new(
            source_stats_dir
                .map(PathBuf::from)
                .unwrap_or_else(LocalSourceStatsCache::default_dir),
        ))
    } else {
        None
    };

    // Source change detection: skip evaluate if all sources unchanged.
    if let (Some(conn_ref), Some(sc)) = (conn.as_deref(), stats_cache.as_ref()) {
        if let Some(cached_result) =
            check_sources_unchanged(&compiled, conn_ref, sc, &eval_cache).await
        {
            return Ok(cached_result);
        }
    }

    let spec = crate::evaluate::compiled_to_asset_spec(&compiled);
    let result =
        crate::evaluate::evaluate_asset_no_log(&compiled.metadata.name, &spec, conn.as_deref())
            .await?;

    eval_cache
        .write(&result)
        .map_err(|e| EvaluateError::Cache(e.to_string()))?;

    if let (Some(conn_ref), Some(sc)) = (conn.as_deref(), stats_cache.as_ref()) {
        update_source_stats(&compiled, conn_ref, sc).await;
    }

    Ok(result)
}

/// Returns the cached eval result if all sources have unchanged stats.
/// Returns `None` if any source changed or if there's no cached result.
async fn check_sources_unchanged(
    compiled: &CompiledAsset,
    conn: &dyn crate::db::Connection,
    stats_cache: &dyn SourceStatsCache,
    eval_cache: &dyn Cache,
) -> Option<AssetEvalResult> {
    for source in &compiled.spec.sources {
        let current = match conn.table_stats(&source.ref_name).await {
            Ok(s) => s,
            Err(_) => return None,
        };
        let cached = match stats_cache.read(&source.ref_name) {
            Ok(Some(s)) => s,
            _ => return None,
        };
        if current != cached {
            return None;
        }
    }
    // All sources unchanged — return cached eval result if available.
    eval_cache.read(&compiled.metadata.name).ok().flatten()
}

/// Updates the source stats cache with current values. Best-effort (errors logged).
async fn update_source_stats(
    compiled: &CompiledAsset,
    conn: &dyn crate::db::Connection,
    stats_cache: &dyn SourceStatsCache,
) {
    for source in &compiled.spec.sources {
        if let Ok(stats) = conn.table_stats(&source.ref_name).await {
            if let Err(e) = stats_cache.write(&source.ref_name, &stats) {
                eprintln!(
                    "warning: failed to cache source stats for {}: {e}",
                    source.ref_name
                );
            }
        }
    }
}

/// Result of a spawned evaluation, including timestamps for logging.
pub struct EvalOutcome {
    pub result: Result<AssetEvalResult, EvaluateError>,
    pub started_at: String,
    pub finished_at: String,
}

/// Spawn wrapper: pairs the asset name with the evaluation result so the
/// Controller can identify which asset completed.
pub async fn spawn_evaluate(
    asset_name: String,
    yaml: String,
    cache_dir: Option<PathBuf>,
    source_stats_dir: Option<PathBuf>,
) -> (String, EvalOutcome) {
    let started_at = chrono::Utc::now().to_rfc3339();
    let result = evaluate_and_cache(&yaml, cache_dir.as_deref(), source_stats_dir.as_deref()).await;
    let finished_at = chrono::Utc::now().to_rfc3339();
    (
        asset_name,
        EvalOutcome {
            result,
            started_at,
            finished_at,
        },
    )
}

/// Executes sync for a compiled asset. Called via `JoinSet::spawn` so all
/// inputs are owned to produce a `Send` future.
///
/// Uses `execute_sync_core` directly (not `sync_from_compiled`) to avoid
/// `LogStore` (!Send) and `post_sync_re_evaluate` — the Controller handles
/// re-evaluation itself via `handle_sync_result`.
/// Lock configuration passed from nagi.yaml to the sync task.
#[derive(Debug, Clone, Copy)]
pub struct LockConfig {
    pub ttl_seconds: u64,
    pub retry_interval_seconds: u64,
    pub retry_max_attempts: u32,
}

pub async fn spawn_sync(
    asset_name: String,
    yaml: String,
    lock: Arc<dyn SyncLock>,
    lock_config: LockConfig,
    notifier: Option<Arc<dyn Notifier>>,
) -> (String, Result<crate::sync::SyncExecutionResult, SyncError>) {
    let result = resolve_and_sync(&asset_name, &yaml, lock, lock_config, notifier).await;
    (asset_name, result)
}

async fn resolve_and_sync(
    asset_name: &str,
    yaml: &str,
    lock: Arc<dyn SyncLock>,
    lock_config: LockConfig,
    notifier: Option<Arc<dyn Notifier>>,
) -> Result<crate::sync::SyncExecutionResult, SyncError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| SyncError::Parse(e.to_string()))?;
    let sync_spec = compiled
        .spec
        .sync
        .as_ref()
        .ok_or_else(|| SyncError::NoSyncSpec {
            asset_name: compiled.metadata.name.clone(),
        })?;

    let execution_id = crate::sync::generate_uuid();
    let sync_ref = compiled
        .spec
        .sync_ref_name
        .as_deref()
        .unwrap_or(&compiled.metadata.name);
    let ttl = std::time::Duration::from_secs(lock_config.ttl_seconds);

    if !acquire_with_retry(
        lock.as_ref(),
        sync_ref,
        ttl,
        &lock_config,
        asset_name,
        &execution_id,
    )
    .await?
    {
        if let Some(n) = &notifier {
            let event = NotifyEvent::SyncLockSkipped {
                asset_name: asset_name.to_string(),
                sync_ref: sync_ref.to_string(),
            };
            if let Err(e) = n.notify(&event).await {
                eprintln!("[serve] warning: notification failed: {e}");
            }
        }
        return Ok(crate::sync::SyncExecutionResult {
            execution_id,
            asset_name: asset_name.to_string(),
            sync_type: crate::sync::SyncType::Sync,
            stages: vec![],
            success: true,
        });
    }

    let result = crate::sync::execute_sync_core(
        &compiled.metadata.name,
        sync_spec,
        crate::sync::SyncType::Sync,
        None,
    )
    .await;

    if let Err(e) = lock.release(sync_ref) {
        eprintln!("[serve] warning: failed to release sync lock for {sync_ref}: {e}");
    }

    result
}

/// Attempts to acquire the lock, retrying up to `max_attempts` times.
/// Each attempt is logged to both stderr and logs.db.
/// Returns `true` if acquired, `false` if all attempts exhausted.
async fn acquire_with_retry(
    lock: &dyn crate::storage::SyncLock,
    sync_ref: &str,
    ttl: std::time::Duration,
    config: &LockConfig,
    asset_name: &str,
    execution_id: &str,
) -> Result<bool, SyncError> {
    for attempt in 0..config.retry_max_attempts {
        match lock
            .acquire(sync_ref, ttl, execution_id)
            .map_err(storage_to_sync_error)?
        {
            true => return Ok(true),
            false => {
                let now = chrono::Utc::now().to_rfc3339();
                eprintln!(
                    "[serve] lock for ref '{sync_ref}' held, sync for '{asset_name}' waiting (attempt {}/{})",
                    attempt + 1,
                    config.retry_max_attempts
                );
                write_lock_log(execution_id, asset_name, attempt + 1, "waiting", &now);
                if attempt + 1 < config.retry_max_attempts {
                    tokio::time::sleep(std::time::Duration::from_secs(
                        config.retry_interval_seconds,
                    ))
                    .await;
                }
            }
        }
    }

    // All retries exhausted — log the skip.
    eprintln!(
        "[serve] skipping sync for '{asset_name}': lock for ref '{sync_ref}' unavailable after {} attempts",
        config.retry_max_attempts
    );
    let now = chrono::Utc::now().to_rfc3339();
    write_lock_log(
        execution_id,
        asset_name,
        config.retry_max_attempts,
        "skipped",
        &now,
    );
    Ok(false)
}

fn write_lock_log(
    execution_id: &str,
    asset_name: &str,
    attempts: u32,
    status: &str,
    timestamp: &str,
) {
    let log_store = match LogStore::open(&init::default_db_path(), &init::default_logs_dir()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[serve] warning: failed to open log store for lock skip log: {e}");
            return;
        }
    };
    if let Err(e) =
        log_store.write_sync_lock_log(execution_id, asset_name, attempts, status, timestamp)
    {
        eprintln!("[serve] warning: failed to write lock skip log: {e}");
    }
}

fn storage_to_sync_error(e: StorageError) -> SyncError {
    SyncError::Io(std::io::Error::other(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::CompiledAssetSpec;
    use crate::db::{ConnectionError, TableStats};
    use crate::evaluate::{AssetEvalResult, ConditionResult, ConditionStatus};
    use crate::kind::asset::SourceRef;
    use crate::kind::Metadata;
    use crate::storage::StorageError;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct MockConn {
        stats: TableStats,
    }

    #[async_trait]
    impl crate::db::Connection for MockConn {
        async fn query_scalar(&self, _sql: &str) -> Result<serde_json::Value, ConnectionError> {
            Ok(serde_json::Value::Bool(true))
        }

        fn freshness_sql(&self, _asset_name: &str, _column: Option<&str>) -> String {
            String::new()
        }

        fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
            Box::new(sqlparser::dialect::BigQueryDialect {})
        }

        async fn table_stats(&self, _table_name: &str) -> Result<TableStats, ConnectionError> {
            Ok(self.stats.clone())
        }
    }

    struct MockStatsCache {
        inner: Mutex<std::collections::HashMap<String, TableStats>>,
    }

    impl MockStatsCache {
        fn new() -> Self {
            Self {
                inner: Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn set(&self, name: &str, stats: TableStats) {
            self.inner.lock().unwrap().insert(name.to_string(), stats);
        }
    }

    impl SourceStatsCache for MockStatsCache {
        fn read(&self, source_name: &str) -> Result<Option<TableStats>, StorageError> {
            Ok(self.inner.lock().unwrap().get(source_name).cloned())
        }

        fn write(&self, source_name: &str, stats: &TableStats) -> Result<(), StorageError> {
            self.inner
                .lock()
                .unwrap()
                .insert(source_name.to_string(), stats.clone());
            Ok(())
        }
    }

    struct MockEvalCache {
        inner: Mutex<std::collections::HashMap<String, AssetEvalResult>>,
    }

    impl MockEvalCache {
        fn new() -> Self {
            Self {
                inner: Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn set(&self, result: AssetEvalResult) {
            self.inner
                .lock()
                .unwrap()
                .insert(result.asset_name.clone(), result);
        }
    }

    impl Cache for MockEvalCache {
        fn write(&self, result: &AssetEvalResult) -> Result<(), StorageError> {
            self.inner
                .lock()
                .unwrap()
                .insert(result.asset_name.clone(), result.clone());
            Ok(())
        }

        fn read(&self, asset_name: &str) -> Result<Option<AssetEvalResult>, StorageError> {
            Ok(self.inner.lock().unwrap().get(asset_name).cloned())
        }

        fn list(&self) -> Result<Vec<AssetEvalResult>, StorageError> {
            Ok(self.inner.lock().unwrap().values().cloned().collect())
        }
    }

    fn sample_compiled(sources: Vec<&str>) -> CompiledAsset {
        CompiledAsset {
            api_version: "v1".to_string(),
            metadata: Metadata {
                name: "test-asset".to_string(),
            },
            spec: CompiledAssetSpec {
                tags: vec![],
                sources: sources
                    .into_iter()
                    .map(|s| SourceRef {
                        ref_name: s.to_string(),
                    })
                    .collect(),
                desired_sets: vec![],
                auto_sync: true,
                sync_ref_name: None,
                sync: None,
                resync: None,
            },
            connection: None,
        }
    }

    fn sample_eval_result() -> AssetEvalResult {
        AssetEvalResult {
            asset_name: "test-asset".to_string(),
            ready: true,
            conditions: vec![ConditionResult {
                condition_name: "check".to_string(),
                condition_type: "SQL".to_string(),
                status: ConditionStatus::Ready,
            }],
            evaluation_id: None,
        }
    }

    struct MockSyncLock {
        /// Sequence of results to return from `acquire`, in order.
        /// `true` = acquired, `false` = held by another.
        results: Mutex<std::collections::VecDeque<bool>>,
        acquire_count: Mutex<u32>,
    }

    impl MockSyncLock {
        fn new(results: Vec<bool>) -> Self {
            Self {
                results: Mutex::new(results.into()),
                acquire_count: Mutex::new(0),
            }
        }

        fn acquire_count(&self) -> u32 {
            *self.acquire_count.lock().unwrap()
        }
    }

    impl crate::storage::SyncLock for MockSyncLock {
        fn acquire(
            &self,
            _sync_ref: &str,
            _ttl: std::time::Duration,
            _execution_id: &str,
        ) -> Result<bool, StorageError> {
            *self.acquire_count.lock().unwrap() += 1;
            Ok(self.results.lock().unwrap().pop_front().unwrap_or(false))
        }

        fn release(&self, _sync_ref: &str) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn instant_lock_config(max_attempts: u32) -> LockConfig {
        LockConfig {
            ttl_seconds: 60,
            retry_interval_seconds: 0,
            retry_max_attempts: max_attempts,
        }
    }

    macro_rules! acquire_with_retry_test {
        ($($name:ident: $results:expr, $max:expr => $expected:expr, $calls:expr;)*) => {
            $(
                #[tokio::test]
                async fn $name() {
                    let lock = MockSyncLock::new($results);
                    let config = instant_lock_config($max);
                    let got = acquire_with_retry(&lock, "ref", std::time::Duration::from_secs(60), &config, "asset", "exec-1")
                        .await
                        .unwrap();
                    assert_eq!(got, $expected, "return value");
                    assert_eq!(lock.acquire_count(), $calls, "acquire call count");
                }
            )*
        };
    }

    acquire_with_retry_test! {
        acquire_succeeds_on_first_attempt:
            vec![true], 3 => true, 1;
        acquire_succeeds_on_second_attempt:
            vec![false, true], 3 => true, 2;
        acquire_succeeds_on_last_attempt:
            vec![false, false, true], 3 => true, 3;
        acquire_exhausted_returns_false:
            vec![false, false, false], 3 => false, 3;
        acquire_single_attempt_succeeds:
            vec![true], 1 => true, 1;
        acquire_single_attempt_fails:
            vec![false], 1 => false, 1;
    }

    #[tokio::test]
    async fn check_sources_unchanged_returns_cached_when_stats_match() {
        let conn = MockConn {
            stats: TableStats {
                num_rows: 100,
                num_bytes: 2048,
            },
        };
        let stats_cache = MockStatsCache::new();
        stats_cache.set(
            "src_table",
            TableStats {
                num_rows: 100,
                num_bytes: 2048,
            },
        );
        let eval_cache = MockEvalCache::new();
        eval_cache.set(sample_eval_result());

        let compiled = sample_compiled(vec!["src_table"]);
        let result = check_sources_unchanged(&compiled, &conn, &stats_cache, &eval_cache).await;

        assert!(result.is_some());
        assert_eq!(result.unwrap().asset_name, "test-asset");
    }

    #[tokio::test]
    async fn check_sources_unchanged_returns_none_when_stats_differ() {
        let conn = MockConn {
            stats: TableStats {
                num_rows: 200,
                num_bytes: 4096,
            },
        };
        let stats_cache = MockStatsCache::new();
        stats_cache.set(
            "src_table",
            TableStats {
                num_rows: 100,
                num_bytes: 2048,
            },
        );
        let eval_cache = MockEvalCache::new();
        eval_cache.set(sample_eval_result());

        let compiled = sample_compiled(vec!["src_table"]);
        let result = check_sources_unchanged(&compiled, &conn, &stats_cache, &eval_cache).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn check_sources_unchanged_returns_none_when_no_cached_stats() {
        let conn = MockConn {
            stats: TableStats {
                num_rows: 100,
                num_bytes: 2048,
            },
        };
        let stats_cache = MockStatsCache::new();
        let eval_cache = MockEvalCache::new();
        eval_cache.set(sample_eval_result());

        let compiled = sample_compiled(vec!["src_table"]);
        let result = check_sources_unchanged(&compiled, &conn, &stats_cache, &eval_cache).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn check_sources_unchanged_returns_none_when_no_cached_eval() {
        let conn = MockConn {
            stats: TableStats {
                num_rows: 100,
                num_bytes: 2048,
            },
        };
        let stats_cache = MockStatsCache::new();
        stats_cache.set(
            "src_table",
            TableStats {
                num_rows: 100,
                num_bytes: 2048,
            },
        );
        let eval_cache = MockEvalCache::new();

        let compiled = sample_compiled(vec!["src_table"]);
        let result = check_sources_unchanged(&compiled, &conn, &stats_cache, &eval_cache).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn update_source_stats_writes_current_values() {
        let conn = MockConn {
            stats: TableStats {
                num_rows: 500,
                num_bytes: 8192,
            },
        };
        let stats_cache = MockStatsCache::new();
        let compiled = sample_compiled(vec!["src_table"]);

        update_source_stats(&compiled, &conn, &stats_cache).await;

        let cached = stats_cache.read("src_table").unwrap().unwrap();
        assert_eq!(cached.num_rows, 500);
        assert_eq!(cached.num_bytes, 8192);
    }
}

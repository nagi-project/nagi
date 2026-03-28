use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::runtime::compile::CompiledAsset;
use crate::runtime::evaluate::{AssetEvalResult, ConditionResult, EvaluateError};
use crate::runtime::log::LogStore;
use crate::runtime::notify::{Notifier, NotifyEvent};
use crate::runtime::storage::local::{LocalCache, LocalConditionCache};
use crate::runtime::storage::{
    Cache, ConditionCache, ConditionCacheEntry, ConditionCacheMap, StorageError, SyncLock,
};
use crate::runtime::sync::SyncError;

/// Evaluates a single compiled asset and writes the result to the local cache.
///
/// This is the "stateless reconciler": it takes all inputs by value so the
/// returned future is `Send` and can be spawned on a `JoinSet`.
/// (`evaluate_from_compiled` cannot be used here because `LogStore` is `!Send`.)
///
/// When `skip_cache` is false, conditions with a valid TTL cache entry are
/// reused without executing the actual query/command.
pub async fn evaluate_and_cache(
    yaml: &str,
    cache_dir: Option<&Path>,
    skip_cache: bool,
) -> Result<AssetEvalResult, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let conn = compiled
        .connection
        .as_ref()
        .map(|c| c.connect().map_err(EvaluateError::Connection))
        .transpose()?;

    let nagi_dir = crate::runtime::config::resolve_nagi_dir(std::path::Path::new("."));
    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| nagi_dir.evaluate_cache_dir());
    let evaluate_cache = LocalCache::new(cache_path);

    // Condition-level TTL cache: collect still-valid cached results.
    let condition_cache = LocalConditionCache::new(nagi_dir.evaluate_cache_dir());
    let cached_conditions = if skip_cache {
        HashMap::new()
    } else {
        resolve_cached_conditions(&compiled, &condition_cache)
    };

    let result = crate::runtime::evaluate::evaluate_asset_cached(
        &compiled.metadata.name,
        &compiled.spec.on_drift,
        conn.as_deref(),
        &cached_conditions,
    )
    .await?;

    evaluate_cache
        .write(&result)
        .map_err(|e| EvaluateError::Cache(e.to_string()))?;

    // Update condition cache with fresh results.
    update_condition_cache(&compiled, &result, &condition_cache);

    Ok(result)
}

/// For each condition in the asset, checks the condition cache and returns
/// entries that are still within their effective TTL.
fn resolve_cached_conditions(
    compiled: &CompiledAsset,
    cache: &dyn ConditionCache,
) -> HashMap<String, ConditionResult> {
    let cached_map = match cache.read(&compiled.metadata.name) {
        Ok(Some(m)) => m,
        _ => return HashMap::new(),
    };

    let now = Utc::now();
    let asset_ttl = compiled.spec.evaluate_cache_ttl.as_ref();
    let mut valid = HashMap::new();

    for entry in &compiled.spec.on_drift {
        for cond in &entry.conditions {
            let effective_ttl = cond.evaluate_cache_ttl().or(asset_ttl).map(|d| d.as_std());
            let Some(ttl) = effective_ttl else {
                continue;
            };
            let Some(cached) = cached_map.get(cond.name()) else {
                continue;
            };
            let Ok(cached_at) = DateTime::parse_from_rfc3339(&cached.cached_at) else {
                continue;
            };
            let elapsed = now.signed_duration_since(cached_at);
            if elapsed >= chrono::Duration::zero()
                && elapsed < chrono::Duration::from_std(ttl).unwrap_or(chrono::TimeDelta::MAX)
            {
                valid.insert(cond.name().to_string(), cached.result.clone());
            }
        }
    }

    valid
}

/// Writes all condition results from the evaluation to the condition cache.
fn update_condition_cache(
    compiled: &CompiledAsset,
    result: &AssetEvalResult,
    cache: &dyn ConditionCache,
) {
    let now = Utc::now().to_rfc3339();
    let map: ConditionCacheMap = result
        .conditions
        .iter()
        .map(|cr| {
            (
                cr.condition_name.clone(),
                ConditionCacheEntry {
                    result: cr.clone(),
                    cached_at: now.clone(),
                },
            )
        })
        .collect();
    if let Err(e) = cache.write(&compiled.metadata.name, &map) {
        tracing::warn!(
            asset = %compiled.metadata.name,
            error = %e,
            "failed to write condition cache"
        );
    }
}

/// Returns the cached evaluate result if available.
/// Returns `None` if any source changed or if there's no cached result.
/// Result of a spawned evaluation, including timestamps for logging.
pub struct EvaluateOutcome {
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
    skip_cache: bool,
) -> (String, EvaluateOutcome) {
    let started_at = chrono::Utc::now().to_rfc3339();
    let result = evaluate_and_cache(&yaml, cache_dir.as_deref(), skip_cache).await;
    let finished_at = chrono::Utc::now().to_rfc3339();
    (
        asset_name,
        EvaluateOutcome {
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
) -> (
    String,
    Result<crate::runtime::sync::SyncExecutionResult, SyncError>,
) {
    let result = resolve_and_sync(&asset_name, &yaml, lock, lock_config, notifier).await;
    (asset_name, result)
}

async fn resolve_and_sync(
    asset_name: &str,
    yaml: &str,
    lock: Arc<dyn SyncLock>,
    lock_config: LockConfig,
    notifier: Option<Arc<dyn Notifier>>,
) -> Result<crate::runtime::sync::SyncExecutionResult, SyncError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| SyncError::Parse(e.to_string()))?;

    // Use the first on_drift entry's sync for serve (first-match).
    let first_entry = compiled
        .spec
        .on_drift
        .first()
        .ok_or_else(|| SyncError::NoSyncSpec {
            asset_name: compiled.metadata.name.clone(),
        })?;
    let sync_spec = &first_entry.sync;

    let execution_id = crate::runtime::sync::generate_uuid();
    let ttl = std::time::Duration::from_secs(lock_config.ttl_seconds);

    if !acquire_with_retry(
        lock.as_ref(),
        asset_name,
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
                sync_ref: asset_name.to_string(),
            };
            if let Err(e) = n.notify(&event).await {
                tracing::warn!(error = %e, "notification failed");
            }
        }
        return Ok(crate::runtime::sync::SyncExecutionResult {
            execution_id,
            asset_name: asset_name.to_string(),
            sync_type: crate::runtime::sync::SyncType::Sync,
            stages: vec![],
            success: true,
        });
    }

    let result = crate::runtime::sync::execute_sync_core(
        &compiled.metadata.name,
        sync_spec,
        crate::runtime::sync::SyncType::Sync,
        None,
    )
    .await;

    if let Err(e) = lock.release(asset_name) {
        tracing::warn!(asset_name = %asset_name, error = %e, "failed to release sync lock");
    }

    result
}

/// Attempts to acquire the lock, retrying up to `max_attempts` times.
/// Each attempt is logged to both stderr and logs.db.
/// Returns `true` if acquired, `false` if all attempts exhausted.
async fn acquire_with_retry(
    lock: &dyn crate::runtime::storage::SyncLock,
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
                tracing::info!(
                    sync_ref = %sync_ref,
                    asset = %asset_name,
                    attempt = attempt + 1,
                    max_attempts = config.retry_max_attempts,
                    "lock held, waiting"
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
    tracing::warn!(
        asset = %asset_name,
        sync_ref = %sync_ref,
        attempts = config.retry_max_attempts,
        "skipping sync, lock unavailable"
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
    let nagi_dir = crate::runtime::config::resolve_nagi_dir(std::path::Path::new("."));
    let log_store = match LogStore::open(&nagi_dir.db_path(), &nagi_dir.logs_dir()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "failed to open log store for lock skip log");
            return;
        }
    };
    if let Err(e) =
        log_store.write_sync_lock_log(execution_id, asset_name, attempts, status, timestamp)
    {
        tracing::warn!(error = %e, "failed to write lock skip log");
    }
}

fn storage_to_sync_error(e: StorageError) -> SyncError {
    SyncError::Io(std::io::Error::other(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::compile::CompiledAssetSpec;
    use crate::runtime::evaluate::{AssetEvalResult, ConditionResult, ConditionStatus};
    use crate::runtime::kind::Metadata;
    use crate::runtime::storage::StorageError;
    use std::sync::Mutex;

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

    impl crate::runtime::storage::SyncLock for MockSyncLock {
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

    // ── TTL cache tests ────────────────────────────────────────────────────

    struct MockConditionCache {
        inner: Mutex<std::collections::HashMap<String, ConditionCacheMap>>,
    }

    impl MockConditionCache {
        fn new() -> Self {
            Self {
                inner: Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn set(&self, asset_name: &str, map: ConditionCacheMap) {
            self.inner
                .lock()
                .unwrap()
                .insert(asset_name.to_string(), map);
        }
    }

    impl ConditionCache for MockConditionCache {
        fn write_condition(
            &self,
            asset_name: &str,
            condition_name: &str,
            entry: &ConditionCacheEntry,
        ) -> Result<(), StorageError> {
            self.inner
                .lock()
                .unwrap()
                .entry(asset_name.to_string())
                .or_default()
                .insert(condition_name.to_string(), entry.clone());
            Ok(())
        }

        fn read(&self, asset_name: &str) -> Result<Option<ConditionCacheMap>, StorageError> {
            Ok(self.inner.lock().unwrap().get(asset_name).cloned())
        }

        fn write(&self, asset_name: &str, map: &ConditionCacheMap) -> Result<(), StorageError> {
            self.inner
                .lock()
                .unwrap()
                .insert(asset_name.to_string(), map.clone());
            Ok(())
        }
    }

    fn sample_compiled_with_conditions(
        asset_ttl: Option<&str>,
        conditions: Vec<crate::runtime::kind::asset::DesiredCondition>,
    ) -> CompiledAsset {
        use crate::runtime::compile::ResolvedOnDriftEntry;
        use crate::runtime::kind::sync::{StepType, SyncSpec, SyncStep};
        CompiledAsset {
            _api_version: "v1".to_string(),
            metadata: Metadata {
                name: "test-asset".to_string(),
            },
            spec: CompiledAssetSpec {
                tags: vec![],
                upstreams: vec![],
                on_drift: vec![ResolvedOnDriftEntry {
                    conditions,
                    conditions_ref: "test-cond".to_string(),
                    sync: SyncSpec {
                        pre: None,
                        run: SyncStep {
                            step_type: StepType::Command,
                            args: vec!["true".to_string()],
                            env: std::collections::HashMap::new(),
                        },
                        post: None,
                    },
                    sync_ref_name: "test-sync".to_string(),
                }],
                auto_sync: true,
                dbt_cloud_job_ids: None,
                evaluate_cache_ttl: asset_ttl.map(|s| serde_yaml::from_str(s).unwrap()),
            },
            connection: None,
        }
    }

    fn condition_cache_entry(
        name: &str,
        ready: bool,
        seconds_ago: i64,
    ) -> (String, ConditionCacheEntry) {
        let cached_at = (chrono::Utc::now() - chrono::Duration::seconds(seconds_ago)).to_rfc3339();
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
                            reason: "cached".to_string(),
                        }
                    },
                },
                cached_at,
            },
        )
    }

    #[test]
    fn resolve_cached_returns_valid_entry_within_ttl() {
        let cache = MockConditionCache::new();
        let map: ConditionCacheMap = [condition_cache_entry("check", true, 60)]
            .into_iter()
            .collect();
        cache.set("test-asset", map);

        let compiled = sample_compiled_with_conditions(
            Some("5m"),
            vec![crate::runtime::kind::asset::DesiredCondition::Sql {
                name: "check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
            }],
        );

        let result = resolve_cached_conditions(&compiled, &cache);
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("check"));
    }

    #[test]
    fn resolve_cached_skips_expired_entry() {
        let cache = MockConditionCache::new();
        // Cached 10 minutes ago, TTL is 5 minutes → expired
        let map: ConditionCacheMap = [condition_cache_entry("check", true, 600)]
            .into_iter()
            .collect();
        cache.set("test-asset", map);

        let compiled = sample_compiled_with_conditions(
            Some("5m"),
            vec![crate::runtime::kind::asset::DesiredCondition::Sql {
                name: "check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
            }],
        );

        let result = resolve_cached_conditions(&compiled, &cache);
        assert!(result.is_empty());
    }

    #[test]
    fn resolve_cached_condition_ttl_overrides_asset_ttl() {
        let cache = MockConditionCache::new();
        // Cached 3 minutes ago
        let map: ConditionCacheMap = [condition_cache_entry("check", true, 180)]
            .into_iter()
            .collect();
        cache.set("test-asset", map);

        // Asset TTL = 5m, but condition TTL = 2m → should be expired
        let compiled = sample_compiled_with_conditions(
            Some("5m"),
            vec![crate::runtime::kind::asset::DesiredCondition::Sql {
                name: "check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: Some(serde_yaml::from_str("2m").unwrap()),
            }],
        );

        let result = resolve_cached_conditions(&compiled, &cache);
        assert!(result.is_empty());
    }

    #[test]
    fn resolve_cached_no_ttl_means_no_caching() {
        let cache = MockConditionCache::new();
        let map: ConditionCacheMap = [condition_cache_entry("check", true, 10)]
            .into_iter()
            .collect();
        cache.set("test-asset", map);

        // No TTL on asset or condition → always re-evaluate
        let compiled = sample_compiled_with_conditions(
            None,
            vec![crate::runtime::kind::asset::DesiredCondition::Sql {
                name: "check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
            }],
        );

        let result = resolve_cached_conditions(&compiled, &cache);
        assert!(result.is_empty());
    }

    #[test]
    fn resolve_cached_empty_cache_returns_empty() {
        let cache = MockConditionCache::new();
        let compiled = sample_compiled_with_conditions(
            Some("5m"),
            vec![crate::runtime::kind::asset::DesiredCondition::Sql {
                name: "check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
            }],
        );

        let result = resolve_cached_conditions(&compiled, &cache);
        assert!(result.is_empty());
    }

    #[test]
    fn update_condition_cache_writes_all_results() {
        let cache = MockConditionCache::new();
        let compiled = sample_compiled_with_conditions(None, vec![]);
        let result = AssetEvalResult {
            asset_name: "test-asset".to_string(),
            ready: true,
            conditions: vec![
                ConditionResult {
                    condition_name: "a".to_string(),
                    condition_type: "SQL".to_string(),
                    status: ConditionStatus::Ready,
                },
                ConditionResult {
                    condition_name: "b".to_string(),
                    condition_type: "Command".to_string(),
                    status: ConditionStatus::Drifted {
                        reason: "exit 1".to_string(),
                    },
                },
            ],
            evaluation_id: None,
        };

        update_condition_cache(&compiled, &result, &cache);

        let map = cache.read("test-asset").unwrap().unwrap();
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("a"));
        assert!(map.contains_key("b"));
    }
}

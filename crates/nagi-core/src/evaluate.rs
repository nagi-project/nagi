mod boolean;
mod command;
mod condition;
mod freshness;

use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::compile::CompiledAsset;
use crate::db::{Connection, ConnectionError};
use crate::dbt::profile::DbtProfilesFile;
use crate::kind::asset::{AssetSpec, DesiredCondition, DesiredSetEntry};
use crate::log::{LogError, LogStore};
use crate::storage::local::LocalCache;
use crate::storage::Cache;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConditionResult {
    /// Name of the evaluated condition.
    pub condition_name: String,
    /// Type discriminator of the condition (e.g. `Freshness`, `SQL`, `Command`).
    pub condition_type: String,
    /// Evaluation outcome for this condition.
    pub status: ConditionStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "state")]
pub enum ConditionStatus {
    Ready,
    NotReady {
        /// Human-readable explanation of why the condition is not satisfied.
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssetEvalResult {
    /// Name of the evaluated Asset resource.
    pub asset_name: String,
    /// true when all conditions are Ready.
    pub ready: bool,
    /// Per-condition evaluation results.
    pub conditions: Vec<ConditionResult>,
    /// Set when the result was logged via `LogStore`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluateError {
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
    #[error("command error: {0}")]
    CommandFailed(String),
    #[error("condition '{condition_name}' requires a DB connection, but none is configured")]
    NoConnection { condition_name: String },
    #[error("log error: {0}")]
    Log(#[from] LogError),
    #[error("compile error: {0}")]
    Compile(#[from] crate::compile::CompileError),
    #[error("failed to parse compiled asset: {0}")]
    Parse(String),
    #[error("profile error: {0}")]
    Profile(String),
    #[error("SQL query must be a single SELECT statement (read-only direct access): {0}")]
    ReadOnlyViolation(String),
    #[error("cache error: {0}")]
    Cache(String),
    #[error("serialization error: {0}")]
    Serialize(String),
}

/// Evaluates all desired conditions of `spec`.
///
/// `conn` is required only for SQL-based conditions (Freshness, SQL).
/// Passing `None` for an Asset that only uses `Command` conditions is valid.
/// Passing `None` when a SQL condition is present returns `EvaluateError::NoConnection`.
/// When `log_store` is `Some`, automatically writes evaluate logs after evaluation.
pub async fn evaluate_asset(
    asset_name: &str,
    spec: &AssetSpec,
    conn: Option<&dyn Connection>,
    log_store: Option<&LogStore>,
) -> Result<AssetEvalResult, EvaluateError> {
    let started_at = Utc::now();

    let mut results = Vec::new();
    for (i, entry) in spec.desired_sets.iter().enumerate() {
        match entry {
            DesiredSetEntry::Ref(_) => {
                // DesiredGroup refs are resolved at compile time; skip during evaluation.
                continue;
            }
            DesiredSetEntry::Inline(condition) => {
                let result =
                    condition::evaluate_condition(condition.name(), i, asset_name, condition, conn)
                        .await?;
                results.push(result);
            }
        }
    }
    let ready = results.iter().all(|r| r.status == ConditionStatus::Ready);

    let mut result = AssetEvalResult {
        asset_name: asset_name.to_string(),
        ready,
        conditions: results,
        evaluation_id: None,
    };

    if let Some(store) = log_store {
        let id = crate::sync::generate_uuid();
        let finished_at = Utc::now();
        let started_str = started_at.to_rfc3339();
        let finished_str = finished_at.to_rfc3339();
        store.write_evaluate_log(&id, &result, &started_str, &finished_str)?;
        result.evaluation_id = Some(id);
    }

    Ok(result)
}

/// Evaluates all desired conditions without logging.
/// Produces a `Send` future (unlike `evaluate_asset` which takes `&LogStore`).
pub(crate) async fn evaluate_asset_no_log(
    asset_name: &str,
    spec: &AssetSpec,
    conn: Option<&dyn Connection>,
) -> Result<AssetEvalResult, EvaluateError> {
    let mut results = Vec::new();
    for (i, entry) in spec.desired_sets.iter().enumerate() {
        match entry {
            DesiredSetEntry::Ref(_) => continue,
            DesiredSetEntry::Inline(condition) => {
                let result =
                    condition::evaluate_condition(condition.name(), i, asset_name, condition, conn)
                        .await?;
                results.push(result);
            }
        }
    }
    let ready = results.iter().all(|r| r.status == ConditionStatus::Ready);
    Ok(AssetEvalResult {
        asset_name: asset_name.to_string(),
        ready,
        conditions: results,
        evaluation_id: None,
    })
}

/// A single condition from an asset's desiredSets, with its name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DryRunCondition {
    /// Name of the condition.
    pub name: String,
    /// Full condition definition.
    #[serde(flatten)]
    pub condition: DesiredCondition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DryRunResult {
    /// Name of the Asset resource.
    pub asset_name: String,
    /// Conditions that would be evaluated.
    pub conditions: Vec<DryRunCondition>,
}

/// Produces a dry-run summary of what `evaluate_asset` would execute.
/// No DB connection or command execution is performed.
pub fn dry_run_asset(asset_name: &str, spec: &AssetSpec) -> DryRunResult {
    let mut conditions = Vec::new();
    for entry in spec.desired_sets.iter() {
        match entry {
            DesiredSetEntry::Ref(_) => continue,
            DesiredSetEntry::Inline(condition) => {
                conditions.push(DryRunCondition {
                    name: condition.name().to_string(),
                    condition: condition.clone(),
                });
            }
        }
    }
    DryRunResult {
        asset_name: asset_name.to_string(),
        conditions,
    }
}

pub(crate) fn compiled_to_asset_spec(compiled: &CompiledAsset) -> AssetSpec {
    AssetSpec {
        tags: compiled.spec.tags.clone(),
        sources: compiled.spec.sources.clone(),
        desired_sets: compiled.spec.desired_sets.clone(),
        auto_sync: compiled.spec.auto_sync,
        sync: None,
        resync: None,
    }
}

pub(crate) fn resolve_connection(
    conn_info: &crate::compile::ResolvedConnection,
) -> Result<Box<dyn Connection>, EvaluateError> {
    match conn_info {
        crate::compile::ResolvedConnection::DbtProfile {
            profile, target, ..
        } => {
            let f = DbtProfilesFile::load_default()
                .map_err(|e| EvaluateError::Profile(e.to_string()))?;
            let output = f
                .resolve(profile, target.as_deref())
                .map_err(|e| EvaluateError::Profile(e.to_string()))?;
            crate::db::create_connection(output).map_err(EvaluateError::Connection)
        }
    }
}

/// Evaluates an asset from its compiled YAML.
///
/// Handles connection resolution, logging, and cache — callers pass only paths.
pub async fn evaluate_from_compiled(
    yaml: &str,
    cache_dir: Option<&Path>,
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
) -> Result<String, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let asset_name = &compiled.metadata.name;
    let spec = compiled_to_asset_spec(&compiled);

    let log_store = match (db_path, logs_dir) {
        (Some(db), Some(logs)) => Some(LogStore::open(db, logs)?),
        _ => None,
    };

    let conn = compiled
        .connection
        .as_ref()
        .map(resolve_connection)
        .transpose()?;

    let conn_ref = conn.as_deref();
    let result = evaluate_asset(asset_name, &spec, conn_ref, log_store.as_ref()).await?;

    let cache_path = cache_dir
        .map(PathBuf::from)
        .unwrap_or_else(LocalCache::default_dir);
    let cache = LocalCache::new(cache_path);
    cache
        .write(&result)
        .map_err(|e| EvaluateError::Cache(e.to_string()))?;

    serde_json::to_string(&result).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

/// Evaluates all compiled assets matching the selectors.
/// Returns a JSON array of evaluation results.
pub async fn evaluate_all(
    target_dir: &Path,
    selectors: &[&str],
    cache_dir: Option<&Path>,
    dry_run: bool,
) -> Result<String, EvaluateError> {
    let assets = crate::compile::load_compiled_assets(target_dir, selectors)?;
    let mut results: Vec<serde_json::Value> = Vec::with_capacity(assets.len());

    for (_name, yaml) in &assets {
        if dry_run {
            let dr = dry_run_from_compiled(yaml)?;
            results.push(
                serde_json::from_str(&dr).map_err(|e| EvaluateError::Serialize(e.to_string()))?,
            );
        } else {
            let r = evaluate_from_compiled(yaml, cache_dir, None, None).await?;
            results.push(
                serde_json::from_str(&r).map_err(|e| EvaluateError::Serialize(e.to_string()))?,
            );
        }
    }

    serde_json::to_string(&results).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

/// Dry-run from compiled YAML.
pub fn dry_run_from_compiled(yaml: &str) -> Result<String, EvaluateError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(yaml).map_err(|e| EvaluateError::Parse(e.to_string()))?;
    let spec = compiled_to_asset_spec(&compiled);
    let result = dry_run_asset(&compiled.metadata.name, &spec);
    serde_json::to_string(&result).map_err(|e| EvaluateError::Serialize(e.to_string()))
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::Value;

    use super::*;
    use crate::db::ConnectionError;
    use crate::duration::Duration;
    use crate::kind::asset::{AssetSpec, DesiredCondition, DesiredSetEntry};

    // ── Helpers ───────────────────────────────────────────────────────────────

    struct MockConnection {
        response: Value,
    }

    #[async_trait]
    impl Connection for MockConnection {
        async fn query_scalar(&self, _sql: &str) -> Result<Value, ConnectionError> {
            Ok(self.response.clone())
        }

        fn freshness_sql(&self, asset_name: &str, column: Option<&str>) -> String {
            stub_freshness_sql(asset_name, column)
        }

        fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
            Box::new(sqlparser::dialect::BigQueryDialect {})
        }

        async fn table_stats(
            &self,
            _table_name: &str,
        ) -> Result<crate::db::TableStats, ConnectionError> {
            Ok(crate::db::TableStats {
                num_rows: 0,
                num_bytes: 0,
            })
        }
    }

    fn stub_freshness_sql(asset_name: &str, column: Option<&str>) -> String {
        match column {
            Some(col) => format!("SELECT MAX(`{col}`) FROM `{asset_name}`"),
            None => format!("SELECT MAX(last_modified_time) FROM `{asset_name}`"),
        }
    }

    fn asset_spec_with(condition: DesiredCondition) -> AssetSpec {
        AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![DesiredSetEntry::Inline(condition)],
            auto_sync: true,
            sync: None,
            resync: None,
        }
    }

    fn duration(secs: u64) -> Duration {
        serde_yaml::from_str(&format!("{}s", secs)).unwrap()
    }

    fn freshness_condition(max_age_secs: u64, column: Option<&str>) -> DesiredCondition {
        DesiredCondition::Freshness {
            name: "freshness".to_string(),
            max_age: duration(max_age_secs),
            interval: duration(3600),
            check_at: None,
            column: column.map(str::to_string),
        }
    }

    fn epoch_secs_ago(secs: f64) -> Value {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        Value::Number(serde_json::Number::from_f64(now - secs).unwrap())
    }

    // ── Freshness ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn freshness_ready_when_within_max_age() {
        // last updated 1 hour ago, max age 2 hours
        let conn = MockConnection {
            response: epoch_secs_ago(3600.0),
        };
        let spec = asset_spec_with(freshness_condition(7200, None));
        let result = evaluate_asset("my_dataset.my_table", &spec, Some(&conn), None)
            .await
            .unwrap();
        assert!(result.ready);
        assert_eq!(result.conditions[0].status, ConditionStatus::Ready);
    }

    #[tokio::test]
    async fn freshness_not_ready_when_exceeds_max_age() {
        // last updated 25 hours ago, max age 24 hours
        let conn = MockConnection {
            response: epoch_secs_ago(25.0 * 3600.0),
        };
        let spec = asset_spec_with(freshness_condition(86400, None));
        let result = evaluate_asset("my_dataset.my_table", &spec, Some(&conn), None)
            .await
            .unwrap();
        assert!(!result.ready);
        assert!(matches!(
            &result.conditions[0].status,
            ConditionStatus::NotReady { .. }
        ));
    }

    #[tokio::test]
    async fn freshness_accepts_rfc3339_timestamp() {
        let conn = MockConnection {
            response: Value::String("2099-01-01T00:00:00Z".to_string()),
        };
        let spec = asset_spec_with(freshness_condition(86400, Some("updated_at")));
        let result = evaluate_asset("my_table", &spec, Some(&conn), None)
            .await
            .unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn freshness_returns_error_on_unexpected_value() {
        let conn = MockConnection {
            response: Value::Null,
        };
        let spec = asset_spec_with(freshness_condition(86400, None));
        let result = evaluate_asset("my_table", &spec, Some(&conn), None).await;
        assert!(matches!(result, Err(EvaluateError::UnexpectedResult(_))));
    }

    // ── SQL ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_ready_when_true() {
        let conn = MockConnection {
            response: Value::Bool(true),
        };
        let spec = asset_spec_with(DesiredCondition::SQL {
            name: "check".to_string(),
            query: "SELECT true".to_string(),
            interval: None,
        });
        let result = evaluate_asset("my_table", &spec, Some(&conn), None)
            .await
            .unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn sql_not_ready_when_false() {
        let conn = MockConnection {
            response: Value::Bool(false),
        };
        let spec = asset_spec_with(DesiredCondition::SQL {
            name: "check".to_string(),
            query: "SELECT false".to_string(),
            interval: None,
        });
        let result = evaluate_asset("my_table", &spec, Some(&conn), None)
            .await
            .unwrap();
        assert!(!result.ready);
    }

    // ── all conditions AND ────────────────────────────────────────────────────

    #[tokio::test]
    async fn all_conditions_must_pass() {
        // First SQL returns true, second returns false → not ready overall
        let call_count = std::sync::atomic::AtomicU32::new(0);
        struct CountingConnection<'a>(&'a std::sync::atomic::AtomicU32);
        #[async_trait]
        impl Connection for CountingConnection<'_> {
            async fn query_scalar(&self, _sql: &str) -> Result<Value, ConnectionError> {
                let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    Ok(Value::Bool(true))
                } else {
                    Ok(Value::Bool(false))
                }
            }

            fn freshness_sql(&self, asset_name: &str, column: Option<&str>) -> String {
                stub_freshness_sql(asset_name, column)
            }

            fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
                Box::new(sqlparser::dialect::BigQueryDialect {})
            }

            async fn table_stats(
                &self,
                _table_name: &str,
            ) -> Result<crate::db::TableStats, ConnectionError> {
                Ok(crate::db::TableStats {
                    num_rows: 0,
                    num_bytes: 0,
                })
            }
        }
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![
                DesiredSetEntry::Inline(DesiredCondition::SQL {
                    name: "check-a".to_string(),
                    query: "SELECT true".to_string(),
                    interval: None,
                }),
                DesiredSetEntry::Inline(DesiredCondition::SQL {
                    name: "check-b".to_string(),
                    query: "SELECT false".to_string(),
                    interval: None,
                }),
            ],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let result = evaluate_asset(
            "my_table",
            &spec,
            Some(&CountingConnection(&call_count)),
            None,
        )
        .await
        .unwrap();
        assert!(!result.ready);
        assert_eq!(result.conditions[0].status, ConditionStatus::Ready);
        assert!(matches!(
            result.conditions[1].status,
            ConditionStatus::NotReady { .. }
        ));
    }

    // ── dry_run ────────────────────────────────────────────────────────────

    #[test]
    fn dry_run_freshness_with_column() {
        let condition = freshness_condition(86400, Some("updated_at"));
        let spec = asset_spec_with(condition.clone());
        let result = dry_run_asset("my_table", &spec);
        assert_eq!(result.conditions.len(), 1);
        assert_eq!(result.conditions[0].condition, condition);
    }

    #[test]
    fn dry_run_freshness_without_column() {
        let condition = freshness_condition(86400, None);
        let spec = asset_spec_with(condition.clone());
        let result = dry_run_asset("my_table", &spec);
        assert_eq!(result.conditions[0].condition, condition);
    }

    #[test]
    fn dry_run_sql() {
        let condition = DesiredCondition::SQL {
            name: "check".to_string(),
            query: "SELECT COUNT(*) > 0 FROM orders".to_string(),
            interval: None,
        };
        let spec = asset_spec_with(condition.clone());
        let result = dry_run_asset("my_table", &spec);
        assert_eq!(result.conditions[0].condition, condition);
    }

    #[test]
    fn dry_run_command() {
        let condition = DesiredCondition::Command {
            name: "dbt-test".to_string(),
            run: vec!["dbt".to_string(), "test".to_string()],
            interval: None,
        };
        let spec = asset_spec_with(condition.clone());
        let result = dry_run_asset("my_table", &spec);
        assert_eq!(result.conditions[0].condition, condition);
    }

    #[test]
    fn dry_run_skips_refs() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![
                DesiredSetEntry::Ref(crate::kind::asset::DesiredGroupRef {
                    ref_name: "group-a".to_string(),
                }),
                DesiredSetEntry::Inline(DesiredCondition::SQL {
                    name: "check".to_string(),
                    query: "SELECT 1".to_string(),
                    interval: None,
                }),
            ],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let result = dry_run_asset("my_table", &spec);
        assert_eq!(result.conditions.len(), 1);
        assert_eq!(result.conditions[0].name, "check");
    }

    #[test]
    fn dry_run_no_conditions() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let result = dry_run_asset("my_table", &spec);
        assert!(result.conditions.is_empty());
    }

    // ── Command without connection ─────────────────────────────────────────────

    #[tokio::test]
    async fn command_condition_does_not_need_connection() {
        // Assets with only Command conditions can be evaluated without a DB connection.
        let spec = asset_spec_with(DesiredCondition::Command {
            name: "always-true".to_string(),
            run: vec!["true".to_string()],
            interval: None,
        });
        let result = evaluate_asset("my_table", &spec, None, None).await.unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn sql_condition_without_connection_returns_error() {
        let spec = asset_spec_with(DesiredCondition::SQL {
            name: "check".to_string(),
            query: "SELECT 1".to_string(),
            interval: None,
        });
        let result = evaluate_asset("my_table", &spec, None, None).await;
        assert!(matches!(
            result,
            Err(EvaluateError::NoConnection { condition_name }) if condition_name == "check"
        ));
    }

    #[tokio::test]
    async fn sql_rejects_non_select_queries() {
        let conn = MockConnection {
            response: Value::Bool(true),
        };
        let forbidden_queries = [
            "INSERT INTO t VALUES (1)",
            "DELETE FROM t WHERE id = 1",
            "UPDATE t SET x = 1",
            "DROP TABLE t",
            "CREATE TABLE t (id INT64)",
            "TRUNCATE TABLE t",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 1",
            "SELECT 1; DROP TABLE t",
        ];
        for query in forbidden_queries {
            let spec = asset_spec_with(DesiredCondition::SQL {
                name: "bad".to_string(),
                query: query.to_string(),
                interval: None,
            });
            let result = evaluate_asset("my_table", &spec, Some(&conn), None).await;
            assert!(
                matches!(&result, Err(EvaluateError::ReadOnlyViolation(_))),
                "expected ReadOnlyViolation for query: {query}, got: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn sql_accepts_valid_select_queries() {
        let conn = MockConnection {
            response: Value::Bool(true),
        };
        let valid_queries = [
            "SELECT true",
            "  \n  SELECT true",
            "select count(*) = 0 from t",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
        ];
        for query in valid_queries {
            let spec = asset_spec_with(DesiredCondition::SQL {
                name: "check".to_string(),
                query: query.to_string(),
                interval: None,
            });
            let result = evaluate_asset("my_table", &spec, Some(&conn), None).await;
            assert!(
                result.is_ok(),
                "expected success for query: {query}, got: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn freshness_condition_without_connection_returns_error() {
        let spec = asset_spec_with(freshness_condition(86400, None));
        let result = evaluate_asset("my_table", &spec, None, None).await;
        assert!(matches!(
            result,
            Err(EvaluateError::NoConnection { condition_name }) if condition_name == "freshness"
        ));
    }
}

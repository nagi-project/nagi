mod command;
mod freshness;

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::runtime::compile::ResolvedOnDriftEntry;
use crate::runtime::kind::asset::DesiredCondition;
use crate::runtime::kind::connection::{Connection, ConnectionError};
use crate::runtime::log::{LogError, LogStore};
use crate::runtime::sync::generate_uuid;

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
    Drifted {
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
    Compile(#[from] crate::runtime::compile::CompileError),
    #[error("failed to parse compiled asset: {0}")]
    Parse(String),
    #[error("SQL query must be a single SELECT statement (read-only direct access): {0}")]
    ReadOnlyViolation(String),
    #[error("cache error: {0}")]
    Cache(String),
    #[error("serialization error: {0}")]
    Serialize(String),
    #[error("subprocess env resolution error: {0}")]
    EnvResolution(#[from] crate::runtime::subprocess::SubprocessEnvError),
    #[error("condition '{condition_name}' timed out after {seconds}s")]
    Timeout {
        condition_name: String,
        seconds: u64,
    },
}

fn evaluate_boolean(value: Value) -> Result<ConditionStatus, EvaluateError> {
    match &value {
        Value::Bool(true) => Ok(ConditionStatus::Ready),
        Value::Bool(false) => Ok(ConditionStatus::Drifted {
            reason: "condition returned false".to_string(),
        }),
        Value::String(s) if s.eq_ignore_ascii_case("true") => Ok(ConditionStatus::Ready),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Ok(ConditionStatus::Drifted {
            reason: "condition returned false".to_string(),
        }),
        other => Err(EvaluateError::UnexpectedResult(format!(
            "SQL condition must return a scalar boolean (true/false), got: {other}"
        ))),
    }
}

/// Parses the query with the connection's SQL dialect and rejects anything
/// other than a single SELECT statement.
fn require_select_only(query: &str, dialect: &dyn Dialect) -> Result<(), EvaluateError> {
    let stmts = Parser::new(dialect)
        .try_with_sql(query)
        .and_then(|mut p| p.parse_statements())
        .map_err(|e| EvaluateError::ReadOnlyViolation(e.to_string()))?;

    match stmts.as_slice() {
        [sqlparser::ast::Statement::Query(_)] => Ok(()),
        _ => Err(EvaluateError::ReadOnlyViolation(query.to_string())),
    }
}

async fn evaluate_condition(
    name: &str,
    asset_name: &str,
    condition: &DesiredCondition,
    conn: Option<&dyn Connection>,
    default_timeout: Duration,
) -> Result<ConditionResult, EvaluateError> {
    let timeout = condition
        .timeout()
        .map(|d| d.as_std())
        .unwrap_or(default_timeout);
    let fut = evaluate_condition_inner(name, asset_name, condition, conn);
    match tokio::time::timeout(timeout, fut).await {
        Ok(res) => res,
        Err(_) => Err(EvaluateError::Timeout {
            condition_name: name.to_string(),
            seconds: timeout.as_secs(),
        }),
    }
}

async fn evaluate_condition_inner(
    name: &str,
    asset_name: &str,
    condition: &DesiredCondition,
    conn: Option<&dyn Connection>,
) -> Result<ConditionResult, EvaluateError> {
    macro_rules! require_conn {
        () => {
            conn.ok_or_else(|| EvaluateError::NoConnection {
                condition_name: name.to_string(),
            })?
        };
    }

    let condition_type = condition.condition_type_name().to_string();
    let status = match condition {
        DesiredCondition::Freshness {
            max_age, column, ..
        } => {
            let c = require_conn!();
            let sql = c.freshness_sql(asset_name, column.as_deref())?;
            let value = c.query_scalar(&sql).await?;
            freshness::evaluate_freshness(value, max_age.as_std())?
        }
        DesiredCondition::Sql { query, .. } => {
            let c = require_conn!();
            require_select_only(query, &*c.sql_dialect())?;
            let value = c.query_scalar(query).await?;
            evaluate_boolean(value)?
        }
        DesiredCondition::Command { run, env, .. } => command::evaluate_command(run, env).await?,
    };
    Ok(ConditionResult {
        condition_name: name.to_string(),
        condition_type,
        status,
    })
}

/// Evaluates all conditions and collects results.
async fn evaluate_conditions(
    asset_name: &str,
    on_drift: &[ResolvedOnDriftEntry],
    conn: Option<&dyn Connection>,
    cached_conditions: &HashMap<String, ConditionResult>,
    default_timeout: Duration,
) -> Result<AssetEvalResult, EvaluateError> {
    let conditions = on_drift.iter().flat_map(|e| &e.conditions);
    let mut results = Vec::new();
    for cond in conditions {
        if let Some(cached) = cached_conditions.get(cond.name()) {
            tracing::debug!(
                asset = %asset_name,
                condition = %cond.name(),
                "using cached condition result (TTL valid)"
            );
            results.push(cached.clone());
        } else {
            let result =
                evaluate_condition(cond.name(), asset_name, cond, conn, default_timeout).await?;
            results.push(result);
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

/// Evaluates all conditions across all on_drift entries.
///
/// `conn` is required only for SQL-based conditions (Freshness, SQL).
/// Passing `None` for an Asset that only uses `Command` conditions is valid.
/// Passing `None` when a SQL condition is present returns `EvaluateError::NoConnection`.
/// When `log_store` is `Some`, automatically writes evaluate logs after evaluation.
pub async fn evaluate_asset(
    asset_name: &str,
    on_drift: &[ResolvedOnDriftEntry],
    conn: Option<&dyn Connection>,
    log_store: Option<&LogStore>,
    default_timeout: Duration,
) -> Result<AssetEvalResult, EvaluateError> {
    let started_at = Utc::now();
    let mut result = evaluate_conditions(
        asset_name,
        on_drift,
        conn,
        &Default::default(),
        default_timeout,
    )
    .await?;

    if let Some(store) = log_store {
        let id = generate_uuid();
        let finished_at = Utc::now();
        let started_str = started_at.to_rfc3339();
        let finished_str = finished_at.to_rfc3339();
        store.write_evaluate_log(&id, &result, &started_str, &finished_str)?;
        result.evaluation_id = Some(id);
    }

    Ok(result)
}

/// Evaluates conditions with TTL-based caching support.
/// Produces a `Send` future (unlike `evaluate_asset` which takes `&LogStore`).
pub(crate) async fn evaluate_asset_cached(
    asset_name: &str,
    on_drift: &[ResolvedOnDriftEntry],
    conn: Option<&dyn Connection>,
    cached_conditions: &HashMap<String, ConditionResult>,
    default_timeout: Duration,
) -> Result<AssetEvalResult, EvaluateError> {
    evaluate_conditions(
        asset_name,
        on_drift,
        conn,
        cached_conditions,
        default_timeout,
    )
    .await
}

/// A single condition from an asset's on_drift entries, with its name.
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
pub fn dry_run_asset(asset_name: &str, on_drift: &[ResolvedOnDriftEntry]) -> DryRunResult {
    let mut conditions = Vec::new();
    for entry in on_drift {
        for cond in &entry.conditions {
            conditions.push(DryRunCondition {
                name: cond.name().to_string(),
                condition: cond.clone(),
            });
        }
    }
    DryRunResult {
        asset_name: asset_name.to_string(),
        conditions,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use serde_json::Value;

    use super::*;
    use crate::runtime::compile::ResolvedOnDriftEntry;
    use crate::runtime::duration::Duration;
    use crate::runtime::kind::asset::DesiredCondition;
    use crate::runtime::kind::connection::ConnectionError;
    use crate::runtime::kind::sync::{SyncSpec, SyncStep};

    // ── Helpers ───────────────────────────────────────────────────────────────

    struct MockConnection {
        response: Value,
    }

    #[async_trait]
    impl Connection for MockConnection {
        async fn query_scalar(&self, _sql: &str) -> Result<Value, ConnectionError> {
            Ok(self.response.clone())
        }

        fn freshness_sql(
            &self,
            asset_name: &str,
            column: Option<&str>,
        ) -> Result<String, ConnectionError> {
            Ok(stub_freshness_sql(asset_name, column))
        }

        fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
            Box::new(sqlparser::dialect::BigQueryDialect {})
        }

        async fn execute_sql(&self, _sql: &str) -> Result<(), ConnectionError> {
            Ok(())
        }

        async fn load_jsonl(
            &self,
            _dataset: &str,
            _table: &str,
            _jsonl_path: &std::path::Path,
        ) -> Result<(), ConnectionError> {
            Ok(())
        }
    }

    fn stub_freshness_sql(asset_name: &str, column: Option<&str>) -> String {
        match column {
            Some(col) => format!("SELECT MAX(`{col}`) FROM `{asset_name}`"),
            None => format!("SELECT MAX(last_modified_time) FROM `{asset_name}`"),
        }
    }

    fn dummy_sync_spec() -> SyncSpec {
        SyncSpec::new(SyncStep::command(vec!["true".to_string()]))
    }

    fn on_drift_with(conditions: Vec<DesiredCondition>) -> Vec<ResolvedOnDriftEntry> {
        vec![ResolvedOnDriftEntry {
            conditions,
            conditions_ref: "test-conditions".to_string(),
            sync: dummy_sync_spec(),
            sync_ref_name: "test-sync".to_string(),
        }]
    }

    fn duration(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    /// 1 hour — arbitrary interval for test freshness conditions.
    const TEST_INTERVAL_SECS: u64 = 3600;

    fn freshness_condition(max_age_secs: u64, column: Option<&str>) -> DesiredCondition {
        DesiredCondition::Freshness {
            name: "freshness".to_string(),
            max_age: duration(max_age_secs),
            interval: duration(TEST_INTERVAL_SECS),
            check_at: None,
            column: column.map(str::to_string),
            evaluate_cache_ttl: None,
            timeout: None,
        }
    }

    fn test_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(30)
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
        let conn = MockConnection {
            response: epoch_secs_ago(3600.0),
        };
        let on_drift = on_drift_with(vec![freshness_condition(7200, None)]);
        let result = evaluate_asset(
            "my_dataset.my_table",
            &on_drift,
            Some(&conn),
            None,
            test_timeout(),
        )
        .await
        .unwrap();
        assert!(result.ready);
        assert_eq!(result.conditions[0].status, ConditionStatus::Ready);
    }

    #[tokio::test]
    async fn freshness_not_ready_when_exceeds_max_age() {
        let conn = MockConnection {
            response: epoch_secs_ago(25.0 * 3600.0),
        };
        let on_drift = on_drift_with(vec![freshness_condition(86400, None)]);
        let result = evaluate_asset(
            "my_dataset.my_table",
            &on_drift,
            Some(&conn),
            None,
            test_timeout(),
        )
        .await
        .unwrap();
        assert!(!result.ready);
        assert!(matches!(
            &result.conditions[0].status,
            ConditionStatus::Drifted { .. }
        ));
    }

    #[tokio::test]
    async fn freshness_accepts_rfc3339_timestamp() {
        let conn = MockConnection {
            response: Value::String("2099-01-01T00:00:00Z".to_string()),
        };
        let on_drift = on_drift_with(vec![freshness_condition(86400, Some("updated_at"))]);
        let result = evaluate_asset("my_table", &on_drift, Some(&conn), None, test_timeout())
            .await
            .unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn freshness_returns_error_on_unexpected_value() {
        let conn = MockConnection {
            response: Value::Null,
        };
        let on_drift = on_drift_with(vec![freshness_condition(86400, None)]);
        let result = evaluate_asset("my_table", &on_drift, Some(&conn), None, test_timeout()).await;
        assert!(matches!(result, Err(EvaluateError::UnexpectedResult(_))));
    }

    // ── SQL ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_ready_when_true() {
        let conn = MockConnection {
            response: Value::Bool(true),
        };
        let on_drift = on_drift_with(vec![DesiredCondition::Sql {
            name: "check".to_string(),
            query: "SELECT true".to_string(),
            interval: None,
            evaluate_cache_ttl: None,
            timeout: None,
        }]);
        let result = evaluate_asset("my_table", &on_drift, Some(&conn), None, test_timeout())
            .await
            .unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn sql_not_ready_when_false() {
        let conn = MockConnection {
            response: Value::Bool(false),
        };
        let on_drift = on_drift_with(vec![DesiredCondition::Sql {
            name: "check".to_string(),
            query: "SELECT false".to_string(),
            interval: None,
            evaluate_cache_ttl: None,
            timeout: None,
        }]);
        let result = evaluate_asset("my_table", &on_drift, Some(&conn), None, test_timeout())
            .await
            .unwrap();
        assert!(!result.ready);
    }

    // ── all conditions AND ────────────────────────────────────────────────────

    #[tokio::test]
    async fn all_conditions_must_pass() {
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

            fn freshness_sql(
                &self,
                asset_name: &str,
                column: Option<&str>,
            ) -> Result<String, ConnectionError> {
                Ok(stub_freshness_sql(asset_name, column))
            }

            fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
                Box::new(sqlparser::dialect::BigQueryDialect {})
            }

            async fn execute_sql(&self, _sql: &str) -> Result<(), ConnectionError> {
                Ok(())
            }

            async fn load_jsonl(
                &self,
                _dataset: &str,
                _table: &str,
                _jsonl_path: &std::path::Path,
            ) -> Result<(), ConnectionError> {
                Ok(())
            }
        }
        let on_drift = on_drift_with(vec![
            DesiredCondition::Sql {
                name: "check-a".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
                timeout: None,
            },
            DesiredCondition::Sql {
                name: "check-b".to_string(),
                query: "SELECT false".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
                timeout: None,
            },
        ]);
        let result = evaluate_asset(
            "my_table",
            &on_drift,
            Some(&CountingConnection(&call_count)),
            None,
            test_timeout(),
        )
        .await
        .unwrap();
        assert!(!result.ready);
        assert_eq!(result.conditions[0].status, ConditionStatus::Ready);
        assert!(matches!(
            result.conditions[1].status,
            ConditionStatus::Drifted { .. }
        ));
    }

    // ── dry_run ────────────────────────────────────────────────────────────

    #[test]
    fn dry_run_freshness_with_column() {
        let condition = freshness_condition(86400, Some("updated_at"));
        let on_drift = on_drift_with(vec![condition.clone()]);
        let result = dry_run_asset("my_table", &on_drift);
        assert_eq!(result.conditions.len(), 1);
        assert_eq!(result.conditions[0].condition, condition);
    }

    #[test]
    fn dry_run_sql() {
        let condition = DesiredCondition::Sql {
            name: "check".to_string(),
            query: "SELECT COUNT(*) > 0 FROM orders".to_string(),
            interval: None,
            evaluate_cache_ttl: None,
            timeout: None,
        };
        let on_drift = on_drift_with(vec![condition.clone()]);
        let result = dry_run_asset("my_table", &on_drift);
        assert_eq!(result.conditions[0].condition, condition);
    }

    #[test]
    fn dry_run_command() {
        let condition = DesiredCondition::Command {
            name: "dbt-test".to_string(),
            run: vec!["dbt".to_string(), "test".to_string()],
            interval: None,
            env: HashMap::new(),
            evaluate_cache_ttl: None,
            timeout: None,
            identity: None,
        };
        let on_drift = on_drift_with(vec![condition.clone()]);
        let result = dry_run_asset("my_table", &on_drift);
        assert_eq!(result.conditions[0].condition, condition);
    }

    // ── Command without connection ─────────────────────────────────────────────

    #[tokio::test]
    async fn command_condition_does_not_need_connection() {
        let on_drift = on_drift_with(vec![DesiredCondition::Command {
            name: "always-true".to_string(),
            run: vec!["true".to_string()],
            interval: None,
            env: HashMap::new(),
            evaluate_cache_ttl: None,
            timeout: None,
            identity: None,
        }]);
        let result = evaluate_asset("my_table", &on_drift, None, None, test_timeout())
            .await
            .unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn sql_condition_without_connection_returns_error() {
        let on_drift = on_drift_with(vec![DesiredCondition::Sql {
            name: "check".to_string(),
            query: "SELECT 1".to_string(),
            interval: None,
            evaluate_cache_ttl: None,
            timeout: None,
        }]);
        let result = evaluate_asset("my_table", &on_drift, None, None, test_timeout()).await;
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
            let on_drift = on_drift_with(vec![DesiredCondition::Sql {
                name: "bad".to_string(),
                query: query.to_string(),
                interval: None,
                evaluate_cache_ttl: None,
                timeout: None,
            }]);
            let result =
                evaluate_asset("my_table", &on_drift, Some(&conn), None, test_timeout()).await;
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
            let on_drift = on_drift_with(vec![DesiredCondition::Sql {
                name: "check".to_string(),
                query: query.to_string(),
                interval: None,
                evaluate_cache_ttl: None,
                timeout: None,
            }]);
            let result =
                evaluate_asset("my_table", &on_drift, Some(&conn), None, test_timeout()).await;
            assert!(
                result.is_ok(),
                "expected success for query: {query}, got: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn freshness_condition_without_connection_returns_error() {
        let on_drift = on_drift_with(vec![freshness_condition(86400, None)]);
        let result = evaluate_asset("my_table", &on_drift, None, None, test_timeout()).await;
        assert!(matches!(
            result,
            Err(EvaluateError::NoConnection { condition_name }) if condition_name == "freshness"
        ));
    }

    // ── cached conditions ─────────────────────────────────────────────────

    #[tokio::test]
    async fn cached_condition_skips_evaluation() {
        // SQL condition that would need a connection — but it's cached, so no error.
        let on_drift = on_drift_with(vec![DesiredCondition::Sql {
            name: "check".to_string(),
            query: "SELECT true".to_string(),
            interval: None,
            evaluate_cache_ttl: None,
            timeout: None,
        }]);
        let cached: HashMap<String, ConditionResult> = [(
            "check".to_string(),
            ConditionResult {
                condition_name: "check".to_string(),
                condition_type: "SQL".to_string(),
                status: ConditionStatus::Ready,
            },
        )]
        .into();
        // No connection provided — would fail without cache.
        let result = evaluate_asset_cached("my_table", &on_drift, None, &cached, test_timeout())
            .await
            .unwrap();
        assert!(result.ready);
        assert_eq!(result.conditions[0].status, ConditionStatus::Ready);
    }

    #[tokio::test]
    async fn uncached_condition_is_evaluated_normally() {
        let conn = MockConnection {
            response: Value::Bool(false),
        };
        let on_drift = on_drift_with(vec![DesiredCondition::Sql {
            name: "check".to_string(),
            query: "SELECT false".to_string(),
            interval: None,
            evaluate_cache_ttl: None,
            timeout: None,
        }]);
        let cached = HashMap::new();
        let result =
            evaluate_asset_cached("my_table", &on_drift, Some(&conn), &cached, test_timeout())
                .await
                .unwrap();
        assert!(!result.ready);
    }

    #[tokio::test]
    async fn mixed_cached_and_uncached_conditions() {
        let conn = MockConnection {
            response: Value::Bool(true),
        };
        let on_drift = on_drift_with(vec![
            DesiredCondition::Sql {
                name: "cached-check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
                timeout: None,
            },
            DesiredCondition::Sql {
                name: "live-check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
                timeout: None,
            },
        ]);
        let cached: HashMap<String, ConditionResult> = [(
            "cached-check".to_string(),
            ConditionResult {
                condition_name: "cached-check".to_string(),
                condition_type: "SQL".to_string(),
                status: ConditionStatus::Drifted {
                    reason: "from cache".to_string(),
                },
            },
        )]
        .into();
        let result =
            evaluate_asset_cached("my_table", &on_drift, Some(&conn), &cached, test_timeout())
                .await
                .unwrap();
        // cached-check is Drifted (from cache), live-check is Ready (evaluated)
        assert!(!result.ready);
        assert!(matches!(
            &result.conditions[0].status,
            ConditionStatus::Drifted { reason } if reason == "from cache"
        ));
        assert_eq!(result.conditions[1].status, ConditionStatus::Ready);
    }

    // ── evaluate_boolean ──────────────────────────────────────────────────

    macro_rules! boolean_ready {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(evaluate_boolean($input).unwrap(), ConditionStatus::Ready);
                }
            )*
        };
    }

    boolean_ready! {
        bool_true_is_ready: Value::Bool(true);
        string_true_is_ready: Value::String("true".to_string());
        string_true_upper_is_ready: Value::String("TRUE".to_string());
        string_true_mixed_is_ready: Value::String("True".to_string());
    }

    macro_rules! boolean_not_ready {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(matches!(
                        evaluate_boolean($input).unwrap(),
                        ConditionStatus::Drifted { .. }
                    ));
                }
            )*
        };
    }

    boolean_not_ready! {
        bool_false_is_not_ready: Value::Bool(false);
        string_false_is_not_ready: Value::String("false".to_string());
        string_false_upper_is_not_ready: Value::String("FALSE".to_string());
    }

    #[test]
    fn boolean_unexpected_value_returns_error() {
        assert!(matches!(
            evaluate_boolean(Value::Null),
            Err(EvaluateError::UnexpectedResult(_))
        ));
        assert!(matches!(
            evaluate_boolean(Value::Number(serde_json::Number::from(1))),
            Err(EvaluateError::UnexpectedResult(_))
        ));
        assert!(matches!(
            evaluate_boolean(Value::String("yes".to_string())),
            Err(EvaluateError::UnexpectedResult(_))
        ));
    }

    #[tokio::test]
    async fn command_not_found_returns_error() {
        let on_drift = on_drift_with(vec![DesiredCondition::Command {
            name: "bad-cmd".to_string(),
            run: vec!["__nagi_no_such_command__".to_string()],
            interval: None,
            env: HashMap::new(),
            evaluate_cache_ttl: None,
            timeout: None,
            identity: None,
        }]);
        let result = evaluate_asset("a", &on_drift, None, None, test_timeout()).await;
        assert!(
            matches!(result, Err(EvaluateError::CommandFailed(_))),
            "expected CommandFailed, got {result:?}"
        );
    }

    #[tokio::test]
    async fn command_exit_nonzero_is_drifted_not_error() {
        let on_drift = on_drift_with(vec![DesiredCondition::Command {
            name: "fail-cmd".to_string(),
            run: vec!["false".to_string()],
            interval: None,
            env: HashMap::new(),
            evaluate_cache_ttl: None,
            timeout: None,
            identity: None,
        }]);
        let result = evaluate_asset("a", &on_drift, None, None, test_timeout())
            .await
            .unwrap();
        assert!(!result.ready);
        assert!(matches!(
            result.conditions[0].status,
            ConditionStatus::Drifted { .. }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_condition_times_out() {
        let on_drift = on_drift_with(vec![DesiredCondition::Command {
            name: "slow-cmd".to_string(),
            run: vec!["sleep".to_string(), "5".to_string()],
            interval: None,
            env: HashMap::new(),
            evaluate_cache_ttl: None,
            timeout: Some(crate::runtime::duration::Duration::from_secs(0)),
            identity: None,
        }]);
        // Per-condition timeout of 0s triggers immediate timeout.
        let result = evaluate_asset("a", &on_drift, None, None, test_timeout()).await;
        assert!(
            matches!(result, Err(EvaluateError::Timeout { .. })),
            "expected Timeout, got {result:?}"
        );
    }
}

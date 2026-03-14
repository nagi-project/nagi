mod boolean;
mod command;
mod condition;
mod freshness;

use serde::{Deserialize, Serialize};

use crate::db::{Connection, ConnectionError};
use crate::kind::asset::{AssetSpec, DesiredSetEntry};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionResult {
    pub index: usize,
    pub condition_type: String,
    pub status: ConditionStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "state")]
pub enum ConditionStatus {
    Ready,
    NotReady { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetEvalResult {
    pub asset_name: String,
    /// true when all conditions are Ready.
    pub ready: bool,
    pub conditions: Vec<ConditionResult>,
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluateError {
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
    #[error("command error: {0}")]
    CommandFailed(String),
}

/// Evaluates all desired conditions of `spec` against the given connection.
/// `asset_name` is used as the default table name for queries.
pub async fn evaluate_asset(
    asset_name: &str,
    spec: &AssetSpec,
    conn: &dyn Connection,
) -> Result<AssetEvalResult, EvaluateError> {
    let mut results = Vec::new();
    for (i, entry) in spec.desired_sets.iter().enumerate() {
        match entry {
            DesiredSetEntry::Ref(_) => {
                // DesiredGroup refs are resolved at compile time; skip during evaluation.
                continue;
            }
            DesiredSetEntry::Inline(condition) => {
                let result = condition::evaluate_condition(i, asset_name, condition, conn).await?;
                results.push(result);
            }
        }
    }
    let ready = results.iter().all(|r| r.status == ConditionStatus::Ready);
    Ok(AssetEvalResult {
        asset_name: asset_name.to_string(),
        ready,
        conditions: results,
    })
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
    }

    fn stub_freshness_sql(asset_name: &str, column: Option<&str>) -> String {
        match column {
            Some(col) => format!("SELECT MAX(`{col}`) FROM `{asset_name}`"),
            None => format!("SELECT MAX(last_modified_time) FROM `{asset_name}`"),
        }
    }

    fn asset_spec_with(condition: DesiredCondition) -> AssetSpec {
        AssetSpec {
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
        let result = evaluate_asset("my_dataset.my_table", &spec, &conn)
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
        let result = evaluate_asset("my_dataset.my_table", &spec, &conn)
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
        let result = evaluate_asset("my_table", &spec, &conn).await.unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn freshness_returns_error_on_unexpected_value() {
        let conn = MockConnection {
            response: Value::Null,
        };
        let spec = asset_spec_with(freshness_condition(86400, None));
        let result = evaluate_asset("my_table", &spec, &conn).await;
        assert!(matches!(result, Err(EvaluateError::UnexpectedResult(_))));
    }

    // ── SQL ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_ready_when_true() {
        let conn = MockConnection {
            response: Value::Bool(true),
        };
        let spec = asset_spec_with(DesiredCondition::SQL {
            query: "SELECT true".to_string(),
        });
        let result = evaluate_asset("my_table", &spec, &conn).await.unwrap();
        assert!(result.ready);
    }

    #[tokio::test]
    async fn sql_not_ready_when_false() {
        let conn = MockConnection {
            response: Value::Bool(false),
        };
        let spec = asset_spec_with(DesiredCondition::SQL {
            query: "SELECT false".to_string(),
        });
        let result = evaluate_asset("my_table", &spec, &conn).await.unwrap();
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
        }
        let spec = AssetSpec {
            sources: vec![],
            desired_sets: vec![
                DesiredSetEntry::Inline(DesiredCondition::SQL {
                    query: "SELECT true".to_string(),
                }),
                DesiredSetEntry::Inline(DesiredCondition::SQL {
                    query: "SELECT false".to_string(),
                }),
            ],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let result = evaluate_asset("my_table", &spec, &CountingConnection(&call_count))
            .await
            .unwrap();
        assert!(!result.ready);
        assert_eq!(result.conditions[0].status, ConditionStatus::Ready);
        assert!(matches!(
            result.conditions[1].status,
            ConditionStatus::NotReady { .. }
        ));
    }
}

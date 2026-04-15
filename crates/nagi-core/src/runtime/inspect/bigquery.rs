use crate::runtime::inspect::{InspectError, PhysicalObjectState};
use crate::runtime::kind::connection::bigquery::{escape_backtick, escape_single_quote};
use crate::runtime::kind::connection::Connection;

use std::collections::HashMap;

/// Resolves the physical object type for a BigQuery table/view.
///
/// Queries `INFORMATION_SCHEMA.TABLES` using the connection's project and
/// dataset, looking up `model_name` as `table_name`. Returns the `table_type`
/// string (e.g. "BASE TABLE", "VIEW", "MATERIALIZED VIEW") or `None` if no
/// matching object exists.
async fn resolve_object_type(
    conn: &dyn Connection,
    project: &str,
    dataset: &str,
    model_name: &str,
) -> Result<Option<String>, InspectError> {
    let project = escape_backtick(project);
    let dataset = escape_backtick(dataset);
    let model_name_lit = escape_single_quote(model_name);
    let sql = format!(
        "SELECT table_type FROM `{project}`.`{dataset}`.INFORMATION_SCHEMA.TABLES \
         WHERE table_name = '{model_name_lit}' LIMIT 1"
    );
    let result = conn
        .query_scalar(&sql)
        .await
        .map_err(|e| InspectError::Connection(e.to_string()))?;
    match result {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => Ok(Some(s)),
        other => Ok(Some(other.to_string())),
    }
}

/// Fetches the row count of a BigQuery object via `SELECT COUNT(*)`.
async fn fetch_row_count(
    conn: &dyn Connection,
    project: &str,
    dataset: &str,
    model_name: &str,
) -> Result<u64, InspectError> {
    let project = escape_backtick(project);
    let dataset = escape_backtick(dataset);
    let model_name = escape_backtick(model_name);
    let sql = format!("SELECT COUNT(*) FROM `{project}`.`{dataset}`.`{model_name}`");
    let result = conn
        .query_scalar(&sql)
        .await
        .map_err(|e| InspectError::Connection(e.to_string()))?;
    match result {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| InspectError::Connection("COUNT(*) returned non-u64".to_string())),
        serde_json::Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| InspectError::Connection(format!("COUNT(*) returned '{s}'"))),
        other => Err(InspectError::Connection(format!(
            "COUNT(*) returned unexpected type: {other}"
        ))),
    }
}

/// Fetches destination jobs from BigQuery `INFORMATION_SCHEMA.JOBS_BY_PROJECT`
/// by matching the `nagi_execution_id` job label.
///
/// `location` specifies the BigQuery region (e.g. "us", "asia-northeast1").
/// If `None`, defaults to "us".
#[allow(dead_code)] // integrated when nagi inspect performs lazy fetch
pub async fn fetch_destination_jobs(
    conn: &dyn Connection,
    project: &str,
    location: Option<&str>,
    execution_id: &str,
) -> Result<Vec<crate::runtime::inspect::DestinationJob>, InspectError> {
    let project = escape_backtick(project);
    let region = location.unwrap_or("us");
    let region = escape_backtick(region);
    let execution_id_lit = escape_single_quote(execution_id);
    let sql = format!(
        "SELECT job_id, statement_type \
         FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, \
              UNNEST(labels) AS l \
         WHERE l.key = 'nagi_execution_id' \
           AND l.value = '{execution_id_lit}'"
    );
    let rows = conn
        .query_rows(&sql)
        .await
        .map_err(|e| InspectError::Connection(e.to_string()))?;

    Ok(rows
        .into_iter()
        .filter_map(row_to_destination_job)
        .collect())
}

fn row_to_destination_job(
    row: serde_json::Value,
) -> Option<crate::runtime::inspect::DestinationJob> {
    let mut obj = match row {
        serde_json::Value::Object(m) => m,
        _ => return None,
    };
    let job_id = obj
        .remove("job_id")
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default();
    let statement_type = obj
        .remove("statement_type")
        .and_then(|v| v.as_str().map(String::from));
    let details = obj.into_iter().collect();
    Some(crate::runtime::inspect::DestinationJob {
        job_id,
        statement_type,
        details,
    })
}

/// Builds a `PhysicalObjectState` for a BigQuery object by querying its type
/// and row count.
pub async fn fetch_physical_object_state(
    conn: &dyn Connection,
    project: &str,
    dataset: &str,
    model_name: &str,
) -> Result<Option<PhysicalObjectState>, InspectError> {
    let object_type = resolve_object_type(conn, project, dataset, model_name).await?;
    let Some(object_type) = object_type else {
        return Ok(None);
    };

    let row_count = fetch_row_count(conn, project, dataset, model_name).await?;
    let mut metrics = HashMap::new();
    metrics.insert("row_count".to_string(), serde_json::json!(row_count));

    Ok(Some(PhysicalObjectState {
        object_type,
        metrics,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::kind::connection::ConnectionError;
    use async_trait::async_trait;

    /// Mock Connection that returns preconfigured responses for specific SQL patterns
    /// and records all SQL statements for assertion.
    ///
    /// For `query_scalar`: matches pattern → returns scalar value.
    /// For `query_rows`: matches pattern → returns value as array (if Array) or wraps in vec.
    struct MockConnection {
        responses: Vec<(String, serde_json::Value)>,
        captured_sql: std::sync::Mutex<Vec<String>>,
    }

    impl MockConnection {
        fn new(responses: Vec<(&str, serde_json::Value)>) -> Self {
            Self {
                responses: responses
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
                captured_sql: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn last_sql(&self) -> String {
            self.captured_sql.lock().unwrap().last().unwrap().clone()
        }
    }

    #[async_trait]
    impl Connection for MockConnection {
        async fn query_scalar(&self, sql: &str) -> Result<serde_json::Value, ConnectionError> {
            self.captured_sql.lock().unwrap().push(sql.to_string());
            for (pattern, value) in &self.responses {
                if sql.contains(pattern) {
                    return Ok(value.clone());
                }
            }
            Err(ConnectionError::QueryFailed(format!(
                "no mock response for: {sql}"
            )))
        }

        fn freshness_sql(
            &self,
            _asset_name: &str,
            _column: Option<&str>,
        ) -> Result<String, ConnectionError> {
            unimplemented!()
        }

        fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
            Box::new(sqlparser::dialect::BigQueryDialect {})
        }

        async fn execute_sql(&self, _sql: &str) -> Result<(), ConnectionError> {
            unimplemented!()
        }

        async fn load_jsonl(
            &self,
            _dataset: &str,
            _table: &str,
            _jsonl_path: &std::path::Path,
        ) -> Result<(), ConnectionError> {
            unimplemented!()
        }

        async fn query_rows(&self, sql: &str) -> Result<Vec<serde_json::Value>, ConnectionError> {
            self.captured_sql.lock().unwrap().push(sql.to_string());
            for (pattern, value) in &self.responses {
                if sql.contains(pattern) {
                    return match value {
                        serde_json::Value::Array(arr) => Ok(arr.clone()),
                        serde_json::Value::Null => Ok(Vec::new()),
                        other => Ok(vec![other.clone()]),
                    };
                }
            }
            Err(ConnectionError::QueryFailed(format!(
                "no mock response for: {sql}"
            )))
        }
    }

    #[tokio::test]
    async fn resolve_base_table() {
        let conn = MockConnection::new(vec![(
            "INFORMATION_SCHEMA.TABLES",
            serde_json::json!("BASE TABLE"),
        )]);
        let result = resolve_object_type(&conn, "my-project", "my_dataset", "daily_sales")
            .await
            .unwrap();
        assert_eq!(result, Some("BASE TABLE".to_string()));
    }

    #[tokio::test]
    async fn resolve_view() {
        let conn = MockConnection::new(vec![(
            "INFORMATION_SCHEMA.TABLES",
            serde_json::json!("VIEW"),
        )]);
        let result = resolve_object_type(&conn, "my-project", "my_dataset", "daily_sales_view")
            .await
            .unwrap();
        assert_eq!(result, Some("VIEW".to_string()));
    }

    #[tokio::test]
    async fn resolve_nonexistent_returns_none() {
        let conn =
            MockConnection::new(vec![("INFORMATION_SCHEMA.TABLES", serde_json::Value::Null)]);
        let result = resolve_object_type(&conn, "my-project", "my_dataset", "does_not_exist")
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn fetch_row_count_numeric() {
        let conn = MockConnection::new(vec![("COUNT(*)", serde_json::json!(42000))]);
        let count = fetch_row_count(&conn, "my-project", "my_dataset", "daily_sales")
            .await
            .unwrap();
        assert_eq!(count, 42000);
    }

    #[tokio::test]
    async fn fetch_row_count_string() {
        // BigQuery sometimes returns numbers as strings
        let conn = MockConnection::new(vec![("COUNT(*)", serde_json::json!("12345"))]);
        let count = fetch_row_count(&conn, "my-project", "my_dataset", "daily_sales")
            .await
            .unwrap();
        assert_eq!(count, 12345);
    }

    #[tokio::test]
    async fn fetch_physical_object_state_for_table() {
        let conn = MockConnection::new(vec![
            ("INFORMATION_SCHEMA.TABLES", serde_json::json!("BASE TABLE")),
            ("COUNT(*)", serde_json::json!(1500)),
        ]);
        let state = fetch_physical_object_state(&conn, "my-project", "my_dataset", "daily_sales")
            .await
            .unwrap();
        let state = state.unwrap();
        assert_eq!(state.object_type, "BASE TABLE");
        assert_eq!(state.metrics["row_count"], serde_json::json!(1500));
    }

    #[tokio::test]
    async fn fetch_physical_object_state_for_nonexistent() {
        let conn =
            MockConnection::new(vec![("INFORMATION_SCHEMA.TABLES", serde_json::Value::Null)]);
        let state =
            fetch_physical_object_state(&conn, "my-project", "my_dataset", "does_not_exist")
                .await
                .unwrap();
        assert!(state.is_none());
    }

    // ── SQL injection prevention ────────────────────────────────────

    #[tokio::test]
    async fn resolve_object_type_escapes_single_quote_in_model_name() {
        let conn = MockConnection::new(vec![(
            "INFORMATION_SCHEMA.TABLES",
            serde_json::json!("BASE TABLE"),
        )]);
        resolve_object_type(&conn, "proj", "ds", "name'; DROP TABLE t--")
            .await
            .unwrap();
        let sql = conn.last_sql();
        assert!(
            sql.contains("name''; DROP TABLE t--"),
            "single quote should be escaped: {sql}"
        );
        assert!(
            !sql.contains("name'; DROP"),
            "raw quote must not appear: {sql}"
        );
    }

    #[tokio::test]
    async fn resolve_object_type_escapes_backtick_in_project() {
        let conn = MockConnection::new(vec![(
            "INFORMATION_SCHEMA.TABLES",
            serde_json::json!("VIEW"),
        )]);
        resolve_object_type(&conn, "proj`ect", "ds", "tbl")
            .await
            .unwrap();
        let sql = conn.last_sql();
        assert!(
            sql.contains("`proj``ect`"),
            "backtick in project should be escaped: {sql}"
        );
    }

    #[tokio::test]
    async fn fetch_row_count_escapes_backtick_in_model_name() {
        let conn = MockConnection::new(vec![("COUNT(*)", serde_json::json!(100))]);
        fetch_row_count(&conn, "proj", "ds", "tbl`name")
            .await
            .unwrap();
        let sql = conn.last_sql();
        assert!(
            sql.contains("`tbl``name`"),
            "backtick in model_name should be escaped: {sql}"
        );
    }

    // ── fetch_destination_jobs ───────────────────────────────────────

    #[tokio::test]
    async fn fetch_destination_jobs_returns_matching_jobs() {
        let conn = MockConnection::new(vec![(
            "INFORMATION_SCHEMA.JOBS",
            serde_json::json!([
                {"job_id": "bqjob_001", "statement_type": "MERGE"},
                {"job_id": "bqjob_002", "statement_type": "INSERT"}
            ]),
        )]);
        let jobs = fetch_destination_jobs(&conn, "my-project", Some("us"), "exec-001")
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, "bqjob_001");
        assert_eq!(jobs[0].statement_type.as_deref(), Some("MERGE"));
        assert_eq!(jobs[1].job_id, "bqjob_002");
        assert_eq!(jobs[1].statement_type.as_deref(), Some("INSERT"));
    }

    #[tokio::test]
    async fn fetch_destination_jobs_returns_empty_for_no_matches() {
        let conn = MockConnection::new(vec![("INFORMATION_SCHEMA.JOBS", serde_json::Value::Null)]);
        let jobs = fetch_destination_jobs(&conn, "my-project", Some("us"), "exec-999")
            .await
            .unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn fetch_destination_jobs_uses_location() {
        let conn = MockConnection::new(vec![("INFORMATION_SCHEMA.JOBS", serde_json::Value::Null)]);
        fetch_destination_jobs(&conn, "my-project", Some("asia-northeast1"), "exec-001")
            .await
            .unwrap();
        let sql = conn.last_sql();
        assert!(
            sql.contains("region-asia-northeast1"),
            "should use specified location: {sql}"
        );
    }

    #[tokio::test]
    async fn fetch_destination_jobs_defaults_to_us() {
        let conn = MockConnection::new(vec![("INFORMATION_SCHEMA.JOBS", serde_json::Value::Null)]);
        fetch_destination_jobs(&conn, "my-project", None, "exec-001")
            .await
            .unwrap();
        let sql = conn.last_sql();
        assert!(sql.contains("region-us"), "should default to us: {sql}");
    }

    #[tokio::test]
    async fn fetch_destination_jobs_escapes_execution_id() {
        let conn = MockConnection::new(vec![("INFORMATION_SCHEMA.JOBS", serde_json::Value::Null)]);
        fetch_destination_jobs(&conn, "proj", Some("us"), "exec'; DROP TABLE t--")
            .await
            .unwrap();
        let sql = conn.last_sql();
        assert!(
            sql.contains("exec''; DROP TABLE t--"),
            "single quote in execution_id should be escaped: {sql}"
        );
    }

    // ── row_to_destination_job ───────────────────────────────────────

    #[test]
    fn row_to_destination_job_extracts_fields() {
        let row = serde_json::json!({
            "job_id": "bqjob_001",
            "statement_type": "MERGE",
            "extra_field": 42
        });
        let job = row_to_destination_job(row).unwrap();
        assert_eq!(job.job_id, "bqjob_001");
        assert_eq!(job.statement_type.as_deref(), Some("MERGE"));
        assert_eq!(job.details["extra_field"], 42);
        assert!(!job.details.contains_key("job_id"));
        assert!(!job.details.contains_key("statement_type"));
    }

    #[test]
    fn row_to_destination_job_handles_missing_statement_type() {
        let row = serde_json::json!({"job_id": "bqjob_002"});
        let job = row_to_destination_job(row).unwrap();
        assert_eq!(job.job_id, "bqjob_002");
        assert!(job.statement_type.is_none());
    }

    #[test]
    fn row_to_destination_job_returns_none_for_non_object() {
        assert!(row_to_destination_job(serde_json::json!("not an object")).is_none());
        assert!(row_to_destination_job(serde_json::json!(42)).is_none());
        assert!(row_to_destination_job(serde_json::Value::Null).is_none());
    }
}

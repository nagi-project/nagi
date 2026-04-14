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
}

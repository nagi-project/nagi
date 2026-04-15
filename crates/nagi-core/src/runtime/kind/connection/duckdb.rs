use async_trait::async_trait;
use tokio::process::Command;

use super::sql::{escape_identifier, escape_literal};
use super::{Connection, ConnectionError};

/// DuckDB connection via the `duckdb` CLI subprocess.
pub struct DuckDbConnection {
    path: String,
}

impl DuckDbConnection {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    async fn run_cli(&self, sql: &str, json: bool) -> Result<String, ConnectionError> {
        let mut cmd = Command::new("duckdb");
        cmd.arg(&self.path).arg(sql);
        if json {
            cmd.arg("-json");
        }
        let output = cmd
            .output()
            .await
            .map_err(|e| ConnectionError::QueryFailed(format!("failed to run duckdb CLI: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ConnectionError::QueryFailed(stderr.into_owned()));
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

#[async_trait]
impl Connection for DuckDbConnection {
    async fn query_scalar(&self, sql: &str) -> Result<serde_json::Value, ConnectionError> {
        let stdout = self.run_cli(sql, true).await?;
        if stdout.trim().is_empty() {
            return Err(ConnectionError::QueryFailed(
                "query returned no rows".to_string(),
            ));
        }
        // DuckDB -json outputs an array of objects: [{"col": value}, ...]
        let rows: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_str(&stdout)
            .map_err(|e| {
            ConnectionError::QueryFailed(format!("failed to parse JSON output: {e}"))
        })?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| ConnectionError::QueryFailed("query returned no rows".to_string()))?;
        row.into_iter()
            .next()
            .map(|(_, v)| v)
            .ok_or_else(|| ConnectionError::QueryFailed("query returned no columns".to_string()))
    }

    fn freshness_sql(
        &self,
        asset_name: &str,
        column: Option<&str>,
    ) -> Result<String, ConnectionError> {
        match column {
            Some(col) => {
                let col = escape_identifier(col);
                let name = escape_identifier(asset_name);
                Ok(format!(
                    "SELECT strftime(MAX(\"{col}\") AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') FROM \"{name}\""
                ))
            }
            None => Err(ConnectionError::QueryFailed(
                "DuckDB does not support metadata-based freshness; specify a column".to_string(),
            )),
        }
    }

    fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
        Box::new(sqlparser::dialect::DuckDbDialect {})
    }

    fn max_concurrency(&self) -> Option<usize> {
        Some(1)
    }

    async fn execute_sql(&self, sql: &str) -> Result<(), ConnectionError> {
        self.run_cli(sql, false).await?;
        Ok(())
    }

    async fn load_jsonl(
        &self,
        dataset: &str,
        table: &str,
        jsonl_path: &std::path::Path,
    ) -> Result<(), ConnectionError> {
        let path_str = jsonl_path
            .to_str()
            .ok_or_else(|| ConnectionError::QueryFailed("invalid path".to_string()))?;
        let qualified = if dataset.is_empty() {
            format!("\"{}\"", escape_identifier(table))
        } else {
            format!(
                "\"{}\".\"{}\"",
                escape_identifier(dataset),
                escape_identifier(table)
            )
        };
        let path_lit = escape_literal(path_str);
        let sql = format!(
            "CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM read_json_auto('{path_lit}')"
        );
        self.run_cli(&sql, false).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_conn() -> DuckDbConnection {
        DuckDbConnection::new(":memory:")
    }

    // ── freshness_sql tests ─────────────────────────────────────────────

    macro_rules! freshness_sql_ok_test {
        ($($name:ident: $asset:expr, $col:expr => contains $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let conn = memory_conn();
                    let sql = conn.freshness_sql($asset, Some($col)).unwrap();
                    assert!(sql.contains($expected), "expected '{}' in '{sql}'", $expected);
                }
            )*
        };
    }

    freshness_sql_ok_test! {
        freshness_sql_with_column:
            "my_table", "updated_at" => contains r#"MAX("updated_at")"#;
        freshness_sql_formats_rfc3339:
            "my_table", "updated_at" => contains "strftime";
        freshness_sql_escapes_column:
            "t", r#"my"col"# => contains r#""my""col""#;
        freshness_sql_escapes_table:
            r#"my"table"#, "c" => contains r#""my""table""#;
    }

    #[test]
    fn freshness_sql_without_column_returns_error() {
        let conn = memory_conn();
        let err = conn.freshness_sql("my_table", None).unwrap_err();
        assert!(matches!(err, ConnectionError::QueryFailed(msg) if msg.contains("column")));
    }

    // ── query_scalar tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn query_scalar_returns_integer() {
        let conn = memory_conn();
        let value = conn.query_scalar("SELECT 42 AS v").await.unwrap();
        assert_eq!(value, serde_json::json!(42));
    }

    #[tokio::test]
    async fn query_scalar_returns_string() {
        let conn = memory_conn();
        let value = conn.query_scalar("SELECT 'hello' AS v").await.unwrap();
        assert_eq!(value, serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn query_scalar_returns_null() {
        let conn = memory_conn();
        let value = conn.query_scalar("SELECT NULL AS v").await.unwrap();
        assert!(value.is_null());
    }

    #[tokio::test]
    async fn query_scalar_invalid_sql() {
        let conn = memory_conn();
        let err = conn.query_scalar("INVALID SQL").await.unwrap_err();
        assert!(matches!(err, ConnectionError::QueryFailed(_)));
    }

    // ── execute_sql + query (combined because :memory: is per-invocation) ──

    #[tokio::test]
    async fn execute_and_query_combined() {
        let conn = memory_conn();
        let value = conn
            .query_scalar(
                "CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1); SELECT COUNT(*) AS c FROM t",
            )
            .await
            .unwrap();
        assert_eq!(value, serde_json::json!(1));
    }

    // ── load_jsonl tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn load_jsonl_creates_table() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let conn = DuckDbConnection::new(db_path.to_str().unwrap());
        let jsonl_path = dir.path().join("data.jsonl");
        std::fs::write(
            &jsonl_path,
            "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n",
        )
        .unwrap();
        conn.load_jsonl("", "test_table", &jsonl_path)
            .await
            .unwrap();
        let count = conn
            .query_scalar("SELECT COUNT(*) AS c FROM test_table")
            .await
            .unwrap();
        assert_eq!(count, serde_json::json!(2));
    }

    #[tokio::test]
    async fn load_jsonl_with_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let conn = DuckDbConnection::new(db_path.to_str().unwrap());
        // Create schema first.
        conn.execute_sql("CREATE SCHEMA my_schema").await.unwrap();
        let jsonl_path = dir.path().join("data.jsonl");
        std::fs::write(&jsonl_path, "{\"id\":1}\n").unwrap();
        conn.load_jsonl("my_schema", "my_table", &jsonl_path)
            .await
            .unwrap();
        let count = conn
            .query_scalar("SELECT COUNT(*) AS c FROM my_schema.my_table")
            .await
            .unwrap();
        assert_eq!(count, serde_json::json!(1));
    }
}

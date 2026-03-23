pub mod bigquery;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dbt::profile::AdapterConfig;

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("unsupported adapter type: {0}")]
    UnsupportedAdapter(String),
    #[error("missing required field '{field}' in profile output")]
    MissingField { field: String },
    #[error("invalid value for field '{field}' in profile output")]
    InvalidField { field: String },
    #[error("authentication failed: {0}")]
    AuthFailed(String),
    #[error("query failed: {0}")]
    QueryFailed(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
}

/// Row count and byte size of a table, used for Source change detection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableStats {
    pub num_rows: u64,
    pub num_bytes: u64,
}

/// Executes a SQL query and returns the first column of the first row as a JSON value.
#[async_trait]
pub trait Connection: Send + Sync {
    async fn query_scalar(&self, sql: &str) -> Result<serde_json::Value, ConnectionError>;

    /// Builds a dialect-specific SQL query to retrieve the last-updated timestamp of a table.
    /// With `column`: `SELECT MAX(column) FROM table`
    /// Without `column`: queries system metadata for the physical last-modified time.
    fn freshness_sql(&self, asset_name: &str, column: Option<&str>) -> String;

    /// Returns the sqlparser dialect for this connection's adapter type.
    fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect>;

    /// Returns row count and byte size of a table via metadata API (no query cost).
    async fn table_stats(&self, table_name: &str) -> Result<TableStats, ConnectionError>;

    /// Executes a DML/DDL statement (e.g. CREATE TABLE, MERGE).
    async fn execute_sql(&self, sql: &str) -> Result<(), ConnectionError>;

    /// Loads a JSONL file into a staging table via the DWH's bulk load mechanism.
    /// `dataset` is the target dataset/schema, `table` is the staging table name.
    async fn load_jsonl(
        &self,
        dataset: &str,
        table: &str,
        jsonl_path: &std::path::Path,
    ) -> Result<(), ConnectionError>;
}

/// Creates a `Connection` implementation based on the adapter type in the profile output.
pub fn create_connection(output: &AdapterConfig) -> Result<Box<dyn Connection>, ConnectionError> {
    match output.adapter_type.as_str() {
        "bigquery" => {
            let config = bigquery::BigQueryConfig::from_output(output)?;
            Ok(Box::new(bigquery::BigQueryConnection::new(config)))
        }
        other => Err(ConnectionError::UnsupportedAdapter(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_connection_rejects_unsupported_adapter() {
        let output = AdapterConfig {
            adapter_type: "snowflake".to_string(),
            fields: Default::default(),
        };
        let result = create_connection(&output);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, ConnectionError::UnsupportedAdapter(a) if a == "snowflake"));
    }

    #[test]
    fn create_connection_accepts_bigquery() {
        let output = AdapterConfig {
            adapter_type: "bigquery".to_string(),
            fields: [
                (
                    "project".to_string(),
                    serde_yaml::Value::String("p".to_string()),
                ),
                (
                    "dataset".to_string(),
                    serde_yaml::Value::String("d".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        };
        assert!(create_connection(&output).is_ok());
    }
}

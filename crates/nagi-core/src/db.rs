pub mod bigquery;

use async_trait::async_trait;
use thiserror::Error;

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

/// Executes a SQL query and returns the first column of the first row as a JSON value.
#[async_trait]
pub trait Connection: Send + Sync {
    async fn query_scalar(&self, sql: &str) -> Result<serde_json::Value, ConnectionError>;

    /// Builds a dialect-specific SQL query to retrieve the last-updated timestamp of a table.
    /// With `column`: `SELECT MAX(column) FROM table`
    /// Without `column`: queries system metadata for the physical last-modified time.
    fn freshness_sql(&self, asset_name: &str, column: Option<&str>) -> String;
}

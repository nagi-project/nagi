pub mod bigquery;
pub mod dbt;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use self::dbt::AdapterConfig;
use super::KindError;

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

    /// Returns the sqlparser dialect for this connection's adapter type.
    fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect>;

    /// Executes a DML/DDL statement (e.g. CREATE TABLE, MERGE).
    async fn execute_sql(&self, sql: &str) -> Result<(), ConnectionError>;

    /// Loads a JSONL file into a staging table via the data warehouse's bulk load mechanism.
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

pub const KIND: &str = "Connection";

/// Spec for `kind: Connection`. Holds external data connection info referenced by Assets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ConnectionSpec {
    /// Connection resolved via dbt profiles.yml.
    #[serde(rename = "dbt", rename_all = "camelCase")]
    Dbt {
        /// Profile name as defined in `~/.dbt/profiles.yml`.
        profile: String,
        /// If omitted, the default target in profiles.yml is used.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<String>,
        /// Optional dbt Cloud configuration for running-job checks before sync.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dbt_cloud: Option<DbtCloudSpec>,
    },
    /// Direct BigQuery connection without dbt profiles.yml.
    #[serde(rename = "bigquery", rename_all = "camelCase")]
    BigQuery {
        project: String,
        dataset: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_project: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        method: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keyfile: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_seconds: Option<u32>,
    },
}

/// dbt Cloud configuration for pre-sync running-job checks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbtCloudSpec {
    /// Path to the dbt Cloud credentials file. Defaults to `~/.dbt/dbt_cloud.yml`.
    pub credentials_file: Option<String>,
}

impl ConnectionSpec {
    pub fn validate(&self) -> Result<(), KindError> {
        match self {
            ConnectionSpec::Dbt { profile, .. } => {
                reject_empty("profile", profile)?;
                Ok(())
            }
            ConnectionSpec::BigQuery {
                project,
                dataset,
                execution_project,
                ..
            } => {
                reject_empty("project", project)?;
                reject_empty("dataset", dataset)?;
                reject_empty_optional("executionProject", execution_project.as_deref())?;
                Ok(())
            }
        }
    }
}

fn spec_error(message: String) -> KindError {
    KindError::InvalidSpec {
        kind: KIND.to_string(),
        message,
    }
}

fn reject_empty(field: &str, value: &str) -> Result<(), KindError> {
    if value.is_empty() {
        return Err(spec_error(format!("{field} must not be empty")));
    }
    Ok(())
}

fn reject_empty_optional(field: &str, value: Option<&str>) -> Result<(), KindError> {
    if let Some(v) = value {
        reject_empty(field, v)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_connection_spec() {
        let yaml = r#"
type: dbt
profile: my_project
target: dev
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Dbt {
                profile,
                target,
                dbt_cloud,
            } => {
                assert_eq!(profile, "my_project");
                assert_eq!(target, &Some("dev".to_string()));
                assert!(dbt_cloud.is_none());
            }
            other => panic!("expected Dbt, got {other:?}"),
        }
    }

    #[test]
    fn parse_connection_spec_without_target() {
        let yaml = r#"
type: dbt
profile: my_project
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Dbt { target, .. } => {
                assert_eq!(target, &None);
            }
            other => panic!("expected Dbt, got {other:?}"),
        }
    }

    #[test]
    fn parse_connection_spec_with_dbt_cloud() {
        let yaml = r#"
type: dbt
profile: my_project
target: dev
dbtCloud:
  credentialsFile: ~/.dbt/dbt_cloud.yml
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Dbt { dbt_cloud, .. } => {
                let cloud = dbt_cloud.as_ref().unwrap();
                assert_eq!(
                    cloud.credentials_file,
                    Some("~/.dbt/dbt_cloud.yml".to_string())
                );
            }
            other => panic!("expected Dbt, got {other:?}"),
        }
    }

    #[test]
    fn parse_connection_spec_with_dbt_cloud_default_path() {
        let yaml = r#"
type: dbt
profile: my_project
dbtCloud: {}
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Dbt { dbt_cloud, .. } => {
                let cloud = dbt_cloud.as_ref().unwrap();
                assert!(cloud.credentials_file.is_none());
            }
            other => panic!("expected Dbt, got {other:?}"),
        }
    }

    // ── BigQuery parsing tests ───────────────────────────────────────────

    #[test]
    fn parse_bigquery_all_fields() {
        let yaml = r#"
type: bigquery
project: my-gcp-project
dataset: raw
executionProject: my-billing-proj
method: service-account
keyfile: /path/to/key.json
timeoutSeconds: 30
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::BigQuery {
                project,
                dataset,
                execution_project,
                method,
                keyfile,
                timeout_seconds,
            } => {
                assert_eq!(project, "my-gcp-project");
                assert_eq!(dataset, "raw");
                assert_eq!(execution_project, &Some("my-billing-proj".to_string()));
                assert_eq!(method, &Some("service-account".to_string()));
                assert_eq!(keyfile, &Some("/path/to/key.json".to_string()));
                assert_eq!(timeout_seconds, &Some(30));
            }
            other => panic!("expected BigQuery, got {other:?}"),
        }
    }

    #[test]
    fn parse_bigquery_required_fields_only() {
        let yaml = r#"
type: bigquery
project: my-gcp-project
dataset: raw
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::BigQuery {
                project,
                dataset,
                execution_project,
                method,
                keyfile,
                timeout_seconds,
            } => {
                assert_eq!(project, "my-gcp-project");
                assert_eq!(dataset, "raw");
                assert!(execution_project.is_none());
                assert!(method.is_none());
                assert!(keyfile.is_none());
                assert!(timeout_seconds.is_none());
            }
            other => panic!("expected BigQuery, got {other:?}"),
        }
    }

    // ── Validation tests ─────────────────────────────────────────────────

    macro_rules! validate_accept_test {
        ($($name:ident: $spec:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!($spec.validate().is_ok());
                }
            )*
        };
    }

    validate_accept_test! {
        validate_dbt_valid:
            ConnectionSpec::Dbt {
                profile: "my_project".to_string(),
                target: Some("dev".to_string()),
                dbt_cloud: None,
            };
        validate_bigquery_oauth:
            ConnectionSpec::BigQuery {
                project: "my-gcp-project".to_string(),
                dataset: "raw".to_string(),
                execution_project: None,
                method: Some("oauth".to_string()),
                keyfile: None,
                timeout_seconds: None,
            };
        validate_bigquery_service_account:
            ConnectionSpec::BigQuery {
                project: "p".to_string(),
                dataset: "d".to_string(),
                execution_project: None,
                method: Some("service-account".to_string()),
                keyfile: Some("/path/to/key.json".to_string()),
                timeout_seconds: None,
            };
    }

    macro_rules! validate_reject_test {
        ($($name:ident: $spec:expr => $msg:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let err = $spec.validate().unwrap_err();
                    match err {
                        KindError::InvalidSpec { message, .. } => {
                            assert!(message.contains($msg), "expected '{}' in '{message}'", $msg);
                        }
                        other => panic!("expected InvalidSpec, got {other:?}"),
                    }
                }
            )*
        };
    }

    validate_reject_test! {
        validate_dbt_rejects_empty_profile:
            ConnectionSpec::Dbt {
                profile: "".to_string(),
                target: None,
                dbt_cloud: None,
            } => "profile must not be empty";
        validate_bigquery_rejects_empty_project:
            ConnectionSpec::BigQuery {
                project: "".to_string(),
                dataset: "d".to_string(),
                execution_project: None,
                method: None,
                keyfile: None,
                timeout_seconds: None,
            } => "project must not be empty";
        validate_bigquery_rejects_empty_dataset:
            ConnectionSpec::BigQuery {
                project: "p".to_string(),
                dataset: "".to_string(),
                execution_project: None,
                method: None,
                keyfile: None,
                timeout_seconds: None,
            } => "dataset must not be empty";
        validate_bigquery_rejects_empty_execution_project:
            ConnectionSpec::BigQuery {
                project: "p".to_string(),
                dataset: "d".to_string(),
                execution_project: Some("".to_string()),
                method: None,
                keyfile: None,
                timeout_seconds: None,
            } => "executionProject must not be empty";
    }

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

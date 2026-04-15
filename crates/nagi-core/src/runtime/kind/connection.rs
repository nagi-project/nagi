#[cfg(feature = "bigquery")]
pub mod bigquery;
pub mod dbt;
pub mod duckdb;
#[cfg(feature = "snowflake")]
pub mod snowflake;
mod sql;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::collections::HashMap;

use self::dbt::AdapterConfig;
use super::KindError;
use crate::runtime::compile::CompileError;

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
    Http(String),
}

/// Runs a synchronous closure on tokio's blocking thread pool.
///
/// ureq is a synchronous HTTP client. Running it on an async task would block
/// the tokio runtime. This function moves the work to a dedicated thread pool.
async fn run_blocking<F, T>(f: F) -> Result<T, ConnectionError>
where
    F: FnOnce() -> Result<T, ConnectionError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .expect("spawn_blocking panicked")
}

/// Executes a SQL query and returns the first column of the first row as a JSON value.
#[async_trait]
pub trait Connection: Send + Sync {
    async fn query_scalar(&self, sql: &str) -> Result<serde_json::Value, ConnectionError>;

    /// Builds a dialect-specific SQL query to retrieve the last-updated timestamp of a table.
    /// With `column`: `SELECT MAX(column) FROM table`
    /// Without `column`: queries system metadata for the physical last-modified time.
    /// Returns an error if the adapter does not support metadata-based freshness.
    fn freshness_sql(
        &self,
        asset_name: &str,
        column: Option<&str>,
    ) -> Result<String, ConnectionError>;

    /// Returns the sqlparser dialect for this connection's adapter type.
    fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect>;

    /// Executes a DML/DDL statement (e.g. CREATE TABLE, MERGE).
    async fn execute_sql(&self, sql: &str) -> Result<(), ConnectionError>;

    /// Maximum number of concurrent operations this connection supports.
    /// Returns `Some(n)` for connections with inherent concurrency limits (e.g. DuckDB file lock).
    /// Returns `None` for connections with no inherent limit.
    fn max_concurrency(&self) -> Option<usize> {
        None
    }

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
        #[cfg(feature = "bigquery")]
        "bigquery" => {
            let config = bigquery::BigQueryConfig::from_output(output)?;
            Ok(Box::new(bigquery::BigQueryConnection::new(config)))
        }
        "duckdb" => {
            let path = require_str(&output.fields, "path")?;
            Ok(Box::new(duckdb::DuckDbConnection::new(&path)))
        }
        #[cfg(feature = "snowflake")]
        "snowflake" => {
            let config = snowflake::SnowflakeConfig::from_output(output)?;
            Ok(Box::new(snowflake::SnowflakeConnection::new(config)))
        }
        other => Err(ConnectionError::UnsupportedAdapter(other.to_string())),
    }
}

/// Connection info resolved from Asset → Connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ResolvedConnection {
    /// Connection resolved via dbt profiles.yml.
    #[serde(rename_all = "camelCase")]
    Dbt {
        /// Original Connection resource name.
        name: String,
        profile: String,
        target: Option<String>,
        /// Directory containing profiles.yml. If None, uses `~/.dbt/`.
        profiles_dir: Option<String>,
        /// Path to the dbt Cloud credentials file, if dbt Cloud is configured.
        dbt_cloud_credentials_file: Option<String>,
        /// Unexpanded Identity env template. Excluded from serialization.
        #[serde(skip)]
        identity_env: Option<HashMap<String, String>>,
    },
    /// Direct BigQuery connection without dbt profiles.yml.
    #[serde(rename = "bigquery", rename_all = "camelCase")]
    BigQuery {
        name: String,
        project: String,
        dataset: String,
        execution_project: Option<String>,
        method: Option<String>,
        keyfile: Option<String>,
        timeout_seconds: Option<u32>,
        /// Unexpanded Identity env template. Excluded from serialization.
        #[serde(skip)]
        identity_env: Option<HashMap<String, String>>,
    },
    /// Direct DuckDB connection.
    #[serde(rename = "duckdb", rename_all = "camelCase")]
    DuckDb {
        name: String,
        path: String,
        /// Unexpanded Identity env template. Excluded from serialization.
        #[serde(skip)]
        identity_env: Option<HashMap<String, String>>,
    },
    /// Direct Snowflake connection via SQL REST API with Key-Pair JWT authentication.
    #[serde(rename = "snowflake", rename_all = "camelCase")]
    Snowflake {
        name: String,
        account: String,
        user: String,
        database: String,
        schema: String,
        warehouse: String,
        role: Option<String>,
        private_key_path: String,
        /// Unexpanded Identity env template. Excluded from serialization.
        #[serde(skip)]
        identity_env: Option<HashMap<String, String>>,
    },
}

impl ResolvedConnection {
    pub fn name(&self) -> &str {
        match self {
            ResolvedConnection::Dbt { name, .. }
            | ResolvedConnection::BigQuery { name, .. }
            | ResolvedConnection::DuckDb { name, .. }
            | ResolvedConnection::Snowflake { name, .. } => name,
        }
    }

    /// Creates a `Box<dyn Connection>` from the resolved connection info.
    pub fn connect(&self) -> Result<Box<dyn Connection>, ConnectionError> {
        match self {
            ResolvedConnection::Dbt {
                profile,
                target,
                profiles_dir,
                ..
            } => connect_dbt(profile, target.as_deref(), profiles_dir.as_deref()),
            #[cfg(feature = "bigquery")]
            ResolvedConnection::BigQuery {
                project,
                dataset,
                execution_project,
                method,
                keyfile,
                timeout_seconds,
                identity_env,
                ..
            } => {
                let conn = bigquery::BigQueryConnection::from_resolved(
                    project,
                    dataset,
                    execution_project,
                    method.as_deref(),
                    keyfile,
                    *timeout_seconds,
                    identity_env.as_ref(),
                )?;
                Ok(Box::new(conn))
            }
            #[cfg(not(feature = "bigquery"))]
            ResolvedConnection::BigQuery { .. } => Err(ConnectionError::UnsupportedAdapter(
                "bigquery (feature disabled)".to_string(),
            )),
            ResolvedConnection::DuckDb { path, .. } => {
                Ok(Box::new(duckdb::DuckDbConnection::new(path)))
            }
            #[cfg(feature = "snowflake")]
            ResolvedConnection::Snowflake {
                account,
                user,
                database,
                schema,
                warehouse,
                role,
                private_key_path,
                ..
            } => connect_snowflake(
                account,
                user,
                database,
                schema,
                warehouse,
                role,
                private_key_path,
            ),
            #[cfg(not(feature = "snowflake"))]
            ResolvedConnection::Snowflake { .. } => Err(ConnectionError::UnsupportedAdapter(
                "snowflake (feature disabled)".to_string(),
            )),
        }
    }
}

fn connect_dbt(
    profile: &str,
    target: Option<&str>,
    profiles_dir: Option<&str>,
) -> Result<Box<dyn Connection>, ConnectionError> {
    let f = match profiles_dir {
        Some(dir) => {
            let path = std::path::Path::new(dir).join("profiles.yml");
            dbt::DbtProfilesFile::load(&path)
        }
        None => dbt::DbtProfilesFile::load_default(),
    }
    .map_err(|e| ConnectionError::AuthFailed(e.to_string()))?;
    let output = f
        .resolve(profile, target)
        .map_err(|e| ConnectionError::AuthFailed(e.to_string()))?;
    create_connection(output)
}

#[cfg(feature = "snowflake")]
fn connect_snowflake(
    account: &str,
    user: &str,
    database: &str,
    schema: &str,
    warehouse: &str,
    role: &Option<String>,
    private_key_path: &str,
) -> Result<Box<dyn Connection>, ConnectionError> {
    let config = snowflake::SnowflakeConfig {
        account: account.to_string(),
        user: user.to_string(),
        database: database.to_string(),
        schema: schema.to_string(),
        warehouse: warehouse.to_string(),
        role: role.clone(),
        private_key_path: private_key_path.to_string(),
    };
    Ok(Box::new(snowflake::SnowflakeConnection::new(config)))
}

/// Resolves a named `ConnectionSpec` into a `ResolvedConnection`.
/// If the connection references an Identity, the unexpanded env template is attached.
pub fn resolve_connection_by_name(
    conn_name: &str,
    connections: &HashMap<String, ConnectionSpec>,
    identities: &HashMap<String, super::identity::IdentitySpec>,
) -> Result<ResolvedConnection, CompileError> {
    let conn_spec = connections
        .get(conn_name)
        .ok_or_else(|| CompileError::UnresolvedRef {
            kind: "Connection".to_string(),
            name: conn_name.to_string(),
        })?;
    let identity_env = resolve_identity_env(connection_identity_ref(conn_spec), identities);
    match conn_spec {
        ConnectionSpec::Dbt {
            ref profile,
            ref target,
            ref profiles_dir,
            ref dbt_cloud,
            ..
        } => Ok(ResolvedConnection::Dbt {
            name: conn_name.to_string(),
            profile: profile.clone(),
            target: target.clone(),
            profiles_dir: profiles_dir.clone(),
            dbt_cloud_credentials_file: dbt_cloud.as_ref().map(|c| {
                c.credentials_file
                    .clone()
                    .unwrap_or_else(|| "~/.dbt/dbt_cloud.yml".to_string())
            }),
            identity_env: identity_env.clone(),
        }),
        ConnectionSpec::BigQuery {
            ref project,
            ref dataset,
            ref execution_project,
            ref method,
            ref keyfile,
            ref timeout_seconds,
            ..
        } => Ok(ResolvedConnection::BigQuery {
            name: conn_name.to_string(),
            project: project.clone(),
            dataset: dataset.clone(),
            execution_project: execution_project.clone(),
            method: method.clone(),
            keyfile: keyfile.clone(),
            timeout_seconds: *timeout_seconds,
            identity_env: identity_env.clone(),
        }),
        ConnectionSpec::DuckDb { ref path, .. } => Ok(ResolvedConnection::DuckDb {
            name: conn_name.to_string(),
            path: path.clone(),
            identity_env: identity_env.clone(),
        }),
        ConnectionSpec::Snowflake {
            ref account,
            ref user,
            ref database,
            ref schema,
            ref warehouse,
            ref role,
            ref private_key_path,
            ..
        } => Ok(ResolvedConnection::Snowflake {
            name: conn_name.to_string(),
            account: account.clone(),
            user: user.clone(),
            database: database.clone(),
            schema: schema.clone(),
            warehouse: warehouse.clone(),
            role: role.clone(),
            private_key_path: private_key_path.clone(),
            identity_env: identity_env.clone(),
        }),
    }
}

pub(crate) fn connection_identity_ref(spec: &ConnectionSpec) -> Option<&str> {
    match spec {
        ConnectionSpec::Dbt { identity, .. }
        | ConnectionSpec::BigQuery { identity, .. }
        | ConnectionSpec::DuckDb { identity, .. }
        | ConnectionSpec::Snowflake { identity, .. } => identity.as_deref(),
    }
}

fn resolve_identity_env(
    identity_ref: Option<&str>,
    identities: &HashMap<String, super::identity::IdentitySpec>,
) -> Option<HashMap<String, String>> {
    let name = identity_ref?;
    let spec = identities.get(name)?;
    match spec {
        super::identity::IdentitySpec::Env { env } => Some(env.clone()),
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
        /// Directory containing profiles.yml. If omitted, uses `~/.dbt/`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        profiles_dir: Option<String>,
        /// Optional dbt Cloud configuration for running-job checks before sync.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dbt_cloud: Option<DbtCloudSpec>,
        /// Reference to a `kind: Identity` resource for authentication scope.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<String>,
    },
    /// BigQuery REST API connection.
    #[serde(rename = "bigquery", rename_all = "camelCase")]
    BigQuery {
        /// GCP project ID that contains the dataset.
        project: String,
        /// BigQuery dataset name.
        dataset: String,
        /// GCP project ID used for query execution billing. Defaults to `project` if omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_project: Option<String>,
        /// Authentication method. `oauth` (Application Default Credentials) or `service-account`. Defaults to `oauth`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        method: Option<String>,
        /// Path to the service account JSON key file. Required when `method` is `service-account`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keyfile: Option<String>,
        /// Query timeout in seconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_seconds: Option<u32>,
        /// Reference to a `kind: Identity` resource for authentication scope.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<String>,
    },
    /// DuckDB connection via the `duckdb` CLI.
    #[serde(rename = "duckdb")]
    DuckDb {
        /// Path to the DuckDB database file.
        path: String,
        /// Reference to a `kind: Identity` resource for authentication scope.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<String>,
    },
    /// Snowflake SQL REST API connection with Key-Pair JWT authentication.
    #[serde(rename = "snowflake", rename_all = "camelCase")]
    Snowflake {
        /// Snowflake account identifier (e.g. `myorg-myaccount`).
        account: String,
        /// Snowflake login user name.
        user: String,
        /// Database name.
        database: String,
        /// Schema name.
        schema: String,
        /// Warehouse name.
        warehouse: String,
        /// Role to use for the session. Uses the user's default role if omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        /// Path to the RSA private key file (PKCS#8 PEM format) for JWT authentication.
        private_key_path: String,
        /// Reference to a `kind: Identity` resource for authentication scope.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<String>,
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
            ConnectionSpec::DuckDb { path, .. } => {
                reject_empty("path", path)?;
                Ok(())
            }
            ConnectionSpec::Snowflake {
                account,
                user,
                database,
                schema,
                warehouse,
                role,
                private_key_path,
                ..
            } => {
                reject_empty("account", account)?;
                reject_empty("user", user)?;
                reject_empty("database", database)?;
                reject_empty("schema", schema)?;
                reject_empty("warehouse", warehouse)?;
                reject_empty_optional("role", role.as_deref())?;
                reject_empty("privateKeyPath", private_key_path)?;
                Ok(())
            }
        }
    }
}

/// Extracts a required string field from an AdapterConfig fields map.
pub(super) fn require_str(
    fields: &HashMap<String, serde_yaml::Value>,
    key: &str,
) -> Result<String, ConnectionError> {
    fields
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| ConnectionError::MissingField {
            field: key.to_string(),
        })
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
                profiles_dir,
                dbt_cloud,
                ..
            } => {
                assert_eq!(profile, "my_project");
                assert_eq!(target, &Some("dev".to_string()));
                assert!(profiles_dir.is_none());
                assert!(dbt_cloud.is_none());
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
    fn parse_connection_spec_with_profiles_dir() {
        let yaml = r#"
type: dbt
profile: my_project
profilesDir: /custom/profiles
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Dbt { profiles_dir, .. } => {
                assert_eq!(profiles_dir, &Some("/custom/profiles".to_string()));
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
                ..
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
                ..
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
                profiles_dir: None,
                dbt_cloud: None,
                identity: None,
            };
        validate_bigquery_oauth:
            ConnectionSpec::BigQuery {
                project: "my-gcp-project".to_string(),
                dataset: "raw".to_string(),
                execution_project: None,
                method: Some("oauth".to_string()),
                keyfile: None,
                timeout_seconds: None,
                identity: None,
            };
        validate_bigquery_service_account:
            ConnectionSpec::BigQuery {
                project: "p".to_string(),
                dataset: "d".to_string(),
                execution_project: None,
                method: Some("service-account".to_string()),
                keyfile: Some("/path/to/key.json".to_string()),
                timeout_seconds: None,
                identity: None,
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
                profiles_dir: None,
                dbt_cloud: None,
                identity: None,
            } => "profile must not be empty";
        validate_bigquery_rejects_empty_project:
            ConnectionSpec::BigQuery {
                project: "".to_string(),
                dataset: "d".to_string(),
                execution_project: None,
                method: None,
                keyfile: None,
                timeout_seconds: None,
                identity: None,
            } => "project must not be empty";
        validate_bigquery_rejects_empty_dataset:
            ConnectionSpec::BigQuery {
                project: "p".to_string(),
                dataset: "".to_string(),
                execution_project: None,
                method: None,
                keyfile: None,
                timeout_seconds: None,
                identity: None,
            } => "dataset must not be empty";
        validate_bigquery_rejects_empty_execution_project:
            ConnectionSpec::BigQuery {
                project: "p".to_string(),
                dataset: "d".to_string(),
                execution_project: Some("".to_string()),
                method: None,
                keyfile: None,
                timeout_seconds: None,
                identity: None,
            } => "executionProject must not be empty";
    }

    #[test]
    fn create_connection_rejects_unsupported_adapter() {
        let output = AdapterConfig {
            adapter_type: "mysql".to_string(),
            fields: Default::default(),
        };
        let result = create_connection(&output);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, ConnectionError::UnsupportedAdapter(a) if a == "mysql"));
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

    // ── resolve_connection tests ───────────────────────────────────────

    #[test]
    fn resolve_connection_by_name_dbt() {
        let mut connections = HashMap::new();
        connections.insert(
            "my-bq".to_string(),
            ConnectionSpec::Dbt {
                profile: "proj".to_string(),
                target: Some("dev".to_string()),
                profiles_dir: None,
                dbt_cloud: None,
                identity: None,
            },
        );
        let conn = resolve_connection_by_name("my-bq", &connections, &HashMap::new()).unwrap();
        assert!(matches!(conn, ResolvedConnection::Dbt { name, profile, .. }
            if name == "my-bq" && profile == "proj"));
    }

    #[test]
    fn resolve_connection_by_name_bigquery() {
        let mut connections = HashMap::new();
        connections.insert(
            "my-bq-direct".to_string(),
            ConnectionSpec::BigQuery {
                project: "my-gcp-project".to_string(),
                dataset: "raw".to_string(),
                execution_project: Some("billing-proj".to_string()),
                method: Some("oauth".to_string()),
                keyfile: None,
                timeout_seconds: Some(30),
                identity: None,
            },
        );
        let conn =
            resolve_connection_by_name("my-bq-direct", &connections, &HashMap::new()).unwrap();
        match conn {
            ResolvedConnection::BigQuery {
                name,
                project,
                dataset,
                execution_project,
                method,
                keyfile,
                timeout_seconds,
                ..
            } => {
                assert_eq!(name, "my-bq-direct");
                assert_eq!(project, "my-gcp-project");
                assert_eq!(dataset, "raw");
                assert_eq!(execution_project, Some("billing-proj".to_string()));
                assert_eq!(method, Some("oauth".to_string()));
                assert!(keyfile.is_none());
                assert_eq!(timeout_seconds, Some(30));
            }
            other => panic!("expected BigQuery, got {other:?}"),
        }
    }

    #[test]
    fn resolve_connection_by_name_missing() {
        let connections = HashMap::new();
        let err = resolve_connection_by_name("missing", &connections, &HashMap::new()).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Connection" && name == "missing"));
    }

    // ── DuckDb parsing tests ────────────────────────────────────────────

    #[test]
    fn parse_duckdb_spec() {
        let yaml = r#"
type: duckdb
path: ./data/warehouse.duckdb
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::DuckDb { path, .. } => {
                assert_eq!(path, "./data/warehouse.duckdb");
            }
            other => panic!("expected DuckDb, got {other:?}"),
        }
    }

    validate_accept_test! {
        validate_duckdb_valid:
            ConnectionSpec::DuckDb {
                path: "./data/warehouse.duckdb".to_string(),
                identity: None,
            };
    }

    validate_reject_test! {
        validate_duckdb_rejects_empty_path:
            ConnectionSpec::DuckDb {
                path: "".to_string(),
                identity: None,
            } => "path must not be empty";
    }

    #[test]
    fn resolve_connection_by_name_duckdb() {
        let mut connections = HashMap::new();
        connections.insert(
            "my-duck".to_string(),
            ConnectionSpec::DuckDb {
                path: "./data/warehouse.duckdb".to_string(),
                identity: None,
            },
        );
        let conn = resolve_connection_by_name("my-duck", &connections, &HashMap::new()).unwrap();
        match conn {
            ResolvedConnection::DuckDb { name, path, .. } => {
                assert_eq!(name, "my-duck");
                assert_eq!(path, "./data/warehouse.duckdb");
            }
            other => panic!("expected DuckDb, got {other:?}"),
        }
    }

    // ── Snowflake parsing tests ──────────────────────────────────────────

    #[test]
    fn parse_snowflake_all_fields() {
        let yaml = r#"
type: snowflake
account: myorg-myacct
user: MY_USER
database: MY_DB
schema: MY_SCHEMA
warehouse: MY_WH
role: MY_ROLE
privateKeyPath: /path/to/rsa_key.p8
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Snowflake {
                account,
                user,
                database,
                schema,
                warehouse,
                role,
                private_key_path,
                ..
            } => {
                assert_eq!(account, "myorg-myacct");
                assert_eq!(user, "MY_USER");
                assert_eq!(database, "MY_DB");
                assert_eq!(schema, "MY_SCHEMA");
                assert_eq!(warehouse, "MY_WH");
                assert_eq!(role, &Some("MY_ROLE".to_string()));
                assert_eq!(private_key_path, "/path/to/rsa_key.p8");
            }
            other => panic!("expected Snowflake, got {other:?}"),
        }
    }

    #[test]
    fn parse_snowflake_required_fields_only() {
        let yaml = r#"
type: snowflake
account: myorg-myacct
user: MY_USER
database: MY_DB
schema: MY_SCHEMA
warehouse: MY_WH
privateKeyPath: /path/to/rsa_key.p8
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Snowflake { role, .. } => {
                assert!(role.is_none());
            }
            other => panic!("expected Snowflake, got {other:?}"),
        }
    }

    validate_accept_test! {
        validate_snowflake_valid:
            ConnectionSpec::Snowflake {
                account: "myorg-myacct".to_string(),
                user: "MY_USER".to_string(),
                database: "MY_DB".to_string(),
                schema: "MY_SCHEMA".to_string(),
                warehouse: "MY_WH".to_string(),
                role: None,
                private_key_path: "/path/to/rsa_key.p8".to_string(),
                identity: None,
            };
    }

    validate_reject_test! {
        validate_snowflake_rejects_empty_account:
            ConnectionSpec::Snowflake {
                account: "".to_string(),
                user: "u".to_string(),
                database: "d".to_string(),
                schema: "s".to_string(),
                warehouse: "w".to_string(),
                role: None,
                private_key_path: "p".to_string(),
                identity: None,
            } => "account must not be empty";
        validate_snowflake_rejects_empty_user:
            ConnectionSpec::Snowflake {
                account: "a".to_string(),
                user: "".to_string(),
                database: "d".to_string(),
                schema: "s".to_string(),
                warehouse: "w".to_string(),
                role: None,
                private_key_path: "p".to_string(),
                identity: None,
            } => "user must not be empty";
        validate_snowflake_rejects_empty_database:
            ConnectionSpec::Snowflake {
                account: "a".to_string(),
                user: "u".to_string(),
                database: "".to_string(),
                schema: "s".to_string(),
                warehouse: "w".to_string(),
                role: None,
                private_key_path: "p".to_string(),
                identity: None,
            } => "database must not be empty";
        validate_snowflake_rejects_empty_schema:
            ConnectionSpec::Snowflake {
                account: "a".to_string(),
                user: "u".to_string(),
                database: "d".to_string(),
                schema: "".to_string(),
                warehouse: "w".to_string(),
                role: None,
                private_key_path: "p".to_string(),
                identity: None,
            } => "schema must not be empty";
        validate_snowflake_rejects_empty_warehouse:
            ConnectionSpec::Snowflake {
                account: "a".to_string(),
                user: "u".to_string(),
                database: "d".to_string(),
                schema: "s".to_string(),
                warehouse: "".to_string(),
                role: None,
                private_key_path: "p".to_string(),
                identity: None,
            } => "warehouse must not be empty";
        validate_snowflake_rejects_empty_private_key_path:
            ConnectionSpec::Snowflake {
                account: "a".to_string(),
                user: "u".to_string(),
                database: "d".to_string(),
                schema: "s".to_string(),
                warehouse: "w".to_string(),
                role: None,
                private_key_path: "".to_string(),
                identity: None,
            } => "privateKeyPath must not be empty";
        validate_snowflake_rejects_empty_role:
            ConnectionSpec::Snowflake {
                account: "a".to_string(),
                user: "u".to_string(),
                database: "d".to_string(),
                schema: "s".to_string(),
                warehouse: "w".to_string(),
                role: Some("".to_string()),
                private_key_path: "p".to_string(),
                identity: None,
            } => "role must not be empty";
    }

    #[test]
    fn resolve_connection_by_name_snowflake() {
        let mut connections = HashMap::new();
        connections.insert(
            "my-sf".to_string(),
            ConnectionSpec::Snowflake {
                account: "myorg-myacct".to_string(),
                user: "MY_USER".to_string(),
                database: "MY_DB".to_string(),
                schema: "MY_SCHEMA".to_string(),
                warehouse: "MY_WH".to_string(),
                role: Some("MY_ROLE".to_string()),
                private_key_path: "/path/to/key.p8".to_string(),
                identity: None,
            },
        );
        let conn = resolve_connection_by_name("my-sf", &connections, &HashMap::new()).unwrap();
        match conn {
            ResolvedConnection::Snowflake {
                name,
                account,
                user,
                database,
                schema,
                warehouse,
                role,
                private_key_path,
                ..
            } => {
                assert_eq!(name, "my-sf");
                assert_eq!(account, "myorg-myacct");
                assert_eq!(user, "MY_USER");
                assert_eq!(database, "MY_DB");
                assert_eq!(schema, "MY_SCHEMA");
                assert_eq!(warehouse, "MY_WH");
                assert_eq!(role, Some("MY_ROLE".to_string()));
                assert_eq!(private_key_path, "/path/to/key.p8");
            }
            other => panic!("expected Snowflake, got {other:?}"),
        }
    }

    #[test]
    fn create_connection_accepts_duckdb() {
        let output = AdapterConfig {
            adapter_type: "duckdb".to_string(),
            fields: [(
                "path".to_string(),
                serde_yaml::Value::String(":memory:".to_string()),
            )]
            .into_iter()
            .collect(),
        };
        assert!(create_connection(&output).is_ok());
    }
}

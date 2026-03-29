use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::runtime::kind::connection::dbt::AdapterConfig;

use super::{require_str, Connection, ConnectionError};

// ── BigQuery config ──────────────────────────────────────────────────────────

/// Authentication method for BigQuery.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthMethod {
    /// Application Default Credentials (`gcloud auth application-default login`).
    OAuth,
    /// Service account JSON key file.
    ServiceAccount { keyfile: String },
}

/// Resolved BigQuery connection configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryConfig {
    pub auth: AuthMethod,
    pub project: String,
    /// API endpoint project for billing. Falls back to `project` when `None`.
    pub execution_project: Option<String>,
    pub dataset: String,
    /// Query timeout in milliseconds. `None` defers to the BigQuery server default.
    pub timeout_ms: Option<u32>,
}

impl BigQueryConfig {
    pub fn from_output(output: &AdapterConfig) -> Result<Self, ConnectionError> {
        if output.adapter_type != "bigquery" {
            return Err(ConnectionError::UnsupportedAdapter(
                output.adapter_type.clone(),
            ));
        }
        let project = require_str(&output.fields, "project")?;
        let dataset = require_str(&output.fields, "dataset")?;
        let method = output
            .fields
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("oauth");
        let auth = match method {
            "oauth" | "oauth-secrets" => AuthMethod::OAuth,
            "service-account" => {
                let keyfile = require_str(&output.fields, "keyfile")?;
                AuthMethod::ServiceAccount { keyfile }
            }
            other => {
                return Err(ConnectionError::UnsupportedAdapter(format!(
                    "bigquery auth method '{other}'"
                )))
            }
        };
        let timeout_ms = output
            .fields
            .get("job_execution_timeout_seconds")
            .and_then(|v| v.as_u64())
            .map(|secs| {
                u32::try_from(secs.saturating_mul(1000)).map_err(|_| {
                    ConnectionError::InvalidField {
                        field: "job_execution_timeout_seconds".to_string(),
                    }
                })
            })
            .transpose()?;
        let execution_project = output
            .fields
            .get("execution_project")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(Self {
            auth,
            project,
            execution_project,
            dataset,
            timeout_ms,
        })
    }
}

// ── Token acquisition ────────────────────────────────────────────────────────

const BIGQUERY_SCOPE: &str = "https://www.googleapis.com/auth/bigquery";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
/// Maximum allowed JWT lifetime for Google service accounts (enforced by Google).
const JWT_LIFETIME_SECS: u64 = 3600;

#[derive(Deserialize)]
struct Credentials {
    client_email: String,
    private_key: String,
    token_uri: String,
}

/// Raw structure of `application_default_credentials.json`.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AdcFile {
    AuthorizedUser {
        client_id: String,
        client_secret: String,
        refresh_token: String,
    },
    ServiceAccount(Credentials),
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Serialize)]
struct JwtClaims {
    iss: String,
    scope: String,
    aud: String,
    exp: u64,
    iat: u64,
}

async fn token_from_adc(client: &reqwest::Client) -> Result<String, ConnectionError> {
    let adc_path = dirs::home_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join(".config/gcloud/application_default_credentials.json");
    token_from_adc_path(client, &adc_path).await
}

async fn token_from_adc_path(
    client: &reqwest::Client,
    path: &Path,
) -> Result<String, ConnectionError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot read ADC file: {e}")))?;
    let adc: AdcFile = serde_json::from_str(&content)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot parse ADC file: {e}")))?;
    match adc {
        AdcFile::AuthorizedUser {
            client_id,
            client_secret,
            refresh_token,
        } => refresh_token_exchange(client, &client_id, &client_secret, &refresh_token).await,
        AdcFile::ServiceAccount(creds) => {
            service_account_token(
                client,
                &creds.client_email,
                &creds.private_key,
                &creds.token_uri,
            )
            .await
        }
    }
}

async fn token_from_keyfile(
    client: &reqwest::Client,
    keyfile: &str,
) -> Result<String, ConnectionError> {
    let content = std::fs::read_to_string(keyfile)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot read keyfile: {e}")))?;
    let creds: Credentials = serde_json::from_str(&content)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot parse keyfile: {e}")))?;
    service_account_token(
        client,
        &creds.client_email,
        &creds.private_key,
        &creds.token_uri,
    )
    .await
}

async fn refresh_token_exchange(
    client: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, ConnectionError> {
    let resp: TokenResponse = client
        .post(TOKEN_URL)
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .await?
        .json()
        .await?;
    Ok(resp.access_token)
}

async fn service_account_token(
    client: &reqwest::Client,
    client_email: &str,
    private_key: &str,
    token_uri: &str,
) -> Result<String, ConnectionError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = JwtClaims {
        iss: client_email.to_string(),
        scope: BIGQUERY_SCOPE.to_string(),
        aud: token_uri.to_string(),
        iat: now,
        exp: now + JWT_LIFETIME_SECS,
    };
    let key = EncodingKey::from_rsa_pem(private_key.as_bytes())
        .map_err(|e| ConnectionError::AuthFailed(format!("invalid private key: {e}")))?;
    let jwt = encode(&Header::new(Algorithm::RS256), &claims, &key)
        .map_err(|e| ConnectionError::AuthFailed(format!("JWT encoding failed: {e}")))?;

    let resp: TokenResponse = client
        .post(token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ])
        .send()
        .await?
        .json()
        .await?;
    Ok(resp.access_token)
}

// ── BigQuery REST client ─────────────────────────────────────────────────────

pub struct BigQueryConnection {
    config: BigQueryConfig,
    client: reqwest::Client,
}

impl BigQueryConnection {
    /// Creates a `BigQueryConnection` from a `ResolvedConnection::BigQuery`.
    pub fn from_resolved(
        project: &str,
        dataset: &str,
        execution_project: &Option<String>,
        method: Option<&str>,
        keyfile: &Option<String>,
        timeout_seconds: Option<u32>,
    ) -> Result<Self, super::ConnectionError> {
        let auth = match method.unwrap_or("oauth") {
            "oauth" => AuthMethod::OAuth,
            "service-account" => AuthMethod::ServiceAccount {
                keyfile: keyfile
                    .clone()
                    .ok_or_else(|| super::ConnectionError::MissingField {
                        field: "keyfile".to_string(),
                    })?,
            },
            other => {
                return Err(super::ConnectionError::UnsupportedAdapter(format!(
                    "bigquery auth method '{other}'"
                )))
            }
        };
        let config = BigQueryConfig {
            auth,
            project: project.to_string(),
            execution_project: execution_project.clone(),
            dataset: dataset.to_string(),
            timeout_ms: timeout_seconds.map(|s| s.saturating_mul(1000)),
        };
        Ok(Self::new(config))
    }

    pub fn new(config: BigQueryConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    async fn access_token(&self) -> Result<String, ConnectionError> {
        match &self.config.auth {
            AuthMethod::OAuth => token_from_adc(&self.client).await,
            AuthMethod::ServiceAccount { keyfile } => {
                token_from_keyfile(&self.client, keyfile).await
            }
        }
    }
}

#[async_trait]
impl Connection for BigQueryConnection {
    async fn query_scalar(&self, sql: &str) -> Result<Value, ConnectionError> {
        let token = self.access_token().await?;
        let api_project = self
            .config
            .execution_project
            .as_deref()
            .unwrap_or(&self.config.project);
        let url =
            format!("https://bigquery.googleapis.com/bigquery/v2/projects/{api_project}/queries",);
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct QueryRequest<'a> {
            query: &'a str,
            use_legacy_sql: bool,
            default_dataset: DatasetRef<'a>,
            #[serde(skip_serializing_if = "Option::is_none")]
            timeout_ms: Option<u32>,
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct DatasetRef<'a> {
            project_id: &'a str,
            dataset_id: &'a str,
        }
        #[derive(Deserialize)]
        struct QueryResponse {
            rows: Option<Vec<Row>>,
        }
        #[derive(Deserialize)]
        struct Row {
            f: Vec<Cell>,
        }
        #[derive(Deserialize)]
        struct Cell {
            v: Value,
        }

        let body = QueryRequest {
            query: sql,
            use_legacy_sql: false,
            default_dataset: DatasetRef {
                project_id: &self.config.project,
                dataset_id: &self.config.dataset,
            },
            timeout_ms: self.config.timeout_ms,
        };
        let resp: QueryResponse = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;

        resp.rows
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.f.into_iter().next())
            .map(|cell| cell.v)
            .ok_or_else(|| ConnectionError::QueryFailed("query returned no rows".to_string()))
    }

    fn freshness_sql(
        &self,
        asset_name: &str,
        column: Option<&str>,
    ) -> Result<String, ConnectionError> {
        /// Escapes backticks for BigQuery backtick-quoted identifiers.
        fn escape_backtick(s: &str) -> String {
            s.replace('`', "``")
        }
        /// Escapes single quotes for BigQuery string literals.
        fn escape_single_quote(s: &str) -> String {
            s.replace('\'', "''")
        }

        Ok(match column {
            Some(col) => {
                let col = escape_backtick(col);
                let name = escape_backtick(asset_name);
                format!("SELECT MAX(`{col}`) FROM `{name}`")
            }
            None => {
                let (dataset, table) = match asset_name.split_once('.') {
                    Some((d, t)) => (d, t),
                    None => ("", asset_name),
                };
                let dataset = escape_backtick(dataset);
                let table = escape_single_quote(table);
                format!(
                    "SELECT MAX(last_modified_time) \
                     FROM `{dataset}`.INFORMATION_SCHEMA.PARTITIONS \
                     WHERE table_name = '{table}'"
                )
            }
        })
    }

    fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
        Box::new(sqlparser::dialect::BigQueryDialect {})
    }

    async fn execute_sql(&self, sql: &str) -> Result<(), ConnectionError> {
        let token = self.access_token().await?;
        let api_project = self
            .config
            .execution_project
            .as_deref()
            .unwrap_or(&self.config.project);
        let url =
            format!("https://bigquery.googleapis.com/bigquery/v2/projects/{api_project}/queries",);

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct QueryRequest<'a> {
            query: &'a str,
            use_legacy_sql: bool,
            default_dataset: DatasetRef<'a>,
            #[serde(skip_serializing_if = "Option::is_none")]
            timeout_ms: Option<u32>,
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct DatasetRef<'a> {
            project_id: &'a str,
            dataset_id: &'a str,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct DmlResponse {
            #[serde(default)]
            errors: Option<Vec<DmlError>>,
        }
        #[derive(Deserialize)]
        struct DmlError {
            message: String,
        }

        let body = QueryRequest {
            query: sql,
            use_legacy_sql: false,
            default_dataset: DatasetRef {
                project_id: &self.config.project,
                dataset_id: &self.config.dataset,
            },
            timeout_ms: self.config.timeout_ms,
        };

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectionError::QueryFailed(format!(
                "execute_sql failed: {body}"
            )));
        }

        let dml_resp: DmlResponse = resp.json().await?;
        if let Some(msg) = dml_resp.errors.as_deref().and_then(|e| e.first()) {
            return Err(ConnectionError::QueryFailed(format!(
                "execute_sql error: {}",
                msg.message
            )));
        }

        Ok(())
    }

    async fn load_jsonl(
        &self,
        dataset: &str,
        table: &str,
        jsonl_path: &Path,
    ) -> Result<(), ConnectionError> {
        let token = self.access_token().await?;
        let content = std::fs::read(jsonl_path)
            .map_err(|e| ConnectionError::QueryFailed(format!("cannot read JSONL file: {e}")))?;

        let api_project = self
            .config
            .execution_project
            .as_deref()
            .unwrap_or(&self.config.project);
        let url = format!(
            "https://bigquery.googleapis.com/upload/bigquery/v2/projects/{api_project}/jobs?\
             uploadType=multipart",
        );

        let job_config = serde_json::json!({
            "configuration": {
                "load": {
                    "destinationTable": {
                        "projectId": self.config.project,
                        "datasetId": dataset,
                        "tableId": table,
                    },
                    "sourceFormat": "NEWLINE_DELIMITED_JSON",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "autodetect": true,
                }
            }
        });

        let metadata_part = reqwest::multipart::Part::text(job_config.to_string())
            .mime_str("application/json")
            .map_err(|e| ConnectionError::QueryFailed(e.to_string()))?;
        let data_part = reqwest::multipart::Part::bytes(content)
            .mime_str("application/octet-stream")
            .map_err(|e| ConnectionError::QueryFailed(e.to_string()))?;
        let form = reqwest::multipart::Form::new()
            .part("metadata", metadata_part)
            .part("data", data_part);

        #[derive(Deserialize)]
        struct JobResponse {
            status: Option<JobStatus>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct JobStatus {
            error_result: Option<JobError>,
        }
        #[derive(Deserialize)]
        struct JobError {
            message: String,
        }

        let resp: JobResponse = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .multipart(form)
            .send()
            .await?
            .json()
            .await?;

        if let Some(err) = resp.status.and_then(|s| s.error_result) {
            return Err(ConnectionError::QueryFailed(format!(
                "load job failed: {}",
                err.message
            )));
        }
        Ok(())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::kind::connection::dbt::DbtProfilesFile;

    const PROFILES_YAML: &str = r#"
my_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      project: my-gcp-project
      dataset: raw
      method: oauth
    sa:
      type: bigquery
      project: my-gcp-project
      dataset: raw
      method: service-account
      keyfile: /path/to/keyfile.json
    no_method:
      type: bigquery
      project: my-gcp-project
      dataset: raw
    with_timeout:
      type: bigquery
      project: my-gcp-project
      dataset: raw
      method: oauth
      job_execution_timeout_seconds: 30
    with_exec_project:
      type: bigquery
      project: my-gcp-project
      dataset: raw
      method: oauth
      execution_project: my-billing-proj
"#;

    fn profiles() -> DbtProfilesFile {
        DbtProfilesFile::parse_str(PROFILES_YAML).unwrap()
    }

    #[test]
    fn parse_oauth() {
        let f = profiles();
        let out = f.resolve("my_project", Some("dev")).unwrap();
        let cfg = BigQueryConfig::from_output(out).unwrap();
        assert_eq!(cfg.project, "my-gcp-project");
        assert_eq!(cfg.dataset, "raw");
        assert_eq!(cfg.auth, AuthMethod::OAuth);
    }

    #[test]
    fn parse_service_account() {
        let f = profiles();
        let out = f.resolve("my_project", Some("sa")).unwrap();
        let cfg = BigQueryConfig::from_output(out).unwrap();
        assert_eq!(
            cfg.auth,
            AuthMethod::ServiceAccount {
                keyfile: "/path/to/keyfile.json".to_string()
            }
        );
    }

    #[test]
    fn defaults_to_oauth_when_method_omitted() {
        let f = profiles();
        let out = f.resolve("my_project", Some("no_method")).unwrap();
        let cfg = BigQueryConfig::from_output(out).unwrap();
        assert_eq!(cfg.auth, AuthMethod::OAuth);
    }

    #[test]
    fn parses_timeout_seconds() {
        let f = profiles();
        let out = f.resolve("my_project", Some("with_timeout")).unwrap();
        let cfg = BigQueryConfig::from_output(out).unwrap();
        assert_eq!(cfg.timeout_ms, Some(30_000));
    }

    #[test]
    fn rejects_timeout_overflow() {
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
                (
                    "job_execution_timeout_seconds".to_string(),
                    serde_yaml::Value::Number(4_295_000.into()),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let err = BigQueryConfig::from_output(&output).unwrap_err();
        assert!(
            matches!(err, ConnectionError::InvalidField { field } if field == "job_execution_timeout_seconds")
        );
    }

    #[test]
    fn parses_execution_project() {
        let f = profiles();
        let out = f.resolve("my_project", Some("with_exec_project")).unwrap();
        let cfg = BigQueryConfig::from_output(out).unwrap();
        assert_eq!(cfg.execution_project, Some("my-billing-proj".to_string()));
    }

    #[test]
    fn execution_project_is_none_when_omitted() {
        let f = profiles();
        let out = f.resolve("my_project", Some("dev")).unwrap();
        let cfg = BigQueryConfig::from_output(out).unwrap();
        assert_eq!(cfg.execution_project, None);
    }

    #[test]
    fn timeout_is_none_when_omitted() {
        let f = profiles();
        let out = f.resolve("my_project", Some("dev")).unwrap();
        let cfg = BigQueryConfig::from_output(out).unwrap();
        assert_eq!(cfg.timeout_ms, None);
    }

    #[test]
    fn rejects_missing_project() {
        let output = AdapterConfig {
            adapter_type: "bigquery".to_string(),
            fields: [(
                "dataset".to_string(),
                serde_yaml::Value::String("raw".to_string()),
            )]
            .into_iter()
            .collect(),
        };
        let err = BigQueryConfig::from_output(&output).unwrap_err();
        assert!(matches!(err, ConnectionError::MissingField { field } if field == "project"));
    }

    #[test]
    fn rejects_unsupported_adapter() {
        let output = AdapterConfig {
            adapter_type: "snowflake".to_string(),
            fields: Default::default(),
        };
        let err = BigQueryConfig::from_output(&output).unwrap_err();
        assert!(matches!(err, ConnectionError::UnsupportedAdapter(_)));
    }

    fn dummy_conn() -> BigQueryConnection {
        BigQueryConnection::new(BigQueryConfig {
            auth: AuthMethod::OAuth,
            project: "p".to_string(),
            execution_project: None,
            dataset: "d".to_string(),
            timeout_ms: None,
        })
    }

    #[test]
    fn freshness_sql_with_column() {
        let conn = dummy_conn();
        let sql = conn.freshness_sql("my_table", Some("updated_at")).unwrap();
        assert_eq!(sql, "SELECT MAX(`updated_at`) FROM `my_table`");
    }

    #[test]
    fn freshness_sql_without_column_uses_information_schema() {
        let conn = dummy_conn();
        let sql = conn.freshness_sql("my_dataset.my_table", None).unwrap();
        assert!(sql.contains("INFORMATION_SCHEMA.PARTITIONS"));
        assert!(sql.contains("my_dataset"));
        assert!(sql.contains("my_table"));
    }

    macro_rules! freshness_sql_escape_test {
        ($($name:ident: $asset:expr, $col:expr => contains $expected:expr, not_contains $forbidden:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let conn = dummy_conn();
                    let sql = conn.freshness_sql($asset, $col).unwrap();
                    assert!(sql.contains($expected), "expected {}: {sql}", $expected);
                    assert!(!sql.contains($forbidden), "unexpected {}: {sql}", $forbidden);
                }
            )*
        };
    }

    freshness_sql_escape_test! {
        freshness_sql_escapes_single_quotes_in_table_name:
            "ds.tab'le", None => contains "tab''le", not_contains "tab'le";
        freshness_sql_escapes_backticks_in_column:
            "my_table", Some("col`umn") => contains "col``umn", not_contains "col`umn";
        freshness_sql_escapes_backticks_in_asset_name:
            "my`table", Some("col") => contains "my``table", not_contains "my`table";
    }
}

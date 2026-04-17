use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::runtime::duration::Duration;
use crate::runtime::kind::connection::dbt::AdapterConfig;

use super::{require_str, Connection, ConnectionError};

/// Expands `${VAR}` references in a single value using process env.
fn expand_env_value(template: &str) -> Result<String, ConnectionError> {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next();
            let mut var = String::new();
            let mut closed = false;
            for vc in chars.by_ref() {
                if vc == '}' {
                    closed = true;
                    break;
                }
                var.push(vc);
            }
            if !closed {
                return Err(ConnectionError::AuthFailed(format!(
                    "unterminated '${{' in identity env template: {template}"
                )));
            }
            let val = std::env::var(&var).map_err(|_| {
                ConnectionError::AuthFailed(format!(
                    "environment variable '{var}' referenced by identity is not set"
                ))
            })?;
            out.push_str(&val);
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

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
    /// Query timeout. `None` when not specified by the user; the global default
    /// is injected by `ResolvedConnection::connect()`.
    pub timeout: Option<Duration>,
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
        let timeout = output
            .fields
            .get("job_execution_timeout_seconds")
            .and_then(|v| v.as_u64())
            .map(Duration::from_secs);
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
            timeout,
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

fn token_from_adc(agent: &ureq::Agent) -> Result<String, ConnectionError> {
    #[cfg(windows)]
    let adc_path = dirs::config_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("gcloud/application_default_credentials.json");
    #[cfg(not(windows))]
    let adc_path = dirs::home_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join(".config/gcloud/application_default_credentials.json");
    token_from_adc_path(agent, &adc_path)
}

fn token_from_adc_path(agent: &ureq::Agent, path: &Path) -> Result<String, ConnectionError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot read ADC file: {e}")))?;
    let adc: AdcFile = serde_json::from_str(&content)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot parse ADC file: {e}")))?;
    match adc {
        AdcFile::AuthorizedUser {
            client_id,
            client_secret,
            refresh_token,
        } => refresh_token_exchange(agent, &client_id, &client_secret, &refresh_token),
        AdcFile::ServiceAccount(creds) => service_account_token(
            agent,
            &creds.client_email,
            &creds.private_key,
            &creds.token_uri,
        ),
    }
}

fn token_from_keyfile(agent: &ureq::Agent, keyfile: &str) -> Result<String, ConnectionError> {
    let content = std::fs::read_to_string(keyfile)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot read keyfile: {e}")))?;
    let creds: Credentials = serde_json::from_str(&content)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot parse keyfile: {e}")))?;
    service_account_token(
        agent,
        &creds.client_email,
        &creds.private_key,
        &creds.token_uri,
    )
}

fn refresh_token_exchange(
    agent: &ureq::Agent,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, ConnectionError> {
    let mut resp = agent
        .post(TOKEN_URL)
        .send_form([
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .map_err(|e| ConnectionError::Http(e.to_string()))?;
    let token_resp: TokenResponse = resp
        .body_mut()
        .read_json()
        .map_err(|e| ConnectionError::Http(e.to_string()))?;
    Ok(token_resp.access_token)
}

fn service_account_token(
    agent: &ureq::Agent,
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

    let mut resp = agent
        .post(token_uri)
        .send_form([
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", jwt.as_str()),
        ])
        .map_err(|e| ConnectionError::Http(e.to_string()))?;
    let token_resp: TokenResponse = resp
        .body_mut()
        .read_json()
        .map_err(|e| ConnectionError::Http(e.to_string()))?;
    Ok(token_resp.access_token)
}

// ── BigQuery REST client ─────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BqQueryRequest {
    query: String,
    use_legacy_sql: bool,
    default_dataset: BqDatasetRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BqDatasetRef {
    project_id: String,
    dataset_id: String,
}

pub struct BigQueryConnection {
    config: BigQueryConfig,
    agent: ureq::Agent,
    /// Unexpanded Identity env template. `${VAR}` references are resolved at `access_token` time.
    identity_env: Option<std::collections::HashMap<String, String>>,
}

impl BigQueryConnection {
    /// Creates a `BigQueryConnection` from a `ResolvedConnection::BigQuery`.
    pub fn from_resolved(
        project: &str,
        dataset: &str,
        execution_project: &Option<String>,
        method: Option<&str>,
        keyfile: &Option<String>,
        timeout: Option<Duration>,
        identity_env: Option<&std::collections::HashMap<String, String>>,
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
            timeout,
        };
        Ok(Self {
            config,
            agent: ureq::Agent::new_with_defaults(),
            identity_env: identity_env.cloned(),
        })
    }

    pub fn new(config: BigQueryConfig) -> Self {
        Self {
            config,
            agent: ureq::Agent::new_with_defaults(),
            identity_env: None,
        }
    }

    fn access_token(&self) -> Result<String, ConnectionError> {
        // If Identity env provides GOOGLE_APPLICATION_CREDENTIALS, expand ${VAR} and use it
        // as the ADC file path. The expanded value is dropped after this scope.
        if let Some(env) = &self.identity_env {
            if let Some(template) = env.get("GOOGLE_APPLICATION_CREDENTIALS") {
                let expanded = expand_env_value(template)?;
                return token_from_adc_path(&self.agent, std::path::Path::new(&expanded));
            }
        }
        match &self.config.auth {
            AuthMethod::OAuth => token_from_adc(&self.agent),
            AuthMethod::ServiceAccount { keyfile } => token_from_keyfile(&self.agent, keyfile),
        }
    }

    fn queries_url(&self) -> String {
        let api_project = self
            .config
            .execution_project
            .as_deref()
            .unwrap_or(&self.config.project);
        format!("https://bigquery.googleapis.com/bigquery/v2/projects/{api_project}/queries")
    }

    fn build_query_request(&self, sql: String) -> BqQueryRequest {
        BqQueryRequest {
            query: sql,
            use_legacy_sql: false,
            default_dataset: BqDatasetRef {
                project_id: self.config.project.clone(),
                dataset_id: self.config.dataset.clone(),
            },
            timeout_ms: self
                .config
                .timeout
                .as_ref()
                .and_then(|d| u32::try_from(d.as_std().as_millis()).ok()),
        }
    }

    fn query_scalar_sync(&self, sql: &str) -> Result<Value, ConnectionError> {
        let token = self.access_token()?;
        let url = self.queries_url();
        let body = self.build_query_request(sql.to_string());
        let mut resp = self
            .agent
            .post(&url)
            .header("Authorization", &format!("Bearer {token}"))
            .send_json(&body)
            .map_err(|e| ConnectionError::Http(e.to_string()))?;
        let json: Value = resp
            .body_mut()
            .read_json()
            .map_err(|e| ConnectionError::Http(e.to_string()))?;
        extract_scalar_from_query_response(json)
    }

    fn execute_sql_sync(&self, sql: &str) -> Result<(), ConnectionError> {
        let token = self.access_token()?;
        let url = self.queries_url();
        let body = self.build_query_request(sql.to_string());
        let mut resp = self
            .agent
            .post(&url)
            .header("Authorization", &format!("Bearer {token}"))
            .send_json(&body)
            .map_err(|e| ConnectionError::Http(e.to_string()))?;
        let json: Value = resp
            .body_mut()
            .read_json()
            .map_err(|e| ConnectionError::Http(e.to_string()))?;
        check_dml_response(json)
    }

    /// Loads a JSONL file into a BigQuery table via the multipart/related upload API.
    ///
    /// Reads the file into memory and sends it as a single load job request with
    /// `WRITE_TRUNCATE` disposition (replaces existing table contents) and
    /// automatic schema detection.
    fn load_jsonl_sync(
        &self,
        dataset: &str,
        table: &str,
        jsonl_path: &Path,
    ) -> Result<(), ConnectionError> {
        let token = self.access_token()?;
        let data = std::fs::read(jsonl_path)
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

        let metadata = build_load_job_metadata(&self.config.project, dataset, table);
        let (content_type, body) = build_multipart_related(&metadata, &data);

        let mut resp = self
            .agent
            .post(&url)
            .header("Authorization", &format!("Bearer {token}"))
            .header("Content-Type", &content_type)
            .send(body.as_slice())
            .map_err(|e| ConnectionError::Http(e.to_string()))?;

        let job_resp: LoadJobResponse = resp
            .body_mut()
            .read_json()
            .map_err(|e| ConnectionError::Http(e.to_string()))?;
        if let Some(err) = job_resp.load_error_message() {
            return Err(ConnectionError::QueryFailed(format!(
                "load job failed: {err}"
            )));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct LoadJobResponse {
    status: Option<LoadJobStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadJobStatus {
    error_result: Option<LoadJobError>,
}

#[derive(Deserialize)]
struct LoadJobError {
    message: String,
}

impl LoadJobResponse {
    fn load_error_message(&self) -> Option<&str> {
        self.status
            .as_ref()?
            .error_result
            .as_ref()
            .map(|e| e.message.as_str())
    }
}

fn build_load_job_metadata(project_id: &str, dataset: &str, table: &str) -> String {
    serde_json::json!({
        "configuration": {
            "load": {
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset,
                    "tableId": table,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": true,
            }
        }
    })
    .to_string()
}

/// Builds a multipart/related body with a JSON metadata part and a binary data part.
/// Returns the Content-Type header value and the assembled body bytes.
fn build_multipart_related(metadata_json: &str, data: &[u8]) -> (String, Vec<u8>) {
    let boundary = format!(
        "nagi_boundary_{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut body = Vec::new();
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(metadata_json.as_bytes());
    body.extend_from_slice(
        format!("\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(data);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let content_type = format!("multipart/related; boundary={boundary}");
    (content_type, body)
}

fn extract_scalar_from_query_response(json: Value) -> Result<Value, ConnectionError> {
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

    let resp: QueryResponse =
        serde_json::from_value(json).map_err(|e| ConnectionError::Http(e.to_string()))?;
    resp.rows
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.f.into_iter().next())
        .map(|cell| cell.v)
        .ok_or_else(|| ConnectionError::QueryFailed("query returned no rows".to_string()))
}

fn check_dml_response(json: Value) -> Result<(), ConnectionError> {
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

    let resp: DmlResponse =
        serde_json::from_value(json).map_err(|e| ConnectionError::Http(e.to_string()))?;
    if let Some(msg) = resp.errors.as_deref().and_then(|e| e.first()) {
        return Err(ConnectionError::QueryFailed(format!(
            "execute_sql error: {}",
            msg.message
        )));
    }
    Ok(())
}

#[async_trait]
impl Connection for BigQueryConnection {
    async fn query_scalar(&self, sql: &str) -> Result<Value, ConnectionError> {
        let config = self.config.clone();
        let agent = self.agent.clone();
        let sql = sql.to_string();
        super::run_blocking(move || {
            BigQueryConnection {
                config,
                agent,
                identity_env: None,
            }
            .query_scalar_sync(&sql)
        })
        .await
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
        let config = self.config.clone();
        let agent = self.agent.clone();
        let sql = sql.to_string();
        super::run_blocking(move || {
            BigQueryConnection {
                config,
                agent,
                identity_env: None,
            }
            .execute_sql_sync(&sql)
        })
        .await
    }

    async fn load_jsonl(
        &self,
        dataset: &str,
        table: &str,
        jsonl_path: &Path,
    ) -> Result<(), ConnectionError> {
        let config = self.config.clone();
        let agent = self.agent.clone();
        let dataset = dataset.to_string();
        let table = table.to_string();
        let jsonl_path = jsonl_path.to_path_buf();
        super::run_blocking(move || {
            BigQueryConnection {
                config,
                agent,
                identity_env: None,
            }
            .load_jsonl_sync(&dataset, &table, &jsonl_path)
        })
        .await
    }
}

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
        assert_eq!(
            cfg.timeout.as_ref().map(Duration::as_std),
            Some(std::time::Duration::from_secs(30))
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
            timeout: None,
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

    // ── queries_url / build_query_request ───────────────────────────────

    #[test]
    fn queries_url_uses_project() {
        let conn = dummy_conn();
        assert_eq!(
            conn.queries_url(),
            "https://bigquery.googleapis.com/bigquery/v2/projects/p/queries"
        );
    }

    #[test]
    fn queries_url_uses_execution_project() {
        let conn = BigQueryConnection::new(BigQueryConfig {
            auth: AuthMethod::OAuth,
            project: "p".to_string(),
            execution_project: Some("billing".to_string()),
            dataset: "d".to_string(),
            timeout: None,
        });
        assert!(conn.queries_url().contains("/billing/queries"));
    }

    #[test]
    fn build_query_request_sends_timeout_ms_from_duration() {
        let conn = BigQueryConnection::new(BigQueryConfig {
            auth: AuthMethod::OAuth,
            project: "p".to_string(),
            execution_project: None,
            dataset: "d".to_string(),
            timeout: Some(Duration::from_secs(45)),
        });
        let req = conn.build_query_request("SELECT 1".to_string());
        assert_eq!(req.timeout_ms, Some(45_000));
    }

    #[test]
    fn build_query_request_fields() {
        let conn = dummy_conn();
        let req = conn.build_query_request("SELECT 1".to_string());
        assert_eq!(req.query, "SELECT 1");
        assert!(!req.use_legacy_sql);
        assert_eq!(req.default_dataset.project_id, "p");
        assert_eq!(req.default_dataset.dataset_id, "d");
        assert_eq!(req.timeout_ms, None);
    }

    // ── build_load_job_metadata ─────────────────────────────────────────
    //
    // Load job metadata: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad

    #[test]
    fn build_load_job_metadata_contains_destination() {
        let json = build_load_job_metadata("proj", "ds", "tbl");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let dest = &v["configuration"]["load"]["destinationTable"];
        assert_eq!(dest["projectId"], "proj");
        assert_eq!(dest["datasetId"], "ds");
        assert_eq!(dest["tableId"], "tbl");
    }

    // ── build_multipart_related ─────────────────────────────────────────
    //
    // Multipart upload: https://cloud.google.com/bigquery/docs/reference/api-uploads#multipart

    #[test]
    fn build_multipart_related_structure() {
        let (content_type, body) = build_multipart_related("{}", b"data");
        assert!(content_type.starts_with("multipart/related; boundary="));
        let body_str = String::from_utf8_lossy(&body);
        assert!(body_str.contains("Content-Type: application/json"));
        assert!(body_str.contains("Content-Type: application/octet-stream"));
        assert!(body_str.contains("{}"));
        assert!(body_str.contains("data"));
    }

    // ── LoadJobResponse ─────────────────────────────────────────────────
    //
    // Job resource: googleapis.com/discovery/v1/apis/bigquery/v2/rest schemas.Job
    // status.errorResult is an ErrorProto: { "message", "reason", "location" }
    // Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatus

    #[test]
    fn load_job_response_no_error() {
        let resp: LoadJobResponse = serde_json::from_str(r#"{"status":{}}"#).unwrap();
        assert!(resp.load_error_message().is_none());
    }

    #[test]
    fn load_job_response_with_error() {
        let resp: LoadJobResponse =
            serde_json::from_str(r#"{"status":{"errorResult":{"message":"bad"}}}"#).unwrap();
        assert_eq!(resp.load_error_message(), Some("bad"));
    }

    // ── extract_scalar_from_query_response ──────────────────────────────
    //
    // Response format: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    // Row structure: { "rows": [{ "f": [{ "v": <value> }] }] }
    // v type is "any" per Discovery API (googleapis.com/discovery/v1/apis/bigquery/v2/rest,
    // schemas.TableCell.properties.v). In practice, scalar values are returned as JSON
    // strings (confirmed by google-cloud-go bigquery/value.go convertBasicType parsing
    // string inputs via strconv).

    #[test]
    fn extract_scalar_returns_first_cell() {
        let json = serde_json::json!({"rows": [{"f": [{"v": "42"}]}]});
        let val = extract_scalar_from_query_response(json).unwrap();
        assert_eq!(val, serde_json::json!("42"));
    }

    #[test]
    fn extract_scalar_errors_on_empty_rows() {
        let json = serde_json::json!({"rows": []});
        let err = extract_scalar_from_query_response(json).unwrap_err();
        assert!(matches!(err, ConnectionError::QueryFailed(_)));
    }

    #[test]
    fn extract_scalar_errors_on_null_rows() {
        let json = serde_json::json!({});
        let err = extract_scalar_from_query_response(json).unwrap_err();
        assert!(matches!(err, ConnectionError::QueryFailed(_)));
    }

    // ── check_dml_response ──────────────────────────────────────────────
    //
    // QueryResponse.errors: array of ErrorProto
    // ErrorProto: { "message", "reason", "location", "debugInfo" }
    // Ref: googleapis.com/discovery/v1/apis/bigquery/v2/rest schemas.QueryResponse, ErrorProto

    #[test]
    fn check_dml_response_ok() {
        let json = serde_json::json!({});
        assert!(check_dml_response(json).is_ok());
    }

    #[test]
    fn check_dml_response_with_errors() {
        let json = serde_json::json!({"errors": [{"message": "syntax error"}]});
        let err = check_dml_response(json).unwrap_err();
        assert!(matches!(err, ConnectionError::QueryFailed(msg) if msg.contains("syntax error")));
    }

    #[test]
    fn check_dml_response_empty_errors_array() {
        let json = serde_json::json!({"errors": []});
        assert!(check_dml_response(json).is_ok());
    }
}

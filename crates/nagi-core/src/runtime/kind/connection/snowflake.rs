use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use super::sql::{escape_identifier, escape_literal};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rsa::pkcs8::DecodePrivateKey;
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime::duration::Duration;
use crate::runtime::kind::connection::dbt::AdapterConfig;

use super::{require_str, Connection, ConnectionError};

/// Resolved Snowflake connection configuration.
/// Does not hold credentials — only the path to the private key file.
#[derive(Debug, Clone)]
pub(super) struct SnowflakeConfig {
    pub(super) account: String,
    pub(super) user: String,
    pub(super) database: String,
    pub(super) schema: String,
    pub(super) warehouse: String,
    pub(super) role: Option<String>,
    pub(super) private_key_path: String,
    pub(super) timeout: Option<Duration>,
}

impl SnowflakeConfig {
    pub(super) fn from_output(output: &AdapterConfig) -> Result<Self, ConnectionError> {
        if output.adapter_type != "snowflake" {
            return Err(ConnectionError::UnsupportedAdapter(
                output.adapter_type.clone(),
            ));
        }
        let account = require_str(&output.fields, "account")?;
        let user = require_str(&output.fields, "user")?;
        let database = require_str(&output.fields, "database")?;
        let schema = require_str(&output.fields, "schema")?;
        let warehouse = require_str(&output.fields, "warehouse")?;
        let role = output
            .fields
            .get("role")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let private_key_path = require_str(&output.fields, "private_key_path")?;
        let timeout = output
            .fields
            .get("query_timeout")
            .and_then(|v| v.as_u64())
            .map(Duration::from_secs);
        Ok(Self {
            account,
            user,
            database,
            schema,
            warehouse,
            role,
            private_key_path,
            timeout,
        })
    }
}

/// JWT lifetime in seconds. Snowflake recommends short-lived tokens (max 60 minutes).
const JWT_LIFETIME_SECS: u64 = 60;

/// JWT payload for Snowflake key-pair authentication.
/// Required claims: iss, sub, iat, exp. No aud claim is needed.
/// See: https://docs.snowflake.com/en/developer-guide/sql-api/authenticating
#[derive(Serialize)]
struct SnowflakeJwtClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

/// Reads the private key, derives the public key fingerprint, and generates a JWT.
/// The private key is dropped at the end of this function scope.
fn generate_jwt(
    account: &str,
    user: &str,
    private_key_path: &str,
) -> Result<String, ConnectionError> {
    // Account identifier: uppercase, dots replaced with hyphens.
    let account_upper = account.to_uppercase().replace('.', "-");
    let user_upper = user.to_uppercase();

    // Read and parse PKCS#8 PEM private key.
    let pem = std::fs::read_to_string(private_key_path)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot read private key file: {e}")))?;
    let private_key = RsaPrivateKey::from_pkcs8_pem(&pem).map_err(|e| {
        ConnectionError::AuthFailed(format!("cannot parse PKCS#8 private key: {e}"))
    })?;

    // Derive public key and compute SHA-256 fingerprint of DER-encoded public key.
    let public_key = private_key.to_public_key();
    let public_key_der = rsa::pkcs8::EncodePublicKey::to_public_key_der(&public_key)
        .map_err(|e| ConnectionError::AuthFailed(format!("cannot encode public key: {e}")))?;
    let fingerprint = {
        let mut hasher = Sha256::new();
        hasher.update(public_key_der.as_ref());
        BASE64_STANDARD.encode(hasher.finalize())
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = SnowflakeJwtClaims {
        iss: format!("{account_upper}.{user_upper}.SHA256:{fingerprint}"),
        sub: format!("{account_upper}.{user_upper}"),
        iat: now,
        exp: now + JWT_LIFETIME_SECS,
    };

    let encoding_key = EncodingKey::from_rsa_pem(pem.as_bytes())
        .map_err(|e| ConnectionError::AuthFailed(format!("invalid RSA key for JWT: {e}")))?;
    // Private key bytes are dropped here when `pem` and `encoding_key` go out of scope.
    encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
        .map_err(|e| ConnectionError::AuthFailed(format!("JWT encoding failed: {e}")))
}

pub(super) struct SnowflakeConnection {
    config: SnowflakeConfig,
    agent: ureq::Agent,
}

impl SnowflakeConnection {
    pub(super) fn new(config: SnowflakeConfig) -> Self {
        Self {
            config,
            agent: ureq::Agent::new_with_defaults(),
        }
    }

    /// Generates a fresh JWT and returns it. Private key is read and dropped per call.
    fn jwt(&self) -> Result<String, ConnectionError> {
        generate_jwt(
            &self.config.account,
            &self.config.user,
            &self.config.private_key_path,
        )
    }

    fn api_url(&self) -> String {
        format!(
            "https://{}.snowflakecomputing.com/api/v2/statements",
            self.config.account
        )
    }

    fn execute_statement_sync(&self, sql: &str) -> Result<StatementResponse, ConnectionError> {
        let token = self.jwt()?;
        let url = self.api_url();

        let timeout = self
            .config
            .timeout
            .as_ref()
            .and_then(|d| u32::try_from(d.as_std().as_secs()).ok());
        let body = StatementRequest {
            statement: sql,
            timeout,
            database: &self.config.database,
            schema: &self.config.schema,
            warehouse: &self.config.warehouse,
            role: self.config.role.as_deref(),
        };

        let mut resp = self
            .agent
            .post(&url)
            .header("Authorization", &format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .send_json(&body)
            .map_err(|e| ConnectionError::Http(e.to_string()))?;

        resp.body_mut()
            .read_json::<StatementResponse>()
            .map_err(|e| {
                ConnectionError::QueryFailed(format!("failed to parse Snowflake response: {e}"))
            })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatementRequest<'a> {
    statement: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u32>,
    database: &'a str,
    schema: &'a str,
    warehouse: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatementResponse {
    data: Option<Vec<Vec<serde_json::Value>>>,
    message: Option<String>,
    code: Option<String>,
}

fn extract_scalar_from_statement(
    resp: &StatementResponse,
) -> Result<serde_json::Value, ConnectionError> {
    resp.data
        .as_ref()
        .and_then(|rows| rows.first())
        .and_then(|row| row.first())
        .cloned()
        .ok_or_else(|| ConnectionError::QueryFailed("query returned no rows".to_string()))
}

fn check_statement_success(resp: &StatementResponse) -> Result<(), ConnectionError> {
    resp.code
        .as_deref()
        .filter(|code| *code != "090001")
        .map_or(Ok(()), |code| {
            let msg = resp.message.as_deref().unwrap_or_default();
            Err(ConnectionError::QueryFailed(format!(
                "Snowflake error {code}: {msg}"
            )))
        })
}

#[async_trait]
impl Connection for SnowflakeConnection {
    async fn query_scalar(&self, sql: &str) -> Result<serde_json::Value, ConnectionError> {
        let agent = self.agent.clone();
        let config = self.config.clone();
        let sql = sql.to_string();
        let resp = super::run_blocking(move || {
            SnowflakeConnection { config, agent }.execute_statement_sync(&sql)
        })
        .await?;
        extract_scalar_from_statement(&resp)
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
                // CONVERT_TIMEZONE to UTC before TO_CHAR so the 'Z' suffix is accurate.
                Ok(format!(
                    "SELECT TO_CHAR(CONVERT_TIMEZONE('UTC', MAX(\"{col}\")), \
                     'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM \"{name}\""
                ))
            }
            None => {
                let (schema, table) = match asset_name.split_once('.') {
                    Some((s, t)) => (s.to_string(), t.to_string()),
                    None => (self.config.schema.clone(), asset_name.to_string()),
                };
                let schema_lit = escape_literal(&schema);
                let table_lit = escape_literal(&table);
                let database = escape_identifier(&self.config.database);
                Ok(format!(
                    "SELECT TO_CHAR(CONVERT_TIMEZONE('UTC', LAST_ALTERED), \
                     'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') \
                     FROM \"{database}\".INFORMATION_SCHEMA.TABLES \
                     WHERE TABLE_SCHEMA = '{schema_lit}' \
                     AND TABLE_NAME = '{table_lit}'"
                ))
            }
        }
    }

    fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
        Box::new(sqlparser::dialect::SnowflakeDialect)
    }

    async fn execute_sql(&self, sql: &str) -> Result<(), ConnectionError> {
        let agent = self.agent.clone();
        let config = self.config.clone();
        let sql = sql.to_string();
        let resp = super::run_blocking(move || {
            SnowflakeConnection { config, agent }.execute_statement_sync(&sql)
        })
        .await?;
        check_statement_success(&resp)
    }

    async fn load_jsonl(
        &self,
        dataset: &str,
        table: &str,
        jsonl_path: &Path,
    ) -> Result<(), ConnectionError> {
        let schema = if dataset.is_empty() {
            self.config.schema.clone()
        } else {
            dataset.to_string()
        };
        let table = table.to_string();
        let jsonl_path = jsonl_path.to_path_buf();
        let agent = self.agent.clone();
        let config = self.config.clone();

        super::run_blocking(move || {
            let content = std::fs::read_to_string(&jsonl_path).map_err(|e| {
                ConnectionError::QueryFailed(format!("cannot read JSONL file: {e}"))
            })?;
            let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
            if lines.is_empty() {
                return Ok(());
            }

            let conn = SnowflakeConnection { config, agent };
            let create_sql = build_create_staging_sql(&schema, &table);
            conn.execute_statement_sync(&create_sql)?;

            for sql in build_insert_batches(&schema, &table, &lines, LOAD_BATCH_SIZE) {
                conn.execute_statement_sync(&sql)?;
            }
            Ok(())
        })
        .await
    }
}

fn build_create_staging_sql(schema: &str, table: &str) -> String {
    let schema = escape_identifier(schema);
    let table = escape_identifier(table);
    format!("CREATE OR REPLACE TABLE \"{schema}\".\"{table}\" (data VARIANT)")
}

/// Maximum rows per INSERT statement. Snowflake allows up to 200,000 rows in
/// a single VALUES clause, but we use a smaller batch to keep SQL size reasonable.
const LOAD_BATCH_SIZE: usize = 10_000;

fn build_insert_json_sql(
    schema: &str,
    table: &str,
    json_lines: &[&str],
    overwrite: bool,
) -> String {
    let schema = escape_identifier(schema);
    let table = escape_identifier(table);
    let keyword = if overwrite {
        "INSERT OVERWRITE INTO"
    } else {
        "INSERT INTO"
    };
    let values: Vec<String> = json_lines
        .iter()
        .map(|line| {
            let escaped = escape_literal(line);
            format!("('{escaped}')")
        })
        .collect();
    format!(
        "{keyword} \"{schema}\".\"{table}\" (data) SELECT PARSE_JSON(column1) FROM VALUES {}",
        values.join(", ")
    )
}

fn build_insert_batches(
    schema: &str,
    table: &str,
    lines: &[&str],
    batch_size: usize,
) -> Vec<String> {
    lines
        .chunks(batch_size)
        .enumerate()
        .map(|(i, chunk)| build_insert_json_sql(schema, table, chunk, i == 0))
        .collect()
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
      type: snowflake
      account: myorg-myacct
      user: MY_USER
      database: MY_DB
      schema: MY_SCHEMA
      warehouse: MY_WH
      role: MY_ROLE
      private_key_path: /path/to/rsa_key.p8
    no_role:
      type: snowflake
      account: myorg-myacct
      user: MY_USER
      database: MY_DB
      schema: MY_SCHEMA
      warehouse: MY_WH
      private_key_path: /path/to/rsa_key.p8
"#;

    fn profiles() -> DbtProfilesFile {
        DbtProfilesFile::parse_str(PROFILES_YAML).unwrap()
    }

    #[test]
    fn parse_all_fields() {
        let f = profiles();
        let out = f.resolve("my_project", Some("dev")).unwrap();
        let cfg = SnowflakeConfig::from_output(out).unwrap();
        assert_eq!(cfg.account, "myorg-myacct");
        assert_eq!(cfg.user, "MY_USER");
        assert_eq!(cfg.database, "MY_DB");
        assert_eq!(cfg.schema, "MY_SCHEMA");
        assert_eq!(cfg.warehouse, "MY_WH");
        assert_eq!(cfg.role, Some("MY_ROLE".to_string()));
        assert_eq!(cfg.private_key_path, "/path/to/rsa_key.p8");
    }

    #[test]
    fn parses_query_timeout() {
        let output = AdapterConfig {
            adapter_type: "snowflake".to_string(),
            fields: [
                (
                    "account".to_string(),
                    serde_yaml::Value::String("a".to_string()),
                ),
                (
                    "user".to_string(),
                    serde_yaml::Value::String("u".to_string()),
                ),
                (
                    "database".to_string(),
                    serde_yaml::Value::String("d".to_string()),
                ),
                (
                    "schema".to_string(),
                    serde_yaml::Value::String("s".to_string()),
                ),
                (
                    "warehouse".to_string(),
                    serde_yaml::Value::String("w".to_string()),
                ),
                (
                    "private_key_path".to_string(),
                    serde_yaml::Value::String("/p".to_string()),
                ),
                (
                    "query_timeout".to_string(),
                    serde_yaml::Value::Number(120.into()),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let cfg = SnowflakeConfig::from_output(&output).unwrap();
        assert_eq!(
            cfg.timeout.as_ref().map(Duration::as_std),
            Some(std::time::Duration::from_secs(120))
        );
    }

    #[test]
    fn rejects_unsupported_adapter() {
        let output = AdapterConfig {
            adapter_type: "bigquery".to_string(),
            fields: Default::default(),
        };
        let err = SnowflakeConfig::from_output(&output).unwrap_err();
        assert!(matches!(err, ConnectionError::UnsupportedAdapter(_)));
    }

    #[test]
    fn rejects_missing_account() {
        let output = AdapterConfig {
            adapter_type: "snowflake".to_string(),
            fields: [
                (
                    "user".to_string(),
                    serde_yaml::Value::String("u".to_string()),
                ),
                (
                    "database".to_string(),
                    serde_yaml::Value::String("d".to_string()),
                ),
                (
                    "schema".to_string(),
                    serde_yaml::Value::String("s".to_string()),
                ),
                (
                    "warehouse".to_string(),
                    serde_yaml::Value::String("w".to_string()),
                ),
                (
                    "private_key_path".to_string(),
                    serde_yaml::Value::String("p".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let err = SnowflakeConfig::from_output(&output).unwrap_err();
        assert!(matches!(err, ConnectionError::MissingField { field } if field == "account"));
    }

    // ── freshness_sql tests ─────────────────────────────────────────────

    fn dummy_conn() -> SnowflakeConnection {
        SnowflakeConnection::new(SnowflakeConfig {
            account: "myorg-myacct".to_string(),
            user: "MY_USER".to_string(),
            database: "MY_DB".to_string(),
            schema: "MY_SCHEMA".to_string(),
            warehouse: "MY_WH".to_string(),
            role: None,
            private_key_path: "/dummy".to_string(),
            timeout: None,
        })
    }

    #[test]
    fn freshness_sql_with_column() {
        let conn = dummy_conn();
        let sql = conn.freshness_sql("my_table", Some("updated_at")).unwrap();
        assert!(sql.contains(r#"MAX("updated_at")"#));
        assert!(sql.contains(r#"FROM "my_table""#));
        assert!(sql.contains("TO_CHAR"));
        assert!(sql.contains("CONVERT_TIMEZONE"));
    }

    #[test]
    fn freshness_sql_without_column_uses_information_schema() {
        let conn = dummy_conn();
        let sql = conn.freshness_sql("my_table", None).unwrap();
        assert!(sql.contains("INFORMATION_SCHEMA.TABLES"));
        assert!(sql.contains("MY_SCHEMA"));
        assert!(sql.contains("my_table"));
        assert!(sql.contains("TO_CHAR"));
        assert!(sql.contains("CONVERT_TIMEZONE"));
    }

    #[test]
    fn freshness_sql_without_column_qualified_name() {
        let conn = dummy_conn();
        let sql = conn.freshness_sql("OTHER_SCHEMA.my_table", None).unwrap();
        assert!(sql.contains("OTHER_SCHEMA"));
        assert!(sql.contains("my_table"));
        assert!(!sql.contains("MY_SCHEMA"));
    }

    macro_rules! freshness_sql_escape_test {
        ($($name:ident: $asset:expr, $col:expr => contains $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let conn = dummy_conn();
                    let sql = conn.freshness_sql($asset, $col).unwrap();
                    assert!(sql.contains($expected), "expected '{}' in '{sql}'", $expected);
                }
            )*
        };
    }

    freshness_sql_escape_test! {
        freshness_sql_escapes_column:
            "t", Some(r#"my"col"#) => contains r#""my""col""#;
        freshness_sql_escapes_table:
            r#"my"table"#, Some("c") => contains r#""my""table""#;
        freshness_sql_escapes_single_quotes_in_table_name:
            "SCHEMA.tab'le", None => contains "tab''le";
    }

    // ── create_connection tests ──────────────────────────────────────────

    #[test]
    fn create_connection_accepts_snowflake() {
        let output = AdapterConfig {
            adapter_type: "snowflake".to_string(),
            fields: [
                (
                    "account".to_string(),
                    serde_yaml::Value::String("a".to_string()),
                ),
                (
                    "user".to_string(),
                    serde_yaml::Value::String("u".to_string()),
                ),
                (
                    "database".to_string(),
                    serde_yaml::Value::String("d".to_string()),
                ),
                (
                    "schema".to_string(),
                    serde_yaml::Value::String("s".to_string()),
                ),
                (
                    "warehouse".to_string(),
                    serde_yaml::Value::String("w".to_string()),
                ),
                (
                    "private_key_path".to_string(),
                    serde_yaml::Value::String("p".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        };
        assert!(super::super::create_connection(&output).is_ok());
    }

    // ── JWT generation tests ────────────────────────────────────────────

    #[test]
    fn generate_jwt_with_test_key() {
        // Generate a temporary RSA key for testing.
        use rsa::pkcs8::EncodePrivateKey;
        let mut rng = rsa::rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = private_key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("test_key.p8");
        std::fs::write(&key_path, pem.as_bytes()).unwrap();

        let jwt = generate_jwt("myorg-myacct", "MY_USER", key_path.to_str().unwrap()).unwrap();
        assert!(!jwt.is_empty());

        // Decode the header to verify RS256.
        let header = jsonwebtoken::decode_header(&jwt).unwrap();
        assert_eq!(header.alg, Algorithm::RS256);
    }

    #[test]
    fn generate_jwt_uppercases_account_and_user() {
        use rsa::pkcs8::EncodePrivateKey;
        let mut rng = rsa::rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = private_key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("test_key.p8");
        std::fs::write(&key_path, pem.as_bytes()).unwrap();

        let jwt = generate_jwt("myorg-myacct", "my_user", key_path.to_str().unwrap()).unwrap();

        // Decode with the corresponding public key to check claims.
        let mut validation = jsonwebtoken::Validation::new(Algorithm::RS256);
        validation.set_audience::<&str>(&[]);
        let public_key = private_key.to_public_key();
        let pub_pem =
            rsa::pkcs8::EncodePublicKey::to_public_key_pem(&public_key, rsa::pkcs8::LineEnding::LF)
                .unwrap();
        let decoding_key = jsonwebtoken::DecodingKey::from_rsa_pem(pub_pem.as_bytes()).unwrap();
        let token_data =
            jsonwebtoken::decode::<serde_json::Value>(&jwt, &decoding_key, &validation).unwrap();
        let claims = token_data.claims;
        let iss = claims["iss"].as_str().unwrap();
        let sub = claims["sub"].as_str().unwrap();
        assert!(iss.starts_with("MYORG-MYACCT.MY_USER.SHA256:"));
        assert_eq!(sub, "MYORG-MYACCT.MY_USER");
    }

    #[test]
    fn generate_jwt_replaces_dots_with_hyphens_in_account() {
        use rsa::pkcs8::EncodePrivateKey;
        let mut rng = rsa::rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = private_key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("test_key.p8");
        std::fs::write(&key_path, pem.as_bytes()).unwrap();

        let jwt = generate_jwt("xy12345.us-east-1", "user", key_path.to_str().unwrap()).unwrap();

        let mut validation = jsonwebtoken::Validation::new(Algorithm::RS256);
        validation.set_audience::<&str>(&[]);
        let public_key = private_key.to_public_key();
        let pub_pem =
            rsa::pkcs8::EncodePublicKey::to_public_key_pem(&public_key, rsa::pkcs8::LineEnding::LF)
                .unwrap();
        let decoding_key = jsonwebtoken::DecodingKey::from_rsa_pem(pub_pem.as_bytes()).unwrap();
        let token_data =
            jsonwebtoken::decode::<serde_json::Value>(&jwt, &decoding_key, &validation).unwrap();
        let sub = token_data.claims["sub"].as_str().unwrap();
        assert_eq!(sub, "XY12345-US-EAST-1.USER");
    }

    #[test]
    fn generate_jwt_fails_on_missing_key_file() {
        let err = generate_jwt("a", "u", "/nonexistent/key.p8").unwrap_err();
        assert!(matches!(err, ConnectionError::AuthFailed(msg) if msg.contains("cannot read")));
    }

    #[test]
    fn generate_jwt_fails_on_invalid_pem() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("bad_key.p8");
        std::fs::write(&key_path, "not a valid pem").unwrap();
        let err = generate_jwt("a", "u", key_path.to_str().unwrap()).unwrap_err();
        assert!(matches!(err, ConnectionError::AuthFailed(msg) if msg.contains("cannot parse")));
    }

    // ── load_jsonl SQL generation tests ────────────────────────────────

    #[test]
    fn build_create_staging_sql_basic() {
        let sql = build_create_staging_sql("MY_SCHEMA", "_staging_t");
        assert_eq!(
            sql,
            r#"CREATE OR REPLACE TABLE "MY_SCHEMA"."_staging_t" (data VARIANT)"#
        );
    }

    #[test]
    fn build_create_staging_sql_escapes_quotes() {
        let sql = build_create_staging_sql("s", r#"tab"le"#);
        assert!(sql.contains(r#""tab""le""#));
    }

    #[test]
    fn build_insert_json_sql_overwrite_first_batch() {
        let sql = build_insert_json_sql("S", "T", &[r#"{"a":1}"#], true);
        assert!(sql.starts_with("INSERT OVERWRITE INTO"), "sql: {sql}");
        assert!(sql.contains("SELECT PARSE_JSON(column1) FROM VALUES"));
        assert!(sql.contains(r#"('{"a":1}')"#));
    }

    #[test]
    fn build_insert_json_sql_append_subsequent_batch() {
        let sql = build_insert_json_sql("S", "T", &[r#"{"a":1}"#], false);
        assert!(sql.starts_with("INSERT INTO"), "sql: {sql}");
        assert!(!sql.contains("OVERWRITE"), "sql: {sql}");
    }

    #[test]
    fn build_insert_json_sql_multiple_rows() {
        let sql = build_insert_json_sql("S", "T", &[r#"{"a":1}"#, r#"{"b":2}"#], true);
        assert!(sql.contains("FROM VALUES"), "sql: {sql}");
        assert!(sql.contains(r#"('{"a":1}'), ('{"b":2}')"#), "sql: {sql}");
    }

    #[test]
    fn build_insert_json_sql_escapes_single_quotes() {
        let sql = build_insert_json_sql("S", "T", &[r#"{"name":"it's"}"#], true);
        assert!(sql.contains("it''s"), "sql: {sql}");
    }

    #[test]
    fn build_insert_batches_single_batch() {
        let lines: Vec<&str> = (0..3).map(|_| r#"{"a":1}"#).collect();
        let sqls = build_insert_batches("S", "T", &lines, 10);
        assert_eq!(sqls.len(), 1);
        assert!(sqls[0].starts_with("INSERT OVERWRITE INTO"));
    }

    #[test]
    fn build_insert_batches_splits_at_limit() {
        let lines: Vec<&str> = (0..5).map(|_| r#"{"a":1}"#).collect();
        let sqls = build_insert_batches("S", "T", &lines, 2);
        assert_eq!(sqls.len(), 3);
        assert!(sqls[0].starts_with("INSERT OVERWRITE INTO"));
        assert!(sqls[1].starts_with("INSERT INTO"));
        assert!(sqls[2].starts_with("INSERT INTO"));
    }

    // ── extract_scalar_from_statement ───────────────────────────────────

    // Response format: https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses
    // data is an array of arrays; all values are strings.
    // Example: { "code": "090001", "data": [["val1", "val2"]] }
    #[test]
    fn extract_scalar_returns_first_cell() {
        let resp = StatementResponse {
            data: Some(vec![vec![serde_json::json!("42")]]),
            message: None,
            code: None,
        };
        assert_eq!(
            extract_scalar_from_statement(&resp).unwrap(),
            serde_json::json!("42")
        );
    }

    #[test]
    fn extract_scalar_errors_on_empty_rows() {
        let resp = StatementResponse {
            data: Some(vec![]),
            message: None,
            code: None,
        };
        assert!(extract_scalar_from_statement(&resp).is_err());
    }

    #[test]
    fn extract_scalar_errors_on_null_data() {
        let resp = StatementResponse {
            data: None,
            message: None,
            code: None,
        };
        assert!(extract_scalar_from_statement(&resp).is_err());
    }

    // ── check_statement_success ─────────────────────────────────────────
    //
    // Success code: https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses
    // code "090001" means "successfully executed".

    #[test]
    fn check_statement_success_code_090001() {
        let resp = StatementResponse {
            data: None,
            message: Some("successfully executed".to_string()),
            code: Some("090001".to_string()),
        };
        assert!(check_statement_success(&resp).is_ok());
    }

    // Any code other than "090001" is treated as an error.
    // The code value here is arbitrary; the SQL API returns errors via HTTP 422,
    // but this function guards against unexpected success-path codes.
    #[test]
    fn check_statement_success_error_code() {
        let resp = StatementResponse {
            data: None,
            message: Some("error".to_string()),
            code: Some("000123".to_string()),
        };
        let err = check_statement_success(&resp).unwrap_err();
        assert!(matches!(err, ConnectionError::QueryFailed(msg) if msg.contains("000123")));
    }
}

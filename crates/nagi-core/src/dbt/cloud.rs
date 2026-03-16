use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbtCloudError {
    #[error("failed to read credentials file: {0}")]
    CredentialsRead(String),

    #[error("failed to parse credentials file: {0}")]
    CredentialsParse(String),

    #[error("missing required field '{0}' in credentials file")]
    MissingField(&'static str),

    /// Defense against URL path injection via attacker-controlled account-id.
    #[error("account-id must be numeric, got: {0}")]
    InvalidAccountId(String),

    /// Defense against URL injection via attacker-controlled account-host.
    #[error("account-host must be a plain hostname (letters, digits, dots, hyphens), got: {0}")]
    InvalidAccountHost(String),

    #[error("API request failed: {0}")]
    Request(String),

    #[error("unexpected API response: {0}")]
    Response(String),
}

/// Credentials parsed from `~/.dbt/dbt_cloud.yml`.
#[derive(Debug, Clone)]
pub struct DbtCloudCredentials {
    pub account_id: String,
    pub account_host: String,
}

/// Raw YAML structure of `~/.dbt/dbt_cloud.yml`.
///
/// Not derived Debug — token_value must never appear in logs or error output.
#[derive(Deserialize)]
struct RawCredentials {
    #[serde(rename = "account-id")]
    account_id: Option<String>,
    #[serde(rename = "token-value")]
    token_value: Option<String>,
    #[serde(rename = "account-host")]
    account_host: Option<String>,
}

/// Defense: account_host must be a plain FQDN (letters, digits, dots, hyphens) to prevent
/// URL injection (path segments, query strings, or userinfo that could redirect the request).
fn validate_account_host(host: &str) -> Result<(), DbtCloudError> {
    if !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
    {
        Ok(())
    } else {
        Err(DbtCloudError::InvalidAccountHost(host.to_string()))
    }
}

/// Parses the dbt Cloud credentials YAML file.
///
/// Returns credentials (without the token — acquired just-in-time per CLAUDE.md
/// security policy: never store credentials beyond the scope that needs them).
pub fn parse_credentials(path: &Path) -> Result<DbtCloudCredentials, DbtCloudError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| DbtCloudError::CredentialsRead(format!("{}: {e}", path.display())))?;
    parse_credentials_str(&content)
}

fn parse_credentials_str(content: &str) -> Result<DbtCloudCredentials, DbtCloudError> {
    let raw: RawCredentials = serde_yaml::from_str(content)
        .map_err(|e| DbtCloudError::CredentialsParse(e.to_string()))?;

    let account_id = raw
        .account_id
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("account-id"))?;
    // Defense against URL path injection: account-id must be numeric.
    if !account_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(DbtCloudError::InvalidAccountId(account_id));
    }
    let account_host = raw
        .account_host
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("account-host"))?;
    // Defense against URL injection: account-host must be a plain hostname.
    validate_account_host(&account_host)?;
    // token-value validated at use time, not stored.
    let _ = raw
        .token_value
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("token-value"))?;

    Ok(DbtCloudCredentials {
        account_id,
        account_host,
    })
}

/// Summary of a running dbt Cloud job.
#[derive(Debug, Clone)]
pub struct RunningJob {
    pub id: i64,
    pub job_name: String,
    pub status_humanized: String,
}

/// Response wrapper for dbt Cloud API `GET /runs/`.
#[derive(Debug, Deserialize)]
struct RunsResponse {
    data: Vec<RunData>,
}

#[derive(Debug, Deserialize)]
struct RunData {
    id: i64,
    #[serde(default)]
    job_id: i64,
    #[serde(default)]
    status_humanized: String,
}

/// Checks for currently running jobs in the dbt Cloud account.
///
/// Calls `GET https://{host}/api/v2/accounts/{account_id}/runs/?status=3`
/// (status=3 = Running in dbt Cloud API).
///
/// The token is read from the credentials file just before use and dropped
/// after the request completes — never stored in a struct field.
pub async fn check_running_jobs(credentials_path: &Path) -> Result<Vec<RunningJob>, DbtCloudError> {
    let content = std::fs::read_to_string(credentials_path).map_err(|e| {
        DbtCloudError::CredentialsRead(format!("{}: {e}", credentials_path.display()))
    })?;
    let raw: RawCredentials = serde_yaml::from_str(&content)
        .map_err(|e| DbtCloudError::CredentialsParse(e.to_string()))?;

    let account_id = raw
        .account_id
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("account-id"))?;
    // Defense against URL path injection: account-id must be numeric.
    if !account_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(DbtCloudError::InvalidAccountId(account_id));
    }
    let account_host = raw
        .account_host
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("account-host"))?;
    // Defense against URL injection: account-host must be a plain hostname.
    validate_account_host(&account_host)?;
    let token = raw
        .token_value
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("token-value"))?;

    let url = format!(
        "https://{}/api/v2/accounts/{}/runs/?status=3",
        account_host, account_id
    );

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Token {}", token))
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|e| DbtCloudError::Request(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(DbtCloudError::Response(format!(
            "HTTP {} from {}",
            resp.status(),
            url
        )));
    }

    let body: RunsResponse = resp
        .json()
        .await
        .map_err(|e| DbtCloudError::Response(e.to_string()))?;

    Ok(body
        .data
        .into_iter()
        .map(|r| RunningJob {
            id: r.id,
            job_name: format!("job-{}", r.job_id),
            status_humanized: r.status_humanized,
        })
        .collect())
}

/// Parses a dbt Cloud API runs response JSON into `RunningJob` entries.
#[cfg(test)]
fn parse_runs_response(json: &str) -> Result<Vec<RunningJob>, DbtCloudError> {
    let body: RunsResponse =
        serde_json::from_str(json).map_err(|e| DbtCloudError::Response(e.to_string()))?;

    Ok(body
        .data
        .into_iter()
        .map(|r| RunningJob {
            id: r.id,
            job_name: format!("job-{}", r.job_id),
            status_humanized: r.status_humanized,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_credentials_yaml() -> &'static str {
        r#"
account-id: "12345"
token-value: "dbtc_secret_token"
account-host: "cloud.getdbt.com"
"#
    }

    #[test]
    fn parse_valid_credentials() {
        let creds = parse_credentials_str(sample_credentials_yaml()).unwrap();
        assert_eq!(creds.account_id, "12345");
        assert_eq!(creds.account_host, "cloud.getdbt.com");
    }

    #[test]
    fn parse_credentials_missing_account_id() {
        let yaml = r#"
token-value: "tok"
account-host: "cloud.getdbt.com"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::MissingField("account-id")));
    }

    #[test]
    fn parse_credentials_missing_token() {
        let yaml = r#"
account-id: "123"
account-host: "cloud.getdbt.com"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::MissingField("token-value")));
    }

    #[test]
    fn parse_credentials_missing_host() {
        let yaml = r#"
account-id: "123"
token-value: "tok"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::MissingField("account-host")));
    }

    #[test]
    fn parse_credentials_empty_field_rejected() {
        let yaml = r#"
account-id: ""
token-value: "tok"
account-host: "cloud.getdbt.com"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::MissingField("account-id")));
    }

    #[test]
    fn parse_credentials_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt_cloud.yml");
        std::fs::write(&path, sample_credentials_yaml()).unwrap();

        let creds = parse_credentials(path.as_path()).unwrap();
        assert_eq!(creds.account_id, "12345");
        assert_eq!(creds.account_host, "cloud.getdbt.com");
    }

    #[test]
    fn parse_credentials_file_not_found() {
        let err = parse_credentials(Path::new("/nonexistent/dbt_cloud.yml")).unwrap_err();
        assert!(matches!(err, DbtCloudError::CredentialsRead(_)));
    }

    #[test]
    fn parse_runs_response_with_running_jobs() {
        let json = r#"{
            "data": [
                {"id": 100, "job_id": 42, "status_humanized": "Running"},
                {"id": 101, "job_id": 43, "status_humanized": "Running"}
            ]
        }"#;
        let jobs = parse_runs_response(json).unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].id, 100);
        assert_eq!(jobs[0].job_name, "job-42");
        assert_eq!(jobs[0].status_humanized, "Running");
        assert_eq!(jobs[1].id, 101);
    }

    #[test]
    fn parse_runs_response_empty() {
        let json = r#"{"data": []}"#;
        let jobs = parse_runs_response(json).unwrap();
        assert!(jobs.is_empty());
    }

    #[test]
    fn parse_runs_response_invalid_json() {
        let err = parse_runs_response("not json").unwrap_err();
        assert!(matches!(err, DbtCloudError::Response(_)));
    }

    // ── Security ──────────────────────────────────────────────────────────────

    #[test]
    fn account_id_with_path_traversal_is_rejected() {
        // Defense against URL path injection: "123/../admin" must not reach
        // a different API endpoint.
        let yaml = r#"
account-id: "123/../admin"
token-value: "tok"
account-host: "cloud.getdbt.com"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::InvalidAccountId(_)));
    }

    #[test]
    fn account_id_with_non_numeric_chars_is_rejected() {
        let yaml = r#"
account-id: "abc"
token-value: "tok"
account-host: "cloud.getdbt.com"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::InvalidAccountId(_)));
    }

    #[test]
    fn account_host_with_path_segment_is_rejected() {
        // Defense: "cloud.getdbt.com/evil" must not be used as a URL host.
        let yaml = r#"
account-id: "123"
token-value: "tok"
account-host: "cloud.getdbt.com/evil"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::InvalidAccountHost(_)));
    }

    #[test]
    fn account_host_with_query_string_is_rejected() {
        let yaml = r#"
account-id: "123"
token-value: "tok"
account-host: "cloud.getdbt.com?inject=1"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::InvalidAccountHost(_)));
    }

    #[test]
    fn account_host_with_at_sign_is_rejected() {
        // "user@host" pattern can redirect HTTP clients to a different host.
        let yaml = r#"
account-id: "123"
token-value: "tok"
account-host: "evil.com@cloud.getdbt.com"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::InvalidAccountHost(_)));
    }
}

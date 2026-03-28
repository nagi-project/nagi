use std::collections::HashSet;
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
#[cfg(test)]
pub fn parse_credentials(path: &Path) -> Result<DbtCloudCredentials, DbtCloudError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| DbtCloudError::CredentialsRead(format!("{}: {e}", path.display())))?;
    parse_credentials_str(&content)
}

#[cfg(test)]
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

/// Reads token from credentials file just-in-time for an API call.
fn read_token(credentials_path: &Path) -> Result<(DbtCloudCredentials, String), DbtCloudError> {
    let content = std::fs::read_to_string(credentials_path).map_err(|e| {
        DbtCloudError::CredentialsRead(format!("{}: {e}", credentials_path.display()))
    })?;
    let raw: RawCredentials = serde_yaml::from_str(&content)
        .map_err(|e| DbtCloudError::CredentialsParse(e.to_string()))?;

    let account_id = raw
        .account_id
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("account-id"))?;
    if !account_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(DbtCloudError::InvalidAccountId(account_id));
    }
    let account_host = raw
        .account_host
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("account-host"))?;
    validate_account_host(&account_host)?;
    let token = raw
        .token_value
        .filter(|s| !s.is_empty())
        .ok_or(DbtCloudError::MissingField("token-value"))?;

    Ok((
        DbtCloudCredentials {
            account_id,
            account_host,
        },
        token,
    ))
}

// ── Jobs API ──────────────────────────────────────────────────────────────

/// A dbt Cloud job definition with its execute_steps.
#[cfg(test)]
#[derive(Debug, Deserialize)]
struct JobData {
    id: i64,
    #[serde(default)]
    execute_steps: Vec<String>,
}

#[cfg(test)]
#[derive(Debug, Deserialize)]
struct JobsResponse {
    data: Vec<JobData>,
}

/// Extracts model names from a dbt command's `--select` / `-s` argument.
/// Uses `select::parse_selector` to parse each selector token, extracting
/// only model names (skipping tag selectors etc.).
///
/// Examples:
///   "dbt run --select daily_sales" → ["daily_sales"]
///   "dbt run --select +daily_sales" → ["daily_sales"]
///   "dbt test --select tag:finance" → [] (tag selectors are not model names)
///   "dbt run" → [] (no --select means all models)
#[cfg(test)]
pub fn extract_model_names_from_command(command: &str) -> Vec<String> {
    let parts: Vec<&str> = command.split_whitespace().collect();
    let mut models = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        if (parts[i] == "--select" || parts[i] == "-s") && i + 1 < parts.len() {
            if let Some(name) = crate::interface::select::extract_model_name(parts[i + 1]) {
                models.push(name);
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    models
}

/// Builds a mapping of model name → set of job IDs from job definitions.
#[cfg(test)]
fn build_model_job_mapping(jobs: &[JobData]) -> std::collections::HashMap<String, HashSet<i64>> {
    let mut mapping: std::collections::HashMap<String, HashSet<i64>> =
        std::collections::HashMap::new();
    for job in jobs {
        for step in &job.execute_steps {
            for model in extract_model_names_from_command(step) {
                mapping.entry(model).or_default().insert(job.id);
            }
        }
    }
    mapping
}

// ── Runs API ──────────────────────────────────────────────────────────────

/// Summary of a running dbt Cloud job.
#[derive(Debug, Clone)]
pub struct RunningJob {
    pub _id: i64,
    pub job_id: i64,
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

/// Checks for running jobs that affect the given asset.
///
/// `relevant_job_ids` is the set of dbt Cloud job IDs that include this asset
/// (resolved at compile time via `fetch_job_model_mapping`).
///
/// Returns only running jobs whose job_id is in `relevant_job_ids`.
pub async fn check_running_jobs_for_asset(
    credentials_path: &Path,
    relevant_job_ids: &HashSet<i64>,
) -> Result<Vec<RunningJob>, DbtCloudError> {
    if relevant_job_ids.is_empty() {
        return Ok(vec![]);
    }

    let (creds, token) = read_token(credentials_path)?;

    let url = format!(
        "https://{}/api/v2/accounts/{}/runs/?status=3",
        creds.account_host, creds.account_id
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

    Ok(filter_runs_by_job_ids(&body.data, relevant_job_ids))
}

fn filter_runs_by_job_ids(runs: &[RunData], job_ids: &HashSet<i64>) -> Vec<RunningJob> {
    runs.iter()
        .filter(|r| job_ids.contains(&r.job_id))
        .map(|r| RunningJob {
            _id: r.id,
            job_id: r.job_id,
            status_humanized: r.status_humanized.clone(),
        })
        .collect()
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

    // ── extract_model_names_from_command ─────────────────────────────────

    macro_rules! extract_models_test {
        ($($name:ident: $input:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let result = extract_model_names_from_command($input);
                    assert_eq!(result, $expected, "input: {}", $input);
                }
            )*
        };
    }

    extract_models_test! {
        extract_simple_select:
            "dbt run --select daily_sales" => vec!["daily_sales"];
        extract_with_upstream_marker:
            "dbt run --select +daily_sales" => vec!["daily_sales"];
        extract_with_downstream_marker:
            "dbt run --select daily_sales+" => vec!["daily_sales"];
        extract_with_both_markers:
            "dbt run --select +daily_sales+" => vec!["daily_sales"];
        extract_short_flag:
            "dbt run -s daily_sales" => vec!["daily_sales"];
        extract_tag_selector_skipped:
            "dbt run --select tag:finance" => Vec::<String>::new();
        extract_no_select:
            "dbt run" => Vec::<String>::new();
        extract_test_command:
            "dbt test --select daily_sales" => vec!["daily_sales"];
    }

    // ── build_model_job_mapping ──────────────────────────────────────────

    #[test]
    fn model_job_mapping_basic() {
        let jobs = vec![
            JobData {
                id: 1,
                execute_steps: vec!["dbt run --select daily_sales".to_string()],
            },
            JobData {
                id: 2,
                execute_steps: vec!["dbt run --select +daily_sales".to_string()],
            },
            JobData {
                id: 3,
                execute_steps: vec!["dbt run --select customers".to_string()],
            },
        ];
        let mapping = build_model_job_mapping(&jobs);
        assert_eq!(mapping.get("daily_sales").unwrap(), &HashSet::from([1, 2]));
        assert_eq!(mapping.get("customers").unwrap(), &HashSet::from([3]));
    }

    #[test]
    fn model_job_mapping_no_select_produces_empty() {
        let jobs = vec![JobData {
            id: 1,
            execute_steps: vec!["dbt run".to_string()],
        }];
        let mapping = build_model_job_mapping(&jobs);
        assert!(mapping.is_empty());
    }

    #[test]
    fn model_job_mapping_tag_only_produces_empty() {
        let jobs = vec![JobData {
            id: 1,
            execute_steps: vec!["dbt run --select tag:finance".to_string()],
        }];
        let mapping = build_model_job_mapping(&jobs);
        assert!(mapping.is_empty());
    }

    // ── filter_runs_by_job_ids ───────────────────────────────────────────

    #[test]
    fn filter_runs_returns_matching_jobs() {
        let runs = vec![
            RunData {
                id: 100,
                job_id: 1,
                status_humanized: "Running".to_string(),
            },
            RunData {
                id: 101,
                job_id: 2,
                status_humanized: "Running".to_string(),
            },
            RunData {
                id: 102,
                job_id: 3,
                status_humanized: "Running".to_string(),
            },
        ];
        let relevant = HashSet::from([1, 3]);
        let result = filter_runs_by_job_ids(&runs, &relevant);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].job_id, 1);
        assert_eq!(result[1].job_id, 3);
    }

    #[test]
    fn filter_runs_empty_when_no_match() {
        let runs = vec![RunData {
            id: 100,
            job_id: 1,
            status_humanized: "Running".to_string(),
        }];
        let relevant = HashSet::from([99]);
        let result = filter_runs_by_job_ids(&runs, &relevant);
        assert!(result.is_empty());
    }

    #[test]
    fn filter_runs_empty_relevant_returns_empty() {
        let runs = vec![RunData {
            id: 100,
            job_id: 1,
            status_humanized: "Running".to_string(),
        }];
        let result = filter_runs_by_job_ids(&runs, &HashSet::new());
        assert!(result.is_empty());
    }

    // ── Security ──────────────────────────────────────────────────────────────

    #[test]
    fn account_id_with_path_traversal_is_rejected() {
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
        let yaml = r#"
account-id: "123"
token-value: "tok"
account-host: "evil.com@cloud.getdbt.com"
"#;
        let err = parse_credentials_str(yaml).unwrap_err();
        assert!(matches!(err, DbtCloudError::InvalidAccountHost(_)));
    }
}

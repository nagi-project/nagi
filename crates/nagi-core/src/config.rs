use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::Deserialize;

use crate::duration::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

#[derive(Debug, Clone, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NagiConfig {
    /// State storage backend configuration.
    #[serde(default)]
    pub backend: BackendConfig,
    /// Notification channel configuration.
    #[serde(default)]
    pub notify: NotifyConfig,
    /// Maximum time in seconds to wait for in-flight sync tasks to finish during shutdown.
    /// When omitted, waits indefinitely.
    pub termination_grace_period_seconds: Option<u64>,
    /// Maximum number of Controllers to run in parallel during `nagi serve`.
    /// When the number of connected components exceeds this limit, serve exits with an error.
    /// When omitted, one Controller is created per connected component.
    pub max_controllers: Option<usize>,
    /// Time-to-live in seconds for sync lock files. Locks expire after this duration,
    /// preventing deadlocks from abnormal process termination. Defaults to 3600 (1 hour).
    #[serde(default = "default_lock_ttl_seconds")]
    pub lock_ttl_seconds: u64,
    /// Interval in seconds between lock acquisition retry attempts. Defaults to 900 (15 minutes).
    #[serde(default = "default_lock_retry_interval_seconds")]
    pub lock_retry_interval_seconds: u64,
    /// Maximum number of lock acquisition retry attempts before skipping. Defaults to 3.
    #[serde(default = "default_lock_retry_max_attempts")]
    pub lock_retry_max_attempts: u32,
    /// Maximum number of concurrent evaluate tasks per Controller.
    /// When omitted, no limit is applied.
    pub max_evaluate_concurrency: Option<usize>,
    /// Maximum number of concurrent sync tasks per Controller.
    /// When omitted, no limit is applied.
    pub max_sync_concurrency: Option<usize>,
    /// Base directory for Nagi state (logs, cache, locks, etc.). Defaults to `~/.nagi`.
    #[serde(default)]
    #[schemars(default = "schema_default_nagi_dir")]
    pub nagi_dir: NagiDir,
    /// Log export configuration. When set, compile generates export Assets
    /// and logs are transferred to the remote DWH.
    pub export: Option<ExportConfig>,
}

impl Default for NagiConfig {
    fn default() -> Self {
        Self {
            backend: BackendConfig::default(),
            notify: NotifyConfig::default(),
            termination_grace_period_seconds: None,
            max_controllers: None,
            lock_ttl_seconds: default_lock_ttl_seconds(),
            lock_retry_interval_seconds: default_lock_retry_interval_seconds(),
            lock_retry_max_attempts: default_lock_retry_max_attempts(),
            max_evaluate_concurrency: None,
            max_sync_concurrency: None,
            nagi_dir: NagiDir::default(),
            export: None,
        }
    }
}

fn default_lock_ttl_seconds() -> u64 {
    3600
}

fn default_lock_retry_interval_seconds() -> u64 {
    900
}

fn default_lock_retry_max_attempts() -> u32 {
    3
}

/// Newtype wrapper around the Nagi state directory path.
/// Provides accessors for all well-known subdirectories.
///
/// Deserializes from a plain string path (e.g. `"~/.nagi"` in YAML).
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct NagiDir(PathBuf);

impl NagiDir {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn root(&self) -> &Path {
        &self.0
    }

    pub fn db_path(&self) -> PathBuf {
        self.0.join("logs.db")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.0.join("logs")
    }

    pub fn cache_dir(&self) -> PathBuf {
        self.0.join("cache")
    }

    pub fn evaluate_cache_dir(&self) -> PathBuf {
        self.0.join("cache").join("evaluate")
    }

    pub fn locks_dir(&self) -> PathBuf {
        self.0.join("locks")
    }

    pub fn suspended_dir(&self) -> PathBuf {
        self.0.join("suspended")
    }

    pub fn watermarks_dir(&self) -> PathBuf {
        self.0.join("watermarks")
    }

    pub fn readiness_dir(&self) -> PathBuf {
        self.0.join("readiness")
    }
}

impl Default for NagiDir {
    fn default() -> Self {
        Self(dirs::home_dir().unwrap_or_default().join(".nagi"))
    }
}

impl AsRef<Path> for NagiDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

pub fn default_nagi_dir() -> NagiDir {
    NagiDir::default()
}

fn schema_default_nagi_dir() -> NagiDir {
    NagiDir::new(PathBuf::from("~/.nagi"))
}

/// Loads config from `project_dir` and returns the resolved `NagiDir`.
/// Falls back to the default if the config file is missing or unreadable.
pub fn resolve_nagi_dir(project_dir: &Path) -> NagiDir {
    load_config(project_dir)
        .map(|c| c.nagi_dir)
        .unwrap_or_default()
}

/// Export format for log data.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Jsonl,
    #[serde(alias = "duckdb")]
    DuckDb,
}

/// Configuration for exporting logs to a remote DWH.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct ExportConfig {
    /// Reference to a `kind: Connection` resource name.
    pub connection: String,
    /// DWH dataset (BigQuery) or schema (Snowflake) to export into.
    pub dataset: String,
    /// Intermediate file format for export.
    #[serde(default = "default_export_format")]
    pub format: ExportFormat,
    /// Condition evaluation interval and export throttling threshold.
    #[serde(default = "default_export_interval")]
    pub interval: Duration,
}

fn default_export_format() -> ExportFormat {
    ExportFormat::Jsonl
}

fn default_export_interval() -> Duration {
    serde_yaml::from_str("30m").expect("default export interval must parse")
}

fn default_backend_type() -> String {
    "local".to_string()
}

#[derive(Debug, Clone, Deserialize, PartialEq, JsonSchema)]
pub struct BackendConfig {
    /// Backend type identifier. One of `local`, `gcs`, `s3`. Defaults to `local`.
    #[serde(default = "default_backend_type")]
    pub r#type: String,
    /// Path prefix for remote storage (e.g. `my-project/nagi`). When set, all
    /// remote paths are prefixed with this value. Ignored for the local backend.
    pub prefix: Option<String>,
    /// Bucket name for GCS or S3 backend. Required when type is `gcs` or `s3`.
    pub bucket: Option<String>,
    /// AWS region for S3 backend (e.g. `us-east-1`). Required when type is `s3`.
    pub region: Option<String>,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            r#type: default_backend_type(),
            prefix: None,
            bucket: None,
            region: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, JsonSchema)]
pub struct NotifyConfig {
    /// Slack notification settings. When set, notifications are sent to the specified channel.
    pub slack: Option<SlackConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, JsonSchema)]
pub struct SlackConfig {
    /// Slack channel to send notifications to (e.g. `#nagi-alerts`).
    pub channel: String,
}

const CONFIG_FILE: &str = "nagi.yaml";

/// Loads `nagi.yaml` from the given project directory.
/// Returns `NagiConfig::default()` if the file does not exist.
pub fn load_config(project_dir: &Path) -> Result<NagiConfig, ConfigError> {
    let path = project_dir.join(CONFIG_FILE);
    match std::fs::read_to_string(&path) {
        Ok(contents) => Ok(serde_yaml::from_str(&contents)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(NagiConfig::default()),
        Err(e) => Err(ConfigError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config, NagiConfig::default());
        assert_eq!(config.backend.r#type, "local");
        assert!(config.notify.slack.is_none());
    }

    #[test]
    fn load_empty_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "").unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config, NagiConfig::default());
        assert_eq!(config.backend.r#type, "local");
        assert!(config.backend.bucket.is_none());
        assert!(config.backend.region.is_none());
        assert!(config.backend.prefix.is_none());
        assert!(config.notify.slack.is_none());
        assert!(config.termination_grace_period_seconds.is_none());
        assert!(config.max_controllers.is_none());
        assert_eq!(config.lock_ttl_seconds, 3600);
        assert_eq!(config.lock_retry_interval_seconds, 900);
        assert_eq!(config.lock_retry_max_attempts, 3);
        assert!(config.max_evaluate_concurrency.is_none());
        assert!(config.max_sync_concurrency.is_none());
        assert!(config.nagi_dir.root().ends_with(".nagi"));
        assert!(config.export.is_none());
    }

    #[test]
    fn load_full_config() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = r##"
backend:
  type: gcs
  bucket: my-nagi-state

notify:
  slack:
    channel: "#nagi-alerts"
"##;
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.backend.r#type, "gcs");
        assert_eq!(config.backend.bucket.as_deref(), Some("my-nagi-state"));
        let slack = config.notify.slack.unwrap();
        assert_eq!(slack.channel, "#nagi-alerts");
    }

    #[test]
    fn load_gcs_backend() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: gcs\n  bucket: my-bucket\n  prefix: proj/nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.backend.r#type, "gcs");
        assert_eq!(config.backend.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(config.backend.prefix.as_deref(), Some("proj/nagi"));
        assert!(config.backend.region.is_none());
    }

    #[test]
    fn load_s3_backend() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: s3\n  bucket: my-bucket\n  region: us-east-1\n  prefix: nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.backend.r#type, "s3");
        assert_eq!(config.backend.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(config.backend.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.backend.prefix.as_deref(), Some("nagi"));
    }

    #[test]
    fn load_notify_only() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = r##"
notify:
  slack:
    channel: "#alerts"
"##;
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.backend.r#type, "local");
        assert!(config.notify.slack.is_some());
    }

    #[test]
    fn load_termination_grace_period() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "terminationGracePeriodSeconds: 300";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.termination_grace_period_seconds, Some(300));
    }

    #[test]
    fn load_max_controllers() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "maxControllers: 4";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.max_controllers, Some(4));
    }

    #[test]
    fn load_backend_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: gcs\n  prefix: my-project/nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.backend.prefix.as_deref(), Some("my-project/nagi"));
    }

    #[test]
    fn load_custom_lock_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "lockTtlSeconds: 120";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.lock_ttl_seconds, 120);
    }

    #[test]
    fn load_custom_lock_retry() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "lockRetryIntervalSeconds: 60\nlockRetryMaxAttempts: 5";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.lock_retry_interval_seconds, 60);
        assert_eq!(config.lock_retry_max_attempts, 5);
    }

    #[test]
    fn load_invalid_yaml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "{{invalid").unwrap();
        assert!(load_config(dir.path()).is_err());
    }

    #[test]
    fn load_custom_nagi_dir() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "nagiDir: /tmp/my-nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.nagi_dir, NagiDir::new(PathBuf::from("/tmp/my-nagi")));
    }

    #[test]
    fn load_export_config() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "\
export:
  connection: my-bigquery
  dataset: nagi_logs
  format: jsonl
  interval: 30m";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        let export = config.export.unwrap();
        assert_eq!(export.connection, "my-bigquery");
        assert_eq!(export.dataset, "nagi_logs");
        assert_eq!(export.format, ExportFormat::Jsonl);
        assert_eq!(
            export.interval.as_std(),
            std::time::Duration::from_secs(30 * 60)
        );
    }

    #[test]
    fn load_export_config_duckdb_format() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "\
export:
  connection: my-bq
  dataset: logs
  format: duckdb";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        let export = config.export.unwrap();
        assert_eq!(export.format, ExportFormat::DuckDb);
    }

    #[test]
    fn load_export_config_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "\
export:
  connection: my-bq
  dataset: logs";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        let export = config.export.unwrap();
        assert_eq!(export.format, ExportFormat::Jsonl);
        assert_eq!(
            export.interval.as_std(),
            std::time::Duration::from_secs(30 * 60)
        );
    }

    #[test]
    fn load_concurrency_limits() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "maxEvaluateConcurrency: 5\nmaxSyncConcurrency: 2";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.max_evaluate_concurrency, Some(5));
        assert_eq!(config.max_sync_concurrency, Some(2));
    }

    // ── NagiDir ──────────────────────────────────────────────────────────

    macro_rules! nagi_dir_path_test {
        ($($name:ident: $method:ident => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let nd = NagiDir::new(PathBuf::from("/state"));
                    assert_eq!(nd.$method(), PathBuf::from($expected));
                }
            )*
        };
    }

    nagi_dir_path_test! {
        nagi_dir_db_path:        db_path        => "/state/logs.db";
        nagi_dir_logs_dir:       logs_dir       => "/state/logs";
        nagi_dir_cache_dir:      cache_dir      => "/state/cache";
        nagi_dir_locks_dir:      locks_dir      => "/state/locks";
        nagi_dir_suspended_dir:  suspended_dir  => "/state/suspended";
        nagi_dir_watermarks:     watermarks_dir => "/state/watermarks";
    }

    #[test]
    fn nagi_dir_root() {
        let nd = NagiDir::new(PathBuf::from("/state"));
        assert_eq!(nd.root(), Path::new("/state"));
    }

    #[test]
    fn nagi_dir_default_ends_with_dot_nagi() {
        let nd = NagiDir::default();
        assert!(nd.root().ends_with(".nagi"));
    }

    #[test]
    fn nagi_dir_as_ref_returns_root() {
        let nd = NagiDir::new(PathBuf::from("/state"));
        let p: &Path = nd.as_ref();
        assert_eq!(p, Path::new("/state"));
    }

    #[test]
    fn nagi_dir_deserializes_from_string() {
        let nd: NagiDir = serde_yaml::from_str("/custom/path").unwrap();
        assert_eq!(nd, NagiDir::new(PathBuf::from("/custom/path")));
    }
}

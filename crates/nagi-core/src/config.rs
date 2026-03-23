use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::Deserialize;

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
    /// Interval in seconds between lock acquisition retry attempts. Defaults to 10.
    #[serde(default = "default_lock_retry_interval_seconds")]
    pub lock_retry_interval_seconds: u64,
    /// Maximum number of lock acquisition retry attempts before skipping. Defaults to 30.
    #[serde(default = "default_lock_retry_max_attempts")]
    pub lock_retry_max_attempts: u32,
    /// Base directory for Nagi state (logs, cache, locks, etc.). Defaults to `~/.nagi`.
    #[serde(default = "default_nagi_dir")]
    pub nagi_dir: PathBuf,
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
            nagi_dir: default_nagi_dir(),
        }
    }
}

impl NagiConfig {
    pub fn db_path(&self) -> PathBuf {
        self.nagi_dir.join("logs.db")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.nagi_dir.join("logs")
    }

    pub fn cache_dir(&self) -> PathBuf {
        self.nagi_dir.join("cache")
    }

    pub fn locks_dir(&self) -> PathBuf {
        self.nagi_dir.join("locks")
    }

    pub fn suspended_dir(&self) -> PathBuf {
        self.nagi_dir.join("suspended")
    }

    pub fn source_stats_dir(&self) -> PathBuf {
        self.nagi_dir.join("source_stats")
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

pub fn default_nagi_dir() -> PathBuf {
    dirs::home_dir().unwrap_or_default().join(".nagi")
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
    fn default_backend_bucket_and_region_are_none() {
        let config = NagiConfig::default();
        assert!(config.backend.bucket.is_none());
        assert!(config.backend.region.is_none());
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
    fn default_termination_grace_period_is_none() {
        let config = NagiConfig::default();
        assert!(config.termination_grace_period_seconds.is_none());
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
    fn load_without_termination_grace_period() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: local";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert!(config.termination_grace_period_seconds.is_none());
    }

    #[test]
    fn default_max_controllers_is_none() {
        let config = NagiConfig::default();
        assert!(config.max_controllers.is_none());
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
    fn default_backend_prefix_is_none() {
        let config = NagiConfig::default();
        assert!(config.backend.prefix.is_none());
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
    fn load_backend_without_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: gcs";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.backend.r#type, "gcs");
        assert!(config.backend.prefix.is_none());
    }

    #[test]
    fn default_lock_ttl_is_3600() {
        let config = NagiConfig::default();
        assert_eq!(config.lock_ttl_seconds, 3600);
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
    fn load_without_lock_ttl_uses_default() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: local";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.lock_ttl_seconds, 3600);
    }

    #[test]
    fn default_lock_retry_values() {
        let config = NagiConfig::default();
        assert_eq!(config.lock_retry_interval_seconds, 900);
        assert_eq!(config.lock_retry_max_attempts, 3);
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
    fn default_nagi_dir_is_dot_nagi() {
        let config = NagiConfig::default();
        assert!(config.nagi_dir.ends_with(".nagi"));
    }

    #[test]
    fn load_custom_nagi_dir() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "nagiDir: /tmp/my-nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.nagi_dir, PathBuf::from("/tmp/my-nagi"));
    }

    #[test]
    fn load_without_nagi_dir_uses_default() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: local";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_config(dir.path()).unwrap();
        assert_eq!(config.nagi_dir, default_nagi_dir());
    }

    #[test]
    fn config_derived_paths() {
        let config = NagiConfig {
            nagi_dir: PathBuf::from("/data/nagi"),
            ..NagiConfig::default()
        };
        assert_eq!(config.db_path(), PathBuf::from("/data/nagi/logs.db"));
        assert_eq!(config.logs_dir(), PathBuf::from("/data/nagi/logs"));
        assert_eq!(config.cache_dir(), PathBuf::from("/data/nagi/cache"));
        assert_eq!(config.locks_dir(), PathBuf::from("/data/nagi/locks"));
        assert_eq!(
            config.suspended_dir(),
            PathBuf::from("/data/nagi/suspended")
        );
        assert_eq!(
            config.source_stats_dir(),
            PathBuf::from("/data/nagi/source_stats")
        );
    }
}

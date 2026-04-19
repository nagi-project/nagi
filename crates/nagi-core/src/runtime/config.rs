use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime::duration::Duration;
use crate::runtime::storage::ProjectConfigStore;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("nagi.yaml has changed since last `nagi init`. Run `nagi init` to apply changes.")]
    LocalConfigChanged,
    #[error("remote project config not found. Run `nagi init` to upload configuration.")]
    RemoteConfigNotFound,
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NagiConfig {
    /// State storage backend configuration.
    #[serde(default)]
    pub backend: BackendConfig,
    /// Project configuration (everything except backend).
    #[serde(flatten)]
    pub project: ProjectConfig,
}

impl NagiConfig {
    /// Assembles a full config from local backend settings and project config.
    pub(crate) fn from_parts(backend: BackendConfig, project: ProjectConfig) -> Self {
        Self { backend, project }
    }
}

fn default_timeout() -> Duration {
    Duration::from_secs(3600)
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

/// State directory path. Contains logs, cache, locks, and other runtime data.
/// Provides accessors for all well-known subdirectories.
///
/// Deserializes from a plain string path (e.g. `"~/.nagi"` in YAML).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct StateDir(PathBuf);

impl StateDir {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn root(&self) -> &Path {
        &self.0
    }

    pub(crate) fn log_store_path(&self) -> PathBuf {
        self.0.join("logs.db")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.0.join("logs")
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

impl Default for StateDir {
    fn default() -> Self {
        Self(dirs::home_dir().unwrap_or_default().join(".nagi"))
    }
}

impl AsRef<Path> for StateDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

pub fn default_state_dir() -> StateDir {
    StateDir::default()
}

fn schema_default_state_dir() -> StateDir {
    StateDir::new(PathBuf::from("~/.nagi"))
}

/// Returns the default timeout from `nagi.yaml` in the current directory.
/// Falls back to `NagiConfig::default()` if the config file is missing or unreadable.
pub fn resolve_default_timeout() -> std::time::Duration {
    load_local_config(Path::new("."))
        .unwrap_or_default()
        .project
        .default_timeout
        .as_std()
}

/// Loads config from `project_dir` and returns the resolved `StateDir`.
/// Falls back to the default if the config file is missing or unreadable.
pub fn resolve_state_dir(project_dir: &Path) -> StateDir {
    load_local_config(project_dir)
        .map(|c| c.project.state_dir.clone())
        .unwrap_or_default()
}

/// Export format for log data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Jsonl,
    #[serde(alias = "duckdb")]
    DuckDb,
}

/// Configuration for exporting logs to a remote data warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ExportConfig {
    /// Reference to a `kind: Connection` resource name.
    pub connection: String,
    /// Data warehouse dataset (BigQuery) or schema (Snowflake) to export into.
    pub dataset: String,
    /// Intermediate file format for export.
    #[serde(default = "default_export_format")]
    pub format: ExportFormat,
    /// Condition evaluation interval and export throttling threshold.
    #[serde(default = "default_export_interval")]
    pub interval: Duration,
    /// Timeout for export operations. Falls back to `defaultTimeout` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Duration>,
}

fn default_export_format() -> ExportFormat {
    ExportFormat::Jsonl
}

fn default_export_interval() -> Duration {
    serde_yaml::from_str("30m").expect("default export interval must parse")
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum BackendType {
    #[default]
    Local,
    Gcs,
    S3,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, JsonSchema)]
pub struct BackendConfig {
    /// Backend type. Defaults to `local`.
    #[serde(default, rename = "type")]
    pub backend_type: BackendType,
    /// Path prefix for remote storage (e.g. `my-project/nagi`). When set, all
    /// remote paths are prefixed with this value. Ignored for the local backend.
    pub prefix: Option<String>,
    /// Bucket name for GCS or S3 backend. Required when type is `gcs` or `s3`.
    pub bucket: Option<String>,
    /// AWS region for S3 backend (e.g. `us-east-1`). Required when type is `s3`.
    pub region: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct NotifyConfig {
    /// Slack notification settings. When set, notifications are sent to the specified channel.
    pub slack: Option<SlackConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct SlackConfig {
    /// Slack channel to send notifications to (e.g. `#nagi-alerts`).
    pub channel: String,
}

/// Project configuration that is stored in the remote backend.
/// Contains all settings except `backend` (which is local-only).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectConfig {
    #[serde(default)]
    pub notify: NotifyConfig,
    #[serde(default)]
    pub termination_grace_period_seconds: Option<u64>,
    #[serde(default)]
    pub max_controllers: Option<usize>,
    #[serde(default = "default_lock_ttl_seconds")]
    pub lock_ttl_seconds: u64,
    #[serde(default = "default_lock_retry_interval_seconds")]
    pub lock_retry_interval_seconds: u64,
    #[serde(default = "default_lock_retry_max_attempts")]
    pub lock_retry_max_attempts: u32,
    #[serde(default)]
    pub max_evaluate_concurrency: Option<usize>,
    #[serde(default)]
    pub max_sync_concurrency: Option<usize>,
    #[serde(default)]
    #[schemars(default = "schema_default_state_dir")]
    pub state_dir: StateDir,
    #[serde(default)]
    pub export: Option<ExportConfig>,
    #[serde(default = "default_timeout")]
    pub default_timeout: Duration,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            notify: NotifyConfig::default(),
            termination_grace_period_seconds: None,
            max_controllers: None,
            lock_ttl_seconds: default_lock_ttl_seconds(),
            lock_retry_interval_seconds: default_lock_retry_interval_seconds(),
            lock_retry_max_attempts: default_lock_retry_max_attempts(),
            max_evaluate_concurrency: None,
            max_sync_concurrency: None,
            state_dir: StateDir::default(),
            export: None,
            default_timeout: default_timeout(),
        }
    }
}

const CONFIG_FILE: &str = "nagi.yaml";
const CONFIG_HASH_DIR: &str = ".nagi";
const CONFIG_HASH_FILE: &str = "config_hash";

/// Computes a SHA-256 hash of the given bytes, returned as a hex string.
fn compute_config_hash(content: &[u8]) -> String {
    let hash = Sha256::digest(content);
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

/// Reads `nagi.yaml` from the given project directory and returns its raw content.
/// Returns `None` if the file does not exist.
fn read_config_file(project_dir: &Path) -> Result<Option<String>, ConfigError> {
    let path = project_dir.join(CONFIG_FILE);
    match std::fs::read_to_string(&path) {
        Ok(contents) => Ok(Some(contents)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ConfigError::Io(e)),
    }
}

/// Saves the hash of the current `nagi.yaml` to `<project_dir>/.nagi/config_hash`.
fn save_config_hash(project_dir: &Path) -> Result<(), ConfigError> {
    let content = read_config_file(project_dir)?.unwrap_or_default();
    let hash = compute_config_hash(content.as_bytes());
    let hash_dir = project_dir.join(CONFIG_HASH_DIR);
    std::fs::create_dir_all(&hash_dir)?;
    std::fs::write(hash_dir.join(CONFIG_HASH_FILE), hash)?;
    Ok(())
}

/// Checks whether the current `nagi.yaml` matches the hash saved by the last `nagi init`.
/// Returns `Ok(true)` if they match, `Ok(false)` if they differ.
/// Returns `Ok(true)` if no hash file exists (first run, init not yet executed).
fn check_config_hash(project_dir: &Path) -> Result<bool, ConfigError> {
    let hash_path = project_dir.join(CONFIG_HASH_DIR).join(CONFIG_HASH_FILE);
    let saved_hash = match std::fs::read_to_string(&hash_path) {
        Ok(h) => h,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(e) => return Err(ConfigError::Io(e)),
    };
    let content = read_config_file(project_dir)?.unwrap_or_default();
    let current_hash = compute_config_hash(content.as_bytes());
    Ok(current_hash == saved_hash)
}

/// Loads `nagi.yaml` from the local file only.
/// Returns `NagiConfig::default()` if the file does not exist.
/// Does not check config hash or load remote config.
pub(crate) fn load_local_config(project_dir: &Path) -> Result<NagiConfig, ConfigError> {
    let path = project_dir.join(CONFIG_FILE);
    match std::fs::read_to_string(&path) {
        Ok(contents) => Ok(serde_yaml::from_str(&contents)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(NagiConfig::default()),
        Err(e) => Err(ConfigError::Io(e)),
    }
}

/// Result of `init_config` indicating what happened with the remote config.
#[derive(Debug, PartialEq)]
pub(crate) enum InitConfigResult {
    /// Local backend; no remote upload needed.
    Local,
    /// Remote config uploaded successfully.
    Uploaded,
    /// Remote config already exists; skipped upload. Use `--force` to overwrite.
    Skipped,
}

/// Initializes the config: for remote backends, uploads project config to the
/// remote store unless it already exists. Always saves the config hash locally.
///
/// `store` is the remote project config store. Pass `None` for local backends.
/// When `force` is true, overwrites existing remote config.
/// When `force` is false and remote config already exists, skips upload.
pub(crate) fn init_config(
    project_dir: &Path,
    store: Option<&dyn ProjectConfigStore>,
    force: bool,
) -> Result<InitConfigResult, ConfigError> {
    let config = load_local_config(project_dir)?;

    let result = match store {
        Some(s) => {
            let existing = s
                .read_project_config()
                .map_err(|e| ConfigError::Storage(e.to_string()))?;

            if existing.is_some() && !force {
                InitConfigResult::Skipped
            } else {
                s.write_project_config(&config.project)
                    .map_err(|e| ConfigError::Storage(e.to_string()))?;
                InitConfigResult::Uploaded
            }
        }
        None => InitConfigResult::Local,
    };

    save_config_hash(project_dir)?;
    Ok(result)
}

/// Loads the project config. Checks config hash, and for remote backends,
/// fetches project config from the remote store and merges with local backend.
///
/// `store` is the remote project config store. Pass `None` for local backends.
/// For local backends, reads the local nagi.yaml directly.
/// For remote backends, reads project config from the store and assembles
/// with the local backend config.
///
/// Returns `ConfigError::LocalConfigChanged` if the local nagi.yaml has changed
/// since the last `nagi init`.
pub fn load_config(
    project_dir: &Path,
    store: Option<&dyn ProjectConfigStore>,
) -> Result<NagiConfig, ConfigError> {
    if !check_config_hash(project_dir)? {
        return Err(ConfigError::LocalConfigChanged);
    }

    let local_config = load_local_config(project_dir)?;

    match store {
        Some(s) => {
            let project = s
                .read_project_config()
                .map_err(|e| ConfigError::Storage(e.to_string()))?
                .ok_or(ConfigError::RemoteConfigNotFound)?;
            Ok(NagiConfig::from_parts(local_config.backend, project))
        }
        None => Ok(local_config),
    }
}

/// Builds a `ProjectConfigStore` from the backend config if the backend is remote.
/// Returns `None` for local backends.
pub(crate) fn build_project_config_store(
    backend: &BackendConfig,
) -> Result<Option<crate::runtime::storage::remote::RemoteObjectStore>, ConfigError> {
    match backend.backend_type {
        BackendType::Local => Ok(None),
        BackendType::Gcs | BackendType::S3 => {
            let store = crate::runtime::storage::remote::create_remote_store(backend)
                .map_err(|e| ConfigError::Storage(e.to_string()))?;
            Ok(Some(store))
        }
    }
}

/// Convenience wrapper: loads the local config, builds the store, and calls `load_config`.
pub fn load_config_from_dir(project_dir: &Path) -> Result<NagiConfig, ConfigError> {
    let local = load_local_config(project_dir)?;
    let store = build_project_config_store(&local.backend)?;
    let store_ref = store.as_ref().map(|s| s as &dyn ProjectConfigStore);
    load_config(project_dir, store_ref)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_empty_or_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "").unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config, NagiConfig::default());
        assert_eq!(config.backend.backend_type, BackendType::Local);
        assert!(config.backend.bucket.is_none());
        assert!(config.backend.region.is_none());
        assert!(config.backend.prefix.is_none());
        assert!(config.project.notify.slack.is_none());
        assert!(config.project.termination_grace_period_seconds.is_none());
        assert!(config.project.max_controllers.is_none());
        assert_eq!(config.project.lock_ttl_seconds, 3600);
        assert_eq!(config.project.lock_retry_interval_seconds, 900);
        assert_eq!(config.project.lock_retry_max_attempts, 3);
        assert!(config.project.max_evaluate_concurrency.is_none());
        assert!(config.project.max_sync_concurrency.is_none());
        assert!(config.project.state_dir.root().ends_with(".nagi"));
        assert!(config.project.export.is_none());
        assert_eq!(
            config.project.default_timeout.as_std(),
            std::time::Duration::from_secs(3600)
        );
    }

    #[test]
    fn load_custom_default_timeout() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "defaultTimeout: 30m").unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(
            config.project.default_timeout.as_std(),
            std::time::Duration::from_secs(30 * 60)
        );
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
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.backend.backend_type, BackendType::Gcs);
        assert_eq!(config.backend.bucket.as_deref(), Some("my-nagi-state"));
        let slack = config.project.notify.slack.clone().unwrap();
        assert_eq!(slack.channel, "#nagi-alerts");
    }

    #[test]
    fn load_gcs_backend() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: gcs\n  bucket: my-bucket\n  prefix: proj/nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.backend.backend_type, BackendType::Gcs);
        assert_eq!(config.backend.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(config.backend.prefix.as_deref(), Some("proj/nagi"));
        assert!(config.backend.region.is_none());
    }

    #[test]
    fn load_s3_backend() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "backend:\n  type: s3\n  bucket: my-bucket\n  region: us-east-1\n  prefix: nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.backend.backend_type, BackendType::S3);
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
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.backend.backend_type, BackendType::Local);
        assert!(config.project.notify.slack.is_some());
    }

    #[test]
    fn load_termination_grace_period() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "terminationGracePeriodSeconds: 300";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.project.termination_grace_period_seconds, Some(300));
    }

    #[test]
    fn load_max_controllers() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "maxControllers: 4";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.project.max_controllers, Some(4));
    }

    #[test]
    fn load_custom_lock_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "lockTtlSeconds: 120";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.project.lock_ttl_seconds, 120);
    }

    #[test]
    fn load_custom_lock_retry() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "lockRetryIntervalSeconds: 60\nlockRetryMaxAttempts: 5";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.project.lock_retry_interval_seconds, 60);
        assert_eq!(config.project.lock_retry_max_attempts, 5);
    }

    #[test]
    fn load_invalid_yaml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "{{invalid").unwrap();
        assert!(load_local_config(dir.path()).is_err());
    }

    #[test]
    fn load_custom_state_dir() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "stateDir: /tmp/my-nagi";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(
            config.project.state_dir,
            StateDir::new(PathBuf::from("/tmp/my-nagi"))
        );
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
        let config = load_local_config(dir.path()).unwrap();
        let export = config.project.export.clone().unwrap();
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
        let config = load_local_config(dir.path()).unwrap();
        let export = config.project.export.clone().unwrap();
        assert_eq!(export.format, ExportFormat::DuckDb);
    }

    #[test]
    fn load_export_config_with_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "\
export:
  connection: my-bq
  dataset: logs
  timeout: 10m";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        let export = config.project.export.clone().unwrap();
        assert_eq!(
            export.timeout.as_ref().map(Duration::as_std),
            Some(std::time::Duration::from_secs(600))
        );
    }

    #[test]
    fn load_export_config_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "\
export:
  connection: my-bq
  dataset: logs";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        let export = config.project.export.clone().unwrap();
        assert_eq!(export.format, ExportFormat::Jsonl);
        assert_eq!(
            export.interval.as_std(),
            std::time::Duration::from_secs(30 * 60)
        );
        assert!(export.timeout.is_none());
    }

    #[test]
    fn load_concurrency_limits() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = "maxEvaluateConcurrency: 5\nmaxSyncConcurrency: 2";
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        assert_eq!(config.project.max_evaluate_concurrency, Some(5));
        assert_eq!(config.project.max_sync_concurrency, Some(2));
    }

    // ── StateDir ──────────────────────────────────────────────────────────

    macro_rules! state_dir_path_test {
        ($($name:ident: $method:ident => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let nd = StateDir::new(PathBuf::from("/state"));
                    assert_eq!(nd.$method(), PathBuf::from($expected));
                }
            )*
        };
    }

    state_dir_path_test! {
        state_dir_log_store_path: log_store_path => "/state/logs.db";
        state_dir_logs_dir:       logs_dir       => "/state/logs";
        state_dir_locks_dir:      locks_dir      => "/state/locks";
        state_dir_suspended_dir:  suspended_dir  => "/state/suspended";
        state_dir_watermarks:     watermarks_dir => "/state/watermarks";
    }

    #[test]
    fn state_dir_default_ends_with_dot_nagi() {
        let nd = StateDir::default();
        assert!(nd.root().ends_with(".nagi"));
    }

    #[test]
    fn state_dir_as_ref_returns_root() {
        let nd = StateDir::new(PathBuf::from("/state"));
        let p: &Path = nd.as_ref();
        assert_eq!(p, Path::new("/state"));
    }

    #[test]
    fn state_dir_deserializes_from_string() {
        let nd: StateDir = serde_yaml::from_str("/custom/path").unwrap();
        assert_eq!(nd, StateDir::new(PathBuf::from("/custom/path")));
    }

    // ── ProjectConfig ────────────────────────────────────────────────────

    #[test]
    fn project_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = r##"
notify:
  slack:
    channel: "#alerts"
terminationGracePeriodSeconds: 300
maxControllers: 4
lockTtlSeconds: 120
lockRetryIntervalSeconds: 60
lockRetryMaxAttempts: 5
maxEvaluateConcurrency: 5
maxSyncConcurrency: 2
stateDir: /tmp/my-nagi
defaultTimeout: 30m
export:
  connection: my-bq
  dataset: nagi_logs
  format: jsonl
  interval: 30m
"##;
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        let project = config.project.clone();

        // Serialize and deserialize ProjectConfig
        let serialized = serde_yaml::to_string(&project).unwrap();
        let deserialized: ProjectConfig = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(project, deserialized);
    }

    #[test]
    fn project_config_default_roundtrip() {
        let project = ProjectConfig::default();
        let serialized = serde_yaml::to_string(&project).unwrap();
        let deserialized: ProjectConfig = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(project, deserialized);
    }

    #[test]
    fn from_parts_assembles_config() {
        let backend = BackendConfig {
            backend_type: BackendType::Gcs,
            bucket: Some("my-bucket".to_string()),
            prefix: Some("proj".to_string()),
            region: None,
        };
        let project = ProjectConfig {
            lock_ttl_seconds: 120,
            ..ProjectConfig::default()
        };
        let config = NagiConfig::from_parts(backend.clone(), project.clone());
        assert_eq!(config.backend, backend);
        assert_eq!(config.project.lock_ttl_seconds, 120);
        assert_eq!(config.project.clone(), project);
    }

    // ── Config Hash ──────────────────────────────────────────────────

    #[test]
    fn save_and_check_hash_matches() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 120").unwrap();
        save_config_hash(dir.path()).unwrap();
        assert!(check_config_hash(dir.path()).unwrap());
    }

    #[test]
    fn check_hash_detects_change() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 120").unwrap();
        save_config_hash(dir.path()).unwrap();

        // Modify nagi.yaml after saving hash
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 999").unwrap();
        assert!(!check_config_hash(dir.path()).unwrap());
    }

    #[test]
    fn check_hash_returns_true_when_no_hash_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 120").unwrap();
        // No save_config_hash call — first run
        assert!(check_config_hash(dir.path()).unwrap());
    }

    #[test]
    fn save_hash_creates_dot_state_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "").unwrap();
        save_config_hash(dir.path()).unwrap();
        assert!(dir.path().join(".nagi").join("config_hash").exists());
    }

    #[test]
    fn check_hash_with_missing_config_file() {
        let dir = tempfile::tempdir().unwrap();
        // Save hash when nagi.yaml is missing (empty content)
        save_config_hash(dir.path()).unwrap();
        // Check should match (both hash empty content)
        assert!(check_config_hash(dir.path()).unwrap());
    }

    #[test]
    fn load_config_local_backend() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 120").unwrap();
        save_config_hash(dir.path()).unwrap();
        let config = load_config(dir.path(), None).unwrap();
        assert_eq!(config.project.lock_ttl_seconds, 120);
        assert_eq!(config.backend.backend_type, BackendType::Local);
    }

    #[test]
    fn load_config_detects_changed_config() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 120").unwrap();
        save_config_hash(dir.path()).unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 999").unwrap();
        let err = load_config(dir.path(), None).unwrap_err();
        assert!(matches!(err, ConfigError::LocalConfigChanged));
    }

    #[test]
    fn load_config_no_hash_file_passes() {
        // First run: no .nagi/config_hash yet → should not error
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 120").unwrap();
        let config = load_config(dir.path(), None).unwrap();
        assert_eq!(config.project.lock_ttl_seconds, 120);
    }

    #[test]
    fn init_config_saves_hash() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 120").unwrap();
        let result = init_config(dir.path(), None, false).unwrap();
        assert_eq!(result, InitConfigResult::Local);
        assert!(dir.path().join(".nagi").join("config_hash").exists());
        assert!(check_config_hash(dir.path()).unwrap());
    }

    // ── init_config / load_config with remote store ────────────────

    fn in_memory_store() -> crate::runtime::storage::remote::RemoteObjectStore {
        crate::runtime::storage::remote::RemoteObjectStore::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            None,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn init_uploads_and_load_retrieves_from_remote() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 42").unwrap();

        let store = in_memory_store();
        let result = init_config(dir.path(), Some(&store), false).unwrap();
        assert_eq!(result, InitConfigResult::Uploaded);

        let config = load_config(dir.path(), Some(&store)).unwrap();
        assert_eq!(config.project.lock_ttl_seconds, 42);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn init_skips_when_remote_config_exists() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 42").unwrap();

        let store = in_memory_store();
        init_config(dir.path(), Some(&store), false).unwrap();

        // Second init without --force should skip
        let result = init_config(dir.path(), Some(&store), false).unwrap();
        assert_eq!(result, InitConfigResult::Skipped);

        // load_config should still work (hash was saved)
        let config = load_config(dir.path(), Some(&store)).unwrap();
        assert_eq!(config.project.lock_ttl_seconds, 42);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn init_force_overwrites_remote_config() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 42").unwrap();

        let store = in_memory_store();
        init_config(dir.path(), Some(&store), false).unwrap();

        // Change local config and force upload
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 99").unwrap();
        let result = init_config(dir.path(), Some(&store), true).unwrap();
        assert_eq!(result, InitConfigResult::Uploaded);

        let config = load_config(dir.path(), Some(&store)).unwrap();
        assert_eq!(config.project.lock_ttl_seconds, 99);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_config_changed_then_reinit_recovers() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 42").unwrap();

        let store = in_memory_store();
        init_config(dir.path(), Some(&store), false).unwrap();

        // Modify nagi.yaml without re-init
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 99").unwrap();
        let err = load_config(dir.path(), Some(&store)).unwrap_err();
        assert!(matches!(err, ConfigError::LocalConfigChanged));

        // Re-init with force to upload new config
        init_config(dir.path(), Some(&store), true).unwrap();
        let config = load_config(dir.path(), Some(&store)).unwrap();
        assert_eq!(config.project.lock_ttl_seconds, 99);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_config_fails_when_remote_config_missing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "lockTtlSeconds: 42").unwrap();
        save_config_hash(dir.path()).unwrap();

        // Store has no config uploaded
        let store = in_memory_store();
        let err = load_config(dir.path(), Some(&store)).unwrap_err();
        assert!(matches!(err, ConfigError::RemoteConfigNotFound));
    }

    #[test]
    fn project_config_excludes_backend_fields() {
        let dir = tempfile::tempdir().unwrap();
        let yaml = r##"
backend:
  type: gcs
  bucket: my-bucket
lockTtlSeconds: 999
"##;
        std::fs::write(dir.path().join("nagi.yaml"), yaml).unwrap();
        let config = load_local_config(dir.path()).unwrap();
        let project = config.project.clone();
        assert_eq!(project.lock_ttl_seconds, 999);
        // ProjectConfig should not contain backend fields
        let serialized = serde_yaml::to_string(&project).unwrap();
        assert!(!serialized.contains("bucket"));
        assert!(!serialized.contains("gcs"));
    }
}

use std::path::Path;

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, JsonSchema)]
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
}

fn default_backend_type() -> String {
    "local".to_string()
}

#[derive(Debug, Clone, Deserialize, PartialEq, JsonSchema)]
pub struct BackendConfig {
    /// Backend type identifier (e.g. `local`, `gcs`). Defaults to `local`.
    #[serde(default = "default_backend_type")]
    pub r#type: String,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            r#type: default_backend_type(),
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
        let slack = config.notify.slack.unwrap();
        assert_eq!(slack.channel, "#nagi-alerts");
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
    fn load_invalid_yaml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nagi.yaml"), "{{invalid").unwrap();
        assert!(load_config(dir.path()).is_err());
    }
}

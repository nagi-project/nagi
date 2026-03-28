use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::KindError;

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
        /// Optional dbt Cloud configuration for running-job checks before sync.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dbt_cloud: Option<DbtCloudSpec>,
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
                if profile.is_empty() {
                    return Err(KindError::InvalidSpec {
                        kind: KIND.to_string(),
                        message: "profile must not be empty".to_string(),
                    });
                }
                Ok(())
            }
        }
    }
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
                dbt_cloud,
            } => {
                assert_eq!(profile, "my_project");
                assert_eq!(target, &Some("dev".to_string()));
                assert!(dbt_cloud.is_none());
            }
        }
    }

    #[test]
    fn parse_connection_spec_without_target() {
        let yaml = r#"
type: dbt
profile: my_project
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Dbt { target, .. } => {
                assert_eq!(target, &None);
            }
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
        }
    }

    #[test]
    fn parse_connection_spec_with_dbt_cloud_default_path() {
        let yaml = r#"
type: dbt
profile: my_project
dbtCloud: {}
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            ConnectionSpec::Dbt { dbt_cloud, .. } => {
                let cloud = dbt_cloud.as_ref().unwrap();
                assert!(cloud.credentials_file.is_none());
            }
        }
    }

    #[test]
    fn validate_rejects_empty_profile() {
        let spec = ConnectionSpec::Dbt {
            profile: "".to_string(),
            target: None,
            dbt_cloud: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = ConnectionSpec::Dbt {
            profile: "my_project".to_string(),
            target: Some("dev".to_string()),
            dbt_cloud: None,
        };
        assert!(spec.validate().is_ok());
    }
}

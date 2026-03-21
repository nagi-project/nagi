use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::KindError;

pub const KIND: &str = "Connection";

/// Spec for `kind: Connection`. Holds external data connection info referenced by Sources.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSpec {
    /// Required in MVP because profiles.yml is the only supported connection resolution mechanism.
    /// SQL-based conditions (Freshness, SQL) rely on it to resolve the adapter config.
    /// When direct connection configuration (host/port/credentials etc.) is added,
    /// this should become `Option<DbtProfile>` alongside alternative connection variants.
    pub dbt_profile: DbtProfile,
    /// Optional dbt Cloud configuration for running-job checks before sync.
    pub dbt_cloud: Option<DbtCloudSpec>,
}

/// Reference to a profile defined in `~/.dbt/profiles.yml`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DbtProfile {
    /// Profile name as defined in `~/.dbt/profiles.yml`.
    pub profile: String,
    /// If omitted, the default target in profiles.yml is used.
    pub target: Option<String>,
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
        if self.dbt_profile.profile.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: "dbtProfile.profile must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dbt_profile_with_target() {
        let yaml = r#"
profile: my_project
target: dev
"#;
        let spec: DbtProfile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.profile, "my_project");
        assert_eq!(spec.target, Some("dev".to_string()));
    }

    #[test]
    fn parse_dbt_profile_without_target() {
        let yaml = r#"
profile: my_project
"#;
        let spec: DbtProfile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.profile, "my_project");
        assert_eq!(spec.target, None);
    }

    #[test]
    fn parse_connection_spec() {
        let yaml = r#"
dbtProfile:
  profile: my_project
  target: dev
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.dbt_profile.profile, "my_project");
        assert_eq!(spec.dbt_profile.target, Some("dev".to_string()));
        assert!(spec.dbt_cloud.is_none());
    }

    #[test]
    fn parse_connection_spec_with_dbt_cloud() {
        let yaml = r#"
dbtProfile:
  profile: my_project
  target: dev
dbtCloud:
  credentialsFile: ~/.dbt/dbt_cloud.yml
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.dbt_profile.profile, "my_project");
        let cloud = spec.dbt_cloud.unwrap();
        assert_eq!(
            cloud.credentials_file,
            Some("~/.dbt/dbt_cloud.yml".to_string())
        );
    }

    #[test]
    fn parse_connection_spec_with_dbt_cloud_default_path() {
        let yaml = r#"
dbtProfile:
  profile: my_project
dbtCloud: {}
"#;
        let spec: ConnectionSpec = serde_yaml::from_str(yaml).unwrap();
        let cloud = spec.dbt_cloud.unwrap();
        assert!(cloud.credentials_file.is_none());
    }

    #[test]
    fn validate_rejects_empty_profile() {
        let spec = ConnectionSpec {
            dbt_profile: DbtProfile {
                profile: "".to_string(),
                target: None,
            },
            dbt_cloud: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = ConnectionSpec {
            dbt_profile: DbtProfile {
                profile: "my_project".to_string(),
                target: Some("dev".to_string()),
            },
            dbt_cloud: None,
        };
        assert!(spec.validate().is_ok());
    }
}

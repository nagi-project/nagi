use serde::{Deserialize, Serialize};

use super::KindError;

pub const KIND: &str = "Connection";

/// Spec for `kind: Connection`. Holds external data connection info referenced by Sources.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSpec {
    pub dbt_profile: DbtProfile,
}

/// Reference to a profile defined in `~/.dbt/profiles.yml`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DbtProfile {
    pub profile: String,
    /// If omitted, the default target in profiles.yml is used.
    pub target: Option<String>,
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
    }

    #[test]
    fn validate_rejects_empty_profile() {
        let spec = ConnectionSpec {
            dbt_profile: DbtProfile {
                profile: "".to_string(),
                target: None,
            },
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
        };
        assert!(spec.validate().is_ok());
    }
}

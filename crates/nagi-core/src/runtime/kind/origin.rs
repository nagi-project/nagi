pub mod dbt;

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{KindError, NagiKind};
use crate::runtime::compile::CompileError;

pub const KIND: &str = "Origin";

/// Override for the auto-generated Sync. Same interface as `onDrift` entries (sync name + with).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DefaultSync {
    /// Name of the user-defined Sync resource to use instead of the auto-generated one.
    pub sync: String,
    /// Template variables passed to the Sync and Conditions resources for argument interpolation.
    #[serde(default)]
    pub with: HashMap<String, String>,
}

/// Spec for `kind: Origin`. References an external project (e.g. dbt) to auto-generate Assets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum OriginSpec {
    #[serde(rename = "DBT", rename_all = "camelCase")]
    Dbt {
        /// Connection resource name for auto-generated Assets.
        connection: String,
        /// Local path to the dbt project directory (relative or absolute).
        project_dir: String,
        /// User-defined Sync to override the auto-generated `nagi-dbt-run`.
        #[serde(default)]
        default_sync: Option<DefaultSync>,
        /// Override `autoSync` for all auto-generated Assets. When `None`, each Asset uses its own default (`true`).
        #[serde(default)]
        auto_sync: Option<bool>,
    },
}

impl OriginSpec {
    pub fn validate(&self) -> Result<(), KindError> {
        let require_non_empty = |field: &str, value: &str| {
            if value.is_empty() {
                return Err(KindError::InvalidSpec {
                    kind: KIND.to_string(),
                    message: format!("{field} must not be empty"),
                });
            }
            Ok(())
        };

        match self {
            OriginSpec::Dbt {
                connection,
                project_dir,
                ..
            } => {
                require_non_empty("connection", connection)?;
                require_non_empty("projectDir", project_dir)?;
                Ok(())
            }
        }
    }
}

/// Expands Origin resources by loading external project data and generating Assets/Syncs.
pub fn expand(resources: Vec<NagiKind>) -> Result<Vec<NagiKind>, CompileError> {
    // Currently only DBT Origins exist. When new Origin types are added,
    // dispatch by OriginSpec variant here.
    dbt::expand::expand(resources)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_origin_spec_dbt() {
        let yaml = r#"
type: DBT
connection: my-bigquery
projectDir: ../dbt-project
"#;
        let spec: OriginSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(
            matches!(&spec, OriginSpec::Dbt { connection, project_dir, default_sync, auto_sync }
            if connection == "my-bigquery" && project_dir == "../dbt-project" && default_sync.is_none() && auto_sync.is_none())
        );
    }

    #[test]
    fn parse_origin_spec_with_default_sync() {
        let yaml = r#"
type: DBT
connection: my-bigquery
projectDir: ../dbt-project
defaultSync:
  sync: my-custom-sync
  with:
    selector: "+{{ asset.name }}"
"#;
        let spec: OriginSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            OriginSpec::Dbt {
                default_sync: Some(ds),
                ..
            } => {
                assert_eq!(ds.sync, "my-custom-sync");
                assert_eq!(ds.with.get("selector").unwrap(), "+{{ asset.name }}");
            }
            _ => panic!("expected DBT with defaultSync"),
        }
    }

    #[test]
    fn parse_origin_spec_with_default_sync_no_with() {
        let yaml = r#"
type: DBT
connection: my-bigquery
projectDir: ../dbt-project
defaultSync:
  sync: my-custom-sync
"#;
        let spec: OriginSpec = serde_yaml::from_str(yaml).unwrap();
        match &spec {
            OriginSpec::Dbt {
                default_sync: Some(ds),
                ..
            } => {
                assert_eq!(ds.sync, "my-custom-sync");
                assert!(ds.with.is_empty());
            }
            _ => panic!("expected DBT with defaultSync"),
        }
    }

    #[test]
    fn validate_rejects_empty_connection() {
        let spec = OriginSpec::Dbt {
            connection: String::new(),
            project_dir: "../dbt".to_string(),
            default_sync: None,
            auto_sync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.to_string().contains("connection must not be empty"));
    }

    #[test]
    fn validate_rejects_empty_project_dir() {
        let spec = OriginSpec::Dbt {
            connection: "my-bq".to_string(),
            project_dir: String::new(),
            default_sync: None,
            auto_sync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.to_string().contains("projectDir must not be empty"));
    }

    #[test]
    fn parse_origin_spec_auto_sync_false() {
        let yaml = r#"
type: DBT
connection: my-bigquery
projectDir: ../dbt-project
autoSync: false
"#;
        let spec: OriginSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &spec,
            OriginSpec::Dbt {
                auto_sync: Some(false),
                ..
            }
        ));
    }

    #[test]
    fn parse_origin_spec_auto_sync_defaults_to_none() {
        let yaml = r#"
type: DBT
connection: my-bigquery
projectDir: ../dbt-project
"#;
        let spec: OriginSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &spec,
            OriginSpec::Dbt {
                auto_sync: None,
                ..
            }
        ));
    }
}

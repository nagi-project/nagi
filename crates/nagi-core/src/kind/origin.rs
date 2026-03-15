use serde::{Deserialize, Serialize};

use super::asset::SyncRef;
use super::KindError;

pub const KIND: &str = "Origin";

/// Spec for `kind: Origin`. References an external project (e.g. dbt) to auto-generate Assets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OriginSpec {
    #[serde(rename_all = "camelCase")]
    DBT {
        /// Connection resource name for auto-generated Sources.
        connection: String,
        /// Local path to the dbt project directory (relative or absolute).
        project_dir: String,
        /// Applied to all auto-generated Assets unless overridden.
        #[serde(default)]
        default_sync: Option<SyncRef>,
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
            OriginSpec::DBT {
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
            matches!(&spec, OriginSpec::DBT { connection, project_dir, default_sync }
            if connection == "my-bigquery" && project_dir == "../dbt-project" && default_sync.is_none())
        );
    }

    #[test]
    fn parse_origin_spec_with_default_sync() {
        let yaml = r#"
type: DBT
connection: my-bigquery
projectDir: ../dbt-project
defaultSync:
  ref: dbt-default
"#;
        let spec: OriginSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(
            matches!(&spec, OriginSpec::DBT { default_sync: Some(sync_ref), .. }
            if sync_ref.ref_name == "dbt-default")
        );
    }

    #[test]
    fn parse_origin_spec_with_default_sync_and_with() {
        let yaml = r#"
type: DBT
connection: my-bigquery
projectDir: ../dbt-project
defaultSync:
  ref: dbt-default
  with:
    selector: "+{{ asset.name }}"
"#;
        let spec: OriginSpec = serde_yaml::from_str(yaml).unwrap();
        if let OriginSpec::DBT {
            default_sync: Some(sync_ref),
            ..
        } = &spec
        {
            assert_eq!(sync_ref.ref_name, "dbt-default");
            assert_eq!(sync_ref.with.get("selector").unwrap(), "+{{ asset.name }}");
        } else {
            panic!("expected DBT with default_sync");
        }
    }

    #[test]
    fn validate_rejects_empty_connection() {
        let spec = OriginSpec::DBT {
            connection: String::new(),
            project_dir: "../dbt".to_string(),
            default_sync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.to_string().contains("connection must not be empty"));
    }

    #[test]
    fn validate_rejects_empty_project_dir() {
        let spec = OriginSpec::DBT {
            connection: "my-bq".to_string(),
            project_dir: String::new(),
            default_sync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.to_string().contains("projectDir must not be empty"));
    }
}

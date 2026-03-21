use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::KindError;

pub const KIND: &str = "Source";

/// Spec for `kind: Source`. Declares the location of raw data, referenced by Assets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceSpec {
    /// Name of the Connection resource this Source uses.
    pub connection: String,
}

impl SourceSpec {
    pub fn validate(&self) -> Result<(), KindError> {
        if self.connection.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: "connection must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_source_spec() {
        let yaml = r#"
connection: my-bigquery
"#;
        let spec: SourceSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.connection, "my-bigquery");
    }

    #[test]
    fn validate_rejects_empty_connection() {
        let spec = SourceSpec {
            connection: "".to_string(),
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = SourceSpec {
            connection: "my-bigquery".to_string(),
        };
        assert!(spec.validate().is_ok());
    }
}

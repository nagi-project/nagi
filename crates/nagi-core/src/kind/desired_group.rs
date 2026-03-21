use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::asset::DesiredCondition;
use super::KindError;

pub const KIND: &str = "DesiredGroup";

/// Spec for `kind: DesiredGroup`. A reusable set of desired conditions referenced from Asset `desiredSets` via `ref:`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesiredGroupSpec(pub Vec<DesiredCondition>);

impl DesiredGroupSpec {
    pub fn validate(&self) -> Result<(), KindError> {
        if self.0.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: "spec must contain at least one condition".to_string(),
            });
        }
        for condition in &self.0 {
            condition.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kind::parse_kind;

    #[test]
    fn parse_desired_group_with_freshness() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: DesiredGroup
metadata:
  name: daily-sla
spec:
  - name: freshness-24h
    type: Freshness
    maxAge: 24h
    interval: 6h
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.kind(), KIND);
        assert_eq!(resource.metadata().name, "daily-sla");
    }

    #[test]
    fn parse_desired_group_with_multiple_conditions() {
        let yaml = r#"
apiVersion: nagi.io/v1alpha1
kind: DesiredGroup
metadata:
  name: sales-quality-checks
spec:
  - name: no-negative-amount
    type: SQL
    query: "SELECT COUNT(*) = 0 FROM daily_sales WHERE amount < 0"
  - name: dbt-test-sales
    type: Command
    run: [dbt, test, --select, "tag:sales"]
"#;
        let resource = parse_kind(yaml).unwrap();
        assert_eq!(resource.kind(), KIND);
    }

    #[test]
    fn validate_rejects_empty_spec() {
        let spec = DesiredGroupSpec(vec![]);
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }
}

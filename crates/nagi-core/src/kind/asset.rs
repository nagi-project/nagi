use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::cron::CronSchedule;
use crate::duration::Duration;

use super::KindError;

pub const KIND: &str = "Asset";

/// Spec for `kind: Asset`. The core resource: declares desired state and convergence operations.
/// The reconciliation loop continuously evaluates `desired` and runs `sync` until all conditions are met.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetSpec {
    #[serde(default)]
    pub sources: Vec<SourceRef>,
    /// All conditions are AND-evaluated. All true → Ready.
    pub desired: Vec<DesiredCondition>,
    /// Controls automatic sync execution in `nagi serve`. Defaults to `true`.
    #[serde(default = "default_auto_sync")]
    pub auto_sync: bool,
    pub sync: Option<SyncRef>,
    /// Falls back to `sync` when omitted.
    pub resync: Option<SyncRef>,
}

fn default_auto_sync() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
    #[serde(default)]
    pub with: HashMap<String, String>,
}

/// A single desired state condition. The Asset is Ready only when all conditions are satisfied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DesiredCondition {
    /// Can transition to Not Ready as time passes beyond `maxAge`.
    Freshness {
        #[serde(rename = "maxAge")]
        max_age: Duration,
        interval: Duration,
        /// Optional cron expression for additional evaluation at a specific time.
        #[serde(rename = "checkAt")]
        check_at: Option<CronSchedule>,
        /// If omitted, freshness is determined from table metadata instead of a column value.
        column: Option<String>,
    },
    NotNull {
        column: String,
    },
    /// Query must return a scalar boolean. Ready when the result is true.
    SQL {
        query: String,
    },
}

impl AssetSpec {
    pub fn validate(&self) -> Result<(), KindError> {
        if self.desired.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: "desired must not be empty".to_string(),
            });
        }
        for condition in &self.desired {
            condition.validate()?;
        }
        Ok(())
    }
}

impl DesiredCondition {
    fn require_non_empty(value: &str, field: &str) -> Result<(), KindError> {
        if value.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: format!("{field} must not be empty"),
            });
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), KindError> {
        match self {
            DesiredCondition::Freshness { .. } => {}
            DesiredCondition::NotNull { column } => {
                Self::require_non_empty(column, "NotNull.column")?
            }
            DesiredCondition::SQL { query } => Self::require_non_empty(query, "SQL.query")?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration as StdDuration;

    use super::*;

    #[test]
    fn parse_full_asset_spec() {
        let yaml = r#"
sources:
  - ref: raw-sales
  - ref: customer-master
desired:
  - type: Freshness
    maxAge: 24h
    interval: 6h
    checkAt: "0 3 * * *"
    column: updated_at
  - type: NotNull
    column: amount
  - type: SQL
    query: "SELECT COUNT(*) = 0 FROM daily_sales WHERE amount < 0"
autoSync: true
sync:
  ref: dbt-default
  with:
    selector: "+daily_sales"
resync:
  ref: sales-full-reload
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(spec.sources.len(), 2);
        assert_eq!(spec.sources[0].ref_name, "raw-sales");
        assert_eq!(spec.sources[1].ref_name, "customer-master");

        assert_eq!(spec.desired.len(), 3);
        assert!(matches!(
            &spec.desired[0],
            DesiredCondition::Freshness {
                max_age,
                interval,
                check_at: Some(check_at),
                column: Some(column),
            } if max_age.as_std() == StdDuration::from_secs(24 * 3600)
                && interval.as_std() == StdDuration::from_secs(6 * 3600)
                && check_at.as_str() == "0 3 * * *"
                && column == "updated_at"
        ));
        assert!(matches!(
            &spec.desired[1],
            DesiredCondition::NotNull { column } if column == "amount"
        ));
        assert!(matches!(
            &spec.desired[2],
            DesiredCondition::SQL { query } if query == "SELECT COUNT(*) = 0 FROM daily_sales WHERE amount < 0"
        ));

        assert!(spec.auto_sync);

        let sync = spec.sync.as_ref().unwrap();
        assert_eq!(sync.ref_name, "dbt-default");
        assert_eq!(sync.with.get("selector").unwrap(), "+daily_sales");

        assert_eq!(spec.resync.as_ref().unwrap().ref_name, "sales-full-reload");
    }

    #[test]
    fn parse_minimal_asset_spec() {
        let yaml = r#"
desired:
  - type: Freshness
    maxAge: 24h
    interval: 6h
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();

        assert!(spec.sources.is_empty());
        assert_eq!(spec.desired.len(), 1);
        assert!(spec.auto_sync, "autoSync should default to true");
        assert!(spec.sync.is_none());
        assert!(spec.resync.is_none());
    }

    #[test]
    fn parse_freshness_without_optional_fields() {
        let yaml = r#"
desired:
  - type: Freshness
    maxAge: 24h
    interval: 6h
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &spec.desired[0],
            DesiredCondition::Freshness {
                check_at: None,
                column: None,
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_cron_in_freshness() {
        let yaml = r#"
desired:
  - type: Freshness
    maxAge: 24h
    interval: 6h
    checkAt: "not-a-cron"
"#;
        let result: Result<AssetSpec, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_duration_in_freshness() {
        let yaml = r#"
desired:
  - type: Freshness
    maxAge: not-a-duration
    interval: 6h
"#;
        let result: Result<AssetSpec, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn auto_sync_defaults_to_true() {
        let yaml = r#"
desired:
  - type: NotNull
    column: id
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(spec.auto_sync);
    }

    #[test]
    fn auto_sync_can_be_set_to_false() {
        let yaml = r#"
desired:
  - type: NotNull
    column: id
autoSync: false
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(!spec.auto_sync);
    }

    #[test]
    fn validate_rejects_empty_desired() {
        let spec = AssetSpec {
            sources: vec![],
            desired: vec![],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = AssetSpec {
            sources: vec![],
            desired: vec![DesiredCondition::NotNull {
                column: "id".to_string(),
            }],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        assert!(spec.validate().is_ok());
    }
}

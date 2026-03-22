use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::cron::CronSchedule;
use crate::duration::Duration;

use super::KindError;

pub const KIND: &str = "Asset";

/// Spec for `kind: Asset`. The core resource: declares desired state and convergence operations.
/// `on_drift` pairs conditions with sync operations. Entries are evaluated in order (first-match).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssetSpec {
    /// Tags for filtering with `--select tag:X`.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Names of upstream Source resources.
    #[serde(default)]
    pub sources: Vec<String>,
    /// Condition-sync pairs evaluated in order. First entry whose conditions detect drift
    /// determines which sync to run. When omitted, the Asset is always Ready.
    #[serde(default)]
    pub on_drift: Vec<OnDriftEntry>,
    /// Controls automatic sync execution in `nagi serve`. Defaults to `true`.
    #[serde(default = "default_auto_sync")]
    pub auto_sync: bool,
}

fn default_auto_sync() -> bool {
    true
}

/// An entry in `on_drift`. Pairs a Conditions reference with a Sync reference.
/// The conditions are evaluated as a group (all must pass for no drift).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OnDriftEntry {
    /// Name of the `kind: Conditions` resource whose conditions define drift.
    pub conditions: String,
    /// Name of the Sync resource to execute when drift is detected.
    pub sync: String,
    /// Template variables passed to the Sync resource for argument interpolation.
    #[serde(default)]
    pub with: HashMap<String, String>,
}

/// A single desired state condition. The Asset is Ready only when all conditions are satisfied.
/// Each condition carries a `name` that is unique within the Asset (after Conditions expansion)
/// and used as a key in execution logs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum DesiredCondition {
    /// Can transition to Not Ready as time passes beyond `maxAge`.
    Freshness {
        /// Unique identifier for this condition within the Asset.
        name: String,
        /// Maximum acceptable age of the data before the condition becomes Not Ready.
        #[serde(rename = "maxAge")]
        max_age: Duration,
        /// Polling interval for re-evaluating this condition.
        interval: Duration,
        /// Optional cron expression for additional evaluation at a specific time.
        #[serde(rename = "checkAt")]
        check_at: Option<CronSchedule>,
        /// If omitted, freshness is determined from table metadata instead of a column value.
        column: Option<String>,
    },
    /// Query must return a scalar boolean. Ready when the result is true.
    SQL {
        /// Unique identifier for this condition within the Asset.
        name: String,
        /// SQL query that must return a scalar boolean.
        query: String,
        /// Optional polling interval. If omitted, only evaluated on upstream state change or after sync.
        #[serde(default)]
        interval: Option<Duration>,
    },
    /// Runs an external command. Ready when the process exits with code 0.
    /// `run` is argv: the first element is the program, the rest are arguments.
    Command {
        /// Unique identifier for this condition within the Asset.
        name: String,
        /// Command and arguments in argv format.
        run: Vec<String>,
        /// Optional polling interval. If omitted, only evaluated on upstream state change or after sync.
        #[serde(default)]
        interval: Option<Duration>,
        /// Environment variables to set for the subprocess.
        #[serde(default)]
        env: HashMap<String, String>,
    },
}

impl AssetSpec {
    pub fn validate(&self) -> Result<(), KindError> {
        for entry in &self.on_drift {
            entry.validate()?;
        }
        Ok(())
    }
}

impl OnDriftEntry {
    fn validate(&self) -> Result<(), KindError> {
        if self.conditions.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: "on_drift conditions ref must not be empty".to_string(),
            });
        }
        if self.sync.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: "on_drift sync ref must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

/// Checks that a resolved (flattened) list of conditions has unique names.
/// Called after compile resolves all conditions references.
pub fn validate_no_duplicate_condition_names(
    conditions: &[DesiredCondition],
) -> Result<(), KindError> {
    let mut seen = std::collections::HashSet::new();
    for condition in conditions {
        if !seen.insert(condition.name()) {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: format!(
                    "on_drift contains duplicate condition name '{}'",
                    condition.name()
                ),
            });
        }
    }
    Ok(())
}

impl DesiredCondition {
    pub fn name(&self) -> &str {
        match self {
            DesiredCondition::Freshness { name, .. } => name,
            DesiredCondition::SQL { name, .. } => name,
            DesiredCondition::Command { name, .. } => name,
        }
    }

    /// Returns the evaluation interval if configured.
    pub fn interval(&self) -> Option<&Duration> {
        match self {
            DesiredCondition::Freshness { interval, .. } => Some(interval),
            DesiredCondition::SQL { interval, .. } => interval.as_ref(),
            DesiredCondition::Command { interval, .. } => interval.as_ref(),
        }
    }

    fn require_non_empty(value: &str, field: &str) -> Result<(), KindError> {
        if value.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: format!("{field} must not be empty"),
            });
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), KindError> {
        Self::require_non_empty(self.name(), "condition name")?;
        match self {
            DesiredCondition::Freshness { .. } => {}
            DesiredCondition::SQL { query, .. } => Self::require_non_empty(query, "SQL.query")?,
            DesiredCondition::Command { run, .. } => {
                if run.is_empty() {
                    return Err(KindError::InvalidSpec {
                        kind: KIND.to_string(),
                        message: "Command.run must not be empty".to_string(),
                    });
                }
                Self::require_non_empty(&run[0], "Command.run[0]")?;
            }
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
  - raw-sales
  - customer-master
onDrift:
  - conditions: daily-sla
    sync: dbt-default
    with:
      selector: "+daily_sales"
  - conditions: sales-quality
    sync: sales-full-reload
autoSync: true
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(spec.sources.len(), 2);
        assert_eq!(spec.sources[0], "raw-sales");
        assert_eq!(spec.sources[1], "customer-master");

        assert_eq!(spec.on_drift.len(), 2);
        assert_eq!(spec.on_drift[0].conditions, "daily-sla");
        assert_eq!(spec.on_drift[0].sync, "dbt-default");
        assert_eq!(
            spec.on_drift[0].with.get("selector").unwrap(),
            "+daily_sales"
        );
        assert_eq!(spec.on_drift[1].conditions, "sales-quality");
        assert_eq!(spec.on_drift[1].sync, "sales-full-reload");

        assert!(spec.auto_sync);
    }

    #[test]
    fn parse_minimal_asset_spec() {
        let yaml = r#"
onDrift:
  - conditions: freshness-check
    sync: dbt-run
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();

        assert!(spec.sources.is_empty());
        assert_eq!(spec.on_drift.len(), 1);
        assert!(spec.auto_sync, "autoSync should default to true");
    }

    #[test]
    fn parse_asset_without_on_drift() {
        let yaml = r#"
sources:
  - raw-sales
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(
            spec.on_drift.is_empty(),
            "omitted onDrift should default to empty (always Ready)"
        );
    }

    #[test]
    fn auto_sync_defaults_to_true() {
        let yaml = r#"
onDrift:
  - conditions: check
    sync: dbt-run
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(spec.auto_sync);
    }

    #[test]
    fn auto_sync_can_be_set_to_false() {
        let yaml = r#"
onDrift:
  - conditions: check
    sync: dbt-run
autoSync: false
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(!spec.auto_sync);
    }

    #[test]
    fn validate_accepts_empty_on_drift() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            on_drift: vec![],
            auto_sync: true,
        };
        assert!(spec.validate().is_ok(), "empty onDrift means always Ready");
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            on_drift: vec![OnDriftEntry {
                conditions: "daily-sla".to_string(),
                sync: "dbt-run".to_string(),
                with: HashMap::new(),
            }],
            auto_sync: true,
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_conditions_ref() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            on_drift: vec![OnDriftEntry {
                conditions: "".to_string(),
                sync: "dbt-run".to_string(),
                with: HashMap::new(),
            }],
            auto_sync: true,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_rejects_empty_sync_ref() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            on_drift: vec![OnDriftEntry {
                conditions: "daily-sla".to_string(),
                sync: "".to_string(),
                with: HashMap::new(),
            }],
            auto_sync: true,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_no_duplicates_accepts_distinct_conditions() {
        let conditions = vec![
            DesiredCondition::SQL {
                name: "check-a".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
            },
            DesiredCondition::SQL {
                name: "check-b".to_string(),
                query: "SELECT false".to_string(),
                interval: None,
            },
        ];
        assert!(validate_no_duplicate_condition_names(&conditions).is_ok());
    }

    #[test]
    fn validate_no_duplicates_rejects_identical_conditions() {
        let conditions = vec![
            DesiredCondition::SQL {
                name: "check-a".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
            },
            DesiredCondition::SQL {
                name: "check-a".to_string(),
                query: "SELECT false".to_string(),
                interval: None,
            },
        ];
        let err = validate_no_duplicate_condition_names(&conditions).unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, message }
            if kind == KIND && message.contains("duplicate")));
    }

    #[test]
    fn validate_no_duplicates_catches_group_and_inline_overlap() {
        let condition = DesiredCondition::Freshness {
            name: "freshness".to_string(),
            max_age: serde_yaml::from_str("24h").unwrap(),
            interval: serde_yaml::from_str("6h").unwrap(),
            check_at: None,
            column: None,
        };
        let conditions = vec![condition.clone(), condition];
        let err = validate_no_duplicate_condition_names(&conditions).unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, message }
            if kind == KIND && message.contains("duplicate")));
    }

    #[test]
    fn validate_rejects_empty_condition_name() {
        let condition = DesiredCondition::SQL {
            name: "".to_string(),
            query: "SELECT true".to_string(),
            interval: None,
        };
        let err = condition.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn parse_freshness_condition() {
        let yaml = r#"
name: data-freshness
type: Freshness
maxAge: 24h
interval: 6h
checkAt: "0 3 * * *"
column: updated_at
"#;
        let condition: DesiredCondition = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &condition,
            DesiredCondition::Freshness {
                name,
                max_age,
                interval,
                check_at: Some(check_at),
                column: Some(column),
            } if name == "data-freshness"
                && max_age.as_std() == StdDuration::from_secs(24 * 3600)
                && interval.as_std() == StdDuration::from_secs(6 * 3600)
                && check_at.as_str() == "0 3 * * *"
                && column == "updated_at"
        ));
    }

    #[test]
    fn parse_freshness_without_optional_fields() {
        let yaml = r#"
name: freshness
type: Freshness
maxAge: 24h
interval: 6h
"#;
        let condition: DesiredCondition = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &condition,
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
name: freshness
type: Freshness
maxAge: 24h
interval: 6h
checkAt: "not-a-cron"
"#;
        let result: Result<DesiredCondition, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_duration_in_freshness() {
        let yaml = r#"
name: freshness
type: Freshness
maxAge: not-a-duration
interval: 6h
"#;
        let result: Result<DesiredCondition, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn parse_command_condition() {
        let yaml = r#"
name: dbt-test
type: Command
run: [dbt, test, --select, my_model]
"#;
        let condition: DesiredCondition = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &condition,
            DesiredCondition::Command { name, run, .. } if name == "dbt-test" && run == &["dbt", "test", "--select", "my_model"]
        ));
    }

    #[test]
    fn validate_rejects_empty_command_run() {
        let condition = DesiredCondition::Command {
            name: "check".to_string(),
            run: vec![],
            interval: None,
            env: HashMap::new(),
        };
        let err = condition.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_rejects_blank_command_program() {
        let condition = DesiredCondition::Command {
            name: "check".to_string(),
            run: vec!["".to_string()],
            interval: None,
            env: HashMap::new(),
        };
        let err = condition.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn parse_multiple_on_drift_entries() {
        let yaml = r#"
onDrift:
  - conditions: daily-sla
    sync: dbt-incremental
  - conditions: sales-quality
    sync: dbt-full-refresh
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.on_drift.len(), 2);
        assert_eq!(spec.on_drift[0].conditions, "daily-sla");
        assert_eq!(spec.on_drift[0].sync, "dbt-incremental");
        assert_eq!(spec.on_drift[1].conditions, "sales-quality");
        assert_eq!(spec.on_drift[1].sync, "dbt-full-refresh");
    }
}

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::cron::CronSchedule;
use crate::duration::Duration;

use super::KindError;

pub const KIND: &str = "Asset";

/// Spec for `kind: Asset`. The core resource: declares desired state and convergence operations.
/// The reconciliation loop continuously evaluates `desiredSets` and runs `sync` until all conditions are met.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetSpec {
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub sources: Vec<SourceRef>,
    /// All entries are AND-evaluated. All true → Ready. When omitted, the Asset is always Ready.
    #[serde(default)]
    pub desired_sets: Vec<DesiredSetEntry>,
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

/// An entry in `desiredSets`. Either a reference to a `kind: DesiredGroup` or an inline condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DesiredSetEntry {
    Ref(DesiredGroupRef),
    Inline(DesiredCondition),
}

/// A reference to a `kind: DesiredGroup` resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DesiredGroupRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
}

/// A single desired state condition. The Asset is Ready only when all conditions are satisfied.
/// Each condition carries a `name` that is unique within the Asset (after DesiredGroup expansion)
/// and used as a key in execution logs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DesiredCondition {
    /// Can transition to Not Ready as time passes beyond `maxAge`.
    Freshness {
        name: String,
        #[serde(rename = "maxAge")]
        max_age: Duration,
        interval: Duration,
        /// Optional cron expression for additional evaluation at a specific time.
        #[serde(rename = "checkAt")]
        check_at: Option<CronSchedule>,
        /// If omitted, freshness is determined from table metadata instead of a column value.
        column: Option<String>,
    },
    /// Query must return a scalar boolean. Ready when the result is true.
    SQL {
        name: String,
        query: String,
        /// Optional polling interval. If omitted, only evaluated via upstream propagation.
        #[serde(default)]
        interval: Option<Duration>,
    },
    /// Runs an external command. Ready when the process exits with code 0.
    /// `run` is argv: the first element is the program, the rest are arguments.
    Command {
        name: String,
        run: Vec<String>,
        /// Optional polling interval. If omitted, only evaluated via upstream propagation.
        #[serde(default)]
        interval: Option<Duration>,
    },
}

impl AssetSpec {
    pub fn validate(&self) -> Result<(), KindError> {
        for entry in &self.desired_sets {
            entry.validate()?;
        }
        Ok(())
    }
}

impl DesiredSetEntry {
    fn validate(&self) -> Result<(), KindError> {
        match self {
            DesiredSetEntry::Ref(r) => {
                if r.ref_name.is_empty() {
                    return Err(KindError::InvalidSpec {
                        kind: KIND.to_string(),
                        message: "desiredSets ref must not be empty".to_string(),
                    });
                }
                Ok(())
            }
            DesiredSetEntry::Inline(condition) => condition.validate(),
        }
    }
}

/// Checks that a resolved (flattened) list of conditions has unique names.
/// Called after compile resolves all `Ref` entries into inline conditions.
pub fn validate_no_duplicate_condition_names(
    conditions: &[DesiredCondition],
) -> Result<(), KindError> {
    let mut seen = std::collections::HashSet::new();
    for condition in conditions {
        if !seen.insert(condition.name()) {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: format!(
                    "desiredSets contains duplicate condition name '{}'",
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
  - ref: raw-sales
  - ref: customer-master
desiredSets:
  - ref: daily-sla
  - name: data-freshness
    type: Freshness
    maxAge: 24h
    interval: 6h
    checkAt: "0 3 * * *"
    column: updated_at
  - name: no-negative-amount
    type: SQL
    query: "SELECT COUNT(*) = 0 FROM daily_sales WHERE amount < 0"
  - name: dbt-test-sales
    type: Command
    run: [dbt, test, --select, daily_sales]
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

        assert_eq!(spec.desired_sets.len(), 4);
        assert!(matches!(
            &spec.desired_sets[0],
            DesiredSetEntry::Ref(r) if r.ref_name == "daily-sla"
        ));
        assert!(matches!(
            &spec.desired_sets[1],
            DesiredSetEntry::Inline(DesiredCondition::Freshness {
                name,
                max_age,
                interval,
                check_at: Some(check_at),
                column: Some(column),
            }) if name == "data-freshness"
                && max_age.as_std() == StdDuration::from_secs(24 * 3600)
                && interval.as_std() == StdDuration::from_secs(6 * 3600)
                && check_at.as_str() == "0 3 * * *"
                && column == "updated_at"
        ));
        assert!(matches!(
            &spec.desired_sets[2],
            DesiredSetEntry::Inline(DesiredCondition::SQL { name, query, .. }) if name == "no-negative-amount" && query == "SELECT COUNT(*) = 0 FROM daily_sales WHERE amount < 0"
        ));
        assert!(matches!(
            &spec.desired_sets[3],
            DesiredSetEntry::Inline(DesiredCondition::Command { name, run, .. }) if name == "dbt-test-sales" && run == &["dbt", "test", "--select", "daily_sales"]
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
desiredSets:
  - name: freshness
    type: Freshness
    maxAge: 24h
    interval: 6h
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();

        assert!(spec.sources.is_empty());
        assert_eq!(spec.desired_sets.len(), 1);
        assert!(spec.auto_sync, "autoSync should default to true");
        assert!(spec.sync.is_none());
        assert!(spec.resync.is_none());
    }

    #[test]
    fn parse_asset_without_desired_sets() {
        let yaml = r#"
sources:
  - ref: raw-sales
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(
            spec.desired_sets.is_empty(),
            "omitted desiredSets should default to empty (always Ready)"
        );
    }

    #[test]
    fn parse_freshness_without_optional_fields() {
        let yaml = r#"
desiredSets:
  - name: freshness
    type: Freshness
    maxAge: 24h
    interval: 6h
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &spec.desired_sets[0],
            DesiredSetEntry::Inline(DesiredCondition::Freshness {
                check_at: None,
                column: None,
                ..
            })
        ));
    }

    #[test]
    fn rejects_invalid_cron_in_freshness() {
        let yaml = r#"
desiredSets:
  - name: freshness
    type: Freshness
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
desiredSets:
  - name: freshness
    type: Freshness
    maxAge: not-a-duration
    interval: 6h
"#;
        let result: Result<AssetSpec, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn auto_sync_defaults_to_true() {
        let yaml = r#"
desiredSets:
  - name: check
    type: SQL
    query: "SELECT true"
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(spec.auto_sync);
    }

    #[test]
    fn auto_sync_can_be_set_to_false() {
        let yaml = r#"
desiredSets:
  - name: check
    type: SQL
    query: "SELECT true"
autoSync: false
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(!spec.auto_sync);
    }

    #[test]
    fn validate_accepts_empty_desired_sets() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        assert!(
            spec.validate().is_ok(),
            "empty desiredSets means always Ready"
        );
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![DesiredSetEntry::Inline(DesiredCondition::SQL {
                name: "check".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
            })],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn validate_accepts_ref_entry() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![DesiredSetEntry::Ref(DesiredGroupRef {
                ref_name: "daily-sla".to_string(),
            })],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_ref_name() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![DesiredSetEntry::Ref(DesiredGroupRef {
                ref_name: "".to_string(),
            })],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn parse_command_condition() {
        let yaml = r#"
desiredSets:
  - name: dbt-test
    type: Command
    run: [dbt, test, --select, my_model]
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(
            &spec.desired_sets[0],
            DesiredSetEntry::Inline(DesiredCondition::Command { name, run, .. }) if name == "dbt-test" && run == &["dbt", "test", "--select", "my_model"]
        ));
    }

    #[test]
    fn validate_rejects_empty_command_run() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![DesiredSetEntry::Inline(DesiredCondition::Command {
                name: "check".to_string(),
                run: vec![],
                interval: None,
            })],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_rejects_blank_command_program() {
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![DesiredSetEntry::Inline(DesiredCondition::Command {
                name: "check".to_string(),
                run: vec!["".to_string()],
                interval: None,
            })],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn parse_mixed_ref_and_inline() {
        let yaml = r#"
desiredSets:
  - ref: daily-sla
  - ref: sales-quality-checks
  - name: no-null-region
    type: SQL
    query: "SELECT COUNT(*) = 0 FROM daily_sales WHERE region IS NULL"
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.desired_sets.len(), 3);
        assert!(
            matches!(&spec.desired_sets[0], DesiredSetEntry::Ref(r) if r.ref_name == "daily-sla")
        );
        assert!(
            matches!(&spec.desired_sets[1], DesiredSetEntry::Ref(r) if r.ref_name == "sales-quality-checks")
        );
        assert!(matches!(
            &spec.desired_sets[2],
            DesiredSetEntry::Inline(DesiredCondition::SQL { .. })
        ));
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
        let spec = AssetSpec {
            tags: vec![],
            sources: vec![],
            desired_sets: vec![DesiredSetEntry::Inline(DesiredCondition::SQL {
                name: "".to_string(),
                query: "SELECT true".to_string(),
                interval: None,
            })],
            auto_sync: true,
            sync: None,
            resync: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }
}

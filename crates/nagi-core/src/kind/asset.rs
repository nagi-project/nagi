use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::cron::CronSchedule;
use crate::duration::Duration;

use super::KindError;

pub const KIND: &str = "Asset";

/// Controls where an overlay onDrift entry is placed relative to Origin-generated entries
/// during Asset overlay merge.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MergePosition {
    #[default]
    BeforeOrigin,
    AfterOrigin,
}

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
    /// Default evaluate cache TTL for all conditions in this Asset.
    /// Conditions can override this with their own `evaluateCacheTtl`.
    #[serde(default, rename = "evaluateCacheTtl")]
    pub evaluate_cache_ttl: Option<Duration>,
}

fn default_auto_sync() -> bool {
    true
}

/// An entry in `on_drift`. Pairs a Conditions reference with a Sync reference.
/// The conditions are evaluated as a group (all must pass for no drift).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OnDriftEntry {
    /// Name of the `kind: Conditions` resource whose conditions define drift.
    pub conditions: String,
    /// Name of the Sync resource to execute when drift is detected.
    pub sync: String,
    /// Template variables passed to the Sync resource for argument interpolation.
    #[serde(default)]
    pub with: HashMap<String, String>,
    /// Controls insertion position during overlay merge. Not included in compiled output.
    #[serde(default)]
    pub merge_position: MergePosition,
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
        /// Per-condition cache TTL override. Takes precedence over the Asset-level default.
        #[serde(default, rename = "evaluateCacheTtl")]
        evaluate_cache_ttl: Option<Duration>,
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
        /// Per-condition cache TTL override. Takes precedence over the Asset-level default.
        #[serde(default, rename = "evaluateCacheTtl")]
        evaluate_cache_ttl: Option<Duration>,
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
        /// Per-condition cache TTL override. Takes precedence over the Asset-level default.
        #[serde(default, rename = "evaluateCacheTtl")]
        evaluate_cache_ttl: Option<Duration>,
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

/// Merges overlay (user-defined) on_drift entries with origin-generated entries.
/// Result order: [beforeOrigin overlay] + [origin] + [afterOrigin overlay].
/// Within each group, input order is preserved.
pub fn merge_on_drift_entries(
    overlay: Vec<OnDriftEntry>,
    origin: Vec<OnDriftEntry>,
) -> Vec<OnDriftEntry> {
    let (before, after): (Vec<_>, Vec<_>) = overlay
        .into_iter()
        .partition(|e| e.merge_position == MergePosition::BeforeOrigin);
    let mut merged = Vec::with_capacity(before.len() + origin.len() + after.len());
    merged.extend(before);
    merged.extend(origin);
    merged.extend(after);
    merged
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

    /// Returns the per-condition evaluate cache TTL if configured.
    pub fn evaluate_cache_ttl(&self) -> Option<&Duration> {
        match self {
            DesiredCondition::Freshness {
                evaluate_cache_ttl, ..
            } => evaluate_cache_ttl.as_ref(),
            DesiredCondition::SQL {
                evaluate_cache_ttl, ..
            } => evaluate_cache_ttl.as_ref(),
            DesiredCondition::Command {
                evaluate_cache_ttl, ..
            } => evaluate_cache_ttl.as_ref(),
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
            evaluate_cache_ttl: None,
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
                merge_position: MergePosition::BeforeOrigin,
            }],
            auto_sync: true,
            evaluate_cache_ttl: None,
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
                merge_position: MergePosition::BeforeOrigin,
            }],
            auto_sync: true,
            evaluate_cache_ttl: None,
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
                merge_position: MergePosition::BeforeOrigin,
            }],
            auto_sync: true,
            evaluate_cache_ttl: None,
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
                evaluate_cache_ttl: None,
            },
            DesiredCondition::SQL {
                name: "check-b".to_string(),
                query: "SELECT false".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
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
                evaluate_cache_ttl: None,
            },
            DesiredCondition::SQL {
                name: "check-a".to_string(),
                query: "SELECT false".to_string(),
                interval: None,
                evaluate_cache_ttl: None,
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
            evaluate_cache_ttl: None,
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
            evaluate_cache_ttl: None,
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
                ..
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
            evaluate_cache_ttl: None,
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
            evaluate_cache_ttl: None,
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

    #[test]
    fn parse_on_drift_with_merge_position() {
        let yaml = r#"
onDrift:
  - conditions: check
    sync: run
    mergePosition: afterOrigin
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.on_drift[0].merge_position, MergePosition::AfterOrigin);
    }

    #[test]
    fn parse_on_drift_default_merge_position() {
        let yaml = r#"
onDrift:
  - conditions: check
    sync: run
"#;
        let spec: AssetSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.on_drift[0].merge_position, MergePosition::BeforeOrigin);
    }

    macro_rules! merge_position_serde_test {
        ($($name:ident: $value:expr => $yaml_str:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let entry = OnDriftEntry {
                        conditions: "c".to_string(),
                        sync: "s".to_string(),
                        with: HashMap::new(),
                        merge_position: $value,
                    };
                    let serialized = serde_yaml::to_string(&entry).unwrap();
                    assert!(serialized.contains($yaml_str), "serialized: {serialized}");
                    let deserialized: OnDriftEntry = serde_yaml::from_str(&serialized).unwrap();
                    assert_eq!(deserialized.merge_position, $value);
                }
            )*
        };
    }

    merge_position_serde_test! {
        merge_position_round_trip_before: MergePosition::BeforeOrigin => "beforeOrigin";
        merge_position_round_trip_after: MergePosition::AfterOrigin => "afterOrigin";
    }

    fn entry(name: &str, pos: MergePosition) -> OnDriftEntry {
        OnDriftEntry {
            conditions: name.to_string(),
            sync: format!("sync-{name}"),
            with: HashMap::new(),
            merge_position: pos,
        }
    }

    fn names(entries: &[OnDriftEntry]) -> Vec<&str> {
        entries.iter().map(|e| e.conditions.as_str()).collect()
    }

    #[test]
    fn merge_all_before_origin() {
        let overlay = vec![entry("user-a", MergePosition::BeforeOrigin)];
        let origin = vec![entry("origin-a", MergePosition::BeforeOrigin)];
        let merged = merge_on_drift_entries(overlay, origin);
        assert_eq!(names(&merged), vec!["user-a", "origin-a"]);
    }

    #[test]
    fn merge_all_after_origin() {
        let overlay = vec![entry("user-a", MergePosition::AfterOrigin)];
        let origin = vec![entry("origin-a", MergePosition::BeforeOrigin)];
        let merged = merge_on_drift_entries(overlay, origin);
        assert_eq!(names(&merged), vec!["origin-a", "user-a"]);
    }

    #[test]
    fn merge_before_and_after_origin() {
        let overlay = vec![
            entry("before-1", MergePosition::BeforeOrigin),
            entry("after-1", MergePosition::AfterOrigin),
            entry("before-2", MergePosition::BeforeOrigin),
            entry("after-2", MergePosition::AfterOrigin),
        ];
        let origin = vec![
            entry("origin-1", MergePosition::BeforeOrigin),
            entry("origin-2", MergePosition::BeforeOrigin),
        ];
        let merged = merge_on_drift_entries(overlay, origin);
        assert_eq!(
            names(&merged),
            vec!["before-1", "before-2", "origin-1", "origin-2", "after-1", "after-2"]
        );
    }

    #[test]
    fn merge_empty_overlay() {
        let origin = vec![entry("origin-a", MergePosition::BeforeOrigin)];
        let merged = merge_on_drift_entries(vec![], origin);
        assert_eq!(names(&merged), vec!["origin-a"]);
    }

    #[test]
    fn merge_empty_origin() {
        let overlay = vec![
            entry("before", MergePosition::BeforeOrigin),
            entry("after", MergePosition::AfterOrigin),
        ];
        let merged = merge_on_drift_entries(overlay, vec![]);
        assert_eq!(names(&merged), vec!["before", "after"]);
    }

    #[test]
    fn merge_both_empty() {
        let merged = merge_on_drift_entries(vec![], vec![]);
        assert!(merged.is_empty());
    }
}

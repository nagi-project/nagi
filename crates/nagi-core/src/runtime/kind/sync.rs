use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::KindError;
use crate::runtime::subprocess;

pub const KIND: &str = "Sync";

/// Spec for `kind: Sync`. Defines convergence operations as a pre/run/post sequence of steps.
/// Reusable across multiple Assets via `ref`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyncSpec {
    /// Optional step executed before the main sync command.
    pub pre: Option<SyncStep>,
    /// The main convergence step.
    pub run: SyncStep,
    /// Optional step executed after the main sync command.
    pub post: Option<SyncStep>,
    /// Reference to a `kind: Identity` resource. Applied to all stages unless overridden per-stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SyncStep {
    /// Execution type for this step (currently only `Command`).
    #[serde(rename = "type")]
    pub step_type: StepType,
    /// Command and arguments in argv format.
    pub args: Vec<String>,
    /// Environment variables to set for the subprocess.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Reference to a `kind: Identity` resource. Overrides the Sync-level identity for this stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

/// Currently only `Command` (subprocess execution) is supported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum StepType {
    Command,
}

impl SyncStep {
    pub fn command(args: Vec<String>) -> Self {
        Self {
            step_type: StepType::Command,
            args,
            env: HashMap::new(),
            identity: None,
        }
    }

    fn validate(&self, step_name: &str) -> Result<(), KindError> {
        if self.args.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: format!("{step_name}.args must not be empty"),
            });
        }
        subprocess::validate_env_keys(&self.env, KIND, &format!("{step_name}.env"))?;
        Ok(())
    }
}

impl SyncSpec {
    pub fn new(run: SyncStep) -> Self {
        Self {
            pre: None,
            run,
            post: None,
            identity: None,
        }
    }

    pub fn validate(&self) -> Result<(), KindError> {
        if let Some(pre) = &self.pre {
            pre.validate("pre")?;
        }
        self.run.validate("run")?;
        if let Some(post) = &self.post {
            post.validate("post")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sync_spec_with_all_steps() {
        let yaml = r#"
pre:
  type: Command
  args: ["python", "pre.py"]
run:
  type: Command
  args: ["dbt", "run", "--select", "daily_sales"]
post:
  type: Command
  args: ["python", "post.py"]
"#;
        let spec: SyncSpec = serde_yaml::from_str(yaml).unwrap();

        let pre = spec.pre.as_ref().unwrap();
        assert_eq!(pre.step_type, StepType::Command);
        assert_eq!(pre.args, vec!["python", "pre.py"]);

        assert_eq!(spec.run.step_type, StepType::Command);
        assert_eq!(spec.run.args, vec!["dbt", "run", "--select", "daily_sales"]);

        let post = spec.post.as_ref().unwrap();
        assert_eq!(post.step_type, StepType::Command);
        assert_eq!(post.args, vec!["python", "post.py"]);
    }

    #[test]
    fn parse_sync_spec_run_only() {
        let yaml = r#"
run:
  type: Command
  args: ["dbt", "run", "--full-refresh", "--select", "daily_sales"]
"#;
        let spec: SyncSpec = serde_yaml::from_str(yaml).unwrap();

        assert!(spec.pre.is_none());
        assert_eq!(
            spec.run.args,
            vec!["dbt", "run", "--full-refresh", "--select", "daily_sales"]
        );
        assert!(spec.post.is_none());
    }

    #[test]
    fn parse_sync_spec_with_template_variable() {
        let yaml = r#"
run:
  type: Command
  args: ["dbt", "run", "--select", "{{ asset.name }}"]
"#;
        let spec: SyncSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.run.args[3], "{{ asset.name }}");
    }

    #[test]
    fn validate_rejects_empty_run_args() {
        let spec = SyncSpec::new(SyncStep::command(vec![]));
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_rejects_empty_pre_args() {
        let mut spec = SyncSpec::new(SyncStep::command(vec!["dbt".to_string()]));
        spec.pre = Some(SyncStep::command(vec![]));
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, message }
            if kind == KIND && message.contains("pre")));
    }

    #[test]
    fn validate_rejects_empty_post_args() {
        let mut spec = SyncSpec::new(SyncStep::command(vec!["dbt".to_string()]));
        spec.post = Some(SyncStep::command(vec![]));
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, message }
            if kind == KIND && message.contains("post")));
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = SyncSpec::new(SyncStep::command(vec![
            "dbt".to_string(),
            "run".to_string(),
        ]));
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn validate_rejects_invalid_env_key_in_run() {
        let mut env = HashMap::new();
        env.insert("FOO-BAR".to_string(), "x".to_string());
        let spec = SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["dbt".to_string()],
                env,
            },
            post: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, message }
            if kind == KIND && message.contains("run.env") && message.contains("FOO-BAR")));
    }

    #[test]
    fn validate_accepts_valid_env_keys() {
        let mut env = HashMap::new();
        env.insert(
            "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
            "${GAC}".to_string(),
        );
        env.insert("_PRIVATE".to_string(), "value".to_string());
        let spec = SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["dbt".to_string()],
                env,
            },
            post: None,
        };
        assert!(spec.validate().is_ok());
    }
}

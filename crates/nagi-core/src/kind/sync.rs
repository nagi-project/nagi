use serde::{Deserialize, Serialize};

use super::KindError;

pub const KIND: &str = "Sync";

/// Spec for `kind: Sync`. Defines convergence operations as a pre/run/post sequence of steps.
/// Reusable across multiple Assets via `ref`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncSpec {
    pub pre: Option<SyncStep>,
    pub run: SyncStep,
    pub post: Option<SyncStep>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncStep {
    #[serde(rename = "type")]
    pub step_type: StepType,
    pub args: Vec<String>,
}

/// Currently only `Command` (subprocess execution) is supported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StepType {
    Command,
}

impl SyncStep {
    fn validate(&self, step_name: &str) -> Result<(), KindError> {
        if self.args.is_empty() {
            return Err(KindError::InvalidSpec {
                kind: KIND.to_string(),
                message: format!("{step_name}.args must not be empty"),
            });
        }
        Ok(())
    }
}

impl SyncSpec {
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
        let spec = SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec![],
            },
            post: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(matches!(err, KindError::InvalidSpec { kind, .. } if kind == KIND));
    }

    #[test]
    fn validate_accepts_valid_spec() {
        let spec = SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["dbt".to_string(), "run".to_string()],
            },
            post: None,
        };
        assert!(spec.validate().is_ok());
    }
}

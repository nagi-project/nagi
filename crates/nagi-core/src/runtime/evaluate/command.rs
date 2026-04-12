use std::collections::HashMap;

use tokio::process::Command;

use super::{ConditionStatus, EvaluateError};
use crate::runtime::subprocess;

pub(super) async fn evaluate_command(
    run: &[String],
    env: &HashMap<String, String>,
) -> Result<ConditionStatus, EvaluateError> {
    let (program, args) = run
        .split_first()
        .expect("run must not be empty; validated at parse time");
    let mut cmd = Command::new(program);
    cmd.args(args);
    cmd.env_clear();
    cmd.envs(subprocess::build_subprocess_env(env)?);
    let status = cmd
        .status()
        .await
        .map_err(|e| EvaluateError::CommandFailed(format!("failed to spawn '{}': {e}", program)))?;
    if status.success() {
        Ok(ConditionStatus::Ready)
    } else {
        let code = status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        Ok(ConditionStatus::Drifted {
            reason: format!("'{}' exited with code {code}", program),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exit_zero_is_ready() {
        #[cfg(not(windows))]
        let run = vec!["true".to_string()];
        #[cfg(windows)]
        let run = vec!["powershell".into(), "-Command".into(), "exit 0".into()];
        let status = evaluate_command(&run, &HashMap::new()).await.unwrap();
        assert_eq!(status, ConditionStatus::Ready);
    }

    #[tokio::test]
    async fn exit_nonzero_is_drifted() {
        #[cfg(not(windows))]
        let run = vec!["false".to_string()];
        #[cfg(windows)]
        let run = vec!["powershell".into(), "-Command".into(), "exit 1".into()];
        let status = evaluate_command(&run, &HashMap::new()).await.unwrap();
        assert!(matches!(status, ConditionStatus::Drifted { .. }));
    }

    #[tokio::test]
    async fn nonexistent_program_returns_error() {
        let run = vec!["__nagi_no_such_command__".to_string()];
        let result = evaluate_command(&run, &HashMap::new()).await;
        assert!(matches!(result, Err(EvaluateError::CommandFailed(_))));
    }

    #[tokio::test]
    async fn env_vars_are_passed_to_subprocess() {
        #[cfg(not(windows))]
        let run = vec![
            "sh".to_string(),
            "-c".to_string(),
            "test \"$NAGI_TEST_VAR\" = hello".to_string(),
        ];
        #[cfg(windows)]
        let run = vec![
            "powershell".into(),
            "-Command".into(),
            "if ($env:NAGI_TEST_VAR -eq 'hello') { exit 0 } else { exit 1 }".into(),
        ];
        let mut env = HashMap::new();
        env.insert("NAGI_TEST_VAR".to_string(), "hello".to_string());
        let status = evaluate_command(&run, &env).await.unwrap();
        assert_eq!(status, ConditionStatus::Ready);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_allowlisted_parent_env_does_not_leak() {
        // CARGO is set by `cargo test` and is not in the allow-list.
        assert!(std::env::var("CARGO").is_ok());
        let run = vec![
            "sh".into(),
            "-c".into(),
            "test -z \"$CARGO\"".into(), // succeeds when CARGO is unset/empty
        ];
        let status = evaluate_command(&run, &HashMap::new()).await.unwrap();
        assert_eq!(status, ConditionStatus::Ready);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn undefined_template_var_returns_env_resolution_error() {
        let run = vec!["true".to_string()];
        let mut env = HashMap::new();
        env.insert(
            "X".to_string(),
            "${NAGI_DEFINITELY_UNSET_12345}".to_string(),
        );
        let err = evaluate_command(&run, &env).await.unwrap_err();
        assert!(matches!(err, EvaluateError::EnvResolution(_)));
    }
}

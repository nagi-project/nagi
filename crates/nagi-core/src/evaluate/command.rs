use tokio::process::Command;

use super::{ConditionStatus, EvaluateError};

pub(super) async fn evaluate_command(run: &[String]) -> Result<ConditionStatus, EvaluateError> {
    let (program, args) = run
        .split_first()
        .expect("run must not be empty; validated at parse time");
    let status = Command::new(program)
        .args(args)
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
        let run = vec!["true".to_string()];
        let status = evaluate_command(&run).await.unwrap();
        assert_eq!(status, ConditionStatus::Ready);
    }

    #[tokio::test]
    async fn exit_nonzero_is_not_ready() {
        let run = vec!["false".to_string()];
        let status = evaluate_command(&run).await.unwrap();
        assert!(matches!(status, ConditionStatus::Drifted { .. }));
    }

    #[tokio::test]
    async fn nonexistent_program_returns_error() {
        let run = vec!["__nagi_no_such_command__".to_string()];
        let result = evaluate_command(&run).await;
        assert!(matches!(result, Err(EvaluateError::CommandFailed(_))));
    }
}

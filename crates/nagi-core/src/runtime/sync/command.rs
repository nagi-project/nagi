use std::process::Stdio;

use chrono::Utc;
use tokio::process::Command;

use super::{Stage, StageResult, SyncError};
use crate::runtime::kind::sync::SyncStep;
use crate::runtime::subprocess;

/// Executes a single sync step as a subprocess.
///
/// The first element of `args` is the program, the rest are arguments passed
/// to it. stdout and stderr are captured in full.
pub async fn execute_step(stage: Stage, step: &SyncStep) -> Result<StageResult, SyncError> {
    let args = &step.args;
    let program = &args[0];
    let cmd_args = &args[1..];

    let started_at = Utc::now().to_rfc3339();

    let mut cmd = Command::new(program);
    cmd.args(cmd_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd.env_clear();
    cmd.envs(subprocess::build_subprocess_env(None, &step.env)?);
    let output = cmd
        .spawn()
        .map_err(|e| SyncError::SpawnFailed(format!("{program}: {e}")))?
        .wait_with_output()
        .await?;

    let finished_at = Utc::now().to_rfc3339();

    Ok(StageResult {
        stage,
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        started_at,
        finished_at,
        args: args.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::subprocess::SubprocessEnvError;
    use std::collections::HashMap;

    fn step_with_env(args: Vec<&str>, env: &[(&str, &str)]) -> SyncStep {
        let mut step = SyncStep::command(args.into_iter().map(String::from).collect());
        step.env = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        step
    }

    #[cfg(unix)]
    async fn run_echo_var(var_name: &str, step_env: &[(&str, &str)]) -> StageResult {
        let step = step_with_env(
            vec!["sh", "-c", &format!("printf %s \"${{{var_name}}}\"")],
            step_env,
        );
        execute_step(Stage::Run, &step).await.unwrap()
    }

    #[cfg(windows)]
    async fn run_echo_var(var_name: &str, step_env: &[(&str, &str)]) -> StageResult {
        let step = step_with_env(
            vec![
                "powershell",
                "-Command",
                &format!("[Console]::Write($env:{var_name})"),
            ],
            step_env,
        );
        execute_step(Stage::Run, &step).await.unwrap()
    }

    #[tokio::test]
    async fn declared_env_reaches_subprocess() {
        let result = run_echo_var("FOO", &[("FOO", "bar")]).await;
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, "bar");
    }

    #[tokio::test]
    async fn non_allowlisted_parent_env_does_not_leak() {
        // CARGO is set by `cargo test` and is not in the allow-list.
        assert!(
            std::env::var("CARGO").is_ok(),
            "test harness invariant: CARGO should be set under `cargo test`"
        );
        let result = run_echo_var("CARGO", &[]).await;
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, "");
    }

    #[tokio::test]
    async fn template_expansion_reads_parent_env() {
        #[cfg(unix)]
        let home_key = "HOME";
        #[cfg(windows)]
        let home_key = "USERPROFILE";

        let home = std::env::var(home_key).expect("home dir env must be set");
        let template = format!("${{{home_key}}}");
        let result = run_echo_var("CUSTOM_HOME", &[("CUSTOM_HOME", &template)]).await;
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, home);
    }

    #[tokio::test]
    async fn undefined_template_var_returns_env_resolution_error() {
        #[cfg(unix)]
        let step = step_with_env(
            vec!["sh", "-c", "true"],
            &[("X", "${NAGI_DEFINITELY_UNSET_12345}")],
        );
        #[cfg(windows)]
        let step = step_with_env(
            vec!["powershell", "-Command", "exit 0"],
            &[("X", "${NAGI_DEFINITELY_UNSET_12345}")],
        );
        let err = execute_step(Stage::Run, &step).await.unwrap_err();
        match err {
            SyncError::EnvResolution(SubprocessEnvError::UndefinedVar(name)) => {
                assert_eq!(name, "NAGI_DEFINITELY_UNSET_12345");
            }
            other => panic!("expected EnvResolution/UndefinedVar, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_declared_key_returns_env_resolution_error() {
        let mut env = HashMap::new();
        env.insert("FOO-BAR".to_string(), "x".to_string());
        #[cfg(unix)]
        let args = vec!["sh".into(), "-c".into(), "true".into()];
        #[cfg(windows)]
        let args = vec!["powershell".into(), "-Command".into(), "exit 0".into()];
        let mut step = SyncStep::command(args);
        step.env = env;
        let err = execute_step(Stage::Run, &step).await.unwrap_err();
        assert!(matches!(
            err,
            SyncError::EnvResolution(SubprocessEnvError::InvalidKeyName(_))
        ));
    }
}

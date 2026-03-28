use std::process::Stdio;

use chrono::Utc;
use tokio::process::Command;

use super::{Stage, StageResult, SyncError};
use crate::runtime::kind::sync::SyncStep;

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
    for (key, value) in &step.env {
        cmd.env(key, value);
    }
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

use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use tokio::process::Command;

use super::{Stage, StageResult, SyncError};
use crate::runtime::kind::sync::SyncStep;
use crate::runtime::subprocess;

/// Executes a single sync step as a subprocess.
///
/// The first element of `args` is the program, the rest are arguments passed
/// to it. stdout and stderr are captured in full. The step is aborted after
/// `timeout` elapses; a timed-out step is killed and returns `SyncError::Timeout`.
///
/// `execution_id` is injected as `NAGI_EXECUTION_ID` and used to derive
/// `TRACEPARENT` (W3C Trace Context). These env vars are set after the
/// user-declared env, so they cannot be overridden by Sync definitions.
pub async fn execute_step(
    stage: Stage,
    step: &SyncStep,
    execution_id: &str,
    timeout: Duration,
) -> Result<StageResult, SyncError> {
    let args = &step.args;
    let program = &args[0];
    let cmd_args = &args[1..];

    let started_at = Utc::now().to_rfc3339();

    let mut env = subprocess::build_subprocess_env(None, &step.env)?;
    env.insert("NAGI_EXECUTION_ID".to_string(), execution_id.to_string());
    env.insert("TRACEPARENT".to_string(), build_traceparent(execution_id));

    let mut cmd = Command::new(program);
    cmd.args(cmd_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd.env_clear();
    cmd.envs(env);
    let child = cmd
        .spawn()
        .map_err(|e| SyncError::SpawnFailed(format!("{program}: {e}")))?;

    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(res) => res?,
        Err(_) => {
            return Err(SyncError::Timeout {
                stage: stage_label(stage).to_string(),
                seconds: timeout.as_secs(),
            });
        }
    };

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

fn stage_label(stage: Stage) -> &'static str {
    match stage {
        Stage::Pre => "pre",
        Stage::Run => "run",
        Stage::Post => "post",
    }
}

/// Builds a W3C Trace Context `traceparent` header value from an execution_id.
///
/// Format: `00-<trace_id>-<span_id>-01`
/// - trace_id: execution_id with hyphens removed (32 hex chars)
/// - span_id: pseudo-random 16 hex chars (8 bytes), guaranteed non-zero
fn build_traceparent(execution_id: &str) -> String {
    let trace_id = execution_id.replace('-', "");
    debug_assert_eq!(trace_id.len(), 32, "execution_id must yield 32 hex chars");
    let span_id = generate_span_id();
    format!("00-{trace_id}-{span_id}-01")
}

fn generate_span_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let tid = format!("{:?}", std::thread::current().id());
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in tid.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let random = hash ^ (now as u64) ^ seq;
    let random = if random == 0 { 1 } else { random };
    format!("{random:016x}")
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

    const TEST_EXECUTION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn test_timeout() -> Duration {
        Duration::from_secs(30)
    }

    #[cfg(unix)]
    async fn run_echo_var(var_name: &str, step_env: &[(&str, &str)]) -> StageResult {
        let step = step_with_env(
            vec!["sh", "-c", &format!("printf %s \"${{{var_name}}}\"")],
            step_env,
        );
        execute_step(Stage::Run, &step, TEST_EXECUTION_ID, test_timeout())
            .await
            .unwrap()
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
        execute_step(Stage::Run, &step, TEST_EXECUTION_ID, test_timeout())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn declared_env_reaches_subprocess() {
        let result = run_echo_var("FOO", &[("FOO", "bar")]).await;
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, "bar");
    }

    #[tokio::test]
    async fn non_allowlisted_parent_env_does_not_leak() {
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
        let err = execute_step(Stage::Run, &step, TEST_EXECUTION_ID, test_timeout())
            .await
            .unwrap_err();
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
        let err = execute_step(Stage::Run, &step, TEST_EXECUTION_ID, test_timeout())
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SyncError::EnvResolution(SubprocessEnvError::InvalidKeyName(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn times_out_when_step_exceeds_deadline() {
        let step = SyncStep::command(vec!["sleep".into(), "5".into()]);
        let err = execute_step(
            Stage::Run,
            &step,
            TEST_EXECUTION_ID,
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SyncError::Timeout { .. }));
    }

    // ── NAGI_EXECUTION_ID / TRACEPARENT injection ───────────────────

    #[tokio::test]
    async fn nagi_execution_id_reaches_subprocess() {
        let result = run_echo_var("NAGI_EXECUTION_ID", &[]).await;
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, TEST_EXECUTION_ID);
    }

    #[tokio::test]
    async fn traceparent_has_valid_w3c_format() {
        let result = run_echo_var("TRACEPARENT", &[]).await;
        assert_eq!(result.exit_code, 0);
        let tp = &result.stdout;
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(
            parts.len(),
            4,
            "TRACEPARENT should have 4 dash-separated parts: {tp}"
        );
        assert_eq!(parts[0], "00", "version should be 00");
        assert_eq!(parts[1].len(), 32, "trace-id should be 32 hex chars");
        assert_eq!(parts[2].len(), 16, "span-id should be 16 hex chars");
        assert_eq!(parts[3], "01", "flags should be 01");
        assert!(
            parts[1].chars().all(|c| c.is_ascii_hexdigit()),
            "trace-id should be hex"
        );
        assert!(
            parts[2].chars().all(|c| c.is_ascii_hexdigit()),
            "span-id should be hex"
        );
    }

    #[tokio::test]
    async fn traceparent_trace_id_derived_from_execution_id() {
        let result = run_echo_var("TRACEPARENT", &[]).await;
        let tp = &result.stdout;
        let parts: Vec<&str> = tp.split('-').collect();
        let trace_id = parts[1];
        let expected_trace_id = TEST_EXECUTION_ID.replace('-', "");
        assert_eq!(trace_id, expected_trace_id);
    }

    #[tokio::test]
    async fn traceparent_span_id_is_not_all_zeros() {
        let result = run_echo_var("TRACEPARENT", &[]).await;
        let tp = &result.stdout;
        let parts: Vec<&str> = tp.split('-').collect();
        let span_id = parts[2];
        assert_ne!(span_id, "0000000000000000", "span-id must not be all zeros");
    }

    #[test]
    fn build_traceparent_format() {
        let tp = build_traceparent(TEST_EXECUTION_ID);
        assert!(tp.starts_with("00-"));
        assert!(tp.ends_with("-01"));
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[1].len(), 32);
        assert_eq!(parts[2].len(), 16);
    }

    #[test]
    fn build_traceparent_trace_id_matches_execution_id() {
        let tp = build_traceparent(TEST_EXECUTION_ID);
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts[1], TEST_EXECUTION_ID.replace('-', ""));
    }

    #[test]
    fn span_id_is_16_hex_chars() {
        let id = generate_span_id();
        assert_eq!(id.len(), 16);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn span_id_is_not_all_zeros() {
        let id = generate_span_id();
        assert_ne!(id, "0000000000000000");
    }

    #[test]
    fn span_id_successive_calls_differ() {
        let a = generate_span_id();
        let b = generate_span_id();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn declared_env_cannot_override_nagi_execution_id() {
        let result =
            run_echo_var("NAGI_EXECUTION_ID", &[("NAGI_EXECUTION_ID", "user-value")]).await;
        assert_eq!(result.exit_code, 0);
        assert_eq!(
            result.stdout, TEST_EXECUTION_ID,
            "Nagi-injected value must take precedence"
        );
    }
}

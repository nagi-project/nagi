mod command;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::runtime::kind::sync::{SyncSpec, SyncStep};
use crate::runtime::log::{LogError, LogStore};

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("sync spec not defined for asset '{asset_name}'")]
    NoSyncSpec { asset_name: String },

    #[error("stage '{stage}' is not defined in sync spec")]
    StageNotDefined { stage: String },

    #[error("failed to spawn process: {0}")]
    SpawnFailed(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("log error: {0}")]
    Log(#[from] LogError),

    #[error("failed to parse compiled asset: {0}")]
    Parse(String),

    #[error("compile error: {0}")]
    Compile(#[from] crate::runtime::compile::CompileError),

    #[error("dbt Cloud error: {0}")]
    DbtCloud(String),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("invalid sync_type: {0}")]
    InvalidSyncType(String),
}

/// Which type of sync operation is being executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncType {
    Sync,
}

impl std::fmt::Display for SyncType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncType::Sync => write!(f, "sync"),
        }
    }
}

/// A stage within a sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stage {
    Pre,
    Run,
    Post,
}

impl Stage {
    /// Parses a comma-separated list of stage names.
    pub fn parse_list(input: &str) -> Result<Vec<Stage>, SyncError> {
        input
            .split(',')
            .map(|s| match s.trim() {
                "pre" => Ok(Stage::Pre),
                "run" => Ok(Stage::Run),
                "post" => Ok(Stage::Post),
                other => Err(SyncError::StageNotDefined {
                    stage: other.to_string(),
                }),
            })
            .collect()
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stage::Pre => write!(f, "pre"),
            Stage::Run => write!(f, "run"),
            Stage::Post => write!(f, "post"),
        }
    }
}

/// Result of executing a single stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageResult {
    pub stage: Stage,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub started_at: String,
    pub finished_at: String,
    pub args: Vec<String>,
}

impl StageResult {
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }
}

/// Result of a complete sync execution (may span multiple stages).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncExecutionResult {
    pub execution_id: String,
    pub asset_name: String,
    pub sync_type: SyncType,
    pub stages: Vec<StageResult>,
    pub success: bool,
}

/// Dry-run summary of what a sync would execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DryRunSyncResult {
    pub asset_name: String,
    pub sync_type: SyncType,
    pub stages: Vec<DryRunStage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DryRunStage {
    pub stage: Stage,
    pub args: Vec<String>,
}

/// Returns the step for a given stage. Panics are impossible when used after
/// `resolve_stages`, which only yields stages whose steps exist.
fn step_for_stage(spec: &SyncSpec, stage: Stage) -> &SyncStep {
    match stage {
        Stage::Pre => spec
            .pre
            .as_ref()
            .expect("resolve_stages guarantees pre exists"),
        Stage::Run => &spec.run,
        Stage::Post => spec
            .post
            .as_ref()
            .expect("resolve_stages guarantees post exists"),
    }
}

/// Determines which stages are defined in the given SyncSpec and filters by the
/// requested stages list. Returns the stages in execution order (pre → run → post).
fn resolve_stages(spec: &SyncSpec, requested: Option<&[Stage]>) -> Vec<Stage> {
    let defined: Vec<Stage> = [
        spec.pre.as_ref().map(|_| Stage::Pre),
        Some(Stage::Run),
        spec.post.as_ref().map(|_| Stage::Post),
    ]
    .into_iter()
    .flatten()
    .collect();

    match requested {
        Some(filter) => defined.into_iter().filter(|s| filter.contains(s)).collect(),
        None => defined,
    }
}

/// Executes the sync operation for the given asset.
///
/// Runs the stages in order (pre → run → post), short-circuiting on the first
/// non-zero exit code. When `stages` is `None`, all defined stages are executed.
/// When `log_store` is `Some`, automatically writes sync logs after execution.
pub async fn execute_sync(
    asset_name: &str,
    sync_spec: &SyncSpec,
    sync_type: SyncType,
    stages: Option<&[Stage]>,
    log_store: Option<&LogStore>,
) -> Result<SyncExecutionResult, SyncError> {
    let result = execute_sync_core(asset_name, sync_spec, sync_type, stages).await?;

    if let Some(store) = log_store {
        store.write_sync_log(&result)?;
    }

    Ok(result)
}

/// Executes sync stages without logging. The returned future is `Send`,
/// making it safe to spawn on a `JoinSet` (unlike `execute_sync` which
/// accepts `&LogStore` that is `!Send`).
pub async fn execute_sync_core(
    asset_name: &str,
    sync_spec: &SyncSpec,
    sync_type: SyncType,
    stages: Option<&[Stage]>,
) -> Result<SyncExecutionResult, SyncError> {
    let execution_id = generate_uuid();
    let stages_to_run = resolve_stages(sync_spec, stages);

    let mut results = Vec::new();
    let mut overall_success = true;

    for stage in stages_to_run {
        let step = step_for_stage(sync_spec, stage);
        let result = command::execute_step(stage, step).await?;
        let succeeded = result.success();
        results.push(result);
        if !succeeded {
            overall_success = false;
            break;
        }
    }

    Ok(SyncExecutionResult {
        execution_id,
        asset_name: asset_name.to_string(),
        sync_type,
        stages: results,
        success: overall_success,
    })
}

/// Returns a dry-run summary without executing anything.
pub fn dry_run_sync(
    asset_name: &str,
    sync_spec: &SyncSpec,
    sync_type: SyncType,
    stages: Option<&[Stage]>,
) -> DryRunSyncResult {
    let stages_to_run = resolve_stages(sync_spec, stages);
    let mut dry_stages = Vec::new();
    for stage in stages_to_run {
        let step = step_for_stage(sync_spec, stage);
        dry_stages.push(DryRunStage {
            stage,
            args: step.args.clone(),
        });
    }
    DryRunSyncResult {
        asset_name: asset_name.to_string(),
        sync_type,
        stages: dry_stages,
    }
}

pub(crate) fn generate_uuid() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let nanos = now.as_nanos();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let random: u64 = {
        // Pseudo-random from time + thread id hash + monotonic counter.
        let tid = format!("{:?}", std::thread::current().id());
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in tid.bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash ^ (nanos as u64) ^ seq
    };
    // UUID v4-like format.
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (nanos & 0xFFFF_FFFF) as u32,
        ((nanos >> 32) & 0xFFFF) as u16,
        (random & 0x0FFF) as u16,
        (0x8000 | (random >> 12) & 0x3FFF) as u16,
        random & 0x0000_FFFF_FFFF_FFFF,
    )
}

pub(crate) fn parse_sync_type(s: &str) -> Result<SyncType, SyncError> {
    match s {
        "sync" => Ok(SyncType::Sync),
        other => Err(SyncError::InvalidSyncType(other.to_string())),
    }
}

/// Resolves the sync spec from the first on_drift entry (first-match).
pub(crate) fn resolve_sync_spec(
    compiled: &crate::runtime::compile::CompiledAsset,
) -> Result<SyncSpec, SyncError> {
    compiled
        .spec
        .on_drift
        .first()
        .map(|entry| entry.sync.clone())
        .ok_or_else(|| SyncError::NoSyncSpec {
            asset_name: compiled.metadata.name.clone(),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::runtime::kind::sync::{StepType, SyncStep};

    fn run_only_spec() -> SyncSpec {
        SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "hello".to_string()],
                env: HashMap::new(),
            },
            post: None,
        }
    }

    fn full_spec() -> SyncSpec {
        SyncSpec {
            pre: Some(SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "pre".to_string()],
                env: HashMap::new(),
            }),
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "run".to_string()],
                env: HashMap::new(),
            },
            post: Some(SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "post".to_string()],
                env: HashMap::new(),
            }),
        }
    }

    // ── resolve_stages ───────────────────────────────────────────────────

    macro_rules! resolve_stages_test {
        ($($name:ident: $spec:expr, $filter:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let filter: Option<Vec<Stage>> = $filter;
                    assert_eq!(resolve_stages(&$spec, filter.as_deref()), $expected);
                }
            )*
        };
    }

    resolve_stages_test! {
        resolve_stages_all_defined_no_filter:
            full_spec(), None => vec![Stage::Pre, Stage::Run, Stage::Post];
        resolve_stages_run_only_no_filter:
            run_only_spec(), None => vec![Stage::Run];
        resolve_stages_filter_pre_run:
            full_spec(), Some(vec![Stage::Pre, Stage::Run]) => vec![Stage::Pre, Stage::Run];
        resolve_stages_filter_undefined_stage_ignored:
            run_only_spec(), Some(vec![Stage::Pre]) => vec![];
        resolve_stages_preserves_order:
            full_spec(), Some(vec![Stage::Post, Stage::Pre]) => vec![Stage::Pre, Stage::Post];
    }

    // ── Stage::parse_list ────────────────────────────────────────────────

    macro_rules! parse_stage_list_test {
        ($($name:ident: $input:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(Stage::parse_list($input).unwrap(), $expected);
                }
            )*
        };
    }

    parse_stage_list_test! {
        parse_stage_list_single: "run" => vec![Stage::Run];
        parse_stage_list_multiple: "pre,run" => vec![Stage::Pre, Stage::Run];
        parse_stage_list_with_spaces: "pre , post" => vec![Stage::Pre, Stage::Post];
    }

    #[test]
    fn parse_stage_list_invalid() {
        let err = Stage::parse_list("pre,invalid").unwrap_err();
        assert!(matches!(err, SyncError::StageNotDefined { stage } if stage == "invalid"));
    }

    // ── execute_sync ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn execute_run_only_success() {
        let result = execute_sync("test-asset", &run_only_spec(), SyncType::Sync, None, None)
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.stages.len(), 1);
        assert_eq!(result.stages[0].stage, Stage::Run);
        assert_eq!(result.stages[0].exit_code, 0);
        assert_eq!(result.asset_name, "test-asset");
        assert_eq!(result.sync_type, SyncType::Sync);
        assert!(!result.execution_id.is_empty());
    }

    #[tokio::test]
    async fn execute_full_spec_success() {
        let result = execute_sync("test-asset", &full_spec(), SyncType::Sync, None, None)
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.stages.len(), 3);
        assert_eq!(result.stages[0].stage, Stage::Pre);
        assert_eq!(result.stages[1].stage, Stage::Run);
        assert_eq!(result.stages[2].stage, Stage::Post);
    }

    #[tokio::test]
    async fn execute_captures_stdout() {
        let spec = SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "hello world".to_string()],
                env: HashMap::new(),
            },
            post: None,
        };
        let result = execute_sync("test-asset", &spec, SyncType::Sync, None, None)
            .await
            .unwrap();
        assert_eq!(result.stages[0].stdout.trim(), "hello world");
    }

    #[tokio::test]
    async fn execute_captures_stderr() {
        let spec = SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "echo error >&2".to_string(),
                ],
                env: HashMap::new(),
            },
            post: None,
        };
        let result = execute_sync("test-asset", &spec, SyncType::Sync, None, None)
            .await
            .unwrap();
        assert_eq!(result.stages[0].stderr.trim(), "error");
    }

    #[tokio::test]
    async fn execute_short_circuits_on_failure() {
        let spec = SyncSpec {
            pre: Some(SyncStep {
                step_type: StepType::Command,
                args: vec!["false".to_string()],
                env: HashMap::new(),
            }),
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "should not run".to_string()],
                env: HashMap::new(),
            },
            post: None,
        };
        let result = execute_sync("test-asset", &spec, SyncType::Sync, None, None)
            .await
            .unwrap();
        assert!(!result.success);
        assert_eq!(result.stages.len(), 1, "only pre should have run");
        assert_eq!(result.stages[0].stage, Stage::Pre);
        assert_ne!(result.stages[0].exit_code, 0);
    }

    #[tokio::test]
    async fn execute_with_stage_filter() {
        let result = execute_sync(
            "test-asset",
            &full_spec(),
            SyncType::Sync,
            Some(&[Stage::Run]),
            None,
        )
        .await
        .unwrap();
        assert!(result.success);
        assert_eq!(result.stages.len(), 1);
        assert_eq!(result.stages[0].stage, Stage::Run);
    }

    #[tokio::test]
    async fn execute_records_timestamps() {
        let result = execute_sync("test-asset", &run_only_spec(), SyncType::Sync, None, None)
            .await
            .unwrap();
        let stage = &result.stages[0];
        // Should be valid ISO 8601 timestamps.
        assert!(!stage.started_at.is_empty());
        assert!(!stage.finished_at.is_empty());
        assert!(stage.started_at <= stage.finished_at);
    }

    #[tokio::test]
    async fn execute_records_args() {
        let result = execute_sync("test-asset", &run_only_spec(), SyncType::Sync, None, None)
            .await
            .unwrap();
        assert_eq!(result.stages[0].args, vec!["echo", "hello"]);
    }

    #[tokio::test]
    async fn execute_nonexistent_command_returns_error() {
        let spec = SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["__nagi_no_such_command__".to_string()],
                env: HashMap::new(),
            },
            post: None,
        };
        let err = execute_sync("test-asset", &spec, SyncType::Sync, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, SyncError::SpawnFailed(_)));
    }

    // ── dry_run_sync ─────────────────────────────────────────────────────

    #[test]
    fn dry_run_shows_all_stages() {
        let result = dry_run_sync("test-asset", &full_spec(), SyncType::Sync, None);
        assert_eq!(result.stages.len(), 3);
        assert_eq!(result.stages[0].stage, Stage::Pre);
        assert_eq!(result.stages[0].args, vec!["echo", "pre"]);
        assert_eq!(result.stages[1].stage, Stage::Run);
        assert_eq!(result.stages[2].stage, Stage::Post);
    }

    #[test]
    fn dry_run_with_stage_filter() {
        let result = dry_run_sync(
            "test-asset",
            &full_spec(),
            SyncType::Sync,
            Some(&[Stage::Run]),
        );
        assert_eq!(result.stages.len(), 1);
        assert_eq!(result.stages[0].stage, Stage::Run);
    }

    #[test]
    fn dry_run_run_only() {
        let result = dry_run_sync("test-asset", &run_only_spec(), SyncType::Sync, None);
        assert_eq!(result.asset_name, "test-asset");
        assert_eq!(result.sync_type, SyncType::Sync);
        assert_eq!(result.stages.len(), 1);
    }

    // ── generate_uuid ────────────────────────────────────────────────────

    #[test]
    fn generate_uuid_is_unique() {
        let a = generate_uuid();
        let b = generate_uuid();
        assert_ne!(a, b);
    }

    #[test]
    fn generate_uuid_has_correct_format() {
        let uuid = generate_uuid();
        let parts: Vec<&str> = uuid.split('-').collect();
        assert_eq!(parts.len(), 5);
        assert!(parts[2].starts_with('4'), "UUID version should be 4");
    }
}

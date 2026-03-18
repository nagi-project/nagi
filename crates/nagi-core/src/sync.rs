mod execute;

use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::compile::CompiledAsset;
use crate::kind::sync::{SyncSpec, SyncStep};
use crate::log::{LogError, LogStore};

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
    Compile(#[from] crate::compile::CompileError),

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
    Resync,
}

impl std::fmt::Display for SyncType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncType::Sync => write!(f, "sync"),
            SyncType::Resync => write!(f, "resync"),
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
        Stage::Pre => spec.pre.as_ref().expect("resolve_stages guarantees pre exists"),
        Stage::Run => &spec.run,
        Stage::Post => spec.post.as_ref().expect("resolve_stages guarantees post exists"),
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
    let execution_id = generate_uuid();
    let stages_to_run = resolve_stages(sync_spec, stages);

    let mut results = Vec::new();
    let mut overall_success = true;

    for stage in stages_to_run {
        let step = step_for_stage(sync_spec, stage);
        let result = execute::execute_step(stage, step).await?;
        let succeeded = result.success();
        results.push(result);
        if !succeeded {
            overall_success = false;
            break;
        }
    }

    let result = SyncExecutionResult {
        execution_id,
        asset_name: asset_name.to_string(),
        sync_type,
        stages: results,
        success: overall_success,
    };

    if let Some(store) = log_store {
        store.write_sync_log(&result)?;
    }

    Ok(result)
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
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let nanos = now.as_nanos();
    let random: u64 = {
        // Simple pseudo-random from time + thread id hash.
        let tid = format!("{:?}", std::thread::current().id());
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in tid.bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash ^ (nanos as u64)
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

fn parse_sync_type(s: &str) -> Result<SyncType, SyncError> {
    match s {
        "sync" => Ok(SyncType::Sync),
        "resync" => Ok(SyncType::Resync),
        other => Err(SyncError::InvalidSyncType(other.to_string())),
    }
}

fn resolve_sync_spec(compiled: &CompiledAsset, st: SyncType) -> Result<SyncSpec, SyncError> {
    let spec = match st {
        SyncType::Sync => compiled.spec.sync.clone(),
        SyncType::Resync => compiled.spec.resync.clone().or(compiled.spec.sync.clone()),
    };
    spec.ok_or_else(|| SyncError::NoSyncSpec {
        asset_name: compiled.metadata.name.clone(),
    })
}

/// Builds sync proposals for all compiled assets matching the selectors.
///
/// Evaluation or dry-run failures are not fatal — each proposal will omit
/// whichever part failed.
pub async fn propose_sync_all(
    target_dir: &Path,
    selectors: &[&str],
    sync_type: &str,
    stages: Option<&str>,
    cache_dir: Option<&Path>,
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
) -> Result<Vec<SyncProposal>, SyncError> {
    let assets = crate::compile::load_compiled_assets(target_dir, selectors)?;
    let st = parse_sync_type(sync_type)?;
    let mut proposals = Vec::with_capacity(assets.len());

    for (name, yaml) in &assets {
        let evaluation = match crate::evaluate::evaluate_from_compiled(yaml, cache_dir, db_path, logs_dir).await {
            Ok(json) => serde_json::from_str(&json).ok(),
            Err(_) => None,
        };

        let compiled: CompiledAsset =
            serde_yaml::from_str(yaml).map_err(|e| SyncError::Parse(e.to_string()))?;
        let dry_run_stages = match resolve_sync_spec(&compiled, st) {
            Ok(sync_spec) => {
                let parsed_stages = stages.map(Stage::parse_list).transpose()?;
                let dr = dry_run_sync(name, &sync_spec, st, parsed_stages.as_deref());
                Some(dr.stages)
            }
            Err(_) => None,
        };

        proposals.push(SyncProposal {
            asset: name.clone(),
            yaml_content: yaml.clone(),
            sync_type: st,
            evaluation,
            stages: dry_run_stages,
        });
    }

    Ok(proposals)
}

/// Result of `propose_sync`: evaluation + dry-run stages for user confirmation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncProposal {
    pub asset: String,
    #[serde(skip)]
    pub yaml_content: String,
    pub sync_type: SyncType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<SyncProposalEvaluation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stages: Option<Vec<DryRunStage>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncProposalEvaluation {
    pub ready: bool,
    pub conditions: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_id: Option<String>,
}

/// Checks dbt Cloud for running jobs. Returns an error if any are found.
async fn check_dbt_cloud_preflight(compiled: &CompiledAsset) -> Result<(), SyncError> {
    let cred_path = compiled
        .connection
        .as_ref()
        .and_then(|c| c.dbt_cloud_credentials_file.as_ref());
    let Some(cred_path) = cred_path else {
        return Ok(());
    };
    let jobs = crate::dbt::cloud::check_running_jobs(Path::new(cred_path))
        .await
        .map_err(|e| SyncError::DbtCloud(e.to_string()))?;
    if !jobs.is_empty() {
        let details: Vec<String> = jobs
            .iter()
            .map(|j| format!("  {} ({})", j.job_name, j.status_humanized))
            .collect();
        return Err(SyncError::DbtCloud(format!(
            "dbt Cloud has running jobs:\n{}\nUse --force to override.",
            details.join("\n")
        )));
    }
    Ok(())
}

fn open_log_store(
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
) -> Result<Option<LogStore>, SyncError> {
    match (db_path, logs_dir) {
        (Some(db), Some(logs)) => Ok(Some(LogStore::open(db, logs)?)),
        _ => Ok(None),
    }
}

fn serialize<T: Serialize>(value: &T) -> Result<String, SyncError> {
    serde_json::to_string(value).map_err(|e| SyncError::Serialize(e.to_string()))
}

/// Executes sync from compiled asset YAML.
///
/// Parameters for `sync_from_compiled`.
pub struct SyncFromCompiledParams<'a> {
    pub yaml: &'a str,
    pub sync_type: &'a str,
    pub stages: Option<&'a str>,
    pub db_path: Option<&'a Path>,
    pub logs_dir: Option<&'a Path>,
    pub cache_dir: Option<&'a Path>,
    pub dry_run: bool,
    pub force: bool,
    pub evaluation_id: Option<&'a str>,
}

/// Handles sync type/spec resolution, dry-run, dbt Cloud pre-flight check,
/// logging, evaluation linking, and post-sync re-evaluation.
pub async fn sync_from_compiled(params: SyncFromCompiledParams<'_>) -> Result<String, SyncError> {
    let compiled: CompiledAsset =
        serde_yaml::from_str(params.yaml).map_err(|e| SyncError::Parse(e.to_string()))?;
    let st = parse_sync_type(params.sync_type)?;
    let sync_spec = resolve_sync_spec(&compiled, st)?;
    let parsed_stages = params.stages.map(Stage::parse_list).transpose()?;

    if params.dry_run {
        let result = dry_run_sync(
            &compiled.metadata.name,
            &sync_spec,
            st,
            parsed_stages.as_deref(),
        );
        return serialize(&result);
    }

    if !params.force {
        check_dbt_cloud_preflight(&compiled).await?;
    }

    let log_store = open_log_store(params.db_path, params.logs_dir)?;
    let result = execute_sync(
        &compiled.metadata.name,
        &sync_spec,
        st,
        parsed_stages.as_deref(),
        log_store.as_ref(),
    )
    .await?;

    // Link pre-sync evaluation to this execution.
    if let (Some(store), Some(eval_id)) = (log_store.as_ref(), params.evaluation_id) {
        let _ = store.write_sync_evaluation(&result.execution_id, eval_id);
    }

    // Re-evaluate after sync (only when no stage filter).
    if params.stages.is_none() {
        let _ =
            post_sync_re_evaluate(params.yaml, params.cache_dir, params.db_path, params.logs_dir, &result)
                .await;
    }

    serialize(&result)
}

/// Re-evaluates the asset after sync and links the new evaluation to the execution.
async fn post_sync_re_evaluate(
    yaml: &str,
    cache_dir: Option<&Path>,
    db_path: Option<&Path>,
    logs_dir: Option<&Path>,
    sync_result: &SyncExecutionResult,
) -> Result<(), SyncError> {
    let eval_json = crate::evaluate::evaluate_from_compiled(yaml, cache_dir, db_path, logs_dir)
        .await
        .map_err(|e| SyncError::Parse(e.to_string()))?;

    if let (Some(db), Some(logs)) = (db_path, logs_dir) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&eval_json) {
            if let Some(eval_id) = val.get("evaluationId").and_then(|v| v.as_str()) {
                let store = LogStore::open(db, logs)?;
                let _ = store.write_sync_evaluation(&sync_result.execution_id, eval_id);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kind::sync::{StepType, SyncStep};

    fn run_only_spec() -> SyncSpec {
        SyncSpec {
            pre: None,
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "hello".to_string()],
            },
            post: None,
        }
    }

    fn full_spec() -> SyncSpec {
        SyncSpec {
            pre: Some(SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "pre".to_string()],
            }),
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "run".to_string()],
            },
            post: Some(SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "post".to_string()],
            }),
        }
    }

    // ── resolve_stages ───────────────────────────────────────────────────

    #[test]
    fn resolve_stages_all_defined_no_filter() {
        let stages = resolve_stages(&full_spec(), None);
        assert_eq!(stages, vec![Stage::Pre, Stage::Run, Stage::Post]);
    }

    #[test]
    fn resolve_stages_run_only_no_filter() {
        let stages = resolve_stages(&run_only_spec(), None);
        assert_eq!(stages, vec![Stage::Run]);
    }

    #[test]
    fn resolve_stages_filter_pre_run() {
        let stages = resolve_stages(&full_spec(), Some(&[Stage::Pre, Stage::Run]));
        assert_eq!(stages, vec![Stage::Pre, Stage::Run]);
    }

    #[test]
    fn resolve_stages_filter_undefined_stage_ignored() {
        // Requesting pre on a spec without pre → empty.
        let stages = resolve_stages(&run_only_spec(), Some(&[Stage::Pre]));
        assert!(stages.is_empty());
    }

    #[test]
    fn resolve_stages_preserves_order() {
        // Even if requested out of order, result follows pre → run → post.
        let stages = resolve_stages(&full_spec(), Some(&[Stage::Post, Stage::Pre]));
        assert_eq!(stages, vec![Stage::Pre, Stage::Post]);
    }

    // ── Stage::parse_list ────────────────────────────────────────────────

    #[test]
    fn parse_stage_list_single() {
        let stages = Stage::parse_list("run").unwrap();
        assert_eq!(stages, vec![Stage::Run]);
    }

    #[test]
    fn parse_stage_list_multiple() {
        let stages = Stage::parse_list("pre,run").unwrap();
        assert_eq!(stages, vec![Stage::Pre, Stage::Run]);
    }

    #[test]
    fn parse_stage_list_with_spaces() {
        let stages = Stage::parse_list("pre , post").unwrap();
        assert_eq!(stages, vec![Stage::Pre, Stage::Post]);
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
            }),
            run: SyncStep {
                step_type: StepType::Command,
                args: vec!["echo".to_string(), "should not run".to_string()],
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
    async fn execute_resync_type() {
        let result = execute_sync("test-asset", &run_only_spec(), SyncType::Resync, None, None)
            .await
            .unwrap();
        assert_eq!(result.sync_type, SyncType::Resync);
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
        let result = dry_run_sync("test-asset", &run_only_spec(), SyncType::Resync, None);
        assert_eq!(result.asset_name, "test-asset");
        assert_eq!(result.sync_type, SyncType::Resync);
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

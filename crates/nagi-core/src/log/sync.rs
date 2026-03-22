use std::path::PathBuf;

use serde::Serialize;

use crate::sync::SyncExecutionResult;

use super::{file, parse_date, sanitize_path_component, LogError, LogStore};

/// Paths to stdout/stderr log files for a single stage.
#[derive(Debug, Clone)]
pub struct SyncLogFilePaths {
    /// Path to the stdout log file.
    pub stdout: PathBuf,
    /// Path to the stderr log file.
    pub stderr: PathBuf,
}

/// A sync log entry as read from SQLite.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncLogEntry {
    /// Unique identifier for this sync execution.
    pub execution_id: String,
    /// Pipeline stage (e.g. `pre`, `run`, `post`).
    pub stage: String,
    /// Name of the Asset that was synced.
    pub asset_name: String,
    /// Whether this was a `sync` or `resync` operation.
    pub sync_type: String,
    /// RFC 3339 timestamp when the stage started.
    pub started_at: String,
    /// RFC 3339 timestamp when the stage finished.
    pub finished_at: String,
    /// Process exit code of the stage command. None for non-execution stages (e.g. lock_retry).
    pub exit_code: Option<i32>,
    /// File path where stdout output is stored. None for non-execution stages.
    pub stdout_path: Option<String>,
    /// File path where stderr output is stored. None for non-execution stages.
    pub stderr_path: Option<String>,
    /// Date partition key (YYYY-MM-DD) derived from `started_at`.
    pub date: String,
}

impl LogStore {
    /// Records a sync execution result into sync_logs and writes stdout/stderr
    /// to log files. Returns the paths of written log files.
    pub fn write_sync_log(
        &self,
        result: &SyncExecutionResult,
    ) -> Result<Vec<SyncLogFilePaths>, LogError> {
        // Defense against path traversal: validate asset_name before using in file paths.
        sanitize_path_component(&result.asset_name)?;

        let tx = self.conn.unchecked_transaction()?;
        let mut all_paths = Vec::new();

        for stage_result in &result.stages {
            let date = parse_date(&stage_result.started_at)?;
            let paths = file::write_stage_logs(
                &self.logs_dir,
                &result.asset_name,
                &stage_result.started_at,
                date,
                stage_result.stage,
                &stage_result.stdout,
                &stage_result.stderr,
            )?;

            tx.execute(
                "INSERT INTO sync_logs
                 (execution_id, stage, asset_name, sync_type,
                  started_at, finished_at, exit_code,
                  stdout_path, stderr_path, date)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    result.execution_id,
                    stage_result.stage.to_string(),
                    result.asset_name,
                    result.sync_type.to_string(),
                    stage_result.started_at,
                    stage_result.finished_at,
                    stage_result.exit_code,
                    paths.stdout.to_string_lossy(),
                    paths.stderr.to_string_lossy(),
                    date,
                ],
            )?;
            all_paths.push(paths);
        }
        tx.commit()?;
        Ok(all_paths)
    }

    /// Links a sync execution to the evaluation(s) that triggered it.
    pub fn write_sync_evaluation(
        &self,
        execution_id: &str,
        evaluation_id: &str,
    ) -> Result<(), LogError> {
        self.conn.execute(
            "INSERT INTO sync_evaluations (execution_id, evaluation_id)
             VALUES (?1, ?2)",
            rusqlite::params![execution_id, evaluation_id],
        )?;
        Ok(())
    }

    /// Records a lock retry attempt in sync_logs.
    pub fn write_sync_lock_log(
        &self,
        execution_id: &str,
        asset_name: &str,
        attempt: u32,
        result: &str,
        timestamp: &str,
    ) -> Result<(), LogError> {
        let stage = format!("lock_retry_{attempt}");
        let date = parse_date(timestamp)?;
        self.conn.execute(
            "INSERT INTO sync_logs
             (execution_id, stage, asset_name, sync_type,
              started_at, finished_at, date)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                execution_id,
                stage,
                asset_name,
                result,
                timestamp,
                timestamp,
                date
            ],
        )?;
        Ok(())
    }

    /// Returns the most recent sync log entries for a given asset.
    pub fn latest_sync_log(&self, asset_name: &str) -> Result<Vec<SyncLogEntry>, LogError> {
        let mut stmt = self.conn.prepare(
            "SELECT execution_id, stage, asset_name, sync_type,
                    started_at, finished_at, exit_code,
                    stdout_path, stderr_path, date
             FROM sync_logs
             WHERE asset_name = ?1
               AND execution_id = (
                   SELECT execution_id FROM sync_logs
                   WHERE asset_name = ?1
                   ORDER BY started_at DESC LIMIT 1
               )
             ORDER BY started_at",
        )?;
        let rows = stmt.query_map(rusqlite::params![asset_name], |row| {
            Ok(SyncLogEntry {
                execution_id: row.get(0)?,
                stage: row.get(1)?,
                asset_name: row.get(2)?,
                sync_type: row.get(3)?,
                started_at: row.get(4)?,
                finished_at: row.get(5)?,
                exit_code: row.get(6)?,
                stdout_path: row.get(7)?,
                stderr_path: row.get(8)?,
                date: row.get(9)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(LogError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{Stage, StageResult, SyncType};

    fn make_sync_result(asset_name: &str) -> SyncExecutionResult {
        SyncExecutionResult {
            execution_id: "exec-001".to_string(),
            asset_name: asset_name.to_string(),
            sync_type: SyncType::Sync,
            stages: vec![
                StageResult {
                    stage: Stage::Pre,
                    exit_code: 0,
                    stdout: "pre output".to_string(),
                    stderr: "".to_string(),
                    started_at: "2026-03-16T10:00:00+09:00".to_string(),
                    finished_at: "2026-03-16T10:00:01+09:00".to_string(),
                    args: vec!["echo".to_string(), "pre".to_string()],
                },
                StageResult {
                    stage: Stage::Run,
                    exit_code: 0,
                    stdout: "run output".to_string(),
                    stderr: "run warning".to_string(),
                    started_at: "2026-03-16T10:00:01+09:00".to_string(),
                    finished_at: "2026-03-16T10:00:05+09:00".to_string(),
                    args: vec!["dbt".to_string(), "run".to_string()],
                },
            ],
            success: true,
        }
    }

    #[test]
    fn write_and_read_sync_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let result = make_sync_result("my-asset");
        let paths = store.write_sync_log(&result).unwrap();
        assert_eq!(paths.len(), 2);

        let entries = store.latest_sync_log("my-asset").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].stage, "pre");
        assert_eq!(entries[0].exit_code, Some(0));
        assert_eq!(entries[1].stage, "run");
    }

    #[test]
    fn sync_log_writes_stdout_stderr_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let result = make_sync_result("my-asset");
        let paths = store.write_sync_log(&result).unwrap();

        let pre_stdout = std::fs::read_to_string(&paths[0].stdout).unwrap();
        assert_eq!(pre_stdout, "pre output");

        let run_stderr = std::fs::read_to_string(&paths[1].stderr).unwrap();
        assert_eq!(run_stderr, "run warning");
    }

    #[test]
    fn sync_evaluation_links_sync_to_eval() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        store.write_sync_evaluation("exec-001", "eval-001").unwrap();
        store.write_sync_evaluation("exec-001", "eval-002").unwrap();

        let count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sync_evaluations WHERE execution_id = 'exec-001'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn latest_sync_log_returns_empty_for_unknown_asset() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let entries = store.latest_sync_log("nonexistent").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn latest_sync_log_returns_only_most_recent_execution() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let mut r1 = make_sync_result("my-asset");
        r1.execution_id = "exec-old".to_string();
        r1.stages[0].started_at = "2026-03-15T10:00:00+09:00".to_string();
        r1.stages[0].finished_at = "2026-03-15T10:00:01+09:00".to_string();
        r1.stages[1].started_at = "2026-03-15T10:00:01+09:00".to_string();
        r1.stages[1].finished_at = "2026-03-15T10:00:05+09:00".to_string();
        store.write_sync_log(&r1).unwrap();

        let r2 = make_sync_result("my-asset");
        store.write_sync_log(&r2).unwrap();

        let entries = store.latest_sync_log("my-asset").unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.execution_id == "exec-001"));
    }

    #[test]
    fn duplicate_sync_log_pk_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let result = make_sync_result("my-asset");
        store.write_sync_log(&result).unwrap();

        let err = store.write_sync_log(&result).unwrap_err();
        assert!(matches!(err, LogError::Sqlite(_)));
    }
}

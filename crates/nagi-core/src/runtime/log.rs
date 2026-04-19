mod evaluate;
mod file;
mod schema;
pub mod subscriber;
mod sync;

use std::path::{Path, PathBuf};

use rusqlite::Connection;
use thiserror::Error;

pub use self::evaluate::EvaluateLogEntry;
pub use self::sync::{SyncLogEntry, SyncLogFilePaths};

#[derive(Debug, Error)]
pub enum LogError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid timestamp format: {0}")]
    InvalidTimestamp(String),

    #[error("invalid asset name: {0}")]
    InvalidAssetName(String),
}

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Extracts YYYY-MM-DD from an ISO 8601 timestamp.
/// Defense against panic on malformed input (slice out-of-bounds).
fn parse_date(timestamp: &str) -> Result<&str, LogError> {
    if timestamp.len() < 10 {
        return Err(LogError::InvalidTimestamp(timestamp.to_string()));
    }
    let date = &timestamp[..10];
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 || parts[0].len() != 4 || parts[1].len() != 2 || parts[2].len() != 2 {
        return Err(LogError::InvalidTimestamp(timestamp.to_string()));
    }
    Ok(date)
}

/// Splits a validated YYYY-MM-DD date into (year, month, day).
fn split_date(date: &str) -> (&str, &str, &str) {
    (&date[..4], &date[5..7], &date[8..10])
}

/// Sanitizes a name for safe use as a filesystem path component.
/// Defense against path traversal (e.g. "../../../etc").
fn sanitize_path_component(name: &str) -> Result<String, LogError> {
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name == "."
        || name == ".."
        || name.contains("..")
    {
        return Err(LogError::InvalidAssetName(name.to_string()));
    }
    Ok(name.to_string())
}

/// Callback for transforming a row before writing to JSONL.
pub type RowTransform = dyn Fn(&mut serde_json::Map<String, serde_json::Value>);

/// Handle for the logging subsystem.
/// Manages SQLite metadata and file-based stdout/stderr storage.
pub struct LogStore {
    conn: Connection,
    logs_dir: PathBuf,
}

impl LogStore {
    /// Opens (or creates) the logs.db at `db_path` and initializes the schema.
    /// `logs_dir` is the directory for stdout/stderr files.
    pub fn open(db_path: &Path, logs_dir: &Path) -> Result<Self, LogError> {
        let conn = Connection::open(db_path)?;
        schema::initialize(&conn)?;
        Ok(Self {
            conn,
            logs_dir: logs_dir.to_path_buf(),
        })
    }

    /// Opens (or creates) the log store using paths derived from a `StateDir`.
    pub(crate) fn from_state_dir(
        state_dir: &crate::runtime::config::StateDir,
    ) -> Result<Self, LogError> {
        Self::open(&state_dir.log_store_path(), &state_dir.logs_dir())
    }

    /// Executes a SQL query that returns a single scalar i64 value.
    pub fn query_row(&self, sql: &str, params: impl rusqlite::Params) -> Result<i64, LogError> {
        Ok(self.conn.query_row(sql, params, |row| row.get(0))?)
    }

    /// Extracts rows from a table where rowid > last_rowid, writing each row
    /// as a JSON object to the provided writer. Returns the max rowid seen
    /// and the number of rows written.
    ///
    /// `transform_row` is called for each row to allow rewriting fields
    /// (e.g. replacing local file paths with remote URIs).
    pub fn extract_rows_jsonl<W: std::io::Write>(
        &self,
        table: &str,
        last_rowid: i64,
        writer: &mut W,
        transform_row: Option<&RowTransform>,
    ) -> Result<(i64, i64), LogError> {
        let sql = format!(
            "SELECT rowid, * FROM {} WHERE rowid > ?1 ORDER BY rowid",
            table
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let column_names: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let mut max_rowid = last_rowid;
        let mut count: i64 = 0;

        let mut rows = stmt.query(rusqlite::params![last_rowid])?;
        while let Some(row) = rows.next()? {
            let rowid: i64 = row.get(0)?;
            max_rowid = rowid;

            let mut map = serde_json::Map::new();
            // Skip column 0 (rowid) - start from 1
            for (i, name) in column_names.iter().enumerate().skip(1) {
                let value: rusqlite::types::Value = row.get(i)?;
                let json_val = match value {
                    rusqlite::types::Value::Null => serde_json::Value::Null,
                    rusqlite::types::Value::Integer(n) => serde_json::Value::Number(n.into()),
                    rusqlite::types::Value::Real(f) => serde_json::json!(f),
                    rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                    rusqlite::types::Value::Blob(b) => serde_json::Value::String(base64_encode(&b)),
                };
                map.insert(name.clone(), json_val);
            }

            if let Some(transform) = transform_row {
                transform(&mut map);
            }

            serde_json::to_writer(&mut *writer, &serde_json::Value::Object(map))
                .map_err(|e| LogError::Io(std::io::Error::other(e.to_string())))?;
            writeln!(writer).map_err(LogError::Io)?;
            count += 1;
        }

        Ok((max_rowid, count))
    }

    /// Prepares a SQL statement for querying.
    pub fn prepare(&self, sql: &str) -> Result<rusqlite::Statement<'_>, LogError> {
        Ok(self.conn.prepare(sql)?)
    }

    #[cfg(test)]
    pub fn open_in_memory(logs_dir: &Path) -> Result<Self, LogError> {
        let conn = Connection::open_in_memory()?;
        schema::initialize(&conn)?;
        Ok(Self {
            conn,
            logs_dir: logs_dir.to_path_buf(),
        })
    }

    #[cfg(test)]
    pub fn execute_batch(&self, sql: &str) -> Result<(), LogError> {
        Ok(self.conn.execute_batch(sql)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::evaluate::{AssetEvalResult, ConditionResult, ConditionStatus};
    use crate::runtime::sync::{Stage, StageResult, SyncExecutionResult, SyncType};

    #[test]
    fn open_creates_db_file_and_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");

        let store = LogStore::open(&db_path, &logs_dir).unwrap();
        assert!(db_path.exists());

        let tables: Vec<String> = store
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(tables.contains(&"evaluate_logs".to_string()));
        assert!(tables.contains(&"sync_logs".to_string()));
        assert!(tables.contains(&"sync_evaluations".to_string()));
    }

    #[test]
    fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");

        LogStore::open(&db_path, &logs_dir).unwrap();
        LogStore::open(&db_path, &logs_dir).unwrap();
    }

    #[test]
    fn file_db_persists_evaluate_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");

        {
            let store = LogStore::open(&db_path, &logs_dir).unwrap();
            let result = AssetEvalResult {
                asset_name: "my-asset".to_string(),
                ready: true,
                conditions: vec![ConditionResult {
                    condition_name: "check".to_string(),
                    condition_type: "SQL".to_string(),
                    status: ConditionStatus::Ready,
                }],
                evaluation_id: None,
            };
            store
                .write_evaluate_log(
                    "eval-file-001",
                    &result,
                    "2026-03-16T10:00:00+09:00",
                    "2026-03-16T10:00:01+09:00",
                )
                .unwrap();
        }

        // Reopen and verify data persisted.
        let store = LogStore::open(&db_path, &logs_dir).unwrap();
        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM evaluate_logs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    // --- Security helper tests ---

    #[test]
    fn parse_date_valid_iso8601() {
        assert_eq!(
            parse_date("2026-03-16T10:00:00+09:00").unwrap(),
            "2026-03-16"
        );
    }

    macro_rules! parse_date_reject {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(parse_date($input).is_err());
                }
            )*
        };
    }

    parse_date_reject! {
        parse_date_rejects_short_input_partial: "2026-03";
        parse_date_rejects_empty: "";
        parse_date_rejects_no_dashes: "20260316T10";
        parse_date_rejects_wrong_lengths: "26-003-16T1";
    }

    macro_rules! sanitize_reject {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(sanitize_path_component($input).is_err());
                }
            )*
        };
    }

    sanitize_reject! {
        sanitize_rejects_dotdot: "..";
        sanitize_rejects_dot: ".";
        sanitize_rejects_traversal: "../etc";
        sanitize_rejects_nested_traversal: "foo/../../etc";
        sanitize_rejects_forward_slash: "a/b";
        sanitize_rejects_backslash: "a\\b";
        sanitize_rejects_empty: "";
    }

    macro_rules! sanitize_accept {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(sanitize_path_component($input).unwrap(), $input);
                }
            )*
        };
    }

    sanitize_accept! {
        sanitize_accepts_hyphenated: "my-asset";
        sanitize_accepts_underscored: "asset_123";
    }

    #[test]
    fn file_db_persists_sync_log_with_files() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");

        let paths = {
            let store = LogStore::open(&db_path, &logs_dir).unwrap();
            let result = SyncExecutionResult {
                execution_id: "exec-file-001".to_string(),
                asset_name: "my-asset".to_string(),
                sync_type: SyncType::Sync,
                stages: vec![StageResult {
                    stage: Stage::Run,
                    exit_code: 0,
                    stdout: "file db output".to_string(),
                    stderr: "".to_string(),
                    started_at: "2026-03-16T12:00:00+09:00".to_string(),
                    finished_at: "2026-03-16T12:00:05+09:00".to_string(),
                    args: vec!["echo".to_string()],
                }],
                success: true,
            };
            store.write_sync_log(&result).unwrap()
        };

        // Reopen and verify data persisted.
        let store = LogStore::open(&db_path, &logs_dir).unwrap();
        let entries = store.latest_sync_log("my-asset").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].execution_id, "exec-file-001");

        // Verify log file on disk.
        let stdout = std::fs::read_to_string(&paths[0].stdout).unwrap();
        assert_eq!(stdout, "file db output");
    }
}

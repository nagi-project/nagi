use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::ExportConfig;
use crate::db::Connection;
use crate::kind::{self, NagiKind};
use crate::log::LogStore;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("log error: {0}")]
    Log(#[from] crate::log::LogError),

    #[error("unknown table: {0}")]
    UnknownTable(String),

    #[error("connection error: {0}")]
    Connection(#[from] crate::db::ConnectionError),
}

/// Tables that can be exported to a remote DWH.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportTable {
    EvaluateLogs,
    SyncLogs,
    SyncEvaluations,
}

impl ExportTable {
    pub const ALL: [ExportTable; 3] = [
        ExportTable::EvaluateLogs,
        ExportTable::SyncLogs,
        ExportTable::SyncEvaluations,
    ];

    pub fn table_name(&self) -> &'static str {
        match self {
            ExportTable::EvaluateLogs => "evaluate_logs",
            ExportTable::SyncLogs => "sync_logs",
            ExportTable::SyncEvaluations => "sync_evaluations",
        }
    }

    pub fn from_name(name: &str) -> Result<Self, ExportError> {
        match name {
            "evaluate_logs" => Ok(ExportTable::EvaluateLogs),
            "sync_logs" => Ok(ExportTable::SyncLogs),
            "sync_evaluations" => Ok(ExportTable::SyncEvaluations),
            _ => Err(ExportError::UnknownTable(name.to_string())),
        }
    }
}

impl fmt::Display for ExportTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.table_name())
    }
}

/// Watermark tracking the last exported rowid for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watermark {
    pub last_rowid: i64,
}

/// Reads the watermark for a table. Returns `last_rowid = 0` if no watermark file exists.
pub fn read_watermark(watermarks_dir: &Path, table: ExportTable) -> Result<Watermark, ExportError> {
    let path = watermarks_dir.join(format!("{}.json", table.table_name()));
    match std::fs::read_to_string(&path) {
        Ok(content) => Ok(serde_json::from_str(&content)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Watermark { last_rowid: 0 }),
        Err(e) => Err(ExportError::Io(e)),
    }
}

/// Writes the watermark for a table.
pub fn write_watermark(
    watermarks_dir: &Path,
    table: ExportTable,
    watermark: &Watermark,
) -> Result<(), ExportError> {
    std::fs::create_dir_all(watermarks_dir)?;
    let path = watermarks_dir.join(format!("{}.json", table.table_name()));
    let content = serde_json::to_string(watermark)?;
    std::fs::write(&path, content)?;
    Ok(())
}

/// Result of a dry-run export check for a single table.
#[derive(Debug, Clone, Serialize)]
pub struct DryRunResult {
    pub table: String,
    pub count: i64,
    /// Number of stdout/stderr files not yet uploaded (sync_logs only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_count: Option<i64>,
}

/// Counts unexported rows for a table.
pub fn dry_run(
    store: &LogStore,
    watermarks_dir: &Path,
    table: ExportTable,
) -> Result<DryRunResult, ExportError> {
    let wm = read_watermark(watermarks_dir, table)?;
    let count: i64 = store.query_row(
        &format!(
            "SELECT COUNT(*) FROM {} WHERE rowid > ?1",
            table.table_name()
        ),
        rusqlite::params![wm.last_rowid],
    )?;

    let file_count = if table == ExportTable::SyncLogs {
        let fc: i64 = store.query_row(
            "SELECT COUNT(*) FROM sync_logs
             WHERE rowid > ?1
               AND (stdout_path IS NOT NULL OR stderr_path IS NOT NULL)",
            rusqlite::params![wm.last_rowid],
        )?;
        Some(fc)
    } else {
        None
    };

    Ok(DryRunResult {
        table: table.table_name().to_string(),
        count,
        file_count,
    })
}

/// Runs dry-run using config-derived paths.
pub fn dry_run_for_config(
    config: &crate::config::NagiConfig,
    select: Option<&str>,
) -> Result<Vec<DryRunResult>, ExportError> {
    let tables = match select {
        Some(name) => vec![ExportTable::from_name(name)?],
        None => vec![],
    };
    dry_run_all(
        &config.nagi_dir.db_path(),
        &config.nagi_dir.logs_dir(),
        &config.nagi_dir.watermarks_dir(),
        &tables,
    )
}

/// Runs dry-run for the specified tables (or all tables if empty).
pub fn dry_run_all(
    db_path: &Path,
    logs_dir: &Path,
    watermarks_dir: &Path,
    tables: &[ExportTable],
) -> Result<Vec<DryRunResult>, ExportError> {
    let store = LogStore::open(db_path, logs_dir)?;
    let targets = if tables.is_empty() {
        ExportTable::ALL.to_vec()
    } else {
        tables.to_vec()
    };
    let mut results = Vec::new();
    for table in targets {
        results.push(dry_run(&store, watermarks_dir, table)?);
    }
    Ok(results)
}

/// Primary keys for MERGE deduplication, keyed by table name.
fn primary_keys(table: ExportTable) -> &'static [&'static str] {
    match table {
        ExportTable::EvaluateLogs => &["evaluation_id", "condition_name"],
        ExportTable::SyncLogs => &["execution_id", "stage"],
        ExportTable::SyncEvaluations => &["execution_id", "evaluation_id"],
    }
}

/// Builds a MERGE statement to upsert from staging into the target table.
fn escape_backtick(s: &str) -> String {
    s.replace('`', "``")
}

fn build_merge_sql(dataset: &str, table: ExportTable) -> String {
    let dataset = escape_backtick(dataset);
    let table_name = table.table_name();
    let staging = format!("{dataset}._staging_{table_name}");
    let target = format!("{dataset}.{table_name}");
    let pks = primary_keys(table);

    let on_clause: Vec<String> = pks
        .iter()
        .map(|pk| format!("T.`{pk}` = S.`{pk}`"))
        .collect();

    format!(
        "MERGE `{target}` T USING `{staging}` S ON {on} \
         WHEN NOT MATCHED THEN INSERT ROW",
        on = on_clause.join(" AND "),
    )
}

/// Exports a single table to the remote DWH.
///
/// Pipeline:
/// 1. For sync_logs: upload stdout/stderr files, build path rewrite map
/// 2. Extract rows after watermark from SQLite into a JSONL temp file
/// 3. Load JSONL into a staging table via bulk load
/// 4. MERGE staging into the target table (PK deduplication)
/// 5. Update watermark
pub async fn export_table(
    store: &LogStore,
    conn: &dyn Connection,
    remote_store: Option<&crate::storage::remote::RemoteObjectStore>,
    config: &ExportConfig,
    watermarks_dir: &Path,
    table: ExportTable,
) -> Result<ExportResult, ExportError> {
    let wm = read_watermark(watermarks_dir, table)?;

    let path_map = upload_path_map(store, remote_store, table, wm.last_rowid).await?;

    let jsonl_path = prepare_jsonl_path(table)?;
    let transform = build_path_rewrite_transform(path_map);

    let mut file = std::fs::File::create(&jsonl_path)?;
    let (max_rowid, count) = store.extract_rows_jsonl(
        table.table_name(),
        wm.last_rowid,
        &mut file,
        transform.as_deref(),
    )?;

    if count == 0 {
        return Ok(ExportResult {
            table: table.table_name().to_string(),
            rows_exported: 0,
        });
    }

    let staging_table = format!("_staging_{}", table.table_name());
    conn.load_jsonl(&config.dataset, &staging_table, &jsonl_path)
        .await?;

    let merge_sql = build_merge_sql(&config.dataset, table);
    conn.execute_sql(&merge_sql).await?;

    write_watermark(
        watermarks_dir,
        table,
        &Watermark {
            last_rowid: max_rowid,
        },
    )?;

    let _ = std::fs::remove_file(&jsonl_path);

    Ok(ExportResult {
        table: table.table_name().to_string(),
        rows_exported: count,
    })
}

/// Uploads sync_log stdout/stderr files and returns a local→remote path map.
/// For non-sync_logs tables or when no remote store is configured, returns an empty map.
async fn upload_path_map(
    store: &LogStore,
    remote_store: Option<&crate::storage::remote::RemoteObjectStore>,
    table: ExportTable,
    last_rowid: i64,
) -> Result<std::collections::HashMap<String, String>, ExportError> {
    if table != ExportTable::SyncLogs {
        return Ok(std::collections::HashMap::new());
    }
    let Some(rs) = remote_store else {
        return Ok(std::collections::HashMap::new());
    };
    upload_sync_log_files(store, rs, last_rowid).await
}

/// Creates the tmp directory and returns the JSONL file path for the given table.
fn prepare_jsonl_path(table: ExportTable) -> Result<PathBuf, ExportError> {
    let tmp_dir = std::env::temp_dir().join("nagi-export");
    std::fs::create_dir_all(&tmp_dir)?;
    Ok(tmp_dir.join(format!("{}.jsonl", table.table_name())))
}

/// Builds an optional row transform that rewrites local file paths to remote URIs.
fn build_path_rewrite_transform(
    path_map: std::collections::HashMap<String, String>,
) -> Option<Box<crate::log::RowTransform>> {
    if path_map.is_empty() {
        return None;
    }
    Some(Box::new(
        move |row: &mut serde_json::Map<String, serde_json::Value>| {
            for field in &["stdout_path", "stderr_path"] {
                let remote_uri = row
                    .get(*field)
                    .and_then(|v| v.as_str())
                    .and_then(|local| path_map.get(local));
                if let Some(uri) = remote_uri {
                    row.insert(field.to_string(), serde_json::Value::String(uri.clone()));
                }
            }
        },
    ))
}

/// Uploads stdout/stderr files referenced by unexported sync_logs rows.
/// Returns a map of local_path → remote_uri for path rewriting in JSONL.
async fn upload_sync_log_files(
    store: &LogStore,
    remote_store: &crate::storage::remote::RemoteObjectStore,
    last_rowid: i64,
) -> Result<std::collections::HashMap<String, String>, ExportError> {
    let mut path_map = std::collections::HashMap::new();

    let sql = "SELECT stdout_path, stderr_path FROM sync_logs \
               WHERE rowid > ?1 \
                 AND (stdout_path IS NOT NULL OR stderr_path IS NOT NULL)";
    let mut stmt = store.prepare(sql)?;
    let mut rows = stmt.query(rusqlite::params![last_rowid])?;

    let mut files_to_upload: Vec<(String, String)> = Vec::new();
    while let Some(row) = rows.next()? {
        let stdout: Option<String> = row.get(0)?;
        let stderr: Option<String> = row.get(1)?;

        for path_str in [stdout, stderr].into_iter().flatten() {
            let local = PathBuf::from(&path_str);
            if local.exists() && !path_map.contains_key(&path_str) {
                let remote_path = path_str.strip_prefix('/').unwrap_or(&path_str);
                let remote = format!("logs/{remote_path}");
                files_to_upload.push((path_str.clone(), remote));
            }
        }
    }
    drop(rows);
    drop(stmt);

    for (local_path, remote_path) in &files_to_upload {
        let uri = remote_store
            .upload_file(Path::new(local_path), remote_path)
            .await
            .map_err(|e| ExportError::Io(std::io::Error::other(e.to_string())))?;
        path_map.insert(local_path.clone(), uri);
    }

    Ok(path_map)
}

/// Result of exporting a single table.
#[derive(Debug, Clone, Serialize)]
pub struct ExportResult {
    pub table: String,
    pub rows_exported: i64,
}

/// Runs full export using config-derived paths.
pub async fn export_for_config(
    config: &crate::config::NagiConfig,
    resources_dir: &Path,
    select: Option<&str>,
) -> Result<Vec<ExportResult>, ExportError> {
    let export_config = config.export.as_ref().ok_or_else(|| {
        ExportError::Io(std::io::Error::other("export not configured in nagi.yaml"))
    })?;

    let tables = match select {
        Some(name) => vec![ExportTable::from_name(name)?],
        None => vec![],
    };

    let log_store = LogStore::open(&config.nagi_dir.db_path(), &config.nagi_dir.logs_dir())?;
    let conn = resolve_export_connection(resources_dir, &export_config.connection)?;
    let remote_store = crate::storage::remote::create_remote_store(&config.backend).ok();
    let wm_dir = config.nagi_dir.watermarks_dir();

    Ok(export_all(
        &log_store,
        conn.as_ref(),
        remote_store.as_ref(),
        export_config,
        &wm_dir,
        &tables,
    )
    .await)
}

/// Exports all specified tables (or all if empty). Returns results per table.
pub async fn export_all(
    store: &LogStore,
    conn: &dyn Connection,
    remote_store: Option<&crate::storage::remote::RemoteObjectStore>,
    config: &ExportConfig,
    watermarks_dir: &Path,
    tables: &[ExportTable],
) -> Vec<ExportResult> {
    let targets = if tables.is_empty() {
        ExportTable::ALL.to_vec()
    } else {
        tables.to_vec()
    };
    let mut results = Vec::new();
    for table in targets {
        match export_table(store, conn, remote_store, config, watermarks_dir, table).await {
            Ok(r) => results.push(r),
            Err(e) => {
                tracing::warn!(%table, %e, "failed to export");
                results.push(ExportResult {
                    table: table.table_name().to_string(),
                    rows_exported: -1,
                });
            }
        }
    }
    results
}

/// Generates Conditions + Sync + Asset resources for export of a single table.
fn generate_export_resources_for_table(table: ExportTable, config: &ExportConfig) -> Vec<NagiKind> {
    let table_name = table.table_name();

    let conditions_name = format!("export-{table_name}-drift");
    let sync_name = format!("export-{table_name}");
    let asset_name = format!("nagi-export-{table_name}");

    let conditions = NagiKind::Conditions {
        api_version: kind::API_VERSION.to_string(),
        metadata: kind::Metadata {
            name: conditions_name.clone(),
        },
        spec: kind::ConditionsSpec(vec![kind::asset::DesiredCondition::Command {
            name: "unexported-rows".to_string(),
            run: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("nagi export --select {table_name} --dry-run | jq -e '.[] | .count == 0'"),
            ],
            interval: Some(crate::duration::Duration::from_secs(
                config.interval.as_std().as_secs(),
            )),
            env: Default::default(),
            evaluate_cache_ttl: None,
        }]),
    };

    let sync_resource = NagiKind::Sync {
        api_version: kind::API_VERSION.to_string(),
        metadata: kind::Metadata {
            name: sync_name.clone(),
        },
        spec: kind::SyncSpec {
            pre: None,
            run: kind::sync::SyncStep {
                step_type: kind::sync::StepType::Command,
                args: vec![
                    "nagi".to_string(),
                    "export".to_string(),
                    "--select".to_string(),
                    table_name.to_string(),
                ],
                env: Default::default(),
            },
            post: None,
        },
    };

    let asset = NagiKind::Asset {
        api_version: kind::API_VERSION.to_string(),
        metadata: kind::Metadata {
            name: asset_name.clone(),
        },
        spec: kind::AssetSpec {
            sources: vec![],
            on_drift: vec![kind::asset::OnDriftEntry {
                conditions: conditions_name,
                sync: sync_name,
                with: Default::default(),
                merge_position: kind::asset::MergePosition::BeforeOrigin,
            }],
            auto_sync: true,
            tags: vec![],
            evaluate_cache_ttl: None,
        },
    };

    vec![conditions, sync_resource, asset]
}

/// Generates all export resources (9 total: 3 tables x 3 kinds).
pub fn generate_export_resources(config: &ExportConfig) -> Vec<NagiKind> {
    ExportTable::ALL
        .iter()
        .flat_map(|table| generate_export_resources_for_table(*table, config))
        .collect()
}

/// Resolves a DWH Connection by loading `resources/` and matching `connection_name`.
pub fn resolve_export_connection(
    resources_dir: &Path,
    connection_name: &str,
) -> Result<Box<dyn crate::db::Connection>, ExportError> {
    let resources = crate::compile::load_resources(resources_dir)
        .map_err(|e| ExportError::Io(std::io::Error::other(e.to_string())))?;

    for r in &resources {
        if let NagiKind::Connection { metadata, spec, .. } = r {
            if metadata.name == connection_name {
                let profiles = crate::dbt::profile::DbtProfilesFile::load_default()
                    .map_err(|e| ExportError::Io(std::io::Error::other(e.to_string())))?;
                let adapter = profiles
                    .resolve(
                        &spec.dbt_profile.profile,
                        spec.dbt_profile.target.as_deref(),
                    )
                    .map_err(|e| ExportError::Io(std::io::Error::other(e.to_string())))?;
                return crate::db::create_connection(adapter).map_err(ExportError::Connection);
            }
        }
    }

    Err(ExportError::Io(std::io::Error::other(format!(
        "connection '{}' not found in {}",
        connection_name,
        resources_dir.display()
    ))))
}

/// Checks whether enough time has elapsed since the last export.
/// Returns `true` if the interval has passed (or no marker exists).
pub fn should_export(watermarks_dir: &Path, interval: &crate::duration::Duration) -> bool {
    let marker = watermarks_dir.join("_last_export_time");
    match std::fs::read_to_string(&marker) {
        Ok(content) => {
            let Ok(last) = content.trim().parse::<f64>() else {
                return true;
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            now - last >= interval.as_std().as_secs_f64()
        }
        Err(_) => true,
    }
}

/// Records the current time as the last export timestamp.
pub fn mark_exported(watermarks_dir: &Path) -> Result<(), ExportError> {
    std::fs::create_dir_all(watermarks_dir)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    std::fs::write(watermarks_dir.join("_last_export_time"), now.to_string())?;
    Ok(())
}

/// Runs export if configured and enough time has elapsed since the last export.
/// Failures are logged as warnings and do not propagate.
pub async fn try_export(resources_dir: &Path, project_dir: &Path) {
    let config = match crate::config::load_config(project_dir) {
        Ok(c) => c,
        Err(_) => return,
    };
    let export_config = match config.export {
        Some(ref c) => c,
        None => return,
    };

    let wm_dir = config.nagi_dir.watermarks_dir();
    if !should_export(&wm_dir, &export_config.interval) {
        return;
    }

    let db_path = config.nagi_dir.db_path();
    let logs_dir = config.nagi_dir.logs_dir();

    let log_store = match LogStore::open(&db_path, &logs_dir) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(%e, "export: failed to open log store");
            return;
        }
    };

    let conn = match resolve_export_connection(resources_dir, &export_config.connection) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(%e, "export: failed to resolve connection");
            return;
        }
    };

    // Build remote store for file uploads if backend is remote.
    let remote_store = crate::storage::remote::create_remote_store(&config.backend).ok();

    let results = export_all(
        &log_store,
        conn.as_ref(),
        remote_store.as_ref(),
        export_config,
        &wm_dir,
        &[],
    )
    .await;
    for r in &results {
        if r.rows_exported >= 0 {
            tracing::info!(table = %r.table, rows = r.rows_exported, "export complete");
        }
    }

    if let Err(e) = mark_exported(&wm_dir) {
        tracing::warn!(%e, "export: failed to update marker");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_test_store() -> (tempfile::TempDir, LogStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn read_watermark_returns_zero_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let wm = read_watermark(dir.path(), ExportTable::EvaluateLogs).unwrap();
        assert_eq!(wm.last_rowid, 0);
    }

    #[test]
    fn write_and_read_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let wm = Watermark { last_rowid: 42 };
        write_watermark(dir.path(), ExportTable::SyncLogs, &wm).unwrap();
        let read = read_watermark(dir.path(), ExportTable::SyncLogs).unwrap();
        assert_eq!(read.last_rowid, 42);
    }

    #[test]
    fn dry_run_counts_unexported_rows() {
        let (_dir, store) = open_test_store();
        store
            .execute_batch(
                "INSERT INTO evaluate_logs
                 (evaluation_id, condition_name, asset_name, started_at, finished_at, result, date)
                 VALUES
                 ('e1', 'c1', 'a1', '2026-03-20T00:00:00Z', '2026-03-20T00:00:01Z', 'Ready', '2026-03-20'),
                 ('e2', 'c1', 'a1', '2026-03-20T00:00:02Z', '2026-03-20T00:00:03Z', 'Drifted', '2026-03-20')",
            )
            .unwrap();

        let wm_dir = tempfile::tempdir().unwrap();
        let result = dry_run(&store, wm_dir.path(), ExportTable::EvaluateLogs).unwrap();
        assert_eq!(result.count, 2);
        assert!(result.file_count.is_none());
    }

    #[test]
    fn dry_run_respects_watermark() {
        let (_dir, store) = open_test_store();
        store
            .execute_batch(
                "INSERT INTO evaluate_logs
                 (evaluation_id, condition_name, asset_name, started_at, finished_at, result, date)
                 VALUES
                 ('e1', 'c1', 'a1', '2026-03-20T00:00:00Z', '2026-03-20T00:00:01Z', 'Ready', '2026-03-20'),
                 ('e2', 'c1', 'a1', '2026-03-20T00:00:02Z', '2026-03-20T00:00:03Z', 'Drifted', '2026-03-20')",
            )
            .unwrap();

        let wm_dir = tempfile::tempdir().unwrap();
        write_watermark(
            wm_dir.path(),
            ExportTable::EvaluateLogs,
            &Watermark { last_rowid: 1 },
        )
        .unwrap();

        let result = dry_run(&store, wm_dir.path(), ExportTable::EvaluateLogs).unwrap();
        assert_eq!(result.count, 1);
    }

    #[test]
    fn dry_run_sync_logs_includes_file_count() {
        let (_dir, store) = open_test_store();
        store
            .execute_batch(
                "INSERT INTO sync_logs
                 (execution_id, stage, asset_name, sync_type, started_at, finished_at, exit_code, stdout_path, stderr_path, date)
                 VALUES
                 ('x1', 'run', 'a1', 'sync', '2026-03-20T00:00:00Z', '2026-03-20T00:00:05Z', 0, '/tmp/out', '/tmp/err', '2026-03-20')",
            )
            .unwrap();

        let wm_dir = tempfile::tempdir().unwrap();
        let result = dry_run(&store, wm_dir.path(), ExportTable::SyncLogs).unwrap();
        assert_eq!(result.count, 1);
        assert_eq!(result.file_count, Some(1));
    }

    #[test]
    fn dry_run_zero_when_all_exported() {
        let (_dir, store) = open_test_store();
        let wm_dir = tempfile::tempdir().unwrap();
        let result = dry_run(&store, wm_dir.path(), ExportTable::SyncEvaluations).unwrap();
        assert_eq!(result.count, 0);
    }

    #[test]
    fn export_table_from_name() {
        assert_eq!(
            ExportTable::from_name("evaluate_logs").unwrap(),
            ExportTable::EvaluateLogs
        );
        assert_eq!(
            ExportTable::from_name("sync_logs").unwrap(),
            ExportTable::SyncLogs
        );
        assert_eq!(
            ExportTable::from_name("sync_evaluations").unwrap(),
            ExportTable::SyncEvaluations
        );
        assert!(ExportTable::from_name("unknown").is_err());
    }

    #[test]
    fn export_table_display() {
        assert_eq!(ExportTable::EvaluateLogs.to_string(), "evaluate_logs");
        assert_eq!(ExportTable::SyncLogs.to_string(), "sync_logs");
        assert_eq!(ExportTable::SyncEvaluations.to_string(), "sync_evaluations");
    }

    #[test]
    fn generate_export_resources_produces_nine_resources() {
        let config = ExportConfig {
            connection: "my-bq".to_string(),
            dataset: "nagi_logs".to_string(),
            format: crate::config::ExportFormat::Jsonl,
            interval: serde_yaml::from_str("30m").unwrap(),
        };
        let resources = generate_export_resources(&config);
        assert_eq!(resources.len(), 9);
    }

    #[test]
    fn generate_export_resources_names_and_kinds() {
        let config = ExportConfig {
            connection: "my-bq".to_string(),
            dataset: "nagi_logs".to_string(),
            format: crate::config::ExportFormat::Jsonl,
            interval: serde_yaml::from_str("30m").unwrap(),
        };
        let resources = generate_export_resources(&config);

        let names: Vec<(&str, &str)> = resources
            .iter()
            .map(|r| (r.kind(), r.metadata().name.as_str()))
            .collect();

        assert!(names.contains(&("Conditions", "export-evaluate_logs-drift")));
        assert!(names.contains(&("Sync", "export-evaluate_logs")));
        assert!(names.contains(&("Asset", "nagi-export-evaluate_logs")));
        assert!(names.contains(&("Conditions", "export-sync_logs-drift")));
        assert!(names.contains(&("Sync", "export-sync_logs")));
        assert!(names.contains(&("Asset", "nagi-export-sync_logs")));
        assert!(names.contains(&("Conditions", "export-sync_evaluations-drift")));
        assert!(names.contains(&("Sync", "export-sync_evaluations")));
        assert!(names.contains(&("Asset", "nagi-export-sync_evaluations")));
    }

    #[test]
    fn build_merge_sql_evaluate_logs() {
        let sql = build_merge_sql("nagi_logs", ExportTable::EvaluateLogs);
        assert!(sql.contains("MERGE"));
        assert!(sql.contains("nagi_logs.evaluate_logs"));
        assert!(sql.contains("nagi_logs._staging_evaluate_logs"));
        assert!(sql.contains("T.`evaluation_id` = S.`evaluation_id`"));
        assert!(sql.contains("T.`condition_name` = S.`condition_name`"));
    }

    #[test]
    fn build_merge_sql_sync_logs() {
        let sql = build_merge_sql("nagi_logs", ExportTable::SyncLogs);
        assert!(sql.contains("T.`execution_id` = S.`execution_id`"));
        assert!(sql.contains("T.`stage` = S.`stage`"));
    }

    #[test]
    fn build_merge_sql_sync_evaluations() {
        let sql = build_merge_sql("nagi_logs", ExportTable::SyncEvaluations);
        assert!(sql.contains("T.`execution_id` = S.`execution_id`"));
        assert!(sql.contains("T.`evaluation_id` = S.`evaluation_id`"));
    }

    #[test]
    fn extract_rows_jsonl_writes_correct_format() {
        let (_dir, store) = open_test_store();
        store
            .execute_batch(
                "INSERT INTO evaluate_logs
                 (evaluation_id, condition_name, asset_name, started_at, finished_at, result, date)
                 VALUES
                 ('e1', 'c1', 'a1', '2026-03-20T00:00:00Z', '2026-03-20T00:00:01Z', 'Ready', '2026-03-20')",
            )
            .unwrap();

        let mut buf = Vec::new();
        let (max_rowid, count) = store
            .extract_rows_jsonl("evaluate_logs", 0, &mut buf, None)
            .unwrap();
        assert_eq!(count, 1);
        assert!(max_rowid > 0);

        let line = String::from_utf8(buf).unwrap();
        let obj: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(obj["evaluation_id"], "e1");
        assert_eq!(obj["result"], "Ready");
    }

    #[test]
    fn extract_rows_jsonl_applies_transform() {
        let (_dir, store) = open_test_store();
        store
            .execute_batch(
                "INSERT INTO sync_logs
                 (execution_id, stage, asset_name, sync_type, started_at, finished_at, exit_code, stdout_path, stderr_path, date)
                 VALUES
                 ('x1', 'run', 'a1', 'sync', '2026-03-20T00:00:00Z', '2026-03-20T00:00:05Z', 0, '/local/out', NULL, '2026-03-20')",
            )
            .unwrap();

        let transform = |row: &mut serde_json::Map<String, serde_json::Value>| {
            if let Some(serde_json::Value::String(p)) = row.get("stdout_path") {
                if p == "/local/out" {
                    row.insert(
                        "stdout_path".to_string(),
                        serde_json::Value::String("gs://bucket/remote/out".to_string()),
                    );
                }
            }
        };

        let mut buf = Vec::new();
        let (_, count) = store
            .extract_rows_jsonl("sync_logs", 0, &mut buf, Some(&transform))
            .unwrap();
        assert_eq!(count, 1);

        let line = String::from_utf8(buf).unwrap();
        let obj: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(obj["stdout_path"], "gs://bucket/remote/out");
    }

    #[test]
    fn extract_rows_jsonl_skips_already_exported() {
        let (_dir, store) = open_test_store();
        store
            .execute_batch(
                "INSERT INTO evaluate_logs
                 (evaluation_id, condition_name, asset_name, started_at, finished_at, result, date)
                 VALUES
                 ('e1', 'c1', 'a1', '2026-03-20T00:00:00Z', '2026-03-20T00:00:01Z', 'Ready', '2026-03-20'),
                 ('e2', 'c2', 'a1', '2026-03-20T00:00:02Z', '2026-03-20T00:00:03Z', 'Drifted', '2026-03-20')",
            )
            .unwrap();

        // Export from rowid 1 (skip first row)
        let mut buf = Vec::new();
        let (max_rowid, count) = store
            .extract_rows_jsonl("evaluate_logs", 1, &mut buf, None)
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(max_rowid, 2);

        let line = String::from_utf8(buf).unwrap();
        let obj: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(obj["evaluation_id"], "e2");
    }

    #[test]
    fn primary_keys_coverage() {
        assert_eq!(
            primary_keys(ExportTable::EvaluateLogs),
            &["evaluation_id", "condition_name"]
        );
        assert_eq!(
            primary_keys(ExportTable::SyncLogs),
            &["execution_id", "stage"]
        );
        assert_eq!(
            primary_keys(ExportTable::SyncEvaluations),
            &["execution_id", "evaluation_id"]
        );
    }

    #[test]
    fn generated_asset_has_auto_sync_true() {
        let config = ExportConfig {
            connection: "my-bq".to_string(),
            dataset: "nagi_logs".to_string(),
            format: crate::config::ExportFormat::Jsonl,
            interval: serde_yaml::from_str("30m").unwrap(),
        };
        let resources = generate_export_resources(&config);
        for r in &resources {
            if let NagiKind::Asset { spec, .. } = r {
                assert!(spec.auto_sync);
            }
        }
    }

    #[test]
    fn build_path_rewrite_transform_none_when_empty() {
        let map = std::collections::HashMap::new();
        assert!(build_path_rewrite_transform(map).is_none());
    }

    #[test]
    fn build_path_rewrite_transform_rewrites_stdout() {
        let mut map = std::collections::HashMap::new();
        map.insert("/local/out".to_string(), "gs://bucket/out".to_string());
        let transform = build_path_rewrite_transform(map).unwrap();

        let mut row = serde_json::Map::new();
        row.insert(
            "stdout_path".to_string(),
            serde_json::Value::String("/local/out".to_string()),
        );
        row.insert(
            "stderr_path".to_string(),
            serde_json::Value::String("/other/err".to_string()),
        );
        transform(&mut row);

        assert_eq!(row["stdout_path"], "gs://bucket/out");
        assert_eq!(row["stderr_path"], "/other/err");
    }

    #[test]
    fn build_path_rewrite_transform_rewrites_both() {
        let mut map = std::collections::HashMap::new();
        map.insert("/local/out".to_string(), "gs://bucket/out".to_string());
        map.insert("/local/err".to_string(), "gs://bucket/err".to_string());
        let transform = build_path_rewrite_transform(map).unwrap();

        let mut row = serde_json::Map::new();
        row.insert(
            "stdout_path".to_string(),
            serde_json::Value::String("/local/out".to_string()),
        );
        row.insert(
            "stderr_path".to_string(),
            serde_json::Value::String("/local/err".to_string()),
        );
        transform(&mut row);

        assert_eq!(row["stdout_path"], "gs://bucket/out");
        assert_eq!(row["stderr_path"], "gs://bucket/err");
    }

    #[test]
    fn build_path_rewrite_transform_ignores_missing_fields() {
        let mut map = std::collections::HashMap::new();
        map.insert("/local/out".to_string(), "gs://bucket/out".to_string());
        let transform = build_path_rewrite_transform(map).unwrap();

        let mut row = serde_json::Map::new();
        row.insert(
            "other_field".to_string(),
            serde_json::Value::String("value".to_string()),
        );
        transform(&mut row);

        assert_eq!(row.len(), 1);
        assert_eq!(row["other_field"], "value");
    }

    #[test]
    fn prepare_jsonl_path_contains_table_name() {
        let path = prepare_jsonl_path(ExportTable::SyncLogs).unwrap();
        assert_eq!(path.file_name().unwrap(), "sync_logs.jsonl");
        assert!(path.parent().unwrap().ends_with("nagi-export"));
    }

    #[test]
    fn escape_backtick_doubles() {
        assert_eq!(escape_backtick("clean"), "clean");
        assert_eq!(escape_backtick("has`tick"), "has``tick");
        assert_eq!(escape_backtick("``"), "````");
    }

    #[test]
    fn should_export_true_when_no_marker() {
        let dir = tempfile::tempdir().unwrap();
        let interval: crate::duration::Duration = serde_yaml::from_str("30m").unwrap();
        assert!(should_export(dir.path(), &interval));
    }

    #[test]
    fn should_export_true_when_marker_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("_last_export_time"), "not_a_number").unwrap();
        let interval: crate::duration::Duration = serde_yaml::from_str("30m").unwrap();
        assert!(should_export(dir.path(), &interval));
    }

    #[test]
    fn should_export_false_when_recently_exported() {
        let dir = tempfile::tempdir().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        std::fs::write(dir.path().join("_last_export_time"), now.to_string()).unwrap();
        let interval: crate::duration::Duration = serde_yaml::from_str("30m").unwrap();
        assert!(!should_export(dir.path(), &interval));
    }

    #[test]
    fn should_export_true_when_interval_elapsed() {
        let dir = tempfile::tempdir().unwrap();
        let past = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            - 3600.0;
        std::fs::write(dir.path().join("_last_export_time"), past.to_string()).unwrap();
        let interval: crate::duration::Duration = serde_yaml::from_str("30m").unwrap();
        assert!(should_export(dir.path(), &interval));
    }

    #[test]
    fn mark_exported_creates_marker() {
        let dir = tempfile::tempdir().unwrap();
        mark_exported(dir.path()).unwrap();
        let content = std::fs::read_to_string(dir.path().join("_last_export_time")).unwrap();
        let ts: f64 = content.trim().parse().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!((now - ts).abs() < 5.0);
    }
}

use std::path::Path;

use crate::runtime::config::NagiConfig;
use crate::runtime::export::{
    dry_run_all, export_all, mark_exported, resolve_export_connection, should_export, DryRunResult,
    ExportError, ExportResult, ExportTable,
};
use crate::runtime::log::LogStore;

/// Runs dry-run export using config-derived paths.
pub fn dry_run_for_config(
    config: &NagiConfig,
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

/// Runs full export using config-derived paths.
pub async fn export_for_config(
    config: &NagiConfig,
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
    let remote_store = crate::runtime::storage::remote::create_remote_store(&config.backend).ok();
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

/// Runs export if configured and enough time has elapsed since the last export.
/// Failures are logged as warnings and do not propagate.
pub async fn try_export(resources_dir: &Path, project_dir: &Path) {
    let config = match crate::runtime::config::load_config(project_dir) {
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

    let remote_store = crate::runtime::storage::remote::create_remote_store(&config.backend).ok();

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

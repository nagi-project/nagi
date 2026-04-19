use std::path::Path;

use crate::runtime::config::NagiConfig;
use crate::runtime::export::{
    dry_run_all, export_all, mark_exported, resolve_export_connection, should_export, DryRunResult,
    ExportError, ExportResult, ExportTable,
};
use crate::runtime::log::LogStore;

/// Runs dry-run export using config-derived paths.
pub(crate) fn dry_run_for_config(
    config: &NagiConfig,
    select: Option<&str>,
) -> Result<Vec<DryRunResult>, ExportError> {
    let tables = match select {
        Some(name) => vec![ExportTable::from_name(name)?],
        None => vec![],
    };
    dry_run_all(
        &config.project.state_dir.log_store_path(),
        &config.project.state_dir.logs_dir(),
        &config.project.state_dir.watermarks_dir(),
        &tables,
    )
}

/// Runs full export using config-derived paths.
pub(crate) async fn export_for_config(
    config: &NagiConfig,
    resources_dir: &Path,
    select: Option<&str>,
) -> Result<Vec<ExportResult>, ExportError> {
    let export_config = config.project.export.as_ref().ok_or_else(|| {
        ExportError::Io(std::io::Error::other("export not configured in nagi.yaml"))
    })?;

    let tables = match select {
        Some(name) => vec![ExportTable::from_name(name)?],
        None => vec![],
    };

    let log_store = LogStore::from_state_dir(&config.project.state_dir)?;
    let conn = resolve_export_connection(resources_dir, &export_config.connection)?;
    let remote_store = crate::runtime::storage::remote::create_remote_store(&config.backend).ok();
    let wm_dir = config.project.state_dir.watermarks_dir();

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
pub(crate) async fn try_export(resources_dir: &Path, project_dir: &Path) {
    let config = match crate::runtime::config::load_config_from_dir(project_dir) {
        Ok(c) => c,
        Err(_) => return,
    };
    let export_config = match config.project.export {
        Some(ref c) => c,
        None => return,
    };

    let wm_dir = config.project.state_dir.watermarks_dir();
    if !should_export(&wm_dir, &export_config.interval) {
        return;
    }

    let log_store = match LogStore::from_state_dir(&config.project.state_dir) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::config::{NagiConfig, ProjectConfig, StateDir};

    fn config_with_tmpdir(dir: &std::path::Path) -> NagiConfig {
        NagiConfig {
            project: ProjectConfig {
                state_dir: StateDir::new(dir.to_path_buf()),
                ..ProjectConfig::default()
            },
            ..NagiConfig::default()
        }
    }

    #[test]
    fn dry_run_for_config_no_select_returns_all_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let logs_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&logs_dir).unwrap();
        let config = config_with_tmpdir(tmp.path());

        let results = dry_run_for_config(&config, None).unwrap();
        // With no select filter, all 3 export tables are included.
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn dry_run_for_config_with_select_returns_single_table() {
        let tmp = tempfile::tempdir().unwrap();
        let logs_dir = tmp.path().join("logs");
        std::fs::create_dir_all(&logs_dir).unwrap();
        let config = config_with_tmpdir(tmp.path());

        let results = dry_run_for_config(&config, Some("evaluate_logs")).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn dry_run_for_config_invalid_table_name_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config_with_tmpdir(tmp.path());

        let result = dry_run_for_config(&config, Some("nonexistent_table"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn export_for_config_without_export_config_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config_with_tmpdir(tmp.path());
        // config.project.export is None by default.

        let result = export_for_config(&config, tmp.path(), None).await;
        assert!(result.is_err());
    }
}

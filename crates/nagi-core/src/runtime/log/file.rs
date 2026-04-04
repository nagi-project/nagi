use std::fs;
use std::path::Path;

use crate::runtime::sync::Stage;

use super::{split_date, SyncLogFilePaths};

/// Writes stdout and stderr for a sync stage to log files.
///
/// Path pattern: `{logs_dir}/{asset_name}/{yyyy}/{mm}/{dd}/{timestamp}_{stage}.stdout|stderr`
///
/// The `timestamp` in the path is derived from `started_at` by replacing colons
/// with hyphens (for filesystem compatibility).
pub fn write_stage_logs(
    logs_dir: &Path,
    asset_name: &str,
    started_at: &str,
    date: &str,
    stage: Stage,
    stdout: &str,
    stderr: &str,
) -> Result<SyncLogFilePaths, std::io::Error> {
    // `date` is already validated by the caller (parse_date + sanitize_path_component).
    let (yyyy, mm, dd) = split_date(date);

    let ts = started_at.replace(':', "-");
    let dir = logs_dir.join(asset_name).join(yyyy).join(mm).join(dd);
    fs::create_dir_all(&dir)?;

    let base = format!("{ts}_{stage}");
    let stdout_path = dir.join(format!("{base}.stdout"));
    let stderr_path = dir.join(format!("{base}.stderr"));

    fs::write(&stdout_path, stdout)?;
    fs::write(&stderr_path, stderr)?;

    Ok(SyncLogFilePaths {
        stdout: stdout_path,
        stderr: stderr_path,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn log_dir_for(logs_dir: &Path, asset_name: &str, date: &str) -> PathBuf {
        let (yyyy, mm, dd) = split_date(date);
        logs_dir.join(asset_name).join(yyyy).join(mm).join(dd)
    }

    #[test]
    fn write_creates_files_with_correct_content() {
        let dir = tempfile::tempdir().unwrap();
        let paths = write_stage_logs(
            dir.path(),
            "my-asset",
            "2026-03-16T10:00:00+09:00",
            "2026-03-16",
            Stage::Run,
            "hello stdout",
            "hello stderr",
        )
        .unwrap();

        assert_eq!(fs::read_to_string(&paths.stdout).unwrap(), "hello stdout");
        assert_eq!(fs::read_to_string(&paths.stderr).unwrap(), "hello stderr");
    }

    #[test]
    fn write_creates_nested_directory_structure() {
        let dir = tempfile::tempdir().unwrap();
        write_stage_logs(
            dir.path(),
            "my-asset",
            "2026-03-16T10:00:00+09:00",
            "2026-03-16",
            Stage::Pre,
            "",
            "",
        )
        .unwrap();

        let expected = dir.path().join("my-asset/2026/03/16");
        assert!(expected.is_dir());
    }

    #[test]
    fn write_replaces_colons_in_filename() {
        let dir = tempfile::tempdir().unwrap();
        let paths = write_stage_logs(
            dir.path(),
            "asset",
            "2026-03-16T10:30:00+09:00",
            "2026-03-16",
            Stage::Post,
            "",
            "",
        )
        .unwrap();

        let filename = paths.stdout.file_name().unwrap().to_string_lossy();
        assert!(!filename.contains(':'));
        assert!(filename.contains("post.stdout"));
    }

    #[test]
    fn log_dir_for_returns_correct_path() {
        let base = PathBuf::from("logs");
        let path = log_dir_for(&base, "my-asset", "2026-03-16");
        assert_eq!(path, PathBuf::from("logs").join("my-asset/2026/03/16"));
    }
}

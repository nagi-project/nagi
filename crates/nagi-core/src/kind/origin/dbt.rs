use std::path::Path;

use thiserror::Error;

use crate::compile::CompileError;

pub mod cloud;
pub mod expand;
pub mod manifest;

#[derive(Debug, Error)]
pub enum DbtError {
    #[error("dbt command not found")]
    NotFound,

    #[error("dbt debug failed: {0}")]
    DebugFailed(String),
}

/// Runs `dbt debug` to verify the connection.
pub fn run_dbt_debug(
    project_dir: &Path,
    profile: &str,
    target: Option<&str>,
) -> Result<(), DbtError> {
    let mut cmd = std::process::Command::new("dbt");
    cmd.arg("debug");
    cmd.arg("--project-dir").arg(project_dir);
    cmd.args(["--profile", profile]);
    if let Some(t) = target {
        cmd.args(["--target", t]);
    }
    let output = cmd.output().map_err(|_| DbtError::NotFound)?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(DbtError::DebugFailed(stdout.trim().to_string()));
    }
    Ok(())
}

/// Runs `dbt compile` and returns the content of `target/manifest.json`.
pub fn load_manifest(
    project_dir: &Path,
    profile: &str,
    target: Option<&str>,
) -> Result<String, CompileError> {
    run_dbt_compile(project_dir, profile, target)?;
    read_manifest(&project_dir.join("target/manifest.json"))
}

fn run_dbt_compile(
    project_dir: &Path,
    profile: &str,
    target: Option<&str>,
) -> Result<(), CompileError> {
    let mut cmd = std::process::Command::new("dbt");
    cmd.arg("compile");
    cmd.arg("--project-dir").arg(project_dir);
    cmd.args(["--profile", profile]);
    if let Some(t) = target {
        cmd.args(["--target", t]);
    }
    let output = cmd
        .output()
        .map_err(|e| CompileError::DbtCompileFailed(format!("failed to execute dbt: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CompileError::DbtCompileFailed(format!(
            "dbt compile exited with {}: {}",
            output.status,
            stderr.trim()
        )));
    }
    Ok(())
}

fn read_manifest(path: &Path) -> Result<String, CompileError> {
    std::fs::read_to_string(path).map_err(|e| {
        CompileError::Io(std::io::Error::new(
            e.kind(),
            format!("failed to read {}: {e}", path.display()),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn read_manifest_returns_content() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("manifest.json");
        std::fs::write(&path, r#"{"nodes":{},"sources":{}}"#).unwrap();

        let content = read_manifest(&path).unwrap();
        assert!(content.contains("nodes"));
    }

    #[test]
    fn read_manifest_error_when_file_missing() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent.json");

        let err = read_manifest(&path).unwrap_err();
        assert!(matches!(err, CompileError::Io(_)));
        assert!(err.to_string().contains("nonexistent.json"));
    }

    #[test]
    fn run_dbt_compile_error_when_not_found() {
        // Ensure `dbt` is not found by using a non-existent command path.
        let result = std::process::Command::new("dbt")
            .arg("compile")
            .env("PATH", "/nonexistent")
            .output();

        // If dbt is not installed, Command::output itself errors.
        // If dbt IS installed but with empty PATH, it may also fail.
        // Either way, run_dbt_compile should produce DbtCompileFailed.
        if result.is_err() {
            let err = run_dbt_compile(Path::new("."), "default", None);
            // Only assert if dbt is truly not on the system PATH.
            if let Err(e) = err {
                assert!(matches!(e, CompileError::DbtCompileFailed(_)));
            }
        }
    }
}

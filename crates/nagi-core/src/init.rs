use std::path::Path;

use thiserror::Error;

use crate::log::{LogError, LogStore};

/// Returns the default nagi home directory (`~/.nagi`).
pub fn default_nagi_dir() -> std::path::PathBuf {
    dirs::home_dir().unwrap_or_default().join(".nagi")
}

/// Returns the default database path (`~/.nagi/logs.db`).
pub fn default_db_path() -> std::path::PathBuf {
    default_nagi_dir().join("logs.db")
}

/// Returns the default logs directory (`~/.nagi/logs`).
pub fn default_logs_dir() -> std::path::PathBuf {
    default_nagi_dir().join("logs")
}

#[derive(Debug, Error)]
pub enum InitError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("log error: {0}")]
    Log(#[from] LogError),

    #[error("dbt_project.yml not found in {0}")]
    DbtProjectNotFound(String),

    #[error("failed to parse dbt_project.yml: {0}")]
    DbtProjectParse(String),
}

/// Creates `assets/` directory under `base_dir` if it does not exist.
pub fn ensure_assets_dir(base_dir: &Path) -> Result<(), InitError> {
    std::fs::create_dir_all(base_dir.join("assets"))?;
    Ok(())
}

/// Creates `~/.nagi/config.yaml` with default content if it does not exist.
pub fn ensure_config() -> Result<(), InitError> {
    let config_dir = dirs::home_dir().unwrap_or_default().join(".nagi");
    std::fs::create_dir_all(&config_dir)?;
    let config_path = config_dir.join("config.yaml");
    if !config_path.exists() {
        std::fs::write(&config_path, "backend:\n  type: local\n")?;
    }
    Ok(())
}

/// Initializes the log store (creates `~/.nagi/logs.db` and `~/.nagi/logs/`).
pub fn ensure_log_store(db_path: &Path, logs_dir: &Path) -> Result<(), InitError> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = LogStore::open(db_path, logs_dir)?;
    Ok(())
}

/// Initialises the workspace: creates `assets/`, config, and log store.
pub fn init_workspace(base_dir: &Path, db_path: &Path, logs_dir: &Path) -> Result<(), InitError> {
    ensure_assets_dir(base_dir)?;
    ensure_config()?;
    ensure_log_store(db_path, logs_dir)?;
    Ok(())
}

/// Builds a Connection YAML string from profile and target.
pub fn build_connection_yaml(profile: &str, target: Option<&str>) -> String {
    let name = connection_name(profile, target);
    let mut yaml = format!(
        "apiVersion: nagi.io/v1alpha1\n\
         kind: Connection\n\
         metadata:\n\
         \x20 name: {name}\n\
         spec:\n\
         \x20 dbtProfile:\n\
         \x20   profile: {profile}\n"
    );
    if let Some(t) = target {
        yaml.push_str(&format!("    target: {t}\n"));
    }
    yaml
}

/// Builds an Origin YAML string from a dbt project directory and connection name.
pub fn build_origin_yaml(project_dir: &Path, connection_name: &str) -> Result<String, InitError> {
    let project_name = read_dbt_project_name(project_dir)?;
    Ok(format!(
        "apiVersion: nagi.io/v1alpha1\n\
         kind: Origin\n\
         metadata:\n\
         \x20 name: {project_name}\n\
         spec:\n\
         \x20 type: DBT\n\
         \x20 connection: {connection_name}\n\
         \x20 projectDir: {}\n",
        project_dir.display()
    ))
}

/// Reads the `name` field from `dbt_project.yml` in the given directory.
fn read_dbt_project_name(project_dir: &Path) -> Result<String, InitError> {
    let path = project_dir.join("dbt_project.yml");
    if !path.exists() {
        return Err(InitError::DbtProjectNotFound(
            project_dir.display().to_string(),
        ));
    }
    let content = std::fs::read_to_string(&path)?;
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&content).map_err(|e| InitError::DbtProjectParse(e.to_string()))?;
    Ok(doc
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("my-dbt-project")
        .to_string())
}

/// Returns the connection name from profile and optional target.
pub fn connection_name(profile: &str, target: Option<&str>) -> String {
    match target {
        Some(t) => format!("{profile}-{t}"),
        None => profile.to_string(),
    }
}

/// A dbt project entry collected from user input.
pub struct DbtProjectEntry {
    pub project_dir: String,
    pub profile: String,
    pub target: Option<String>,
}

/// Result of `write_init_dbt_files`.
pub struct InitDbtFilesResult {
    pub connection_path: Option<std::path::PathBuf>,
    pub origin_path: Option<std::path::PathBuf>,
}

/// Generates and writes connection.yaml and origin.yaml from collected dbt project entries.
///
/// Skips writing if the target file already exists. Deduplicates connections by name.
pub fn write_init_dbt_files(
    base_dir: &Path,
    entries: &[DbtProjectEntry],
) -> Result<InitDbtFilesResult, InitError> {
    let assets_dir = base_dir.join("assets");
    let connection_path = assets_dir.join("connection.yaml");
    let origin_path = assets_dir.join("origin.yaml");

    if origin_path.exists() {
        return Ok(InitDbtFilesResult {
            connection_path: None,
            origin_path: None,
        });
    }

    let mut connections: Vec<(String, String)> = Vec::new();
    let mut seen_connections: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut origins: Vec<String> = Vec::new();

    for entry in entries {
        let target = entry.target.as_deref();
        let conn_name = connection_name(&entry.profile, target);

        if seen_connections.insert(conn_name.clone()) {
            connections.push((
                conn_name.clone(),
                build_connection_yaml(&entry.profile, target),
            ));
        }

        origins.push(build_origin_yaml(
            Path::new(&entry.project_dir),
            &conn_name,
        )?);
    }

    let wrote_connection = if !connections.is_empty() && !connection_path.exists() {
        let content: Vec<&str> = connections.iter().map(|(_, y)| y.as_str()).collect();
        std::fs::write(&connection_path, content.join("---\n"))?;
        Some(connection_path)
    } else {
        None
    };

    let wrote_origin = if !origins.is_empty() {
        std::fs::write(&origin_path, origins.join("---\n"))?;
        Some(origin_path)
    } else {
        None
    };

    Ok(InitDbtFilesResult {
        connection_path: wrote_connection,
        origin_path: wrote_origin,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_name_with_target() {
        assert_eq!(connection_name("my_profile", Some("dev")), "my_profile-dev");
    }

    #[test]
    fn connection_name_without_target() {
        assert_eq!(connection_name("my_profile", None), "my_profile");
    }

    #[test]
    fn build_connection_yaml_with_target() {
        let yaml = build_connection_yaml("my_profile", Some("dev"));
        assert!(yaml.contains("name: my_profile-dev"));
        assert!(yaml.contains("profile: my_profile"));
        assert!(yaml.contains("target: dev"));
    }

    #[test]
    fn build_connection_yaml_without_target() {
        let yaml = build_connection_yaml("my_profile", None);
        assert!(yaml.contains("name: my_profile"));
        assert!(!yaml.contains("target:"));
    }

    #[test]
    fn build_origin_yaml_reads_project_name() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: test-project\nversion: '1.0.0'\n",
        )
        .unwrap();
        let yaml = build_origin_yaml(dir.path(), "my-conn").unwrap();
        assert!(yaml.contains("name: test-project"));
        assert!(yaml.contains("connection: my-conn"));
        assert!(yaml.contains(&format!("projectDir: {}", dir.path().display())));
    }

    #[test]
    fn build_origin_yaml_missing_dbt_project() {
        let dir = tempfile::tempdir().unwrap();
        let err = build_origin_yaml(dir.path(), "conn").unwrap_err();
        assert!(matches!(err, InitError::DbtProjectNotFound(_)));
    }

    #[test]
    fn ensure_assets_dir_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        ensure_assets_dir(dir.path()).unwrap();
        assert!(dir.path().join("assets").exists());
    }

    #[test]
    fn ensure_log_store_creates_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        ensure_log_store(&db_path, &logs_dir).unwrap();
        assert!(db_path.exists());
    }

    #[test]
    fn write_init_dbt_files_creates_both_files() {
        let dir = tempfile::tempdir().unwrap();
        ensure_assets_dir(dir.path()).unwrap();

        let dbt_dir = tempfile::tempdir().unwrap();
        std::fs::write(dbt_dir.path().join("dbt_project.yml"), "name: my-project\n").unwrap();

        let entries = vec![DbtProjectEntry {
            project_dir: dbt_dir.path().display().to_string(),
            profile: "prof".to_string(),
            target: Some("dev".to_string()),
        }];

        let result = write_init_dbt_files(dir.path(), &entries).unwrap();
        assert!(result.connection_path.is_some());
        assert!(result.origin_path.is_some());

        let conn = std::fs::read_to_string(dir.path().join("assets/connection.yaml")).unwrap();
        assert!(conn.contains("name: prof-dev"));

        let origin = std::fs::read_to_string(dir.path().join("assets/origin.yaml")).unwrap();
        assert!(origin.contains("name: my-project"));
    }

    #[test]
    fn write_init_dbt_files_skips_if_origin_exists() {
        let dir = tempfile::tempdir().unwrap();
        ensure_assets_dir(dir.path()).unwrap();
        std::fs::write(dir.path().join("assets/origin.yaml"), "existing").unwrap();

        let result = write_init_dbt_files(dir.path(), &[]).unwrap();
        assert!(result.connection_path.is_none());
        assert!(result.origin_path.is_none());
    }

    #[test]
    fn write_init_dbt_files_deduplicates_connections() {
        let dir = tempfile::tempdir().unwrap();
        ensure_assets_dir(dir.path()).unwrap();

        let dbt_dir1 = tempfile::tempdir().unwrap();
        std::fs::write(dbt_dir1.path().join("dbt_project.yml"), "name: proj1\n").unwrap();
        let dbt_dir2 = tempfile::tempdir().unwrap();
        std::fs::write(dbt_dir2.path().join("dbt_project.yml"), "name: proj2\n").unwrap();

        let entries = vec![
            DbtProjectEntry {
                project_dir: dbt_dir1.path().display().to_string(),
                profile: "prof".to_string(),
                target: Some("dev".to_string()),
            },
            DbtProjectEntry {
                project_dir: dbt_dir2.path().display().to_string(),
                profile: "prof".to_string(),
                target: Some("dev".to_string()),
            },
        ];

        let result = write_init_dbt_files(dir.path(), &entries).unwrap();
        assert!(result.connection_path.is_some());

        let conn = std::fs::read_to_string(dir.path().join("assets/connection.yaml")).unwrap();
        // Should contain only one connection definition
        assert_eq!(conn.matches("kind: Connection").count(), 1);

        let origin = std::fs::read_to_string(dir.path().join("assets/origin.yaml")).unwrap();
        assert_eq!(origin.matches("kind: Origin").count(), 2);
    }

    #[test]
    fn init_workspace_creates_all() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("logs.db");
        let logs_dir = dir.path().join("logs");
        init_workspace(dir.path(), &db_path, &logs_dir).unwrap();
        assert!(dir.path().join("assets").exists());
        assert!(db_path.exists());
    }
}

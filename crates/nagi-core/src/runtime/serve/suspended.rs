use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Metadata persisted when an asset is suspended from automatic sync.
///
/// Each suspended asset gets a JSON file at `{suspended_dir}/{asset_name}.json`.
/// The serve loop reads these flags to skip sync for suspended assets;
/// `nagi serve resume` deletes them to re-enable sync.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SuspendedInfo {
    /// Name of the suspended Asset resource.
    pub asset_name: String,
    /// Human-readable reason for the suspension.
    pub reason: String,
    /// RFC 3339 timestamp when the asset was suspended.
    pub suspended_at: String,
    /// The sync execution_id that triggered the suspension, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
}

fn validate_filename(name: &str) -> std::io::Result<()> {
    crate::runtime::storage::validate_filename(name)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))
}

pub fn suspended_path(dir: &Path, asset_name: &str) -> std::io::Result<PathBuf> {
    validate_filename(asset_name)?;
    Ok(dir.join(format!("{asset_name}.json")))
}

pub fn remove_suspended(dir: &Path, asset_name: &str) -> std::io::Result<()> {
    let path = suspended_path(dir, asset_name)?;
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn list_suspended(dir: &Path) -> std::io::Result<Vec<SuspendedInfo>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut result = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            let data = std::fs::read_to_string(&path)?;
            if let Ok(info) = serde_json::from_str::<SuspendedInfo>(&data) {
                result.push(info);
            }
        }
    }
    result.sort_by(|a, b| a.asset_name.cmp(&b.asset_name));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_suspended(name: &str) -> SuspendedInfo {
        SuspendedInfo {
            asset_name: name.to_string(),
            reason: "3 consecutive sync failures".to_string(),
            suspended_at: "2025-06-15T03:12:00Z".to_string(),
            execution_id: Some("exec-001".to_string()),
        }
    }

    fn write_suspended(dir: &Path, info: &SuspendedInfo) -> std::io::Result<()> {
        validate_filename(&info.asset_name)?;
        std::fs::create_dir_all(dir)?;
        let json = serde_json::to_string_pretty(info)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(suspended_path(dir, &info.asset_name)?, json)
    }

    fn read_suspended(dir: &Path, asset_name: &str) -> std::io::Result<SuspendedInfo> {
        let data = std::fs::read_to_string(suspended_path(dir, asset_name)?)?;
        serde_json::from_str(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    #[test]
    fn suspended_write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let info = sample_suspended("daily-sales");
        write_suspended(dir.path(), &info).unwrap();
        let loaded = read_suspended(dir.path(), "daily-sales").unwrap();
        assert_eq!(loaded.asset_name, "daily-sales");
        assert_eq!(loaded.reason, info.reason);
        assert_eq!(loaded.suspended_at, info.suspended_at);
    }

    #[test]
    fn suspended_remove_existing() {
        let dir = tempfile::tempdir().unwrap();
        write_suspended(dir.path(), &sample_suspended("a")).unwrap();
        remove_suspended(dir.path(), "a").unwrap();
        assert!(read_suspended(dir.path(), "a").is_err());
    }

    #[test]
    fn suspended_remove_nonexistent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        remove_suspended(dir.path(), "nonexistent").unwrap();
    }

    #[test]
    fn suspended_list_returns_sorted() {
        let dir = tempfile::tempdir().unwrap();
        write_suspended(dir.path(), &sample_suspended("z-asset")).unwrap();
        write_suspended(dir.path(), &sample_suspended("a-asset")).unwrap();
        let list = list_suspended(dir.path()).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].asset_name, "a-asset");
        assert_eq!(list[1].asset_name, "z-asset");
    }

    #[test]
    fn suspended_list_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let list = list_suspended(dir.path()).unwrap();
        assert!(list.is_empty());
    }

    #[test]
    fn suspended_list_nonexistent_dir() {
        let list = list_suspended(&std::env::temp_dir().join("nonexistent-nagi-test-dir")).unwrap();
        assert!(list.is_empty());
    }
}

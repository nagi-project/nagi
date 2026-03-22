use schemars::schema_for;

use crate::config::NagiConfig;
use crate::evaluate::AssetEvalResult;
use crate::kind::{
    AssetSpec, ConditionsSpec, ConnectionSpec, NagiKind, OriginSpec, SourceSpec, SyncSpec,
};
use crate::log::{EvaluateLogEntry, SyncLogEntry};
use crate::serve::SuspendedInfo;
use crate::storage::lock::LockInfo;

/// Generates JSON Schema for all Nagi resource kinds and writes them to the given directory.
pub fn generate_schemas(output_dir: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let schemas: Vec<(&str, serde_json::Value)> = vec![
        (
            "NagiKind",
            serde_json::to_value(schema_for!(NagiKind)).unwrap(),
        ),
        (
            "AssetSpec",
            serde_json::to_value(schema_for!(AssetSpec)).unwrap(),
        ),
        (
            "SourceSpec",
            serde_json::to_value(schema_for!(SourceSpec)).unwrap(),
        ),
        (
            "ConnectionSpec",
            serde_json::to_value(schema_for!(ConnectionSpec)).unwrap(),
        ),
        (
            "SyncSpec",
            serde_json::to_value(schema_for!(SyncSpec)).unwrap(),
        ),
        (
            "ConditionsSpec",
            serde_json::to_value(schema_for!(ConditionsSpec)).unwrap(),
        ),
        (
            "OriginSpec",
            serde_json::to_value(schema_for!(OriginSpec)).unwrap(),
        ),
        (
            "NagiConfig",
            serde_json::to_value(schema_for!(NagiConfig)).unwrap(),
        ),
        (
            "AssetEvalResult",
            serde_json::to_value(schema_for!(AssetEvalResult)).unwrap(),
        ),
        (
            "LockInfo",
            serde_json::to_value(schema_for!(LockInfo)).unwrap(),
        ),
        (
            "SuspendedInfo",
            serde_json::to_value(schema_for!(SuspendedInfo)).unwrap(),
        ),
        (
            "SyncLogEntry",
            serde_json::to_value(schema_for!(SyncLogEntry)).unwrap(),
        ),
        (
            "EvaluateLogEntry",
            serde_json::to_value(schema_for!(EvaluateLogEntry)).unwrap(),
        ),
    ];

    for (name, schema) in schemas {
        let path = output_dir.join(format!("{name}.json"));
        let json = serde_json::to_string_pretty(&schema).unwrap();
        std::fs::write(path, json)?;
    }

    Ok(())
}

/// Generates JSON Schema for all specs and writes to `docs/schemas/`.
/// Intended to be called from `mise run gen-schema`.
pub fn generate_schemas_to_docs() -> std::io::Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let root = std::path::Path::new(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(std::path::Path::new("."));
    let output_dir = root.join("docs").join("schemas");
    generate_schemas(&output_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_schemas_creates_files() {
        let dir = tempfile::tempdir().unwrap();
        generate_schemas(dir.path()).unwrap();

        let expected = [
            "NagiKind.json",
            "AssetSpec.json",
            "SourceSpec.json",
            "ConnectionSpec.json",
            "SyncSpec.json",
            "ConditionsSpec.json",
            "OriginSpec.json",
            "NagiConfig.json",
            "AssetEvalResult.json",
            "LockInfo.json",
            "SuspendedInfo.json",
            "SyncLogEntry.json",
            "EvaluateLogEntry.json",
        ];
        for name in expected {
            let path = dir.path().join(name);
            assert!(path.exists(), "{name} should exist");
            let content = std::fs::read_to_string(&path).unwrap();
            let _: serde_json::Value = serde_json::from_str(&content).unwrap();
        }
    }
}

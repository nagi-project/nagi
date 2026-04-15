use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use thiserror::Error;

use crate::runtime::compile::load_compiled_assets;
use crate::runtime::compile::CompiledAsset;

#[derive(Debug, Error)]
pub(crate) enum LsError {
    #[error(transparent)]
    Compile(#[from] crate::runtime::compile::CompileError),

    #[error("failed to parse compiled asset: {0}")]
    Parse(String),
}

#[derive(Debug, Serialize)]
pub struct LsOutput {
    pub assets: Vec<LsAsset>,
    pub connections: Vec<LsConnection>,
    pub conditions: Vec<LsConditions>,
    pub syncs: Vec<LsSync>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LsAsset {
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub upstreams: Vec<String>,
    pub auto_sync: bool,
    pub on_drift: Vec<LsOnDriftEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LsOnDriftEntry {
    pub conditions: String,
    pub sync: String,
}

#[derive(Debug, Serialize)]
pub struct LsConnection {
    pub name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LsConditions {
    pub name: String,
    pub condition_names: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LsSync {
    pub name: String,
}

// Lowercase list of kinds present in compiled output (Origin is excluded because
// it is expanded into Assets during compilation).
const VALID_KINDS: &[&str] = &["asset", "connection", "conditions", "sync"];

fn validate_kinds(kinds: &[&str]) -> Result<(), LsError> {
    for kind in kinds {
        if !VALID_KINDS.contains(&kind.to_lowercase().as_str()) {
            return Err(
                crate::runtime::compile::CompileError::InvalidKind(kind.to_string()).into(),
            );
        }
    }
    Ok(())
}

fn has_kind(kinds: &[String], kind: &str) -> bool {
    kinds.is_empty() || kinds.iter().any(|k| k == kind)
}

/// Reads compiled target/ directory and returns a structured listing of all resources.
pub(crate) fn ls(target_dir: &std::path::Path, kinds: &[&str]) -> Result<LsOutput, LsError> {
    validate_kinds(kinds)?;

    let normalized: Vec<String> = kinds.iter().map(|k| k.to_lowercase()).collect();

    let loaded = load_compiled_assets(target_dir, &[], &[])?;
    let compiled_assets = parse_compiled_assets(&loaded)?;

    let assets = if has_kind(&normalized, "asset") {
        collect_assets(&compiled_assets)
    } else {
        Vec::new()
    };
    let connections = if has_kind(&normalized, "connection") {
        collect_connections(&compiled_assets)
    } else {
        Vec::new()
    };
    let conditions = if has_kind(&normalized, "conditions") {
        collect_conditions(&compiled_assets)
    } else {
        Vec::new()
    };
    let syncs = if has_kind(&normalized, "sync") {
        collect_syncs(&compiled_assets)
    } else {
        Vec::new()
    };

    Ok(LsOutput {
        assets,
        connections,
        conditions,
        syncs,
    })
}

fn parse_compiled_assets(
    loaded: &[(String, String)],
) -> Result<Vec<(String, CompiledAsset)>, LsError> {
    loaded
        .iter()
        .map(|(name, yaml)| {
            let compiled: CompiledAsset =
                serde_yaml::from_str(yaml).map_err(|e| LsError::Parse(e.to_string()))?;
            Ok((name.clone(), compiled))
        })
        .collect()
}

fn collect_assets(compiled_assets: &[(String, CompiledAsset)]) -> Vec<LsAsset> {
    compiled_assets
        .iter()
        .map(|(name, compiled)| {
            let on_drift = compiled
                .spec
                .on_drift
                .iter()
                .map(|entry| LsOnDriftEntry {
                    conditions: entry.conditions_ref.clone(),
                    sync: entry.sync_ref_name.clone(),
                })
                .collect();
            LsAsset {
                name: name.clone(),
                labels: compiled.metadata.labels.clone(),
                upstreams: compiled.spec.upstreams.clone(),
                auto_sync: compiled.spec.auto_sync,
                on_drift,
            }
        })
        .collect()
}

fn collect_connections(compiled_assets: &[(String, CompiledAsset)]) -> Vec<LsConnection> {
    compiled_assets
        .iter()
        .filter_map(|(_, compiled)| compiled.connection.as_ref())
        .map(|conn| conn.name().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|name| LsConnection { name })
        .collect()
}

fn collect_conditions(compiled_assets: &[(String, CompiledAsset)]) -> Vec<LsConditions> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (_, compiled) in compiled_assets {
        for entry in &compiled.spec.on_drift {
            let names: Vec<String> = entry
                .conditions
                .iter()
                .map(|c| c.name().to_string())
                .collect();
            map.insert(entry.conditions_ref.clone(), names);
        }
    }
    map.into_iter()
        .map(|(name, condition_names)| LsConditions {
            name,
            condition_names,
        })
        .collect()
}

fn collect_syncs(compiled_assets: &[(String, CompiledAsset)]) -> Vec<LsSync> {
    let mut syncs = BTreeSet::new();
    for (_, compiled) in compiled_assets {
        for entry in &compiled.spec.on_drift {
            syncs.insert(entry.sync_ref_name.clone());
        }
    }
    syncs.into_iter().map(|name| LsSync { name }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::compile::{resolve, write_output};
    use crate::runtime::kind::parse_kinds;

    fn yaml_docs(docs: &[&str]) -> String {
        docs.join("\n---\n")
    }

    fn setup_target(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let resources = parse_kinds(yaml).unwrap();
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();
        (dir, target)
    }

    fn setup_compiled_assets(yaml: &str) -> Vec<(String, CompiledAsset)> {
        let (_dir, target) = setup_target(yaml);
        let loaded = load_compiled_assets(&target, &[], &[]).unwrap();
        parse_compiled_assets(&loaded).unwrap()
    }

    const CONNECTION: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bq
spec:
  type: dbt
  profile: my_project
  target: dev";

    const UPSTREAM_ASSET: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: raw-sales
spec:
  connection: my-bq";

    const CONDITIONS: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: freshness-check
spec:
  - name: freshness-24h
    type: Freshness
    maxAge: 24h
    interval: 6h";

    const SYNC: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-run
spec:
  run:
    type: Command
    args: [dbt, run]";

    const ASSET: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
  labels:
    dbt/finance: ''
spec:
  connection: my-bq
  upstreams: [raw-sales]
  onDrift:
    - conditions: freshness-check
      sync: dbt-run";

    const ALL_YAML: &[&str] = &[CONNECTION, UPSTREAM_ASSET, CONDITIONS, SYNC, ASSET];

    // ── collect_assets ──────────────────────────────────────────────────

    #[test]
    fn collect_assets_extracts_name_labels_upstreams() {
        let compiled = setup_compiled_assets(&yaml_docs(ALL_YAML));
        let assets = collect_assets(&compiled);

        assert_eq!(assets.len(), 2);
        let daily = assets.iter().find(|a| a.name == "daily-sales").unwrap();
        assert_eq!(daily.labels.get("dbt/finance"), Some(&String::new()));
        assert_eq!(daily.upstreams, vec!["raw-sales"]);
        assert!(daily.auto_sync);
    }

    #[test]
    fn collect_assets_extracts_on_drift_refs() {
        let compiled = setup_compiled_assets(&yaml_docs(ALL_YAML));
        let assets = collect_assets(&compiled);

        let daily = assets.iter().find(|a| a.name == "daily-sales").unwrap();
        assert_eq!(daily.on_drift.len(), 1);
        assert_eq!(daily.on_drift[0].conditions, "freshness-check");
        assert_eq!(daily.on_drift[0].sync, "dbt-run");
    }

    #[test]
    fn collect_assets_empty_on_drift_when_no_conditions() {
        let yaml = yaml_docs(&[
            CONNECTION,
            UPSTREAM_ASSET,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: passive
spec:
  upstreams: [raw-sales]",
        ]);
        let compiled = setup_compiled_assets(&yaml);
        let assets = collect_assets(&compiled);

        assert!(assets[0].on_drift.is_empty());
    }

    // ── collect_connections ──────────────────────────────────────────────

    #[test]
    fn collect_connections_from_compiled_assets() {
        let compiled = setup_compiled_assets(&yaml_docs(ALL_YAML));
        let connections = collect_connections(&compiled);

        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].name, "my-bq");
    }

    #[test]
    fn collect_connections_empty_when_no_connection() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: no-conn
spec:
  onDrift: []";
        let compiled = setup_compiled_assets(yaml);
        let connections = collect_connections(&compiled);

        assert!(connections.is_empty());
    }

    #[test]
    fn collect_connections_deduplicates() {
        let yaml = yaml_docs(&[
            CONNECTION,
            UPSTREAM_ASSET,
            CONDITIONS,
            SYNC,
            ASSET,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: another
spec:
  sources: [raw-sales]
  onDrift:
    - conditions: freshness-check
      sync: dbt-run",
        ]);
        let compiled = setup_compiled_assets(&yaml);
        let connections = collect_connections(&compiled);

        assert_eq!(connections.len(), 1);
    }

    // ── collect_conditions ──────────────────────────────────────────────

    #[test]
    fn collect_conditions_extracts_ref_and_names() {
        let compiled = setup_compiled_assets(&yaml_docs(ALL_YAML));
        let conditions = collect_conditions(&compiled);

        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].name, "freshness-check");
        assert_eq!(conditions[0].condition_names, vec!["freshness-24h"]);
    }

    #[test]
    fn collect_conditions_empty_when_no_on_drift() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: passive
spec:
  onDrift: []";
        let compiled = setup_compiled_assets(yaml);
        let conditions = collect_conditions(&compiled);

        assert!(conditions.is_empty());
    }

    #[test]
    fn collect_conditions_deduplicates_across_assets() {
        let yaml = yaml_docs(&[
            CONNECTION,
            UPSTREAM_ASSET,
            CONDITIONS,
            SYNC,
            ASSET,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: another
spec:
  sources: [raw-sales]
  onDrift:
    - conditions: freshness-check
      sync: dbt-run",
        ]);
        let compiled = setup_compiled_assets(&yaml);
        let conditions = collect_conditions(&compiled);

        assert_eq!(conditions.len(), 1);
    }

    // ── collect_syncs ───────────────────────────────────────────────────

    #[test]
    fn collect_syncs_extracts_ref_names() {
        let compiled = setup_compiled_assets(&yaml_docs(ALL_YAML));
        let syncs = collect_syncs(&compiled);

        assert_eq!(syncs.len(), 1);
        assert_eq!(syncs[0].name, "dbt-run");
    }

    #[test]
    fn collect_syncs_empty_when_no_on_drift() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: passive
spec:
  onDrift: []";
        let compiled = setup_compiled_assets(yaml);
        let syncs = collect_syncs(&compiled);

        assert!(syncs.is_empty());
    }

    #[test]
    fn collect_syncs_deduplicates_across_assets() {
        let yaml = yaml_docs(&[
            CONNECTION,
            UPSTREAM_ASSET,
            CONDITIONS,
            SYNC,
            ASSET,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: another
spec:
  sources: [raw-sales]
  onDrift:
    - conditions: freshness-check
      sync: dbt-run",
        ]);
        let compiled = setup_compiled_assets(&yaml);
        let syncs = collect_syncs(&compiled);

        assert_eq!(syncs.len(), 1);
    }

    // ── ls (integration) ────────────────────────────────────────────────

    #[test]
    fn ls_returns_all_resource_kinds() {
        let (_dir, target) = setup_target(&yaml_docs(ALL_YAML));
        let output = ls(&target, &[]).unwrap();

        assert_eq!(output.assets.len(), 2);
        assert_eq!(output.connections.len(), 1);
        assert_eq!(output.conditions.len(), 1);
        assert_eq!(output.syncs.len(), 1);
    }

    #[test]
    fn ls_returns_error_for_missing_target() {
        let result = ls(std::path::Path::new("/nonexistent/target"), &[]);
        assert!(result.is_err());
    }

    #[test]
    fn ls_filter_single_kind() {
        let (_dir, target) = setup_target(&yaml_docs(ALL_YAML));
        let output = ls(&target, &["Asset"]).unwrap();

        assert_eq!(output.assets.len(), 2);
        assert!(output.connections.is_empty());
        assert!(output.conditions.is_empty());
        assert!(output.syncs.is_empty());
    }

    #[test]
    fn ls_filter_multiple_kinds() {
        let (_dir, target) = setup_target(&yaml_docs(ALL_YAML));
        let output = ls(&target, &["Asset", "Sync"]).unwrap();

        assert_eq!(output.assets.len(), 2);
        assert!(output.connections.is_empty());
        assert!(output.conditions.is_empty());
        assert_eq!(output.syncs.len(), 1);
    }

    #[test]
    fn ls_filter_invalid_kind_returns_error() {
        let (_dir, target) = setup_target(&yaml_docs(ALL_YAML));
        let result = ls(&target, &["Invalid"]);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid"));
    }

    macro_rules! ls_filter_case_insensitive_test {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let (_dir, target) = setup_target(&yaml_docs(ALL_YAML));
                    let output = ls(&target, &[$input]).unwrap();
                    assert_eq!(output.assets.len(), 2);
                    assert!(output.connections.is_empty());
                }
            )*
        };
    }

    ls_filter_case_insensitive_test! {
        ls_filter_case_lower: "asset";
        ls_filter_case_upper: "ASSET";
        ls_filter_case_mixed: "Asset";
    }
}

use std::collections::HashMap;
use std::path::Path;

use crate::compile::CompileError;
use crate::dbt::manifest::{self, DbtManifest};
use crate::kind::connection::ConnectionSpec;
use crate::kind::origin::OriginSpec;
use crate::kind::NagiKind;

/// Per-Origin dbt configuration extracted from resources.
pub(crate) struct DbtOriginConfig {
    pub(crate) origin_name: String,
    pub(crate) project_dir: String,
    pub(crate) profile: String,
    pub(crate) target: Option<String>,
}

/// Extracts dbt configuration for each Origin by resolving its Connection.
pub(crate) fn collect_dbt_origin_configs(resources: &[NagiKind]) -> Vec<DbtOriginConfig> {
    let connection_profiles: HashMap<&str, (&str, Option<&str>)> = resources
        .iter()
        .filter_map(|r| match r {
            NagiKind::Connection {
                metadata,
                spec:
                    ConnectionSpec::Dbt {
                        ref profile,
                        ref target,
                        ..
                    },
                ..
            } => Some((
                metadata.name.as_str(),
                (profile.as_str(), target.as_deref()),
            )),
            _ => None,
        })
        .collect();

    resources
        .iter()
        .filter_map(|r| match r {
            NagiKind::Origin {
                metadata,
                spec:
                    OriginSpec::DBT {
                        connection,
                        project_dir,
                        ..
                    },
                ..
            } => {
                let (profile, target) = connection_profiles
                    .get(connection.as_str())
                    .map(|(p, t)| (p.to_string(), t.map(|s| s.to_string())))
                    .unwrap_or_default();
                Some(DbtOriginConfig {
                    origin_name: metadata.name.clone(),
                    project_dir: project_dir.clone(),
                    profile,
                    target,
                })
            }
            _ => None,
        })
        .collect()
}

/// Expands dbt Origin resources: loads manifests and generates Assets/Syncs.
///
/// 1. Extracts Origins from resources
/// 2. Runs `dbt compile` and reads manifest for each Origin
/// 3. Generates Assets and Syncs from manifest
/// 4. Returns all resources with generated ones appended
pub fn expand(resources: Vec<NagiKind>) -> Result<Vec<NagiKind>, CompileError> {
    let configs = collect_dbt_origin_configs(&resources);
    if configs.is_empty() {
        return Ok(resources);
    }

    let mut manifests = HashMap::new();
    for config in &configs {
        let manifest_json = crate::dbt::load_manifest(
            Path::new(&config.project_dir),
            &config.profile,
            config.target.as_deref(),
        )?;
        manifests.insert(config.origin_name.clone(), manifest_json);
    }

    expand_with_manifests(resources, &manifests)
}

/// Expands Origin resources using pre-loaded manifest JSON strings.
pub(crate) fn expand_with_manifests(
    resources: Vec<NagiKind>,
    manifests: &HashMap<String, String>,
) -> Result<Vec<NagiKind>, CompileError> {
    let origins: Vec<(String, OriginSpec)> = resources
        .iter()
        .filter_map(|r| match r {
            NagiKind::Origin { metadata, spec, .. } => Some((metadata.name.clone(), spec.clone())),
            _ => None,
        })
        .collect();

    if origins.is_empty() {
        return Ok(resources);
    }

    let mut expanded = resources;
    for (name, spec) in &origins {
        let manifest_str = manifests.get(name).ok_or_else(|| {
            CompileError::ManifestParse(format!("no manifest found for Origin '{name}'"))
        })?;
        let manifest: DbtManifest = serde_json::from_str(manifest_str)
            .map_err(|e| CompileError::ManifestParse(e.to_string()))?;
        let generated = manifest::manifest_to_resources(&manifest, spec);
        expanded.extend(generated);
    }

    Ok(expanded)
}

/// Returns the list of dbt Origin names and their project directories.
pub fn list_origin_dirs(resources_dir: &Path) -> Result<Vec<(String, String)>, CompileError> {
    let resources = crate::compile::load_resources(resources_dir)?;
    Ok(collect_dbt_origin_configs(&resources)
        .into_iter()
        .map(|c| (c.origin_name, c.project_dir))
        .collect())
}

/// Applies dbt Cloud job-to-model mapping to resolved assets.
/// For each asset, sets `dbt_cloud_job_ids` to the set of job IDs whose
/// `execute_steps` reference that asset's name.
pub fn apply_cloud_job_mapping(
    output: &mut crate::compile::CompileOutput,
    model_job_mapping: &HashMap<String, std::collections::HashSet<i64>>,
) {
    for asset in &mut output.assets {
        if let Some(job_ids) = model_job_mapping.get(&asset.metadata.name) {
            asset.dbt_cloud_job_ids = Some(job_ids.clone());
        }
    }
}

/// Detects if a sync step uses a dbt command that updates multiple Assets.
pub(crate) fn detect_multi_asset_step(args: &[String]) -> Option<String> {
    if args.iter().any(|a| a == "dbt") && args.iter().any(|a| a == "build") {
        return Some(
            "uses `dbt build` which updates multiple models in a single execution".to_string(),
        );
    }
    if let Some(tag) = args.iter().find(|a| a.starts_with("tag:")) {
        return Some(format!(
            "uses tag-based selector '{tag}' which may update multiple models in a single execution",
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kind::parse_kinds;

    // ── collect_dbt_origin_configs tests ─────────────────────────────

    #[test]
    fn collect_configs_returns_target_from_connection() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bq
spec:
  type: dbt
  profile: my_project
  target: prod
---
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: dbt-origin
spec:
  type: DBT
  connection: my-bq
  projectDir: ../dbt-project";
        let resources = parse_kinds(yaml).unwrap();
        let configs = collect_dbt_origin_configs(&resources);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].origin_name, "dbt-origin");
        assert_eq!(configs[0].project_dir, "../dbt-project");
        assert_eq!(configs[0].profile, "my_project");
        assert_eq!(configs[0].target.as_deref(), Some("prod"));
    }

    #[test]
    fn collect_configs_returns_none_target_when_connection_has_no_target() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bq
spec:
  type: dbt
  profile: my_project
---
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: dbt-origin
spec:
  type: DBT
  connection: my-bq
  projectDir: ../dbt-project";
        let resources = parse_kinds(yaml).unwrap();
        let configs = collect_dbt_origin_configs(&resources);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].profile, "my_project");
        assert_eq!(configs[0].target, None);
    }

    #[test]
    fn collect_configs_returns_empty_when_no_origin() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bq
spec:
  type: dbt
  profile: my_project
  target: dev";
        let resources = parse_kinds(yaml).unwrap();
        let configs = collect_dbt_origin_configs(&resources);
        assert!(configs.is_empty());
    }

    #[test]
    fn collect_configs_handles_multiple_origins() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: bq-prod
spec:
  type: dbt
  profile: prod_profile
  target: prod
---
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: bq-dev
spec:
  type: dbt
  profile: dev_profile
---
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: dbt-main
spec:
  type: DBT
  connection: bq-prod
  projectDir: ../dbt-main
---
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: dbt-sub
spec:
  type: DBT
  connection: bq-dev
  projectDir: ../dbt-sub";
        let resources = parse_kinds(yaml).unwrap();
        let configs = collect_dbt_origin_configs(&resources);
        assert_eq!(configs.len(), 2);

        let main = configs
            .iter()
            .find(|c| c.origin_name == "dbt-main")
            .unwrap();
        assert_eq!(main.project_dir, "../dbt-main");
        assert_eq!(main.profile, "prod_profile");
        assert_eq!(main.target.as_deref(), Some("prod"));

        let sub = configs.iter().find(|c| c.origin_name == "dbt-sub").unwrap();
        assert_eq!(sub.project_dir, "../dbt-sub");
        assert_eq!(sub.profile, "dev_profile");
        assert_eq!(sub.target, None);
    }

    // ── detect_multi_asset_step tests ───────────────────────────────

    fn args(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    macro_rules! detect_multi_asset_step_test {
        ($($name:ident: $args:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let a = args($args);
                    let result = detect_multi_asset_step(&a);
                    assert_eq!(result.is_some(), $expected);
                }
            )*
        };
    }

    detect_multi_asset_step_test! {
        detect_dbt_build: &["dbt", "build", "--select", "model_a"] => true;
        detect_dbt_build_no_select: &["dbt", "build"] => true;
        detect_tag_selector: &["dbt", "run", "--select", "tag:finance"] => true;
        detect_tag_selector_combo: &["dbt", "run", "-s", "tag:finance,tag:daily"] => true;
        ignore_model_select: &["dbt", "run", "--select", "my_model"] => false;
        ignore_non_dbt_command: &["python", "run.py"] => false;
        ignore_empty_args: &[] => false;
    }
}

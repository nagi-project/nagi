use std::collections::HashMap;
use std::path::Path;

use crate::runtime::compile::CompileError;
use crate::runtime::kind::connection::ConnectionSpec;
use crate::runtime::kind::origin::dbt::manifest::{self, DbtManifest};
use crate::runtime::kind::origin::OriginSpec;
use crate::runtime::kind::NagiKind;

/// Per-Origin dbt configuration extracted from resources.
pub(crate) struct DbtOriginConfig {
    pub(crate) origin_name: String,
    pub(crate) project_dir: String,
    pub(crate) profile: String,
    pub(crate) target: Option<String>,
    pub(crate) profiles_dir: Option<String>,
}

/// Extracts dbt configuration for each Origin by resolving its Connection.
pub(crate) fn collect_dbt_origin_configs(resources: &[NagiKind]) -> Vec<DbtOriginConfig> {
    let connection_info: HashMap<&str, (&str, Option<&str>, Option<&str>)> = resources
        .iter()
        .filter_map(|r| match r {
            NagiKind::Connection {
                metadata,
                spec:
                    ConnectionSpec::Dbt {
                        ref profile,
                        ref target,
                        ref profiles_dir,
                        ..
                    },
                ..
            } => Some((
                metadata.name.as_str(),
                (profile.as_str(), target.as_deref(), profiles_dir.as_deref()),
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
                    OriginSpec::Dbt {
                        connection,
                        project_dir,
                        ..
                    },
                ..
            } => {
                let (profile, target, profiles_dir) = connection_info
                    .get(connection.as_str())
                    .map(|(p, t, d)| {
                        (
                            p.to_string(),
                            t.map(|s| s.to_string()),
                            d.map(|s| s.to_string()),
                        )
                    })
                    .unwrap_or_default();
                Some(DbtOriginConfig {
                    origin_name: metadata.name.clone(),
                    project_dir: project_dir.clone(),
                    profile,
                    target,
                    profiles_dir,
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
        let manifest_json = crate::runtime::kind::origin::dbt::load_manifest(
            Path::new(&config.project_dir),
            &config.profile,
            config.target.as_deref(),
            config.profiles_dir.as_deref(),
        )?;
        manifests.insert(config.origin_name.clone(), manifest_json);
    }

    let profiles_dirs: HashMap<String, Option<String>> = configs
        .into_iter()
        .map(|c| (c.origin_name, c.profiles_dir))
        .collect();

    expand_with_manifests(resources, &manifests, Some(&profiles_dirs))
}

/// Expands Origin resources using pre-loaded manifest JSON strings.
pub(crate) fn expand_with_manifests(
    resources: Vec<NagiKind>,
    manifests: &HashMap<String, String>,
    profiles_dirs: Option<&HashMap<String, Option<String>>>,
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
        let profiles_dir = profiles_dirs
            .and_then(|m| m.get(name))
            .and_then(|d| d.as_deref());
        let generated = manifest::manifest_to_resources(&manifest, spec, profiles_dir);
        expanded.extend(generated);
    }

    Ok(expanded)
}

/// Returns the list of dbt Origin names and their project directories.
pub fn list_origin_dirs(resources_dir: &Path) -> Result<Vec<(String, String)>, CompileError> {
    let resources = crate::runtime::compile::load_resources(resources_dir)?;
    Ok(collect_dbt_origin_configs(&resources)
        .into_iter()
        .map(|c| (c.origin_name, c.project_dir))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::kind::parse_kinds;

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
}

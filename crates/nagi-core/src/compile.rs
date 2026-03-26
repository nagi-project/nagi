use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dbt::manifest::{self, DbtManifest};
use crate::kind::asset::{
    self as asset, validate_no_duplicate_condition_names, AssetSpec, DesiredCondition,
};
use crate::kind::origin::OriginSpec;
use crate::kind::sync::SyncSpec;
use crate::kind::{self, KindError, Metadata, NagiKind};

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("failed to read assets directory: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Kind(#[from] KindError),

    #[error("unresolved reference: {kind} '{name}' not found")]
    UnresolvedRef { kind: String, name: String },

    #[error("duplicate resource: {kind} '{name}' defined more than once")]
    DuplicateName { kind: String, name: String },

    #[error("dependency cycle detected involving '{name}'")]
    CycleDetected { name: String },

    #[error("dbt compile failed: {0}")]
    DbtCompileFailed(String),

    #[error("manifest.json parse error: {0}")]
    ManifestParse(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DependencyGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphNode {
    pub name: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug)]
pub struct CompileOutput {
    pub assets: Vec<ResolvedAsset>,
    pub graph: DependencyGraph,
}

#[derive(Debug, Clone)]
pub struct ResolvedAsset {
    pub metadata: Metadata,
    pub spec: AssetSpec,
    /// Resolved on_drift entries: conditions expanded + sync specs resolved.
    pub resolved_on_drift: Vec<ResolvedOnDriftEntry>,
    pub connection: Option<ResolvedConnection>,
    /// dbt Cloud job IDs whose execute_steps include this asset.
    pub dbt_cloud_job_ids: Option<HashSet<i64>>,
}

/// A compiled on_drift entry with resolved conditions and sync spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedOnDriftEntry {
    /// Resolved conditions from the referenced Conditions.
    pub conditions: Vec<DesiredCondition>,
    /// Name of the conditions group (for display/logging).
    pub conditions_ref: String,
    /// Resolved and template-expanded sync spec.
    pub sync: SyncSpec,
    /// Name of the sync ref (for lock coordination).
    pub sync_ref_name: String,
}

/// Connection info resolved from Asset → Connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ResolvedConnection {
    /// Connection resolved via dbt profiles.yml.
    #[serde(rename_all = "camelCase")]
    DbtProfile {
        /// Original Connection resource name.
        name: String,
        profile: String,
        target: Option<String>,
        /// Path to the dbt Cloud credentials file, if dbt Cloud is configured.
        dbt_cloud_credentials_file: Option<String>,
    },
}

/// Applies dbt Cloud job-to-model mapping to resolved assets.
/// For each asset, sets `dbt_cloud_job_ids` to the set of job IDs whose
/// `execute_steps` reference that asset's name.
pub fn apply_dbt_cloud_job_mapping(
    output: &mut CompileOutput,
    model_job_mapping: &HashMap<String, HashSet<i64>>,
) {
    for asset in &mut output.assets {
        if let Some(job_ids) = model_job_mapping.get(&asset.metadata.name) {
            asset.dbt_cloud_job_ids = Some(job_ids.clone());
        }
    }
}

/// Compiles all YAML resources from `resources_dir` and writes resolved output to `target_dir`.
/// When `export_config` is provided, auto-generates export Assets for log tables.
pub fn compile(
    resources_dir: &Path,
    target_dir: &Path,
    export_config: Option<&crate::config::ExportConfig>,
) -> Result<CompileOutput, CompileError> {
    let resources = load_resources(resources_dir)?;

    let manifests = load_dbt_manifests(&resources)?;

    let mut resources = expand_origins(resources, &manifests)?;

    if let Some(cfg) = export_config {
        resources.extend(crate::export::generate_export_resources(cfg));
    }

    let output = resolve(resources)?;
    write_output(&output, target_dir)?;
    Ok(output)
}

/// Expands Origin resources by generating Assets and Syncs from dbt manifests.
pub fn expand_origins(
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

/// Per-Origin dbt configuration extracted from resources.
struct DbtOriginConfig {
    origin_name: String,
    project_dir: String,
    profile: String,
    target: Option<String>,
}

/// Extracts dbt configuration for each Origin by resolving its Connection.
fn collect_dbt_origin_configs(resources: &[NagiKind]) -> Vec<DbtOriginConfig> {
    let connection_profiles: HashMap<&str, (&str, Option<&str>)> = resources
        .iter()
        .filter_map(|r| match r {
            NagiKind::Connection { metadata, spec, .. } => Some((
                metadata.name.as_str(),
                (
                    spec.dbt_profile.profile.as_str(),
                    spec.dbt_profile.target.as_deref(),
                ),
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

/// Returns the list of dbt Origin names and their project directories.
pub fn list_dbt_origin_dirs(resources_dir: &Path) -> Result<Vec<(String, String)>, CompileError> {
    let resources = load_resources(resources_dir)?;
    Ok(collect_dbt_origin_configs(&resources)
        .into_iter()
        .map(|c| (c.origin_name, c.project_dir))
        .collect())
}

/// Resolves asset names from compiled output.
///
/// When selectors are provided, uses the graph to filter.
/// Otherwise, lists all `.yaml` files in `assets_path`.
pub fn resolve_asset_names(
    graph_json: &str,
    selectors: &[&str],
    assets_path: &Path,
) -> Result<Vec<String>, CompileError> {
    if !selectors.is_empty() {
        let graph: DependencyGraph = serde_json::from_str(graph_json)
            .map_err(|e| CompileError::ManifestParse(e.to_string()))?;
        let selected = crate::select::select_assets(&graph, selectors).map_err(|e| {
            CompileError::UnresolvedRef {
                kind: "asset".to_string(),
                name: e.to_string(),
            }
        })?;
        return Ok(selected);
    }
    let mut names: Vec<String> = std::fs::read_dir(assets_path)
        .map_err(CompileError::Io)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("yaml") {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();
    names.sort();
    Ok(names)
}

/// Loads and parses the dependency graph from `target/graph.json`.
pub fn load_graph(target_dir: &Path) -> Result<DependencyGraph, CompileError> {
    let graph_path = target_dir.join("graph.json");
    let graph_json = std::fs::read_to_string(&graph_path).map_err(CompileError::Io)?;
    serde_json::from_str(&graph_json).map_err(|e| CompileError::ManifestParse(e.to_string()))
}

/// Resolves asset names from a compiled target directory.
pub fn resolve_compiled_asset_names(
    target_dir: &Path,
    selectors: &[&str],
) -> Result<Vec<String>, CompileError> {
    let assets_path = target_dir.join("assets");
    let graph_path = target_dir.join("graph.json");
    let graph_json = std::fs::read_to_string(&graph_path).map_err(CompileError::Io)?;
    resolve_asset_names(&graph_json, selectors, &assets_path)
}

/// Resolves asset names from selectors or directory listing, then reads each
/// compiled YAML file. Returns `Vec<(name, yaml_content)>`.
pub fn load_compiled_assets(
    target_dir: &Path,
    selectors: &[&str],
) -> Result<Vec<(String, String)>, CompileError> {
    let names = resolve_compiled_asset_names(target_dir, selectors)?;
    let assets_path = target_dir.join("assets");
    let mut result = Vec::with_capacity(names.len());
    for name in names {
        let yaml_path = assets_path.join(format!("{name}.yaml"));
        let content = std::fs::read_to_string(&yaml_path).map_err(CompileError::Io)?;
        result.push((name, content));
    }
    Ok(result)
}

/// Loads dbt manifests for all DBT Origins.
///
/// Returns a map of origin name → manifest JSON.
fn load_dbt_manifests(resources: &[NagiKind]) -> Result<HashMap<String, String>, CompileError> {
    let configs = collect_dbt_origin_configs(resources);
    let mut manifests = HashMap::new();
    for config in &configs {
        let manifest_json = crate::dbt::load_manifest(
            Path::new(&config.project_dir),
            &config.profile,
            config.target.as_deref(),
        )?;
        manifests.insert(config.origin_name.clone(), manifest_json);
    }
    Ok(manifests)
}

pub fn load_resources(dir: &Path) -> Result<Vec<NagiKind>, CompileError> {
    if !dir.exists() {
        return Err(CompileError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("assets directory not found: {}", dir.display()),
        )));
    }
    let mut resources = Vec::new();
    let mut visited = HashSet::new();
    load_resources_recursive(dir, &mut resources, &mut visited)?;
    Ok(resources)
}

fn load_resources_recursive(
    dir: &Path,
    resources: &mut Vec<NagiKind>,
    visited: &mut HashSet<std::path::PathBuf>,
) -> Result<(), CompileError> {
    let canonical = dir.canonicalize()?;
    if !visited.insert(canonical) {
        return Ok(());
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|e| e.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            load_resources_recursive(&path, resources, visited)?;
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("yaml") | Some("yml")
        ) {
            let content = std::fs::read_to_string(&path)?;
            let kinds = kind::parse_kinds(&content)?;
            resources.extend(kinds);
        }
    }
    Ok(())
}

use crate::kind::connection::ConnectionSpec;

struct CategorizedResources {
    connections: HashMap<String, ConnectionSpec>,
    conditions_groups: HashMap<String, Vec<DesiredCondition>>,
    syncs: HashMap<String, SyncSpec>,
    assets: Vec<(Metadata, AssetSpec)>,
}

fn categorize(resources: Vec<NagiKind>) -> Result<CategorizedResources, CompileError> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut result = CategorizedResources {
        connections: HashMap::new(),
        conditions_groups: HashMap::new(),
        syncs: HashMap::new(),
        assets: Vec::new(),
    };
    // Track Asset names separately for overlay merge (max 2 allowed).
    let mut asset_indices: HashMap<String, usize> = HashMap::new();

    let check_dup = |seen: &mut HashSet<(String, String)>,
                     kind: String,
                     name: String|
     -> Result<(), CompileError> {
        if !seen.insert((kind.clone(), name.clone())) {
            return Err(CompileError::DuplicateName { kind, name });
        }
        Ok(())
    };

    for resource in resources {
        let kind = resource.kind().to_string();
        let name = resource.metadata().name.clone();
        match resource {
            NagiKind::Asset { metadata, spec, .. } => {
                if let Some(&idx) = asset_indices.get(&name) {
                    check_dup(&mut seen, kind, name)?;
                    let overlay = std::mem::take(&mut result.assets[idx].1.on_drift);
                    result.assets[idx].1.on_drift =
                        asset::merge_on_drift_entries(overlay, spec.on_drift);
                } else {
                    asset_indices.insert(name, result.assets.len());
                    result.assets.push((metadata, spec));
                }
            }
            NagiKind::Connection { spec, .. } => {
                check_dup(&mut seen, kind, name.clone())?;
                result.connections.insert(name, spec);
            }
            NagiKind::Conditions { spec, .. } => {
                check_dup(&mut seen, kind, name.clone())?;
                result.conditions_groups.insert(name, spec.0.clone());
            }
            NagiKind::Sync { spec, .. } => {
                check_dup(&mut seen, kind, name.clone())?;
                result.syncs.insert(name, spec);
            }
            NagiKind::Origin { .. } => {}
        }
    }

    Ok(result)
}

fn require_sync_ref(syncs: &HashMap<String, SyncSpec>, name: &str) -> Result<(), CompileError> {
    if !syncs.contains_key(name) {
        return Err(CompileError::UnresolvedRef {
            kind: "Sync".to_string(),
            name: name.to_string(),
        });
    }
    Ok(())
}

/// Expands template variables in a SyncSpec's args.
/// Supported variables: `{{ asset.name }}`, `{{ sync.<key> }}` (from `with` map).
fn expand_sync_templates(
    sync_spec: &SyncSpec,
    asset_name: &str,
    with: &HashMap<String, String>,
) -> SyncSpec {
    let expand_step = |step: &crate::kind::sync::SyncStep| -> crate::kind::sync::SyncStep {
        crate::kind::sync::SyncStep {
            step_type: step.step_type.clone(),
            args: step
                .args
                .iter()
                .map(|arg| expand_template_string(arg, asset_name, with))
                .collect(),
            env: step.env.clone(),
        }
    };
    SyncSpec {
        pre: sync_spec.pre.as_ref().map(&expand_step),
        run: expand_step(&sync_spec.run),
        post: sync_spec.post.as_ref().map(&expand_step),
    }
}

fn expand_template_string(s: &str, asset_name: &str, with: &HashMap<String, String>) -> String {
    let mut result = s.replace("{{ asset.name }}", asset_name);
    for (key, value) in with {
        result = result.replace(&format!("{{{{ sync.{key} }}}}"), value);
    }
    result
}

/// Resolves all references and builds the dependency graph.
pub fn resolve(resources: Vec<NagiKind>) -> Result<CompileOutput, CompileError> {
    let CategorizedResources {
        connections,
        conditions_groups,
        syncs,
        assets,
    } = categorize(resources)?;

    let asset_names: HashSet<String> = assets.iter().map(|(m, _)| m.name.clone()).collect();

    let mut resolved_assets = Vec::new();
    for (metadata, spec) in assets {
        resolved_assets.push(resolve_asset(
            metadata,
            spec,
            &asset_names,
            &connections,
            &conditions_groups,
            &syncs,
        )?);
    }

    let graph = build_graph(&resolved_assets)?;
    detect_cycles(&graph)?;

    Ok(CompileOutput {
        assets: resolved_assets,
        graph,
    })
}

/// Validates references and resolves a single Asset's on_drift entries and connection.
fn resolve_asset(
    metadata: Metadata,
    spec: AssetSpec,
    asset_names: &HashSet<String>,
    connections: &HashMap<String, ConnectionSpec>,
    conditions_groups: &HashMap<String, Vec<DesiredCondition>>,
    syncs: &HashMap<String, SyncSpec>,
) -> Result<ResolvedAsset, CompileError> {
    for upstream_ref in &spec.upstreams {
        if !asset_names.contains(upstream_ref) {
            return Err(CompileError::UnresolvedRef {
                kind: "Asset".to_string(),
                name: upstream_ref.clone(),
            });
        }
    }

    let resolved_on_drift =
        resolve_on_drift(&metadata.name, &spec.on_drift, conditions_groups, syncs)?;
    let connection = resolve_connection(&spec.connection, connections)?;

    Ok(ResolvedAsset {
        metadata,
        spec,
        resolved_on_drift,
        connection,
        dbt_cloud_job_ids: None,
    })
}

/// Resolves on_drift entries: validates conditions/sync refs and expands templates.
fn resolve_on_drift(
    asset_name: &str,
    on_drift: &[asset::OnDriftEntry],
    conditions_groups: &HashMap<String, Vec<DesiredCondition>>,
    syncs: &HashMap<String, SyncSpec>,
) -> Result<Vec<ResolvedOnDriftEntry>, CompileError> {
    let mut resolved = Vec::new();
    let mut all_conditions: Vec<DesiredCondition> = Vec::new();

    for entry in on_drift {
        let conditions = conditions_groups.get(&entry.conditions).ok_or_else(|| {
            CompileError::UnresolvedRef {
                kind: "Conditions".to_string(),
                name: entry.conditions.clone(),
            }
        })?;

        require_sync_ref(syncs, &entry.sync)?;
        let sync_spec = &syncs[&entry.sync];
        let resolved_sync = expand_sync_templates(sync_spec, asset_name, &entry.with);

        all_conditions.extend(conditions.iter().cloned());
        resolved.push(ResolvedOnDriftEntry {
            conditions: conditions.clone(),
            conditions_ref: entry.conditions.clone(),
            sync: resolved_sync,
            sync_ref_name: entry.sync.clone(),
        });
    }

    validate_no_duplicate_condition_names(&all_conditions)?;
    Ok(resolved)
}

/// Validates and resolves the connection reference.
fn resolve_connection(
    connection_ref: &Option<String>,
    connections: &HashMap<String, ConnectionSpec>,
) -> Result<Option<ResolvedConnection>, CompileError> {
    let Some(conn_name) = connection_ref else {
        return Ok(None);
    };
    let conn_spec = connections
        .get(conn_name)
        .ok_or_else(|| CompileError::UnresolvedRef {
            kind: "Connection".to_string(),
            name: conn_name.clone(),
        })?;
    Ok(Some(ResolvedConnection::DbtProfile {
        name: conn_name.clone(),
        profile: conn_spec.dbt_profile.profile.clone(),
        target: conn_spec.dbt_profile.target.clone(),
        dbt_cloud_credentials_file: conn_spec.dbt_cloud.as_ref().map(|c| {
            c.credentials_file
                .clone()
                .unwrap_or_else(|| "~/.dbt/dbt_cloud.yml".to_string())
        }),
    }))
}

fn build_graph(assets: &[ResolvedAsset]) -> Result<DependencyGraph, CompileError> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for asset in assets {
        nodes.push(GraphNode {
            name: asset.metadata.name.clone(),
            kind: "Asset".to_string(),
            tags: asset.spec.tags.clone(),
        });
        for upstream in &asset.spec.upstreams {
            edges.push(GraphEdge {
                from: upstream.clone(),
                to: asset.metadata.name.clone(),
            });
        }
    }

    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    edges.sort_by(|a, b| (&a.from, &a.to).cmp(&(&b.from, &b.to)));

    Ok(DependencyGraph { nodes, edges })
}

/// Detects cycles using Kahn's algorithm (topological sort via BFS).
/// If not all nodes are visited, at least one cycle exists.
fn detect_cycles(graph: &DependencyGraph) -> Result<(), CompileError> {
    let mut in_degree: HashMap<&str, usize> =
        graph.nodes.iter().map(|n| (n.name.as_str(), 0)).collect();
    let mut adjacency: HashMap<&str, Vec<&str>> = graph
        .nodes
        .iter()
        .map(|n| (n.name.as_str(), vec![]))
        .collect();

    for edge in &graph.edges {
        adjacency
            .get_mut(edge.from.as_str())
            .unwrap()
            .push(&edge.to);
        *in_degree.get_mut(edge.to.as_str()).unwrap() += 1;
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&name, _)| name)
        .collect();

    let mut visited = 0usize;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        for &neighbor in &adjacency[node] {
            let deg = in_degree.get_mut(neighbor).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(neighbor);
            }
        }
    }

    if visited < in_degree.len() {
        let name = in_degree
            .into_iter()
            .find(|(_, deg)| *deg > 0)
            .map(|(name, _)| name.to_string())
            .unwrap_or_default();
        return Err(CompileError::CycleDetected { name });
    }

    Ok(())
}

/// Serialization-only struct for writing compiled assets to `target/`.
/// Embeds resolved SyncSpec directly instead of SyncRef.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompiledAssetYaml<'a> {
    api_version: &'static str,
    kind: &'static str,
    metadata: &'a Metadata,
    spec: CompiledAssetSpecYaml<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connection: &'a Option<ResolvedConnection>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompiledAssetSpecYaml<'a> {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: &'a Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    upstreams: &'a Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    on_drift: &'a Vec<ResolvedOnDriftEntry>,
    auto_sync: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    dbt_cloud_job_ids: &'a Option<HashSet<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    evaluate_cache_ttl: &'a Option<crate::duration::Duration>,
}

/// Deserialization struct for reading compiled asset YAML from `target/`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledAsset {
    pub api_version: String,
    pub metadata: Metadata,
    pub spec: CompiledAssetSpec,
    #[serde(default)]
    pub connection: Option<ResolvedConnection>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledAssetSpec {
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub upstreams: Vec<String>,
    #[serde(default)]
    pub on_drift: Vec<ResolvedOnDriftEntry>,
    #[serde(default = "default_true")]
    pub auto_sync: bool,
    /// dbt Cloud job IDs that include this asset in their execute_steps.
    /// Resolved at compile time. Used for running-job checks before sync.
    #[serde(default)]
    pub dbt_cloud_job_ids: Option<HashSet<i64>>,
    /// Asset-level default evaluate cache TTL.
    #[serde(default, rename = "evaluateCacheTtl")]
    pub evaluate_cache_ttl: Option<crate::duration::Duration>,
}

fn default_true() -> bool {
    true
}

pub fn write_output(output: &CompileOutput, target_dir: &Path) -> Result<(), CompileError> {
    let assets_dir = target_dir.join("assets");
    std::fs::create_dir_all(&assets_dir)?;

    for asset in &output.assets {
        let compiled = CompiledAssetYaml {
            api_version: kind::API_VERSION,
            kind: "Asset",
            metadata: &asset.metadata,
            spec: CompiledAssetSpecYaml {
                tags: &asset.spec.tags,
                upstreams: &asset.spec.upstreams,
                on_drift: &asset.resolved_on_drift,
                auto_sync: asset.spec.auto_sync,
                dbt_cloud_job_ids: &asset.dbt_cloud_job_ids,
                evaluate_cache_ttl: &asset.spec.evaluate_cache_ttl,
            },
            connection: &asset.connection,
        };
        let yaml = serde_yaml::to_string(&compiled).map_err(KindError::YamlParse)?;
        std::fs::write(
            assets_dir.join(format!("{}.yaml", asset.metadata.name)),
            yaml,
        )?;
    }

    let graph_json = serde_json::to_string_pretty(&output.graph).map_err(std::io::Error::other)?;
    std::fs::write(target_dir.join("graph.json"), graph_json)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kind::parse_kinds;
    use tempfile::TempDir;

    // ── YAML fragments ──────────────────────────────────────────────────

    const CONNECTION_MY_BQ: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bq
spec:
  dbtProfile:
    profile: my_project";

    const ASSET_RAW_SALES: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: raw-sales
spec:
  connection: my-bq";

    const SYNC_DBT_RUN: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-run
spec:
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--select\", \"{{ asset.name }}\"]";

    const SYNC_DBT_FULL: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-full
spec:
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--full-refresh\", \"--select\", \"{{ asset.name }}\"]";

    const DESIRED_GROUP_DAILY_SLA: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: daily-sla
spec:
  - name: freshness-24h
    type: Freshness
    maxAge: 24h
    interval: 6h";

    /// Joins YAML documents with `---` separator.
    fn yaml_docs(docs: &[&str]) -> String {
        docs.join("\n---\n")
    }

    fn write_yaml(dir: &Path, filename: &str, content: &str) {
        std::fs::write(dir.join(filename), content).unwrap();
    }

    fn parse(yaml: &str) -> Vec<NagiKind> {
        parse_kinds(yaml).unwrap()
    }

    // ── resolve_connection tests ───────────────────────────────────────

    #[test]
    fn resolve_connection_none_returns_none() {
        let connections = HashMap::new();
        let result = resolve_connection(&None, &connections).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn resolve_connection_valid_ref() {
        let mut connections = HashMap::new();
        connections.insert(
            "my-bq".to_string(),
            ConnectionSpec {
                dbt_profile: crate::kind::connection::DbtProfile {
                    profile: "proj".to_string(),
                    target: Some("dev".to_string()),
                },
                dbt_cloud: None,
            },
        );
        let result = resolve_connection(&Some("my-bq".to_string()), &connections).unwrap();
        let conn = result.unwrap();
        assert!(
            matches!(conn, ResolvedConnection::DbtProfile { name, profile, .. }
            if name == "my-bq" && profile == "proj")
        );
    }

    #[test]
    fn resolve_connection_invalid_ref() {
        let connections = HashMap::new();
        let err = resolve_connection(&Some("missing".to_string()), &connections).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Connection" && name == "missing"));
    }

    // ── resolve_on_drift tests ──────────────────────────────────────────

    fn sample_conditions() -> HashMap<String, Vec<DesiredCondition>> {
        HashMap::from([(
            "daily-sla".to_string(),
            vec![DesiredCondition::Freshness {
                name: "freshness-24h".to_string(),
                max_age: crate::duration::Duration::from_secs(86400),
                column: None,
                interval: crate::duration::Duration::from_secs(21600),
                check_at: None,
                evaluate_cache_ttl: None,
            }],
        )])
    }

    fn sample_syncs() -> HashMap<String, SyncSpec> {
        HashMap::from([(
            "dbt-run".to_string(),
            SyncSpec {
                pre: None,
                run: crate::kind::sync::SyncStep {
                    step_type: crate::kind::sync::StepType::Command,
                    args: vec![
                        "dbt".to_string(),
                        "run".to_string(),
                        "--select".to_string(),
                        "{{ asset.name }}".to_string(),
                    ],
                    env: HashMap::new(),
                },
                post: None,
            },
        )])
    }

    #[test]
    fn resolve_on_drift_empty() {
        let result = resolve_on_drift("a", &[], &HashMap::new(), &HashMap::new()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn resolve_on_drift_expands_conditions_and_templates() {
        let entry = asset::OnDriftEntry {
            conditions: "daily-sla".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::new(),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let result = resolve_on_drift(
            "daily-sales",
            &[entry],
            &sample_conditions(),
            &sample_syncs(),
        )
        .unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(
            &result[0].conditions[0],
            DesiredCondition::Freshness { .. }
        ));
        assert_eq!(result[0].sync.run.args[3], "daily-sales");
    }

    #[test]
    fn resolve_on_drift_rejects_missing_conditions_ref() {
        let entry = asset::OnDriftEntry {
            conditions: "nonexistent".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::new(),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let err = resolve_on_drift("a", &[entry], &HashMap::new(), &sample_syncs()).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, .. } if kind == "Conditions"));
    }

    #[test]
    fn resolve_on_drift_rejects_missing_sync_ref() {
        let entry = asset::OnDriftEntry {
            conditions: "daily-sla".to_string(),
            sync: "nonexistent".to_string(),
            with: HashMap::new(),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let err =
            resolve_on_drift("a", &[entry], &sample_conditions(), &HashMap::new()).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, .. } if kind == "Sync"));
    }

    #[test]
    fn resolve_on_drift_rejects_duplicate_condition_names() {
        let conditions = HashMap::from([
            (
                "group-a".to_string(),
                vec![DesiredCondition::Command {
                    name: "dup-name".to_string(),
                    run: vec!["true".to_string()],
                    interval: None,
                    env: HashMap::new(),
                    evaluate_cache_ttl: None,
                }],
            ),
            (
                "group-b".to_string(),
                vec![DesiredCondition::Command {
                    name: "dup-name".to_string(),
                    run: vec!["true".to_string()],
                    interval: None,
                    env: HashMap::new(),
                    evaluate_cache_ttl: None,
                }],
            ),
        ]);
        let entries = vec![
            asset::OnDriftEntry {
                conditions: "group-a".to_string(),
                sync: "dbt-run".to_string(),
                with: HashMap::new(),
                merge_position: asset::MergePosition::BeforeOrigin,
            },
            asset::OnDriftEntry {
                conditions: "group-b".to_string(),
                sync: "dbt-run".to_string(),
                with: HashMap::new(),
                merge_position: asset::MergePosition::BeforeOrigin,
            },
        ];
        let err = resolve_on_drift("a", &entries, &conditions, &sample_syncs()).unwrap_err();
        assert!(matches!(err, CompileError::Kind(_)));
    }

    #[test]
    fn resolve_on_drift_with_variables() {
        let syncs = HashMap::from([(
            "dbt-run".to_string(),
            SyncSpec {
                pre: None,
                run: crate::kind::sync::SyncStep {
                    step_type: crate::kind::sync::StepType::Command,
                    args: vec![
                        "dbt".to_string(),
                        "run".to_string(),
                        "--select".to_string(),
                        "{{ sync.selector }}".to_string(),
                    ],
                    env: HashMap::new(),
                },
                post: None,
            },
        )]);
        let entry = asset::OnDriftEntry {
            conditions: "daily-sla".to_string(),
            sync: "dbt-run".to_string(),
            with: HashMap::from([("selector".to_string(), "+daily_sales".to_string())]),
            merge_position: asset::MergePosition::BeforeOrigin,
        };
        let result =
            resolve_on_drift("daily-sales", &[entry], &sample_conditions(), &syncs).unwrap();
        assert_eq!(result[0].sync.run.args[3], "+daily_sales");
    }

    // ── resolve (integration) tests ─────────────────────────────────────

    #[test]
    fn resolve_minimal_asset() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}",
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        assert_eq!(output.assets[0].metadata.name, "daily-sales");
        assert_eq!(output.graph.nodes.len(), 1);
        assert!(output.graph.edges.is_empty());
    }

    #[test]
    fn resolve_rejects_unresolved_upstream_ref() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  upstreams:
    - nonexistent-asset",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Asset" && name == "nonexistent-asset"));
    }

    #[test]
    fn resolve_merges_duplicate_asset_on_drift() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: quality-checks
spec:
  - name: check-b
    type: SQL
    query: \"SELECT true\"",
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: quality-checks
      sync: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        assert_eq!(output.assets[0].metadata.name, "daily-sales");
        assert_eq!(output.assets[0].resolved_on_drift.len(), 2);
    }

    #[test]
    fn resolve_merge_preserves_first_asset_fields() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: quality-checks
spec:
  - name: check-b
    type: SQL
    query: \"SELECT true\"",
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  tags: [finance]
  onDrift:
    - conditions: daily-sla
      sync: dbt-run
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  tags: [other]
  onDrift:
    - conditions: quality-checks
      sync: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        let asset = &output.assets[0];
        assert_eq!(asset.spec.tags, vec!["finance".to_string()]);
        assert_eq!(asset.resolved_on_drift.len(), 2);
    }

    #[test]
    fn resolve_rejects_triple_duplicate_asset() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::DuplicateName { kind, name }
            if kind == "Asset" && name == "daily-sales"));
    }

    #[test]
    fn resolve_merge_orders_by_merge_position() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: quality-checks
spec:
  - name: check-b
    type: SQL
    query: \"SELECT true\"",
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: post-checks
spec:
  - name: check-c
    type: SQL
    query: \"SELECT 1\"",
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-post
spec:
  run:
    type: Command
    args: [\"dbt\", \"test\"]",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run
      mergePosition: beforeOrigin
    - conditions: post-checks
      sync: dbt-post
      mergePosition: afterOrigin
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: quality-checks
      sync: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        let on_drift = &output.assets[0].resolved_on_drift;
        assert_eq!(on_drift.len(), 3);
        // [beforeOrigin user entry] + [origin entry] + [afterOrigin user entry]
        assert_eq!(on_drift[0].conditions_ref, "daily-sla");
        assert_eq!(on_drift[1].conditions_ref, "quality-checks");
        assert_eq!(on_drift[2].conditions_ref, "post-checks");
    }

    #[test]
    fn resolve_merge_default_position_is_before_origin() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: quality-checks
spec:
  - name: check-b
    type: SQL
    query: \"SELECT true\"",
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: quality-checks
      sync: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        let on_drift = &output.assets[0].resolved_on_drift;
        assert_eq!(on_drift.len(), 2);
        // Default mergePosition is beforeOrigin, so user entry comes first
        assert_eq!(on_drift[0].conditions_ref, "daily-sla");
        assert_eq!(on_drift[1].conditions_ref, "quality-checks");
    }

    #[test]
    fn resolve_builds_dependency_graph() {
        let resources = parse(&yaml_docs(&[
            CONNECTION_MY_BQ,
            ASSET_RAW_SALES,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  tags: [finance]
  upstreams:
    - raw-sales",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.graph.nodes.len(), 2);

        let asset_node = output
            .graph
            .nodes
            .iter()
            .find(|n| n.name == "daily-sales")
            .unwrap();
        assert_eq!(asset_node.tags, vec!["finance"]);

        assert_eq!(output.graph.edges.len(), 1);
        assert_eq!(output.graph.edges[0].from, "raw-sales");
        assert_eq!(output.graph.edges[0].to, "daily-sales");
    }

    #[test]
    fn resolve_asset_chain_dependency() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: raw
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: staging
spec:
  upstreams: [raw]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: mart
spec:
  upstreams: [staging]",
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 3);
        assert_eq!(output.graph.nodes.len(), 3);
        assert!(output.graph.nodes.iter().all(|n| n.kind == "Asset"));
        assert_eq!(output.graph.edges.len(), 2);

        let edge_pairs: Vec<(&str, &str)> = output
            .graph
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert!(edge_pairs.contains(&("raw", "staging")));
        assert!(edge_pairs.contains(&("staging", "mart")));
    }

    #[test]
    fn resolve_diamond_dependency() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: root
spec: {}
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: left
spec:
  upstreams: [root]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: right
spec:
  upstreams: [root]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: sink
spec:
  upstreams: [left, right]",
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 4);
        assert_eq!(output.graph.edges.len(), 4);

        let edge_pairs: Vec<(&str, &str)> = output
            .graph
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert!(edge_pairs.contains(&("root", "left")));
        assert!(edge_pairs.contains(&("root", "right")));
        assert!(edge_pairs.contains(&("left", "sink")));
        assert!(edge_pairs.contains(&("right", "sink")));
    }

    // ── write_output tests ────────────────────────────────────────────────

    #[test]
    fn write_output_creates_asset_yaml() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec: {}",
        );
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let yaml_path = target.join("assets/daily-sales.yaml");
        assert!(yaml_path.exists());

        let content = std::fs::read_to_string(&yaml_path).unwrap();
        let kinds = parse_kinds(&content).unwrap();
        assert_eq!(kinds.len(), 1);
        assert!(
            matches!(&kinds[0], NagiKind::Asset { metadata, .. } if metadata.name == "daily-sales")
        );
    }

    #[test]
    fn write_output_creates_graph_json() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(&yaml_docs(&[
            CONNECTION_MY_BQ,
            ASSET_RAW_SALES,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  upstreams:
    - raw-sales",
        ]));
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let graph_content = std::fs::read_to_string(target.join("graph.json")).unwrap();
        let graph: DependencyGraph = serde_json::from_str(&graph_content).unwrap();
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].from, "raw-sales");
        assert_eq!(graph.edges[0].to, "daily-sales");
    }

    #[test]
    fn write_output_embeds_resolved_on_drift_sync() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            SYNC_DBT_RUN,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sla
      sync: dbt-run",
        ]));
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let content = std::fs::read_to_string(target.join("assets/daily-sales.yaml")).unwrap();
        let value: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let sync_args = &value["spec"]["onDrift"][0]["sync"]["run"]["args"];
        let args: Vec<String> = serde_yaml::from_value(sync_args.clone()).unwrap();
        assert_eq!(args, vec!["dbt", "run", "--select", "daily-sales"]);
        assert!(!target.join("syncs").exists());
    }

    // ── load_resources tests ──────────────────────────────────────────────

    #[test]
    fn load_resources_reads_subdirectories() {
        let tmp = TempDir::new().unwrap();
        let resources_dir = tmp.path().join("resources");
        let subdir = resources_dir.join("subdir");
        std::fs::create_dir_all(&subdir).unwrap();

        write_yaml(
            &subdir,
            "asset.yaml",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: nested-asset
spec: {}",
        );

        let resources = load_resources(&resources_dir).unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].metadata().name, "nested-asset");
    }

    #[test]
    fn load_resources_errors_on_missing_directory() {
        let tmp = TempDir::new().unwrap();
        let assets = tmp.path().join("nonexistent");

        let err = load_resources(&assets).unwrap_err();
        assert!(matches!(err, CompileError::Io(_)));
    }

    #[test]
    fn load_resources_handles_circular_symlink() {
        let tmp = TempDir::new().unwrap();
        let resources_dir = tmp.path().join("resources");
        std::fs::create_dir_all(&resources_dir).unwrap();

        write_yaml(
            &resources_dir,
            "asset.yaml",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-asset
spec: {}",
        );

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&resources_dir, resources_dir.join("loop")).unwrap();
            let resources = load_resources(&resources_dir).unwrap();
            assert_eq!(resources.len(), 1);
            assert_eq!(resources[0].metadata().name, "my-asset");
        }
    }

    // ── expand_origins tests ────────────────────────────────────────────

    const MANIFEST_JSON: &str = r#"{
  "nodes": {
    "model.shop.stg_customers": {
      "unique_id": "model.shop.stg_customers",
      "resource_type": "model",
      "name": "stg_customers",
      "package_name": "shop",
      "tags": [],
      "depends_on": { "nodes": ["source.shop.raw.customers"] }
    },
    "model.shop.customers": {
      "unique_id": "model.shop.customers",
      "resource_type": "model",
      "name": "customers",
      "package_name": "shop",
      "tags": ["finance"],
      "depends_on": { "nodes": ["model.shop.stg_customers"] }
    },
    "test.shop.not_null_customers_id.abc": {
      "unique_id": "test.shop.not_null_customers_id.abc",
      "resource_type": "test",
      "name": "not_null_customers_id",
      "package_name": "shop",
      "tags": [],
      "depends_on": { "nodes": ["model.shop.customers"] },
      "test_metadata": { "name": "not_null", "kwargs": { "column_name": "id" } }
    }
  },
  "sources": {
    "source.shop.raw.customers": {
      "unique_id": "source.shop.raw.customers",
      "name": "customers",
      "source_name": "raw"
    }
  }
}"#;

    const ORIGIN_YAML: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: my-dbt
spec:
  type: DBT
  connection: my-bq
  projectDir: ../dbt-project
  defaultSync: dbt-run";

    fn manifests_for(origin_name: &str) -> HashMap<String, String> {
        HashMap::from([(origin_name.to_string(), MANIFEST_JSON.to_string())])
    }

    #[test]
    fn expand_origins_generates_resources_from_manifest() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]));
        let manifests = manifests_for("my-dbt");
        let expanded = expand_origins(resources, &manifests).unwrap();

        let assets: Vec<_> = expanded.iter().filter(|r| r.kind() == "Asset").collect();
        // 1 dbt source Asset + 2 model Assets
        assert_eq!(assets.len(), 3);

        let syncs: Vec<_> = expanded.iter().filter(|r| r.kind() == "Sync").collect();
        // dbt-run (user) + dbt-tag-finance (auto)
        assert_eq!(syncs.len(), 2);
    }

    #[test]
    fn expand_origins_noop_without_origin() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, ASSET_RAW_SALES]));
        let count = resources.len();
        let expanded = expand_origins(resources, &HashMap::new()).unwrap();
        assert_eq!(expanded.len(), count);
    }

    #[test]
    fn expand_origins_error_when_no_manifest() {
        let resources = parse(ORIGIN_YAML);
        let err = expand_origins(resources, &HashMap::new()).unwrap_err();
        assert!(matches!(err, CompileError::ManifestParse(_)));
    }

    #[test]
    fn resolve_with_origin_expansion() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]));
        let manifests = manifests_for("my-dbt");
        let expanded = expand_origins(resources, &manifests).unwrap();
        let output = resolve(expanded).unwrap();

        // 1 dbt source Asset + 2 model Assets
        assert_eq!(output.assets.len(), 3);
        let customer_asset = output
            .assets
            .iter()
            .find(|a| a.metadata.name == "customers")
            .unwrap();
        assert!(!customer_asset.resolved_on_drift.is_empty());

        // Verify model-to-model dependency edge exists (previously discarded).
        let edge_pairs: Vec<(&str, &str)> = output
            .graph
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert!(
            edge_pairs.contains(&("stg_customers", "customers")),
            "model-to-model dependency must produce a graph edge: edges = {edge_pairs:?}"
        );
        // raw.customers → stg_customers edge
        assert!(
            edge_pairs.contains(&("raw.customers", "stg_customers")),
            "upstream dependency must produce a graph edge: edges = {edge_pairs:?}"
        );
    }

    #[test]
    fn compile_with_origin_writes_target() {
        let tmp = TempDir::new().unwrap();
        let resources_dir = tmp.path().join("resources");
        let target_dir = tmp.path().join("nagi_target");
        std::fs::create_dir_all(&resources_dir).unwrap();

        write_yaml(
            &resources_dir,
            "infra.yaml",
            &yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]),
        );

        let resources = load_resources(&resources_dir).unwrap();
        let manifests = manifests_for("my-dbt");
        let resources = expand_origins(resources, &manifests).unwrap();
        let output = resolve(resources).unwrap();
        write_output(&output, &target_dir).unwrap();

        assert!(target_dir.join("graph.json").exists());
        assert!(target_dir.join("assets/customers.yaml").exists());
        assert!(target_dir.join("assets/stg_customers.yaml").exists());
    }

    #[test]
    fn collect_configs_returns_target_from_connection() {
        let yaml = "\
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bq
spec:
  dbtProfile:
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
  dbtProfile:
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
  dbtProfile:
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
  dbtProfile:
    profile: prod_profile
    target: prod
---
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: bq-dev
spec:
  dbtProfile:
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

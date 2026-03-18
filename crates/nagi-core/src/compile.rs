use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dbt::manifest::{self, DbtManifest};
use crate::kind::asset::{
    validate_no_duplicate_condition_names, AssetSpec, DesiredCondition, DesiredSetEntry,
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
    pub sync: Option<SyncSpec>,
    pub resync: Option<SyncSpec>,
    pub connection: Option<ResolvedConnection>,
}

/// Connection info resolved from Asset → Source → Connection chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedConnection {
    pub profile: String,
    pub target: Option<String>,
    /// Path to the dbt Cloud credentials file, if dbt Cloud is configured.
    pub dbt_cloud_credentials_file: Option<String>,
}

/// Compiles all YAML resources from `assets_dir` and writes resolved output to `target_dir`.
pub fn compile(assets_dir: &Path, target_dir: &Path) -> Result<CompileOutput, CompileError> {
    let resources = load_resources(assets_dir)?;

    let manifests = load_dbt_manifests(&resources)?;

    let resources = expand_origins(resources, &manifests)?;
    let output = resolve(resources)?;
    write_output(&output, target_dir)?;
    Ok(output)
}

/// Expands Origin resources by generating Assets, Sources, and Syncs from dbt manifests.
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
pub fn list_dbt_origin_dirs(assets_dir: &Path) -> Result<Vec<(String, String)>, CompileError> {
    let resources = load_resources(assets_dir)?;
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

fn load_resources(dir: &Path) -> Result<Vec<NagiKind>, CompileError> {
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
    sources: HashSet<String>,
    source_connections: HashMap<String, String>,
    desired_groups: HashMap<String, Vec<DesiredCondition>>,
    syncs: HashMap<String, SyncSpec>,
    assets: Vec<(Metadata, AssetSpec)>,
}

fn categorize(resources: Vec<NagiKind>) -> Result<CategorizedResources, CompileError> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut result = CategorizedResources {
        connections: HashMap::new(),
        sources: HashSet::new(),
        source_connections: HashMap::new(),
        desired_groups: HashMap::new(),
        syncs: HashMap::new(),
        assets: Vec::new(),
    };

    for resource in resources {
        let kind = resource.kind().to_string();
        let name = resource.metadata().name.clone();
        if !seen.insert((kind.clone(), name.clone())) {
            return Err(CompileError::DuplicateName { kind, name });
        }
        match resource {
            NagiKind::Connection { spec, .. } => {
                result.connections.insert(name, spec);
            }
            NagiKind::Source { spec, .. } => {
                result
                    .source_connections
                    .insert(name.clone(), spec.connection);
                result.sources.insert(name);
            }
            NagiKind::DesiredGroup { spec, .. } => {
                result.desired_groups.insert(name, spec.0.clone());
            }
            NagiKind::Sync { spec, .. } => {
                result.syncs.insert(name, spec);
            }
            NagiKind::Asset { metadata, spec, .. } => {
                result.assets.push((metadata, spec));
            }
            NagiKind::Origin { .. } => {
                // Origin resources are processed before categorize; skip here.
            }
        }
    }

    Ok(result)
}

fn require_ref(set: &HashSet<String>, kind: &str, name: &str) -> Result<(), CompileError> {
    if !set.contains(name) {
        return Err(CompileError::UnresolvedRef {
            kind: kind.to_string(),
            name: name.to_string(),
        });
    }
    Ok(())
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
        sources,
        source_connections,
        desired_groups,
        syncs,
        assets,
    } = categorize(resources)?;

    // Validate cross-kind references and resolve DesiredGroup refs.
    let mut resolved_assets = Vec::new();
    for (metadata, mut spec) in assets {
        for source_ref in &spec.sources {
            require_ref(&sources, "Source", &source_ref.ref_name)?;
        }
        // Resolve sync and resync: validate refs and expand templates.
        let resolved_sync = if let Some(sync_ref) = &spec.sync {
            require_sync_ref(&syncs, &sync_ref.ref_name)?;
            let sync_spec = &syncs[&sync_ref.ref_name];
            Some(expand_sync_templates(
                sync_spec,
                &metadata.name,
                &sync_ref.with,
            ))
        } else {
            None
        };
        let resolved_resync = if let Some(resync_ref) = &spec.resync {
            require_sync_ref(&syncs, &resync_ref.ref_name)?;
            let sync_spec = &syncs[&resync_ref.ref_name];
            Some(expand_sync_templates(
                sync_spec,
                &metadata.name,
                &resync_ref.with,
            ))
        } else {
            None
        };

        // Expand DesiredGroup refs to inline conditions.
        let mut resolved_entries = Vec::new();
        for entry in &spec.desired_sets {
            match entry {
                DesiredSetEntry::Ref(group_ref) => {
                    let conditions = desired_groups.get(&group_ref.ref_name).ok_or_else(|| {
                        CompileError::UnresolvedRef {
                            kind: "DesiredGroup".to_string(),
                            name: group_ref.ref_name.clone(),
                        }
                    })?;
                    for condition in conditions {
                        resolved_entries.push(DesiredSetEntry::Inline(condition.clone()));
                    }
                }
                DesiredSetEntry::Inline(c) => {
                    resolved_entries.push(DesiredSetEntry::Inline(c.clone()));
                }
            }
        }
        spec.desired_sets = resolved_entries;

        // All refs have been expanded to inline; extract conditions for duplicate check.
        let all_conditions: Vec<DesiredCondition> = spec
            .desired_sets
            .iter()
            .map(|e| match e {
                DesiredSetEntry::Inline(c) => c.clone(),
                DesiredSetEntry::Ref(_) => unreachable!("refs are expanded above"),
            })
            .collect();
        validate_no_duplicate_condition_names(&all_conditions)?;

        // Resolve connection from the first source's connection chain.
        let connection = spec
            .sources
            .first()
            .and_then(|s| source_connections.get(&s.ref_name))
            .and_then(|conn_name| connections.get(conn_name))
            .map(|conn_spec| ResolvedConnection {
                profile: conn_spec.dbt_profile.profile.clone(),
                target: conn_spec.dbt_profile.target.clone(),
                dbt_cloud_credentials_file: conn_spec.dbt_cloud.as_ref().map(|c| {
                    c.credentials_file
                        .clone()
                        .unwrap_or_else(|| "~/.dbt/dbt_cloud.yml".to_string())
                }),
            });

        resolved_assets.push(ResolvedAsset {
            metadata,
            spec,
            sync: resolved_sync,
            resync: resolved_resync,
            connection,
        });
    }

    let graph = build_graph(&resolved_assets, &sources)?;
    detect_cycles(&graph)?;

    Ok(CompileOutput {
        assets: resolved_assets,
        graph,
    })
}

fn build_graph(
    assets: &[ResolvedAsset],
    sources: &HashSet<String>,
) -> Result<DependencyGraph, CompileError> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for name in sources {
        nodes.push(GraphNode {
            name: name.clone(),
            kind: "Source".to_string(),
            tags: vec![],
        });
    }

    for asset in assets {
        nodes.push(GraphNode {
            name: asset.metadata.name.clone(),
            kind: "Asset".to_string(),
            tags: asset.spec.tags.clone(),
        });
        for source_ref in &asset.spec.sources {
            edges.push(GraphEdge {
                from: source_ref.ref_name.clone(),
                to: asset.metadata.name.clone(),
            });
        }
    }

    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    edges.sort_by(|a, b| (&a.from, &a.to).cmp(&(&b.from, &b.to)));

    Ok(DependencyGraph { nodes, edges })
}

fn detect_cycles(graph: &DependencyGraph) -> Result<(), CompileError> {
    let node_names: HashSet<&str> = graph.nodes.iter().map(|n| n.name.as_str()).collect();
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();

    for name in &node_names {
        in_degree.insert(name, 0);
        adjacency.insert(name, vec![]);
    }

    for edge in &graph.edges {
        if let Some(adj) = adjacency.get_mut(edge.from.as_str()) {
            adj.push(&edge.to);
        }
        if let Some(deg) = in_degree.get_mut(edge.to.as_str()) {
            *deg += 1;
        }
    }

    let mut queue: VecDeque<&str> = VecDeque::new();
    for (name, deg) in &in_degree {
        if *deg == 0 {
            queue.push_back(name);
        }
    }

    let mut visited = 0;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        if let Some(neighbors) = adjacency.get(node) {
            for neighbor in neighbors {
                if let Some(deg) = in_degree.get_mut(neighbor) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(neighbor);
                    }
                }
            }
        }
    }

    if visited < node_names.len() {
        let cycle_node = in_degree
            .iter()
            .find(|(_, deg)| **deg > 0)
            .map(|(name, _)| name.to_string())
            .unwrap_or_default();
        return Err(CompileError::CycleDetected { name: cycle_node });
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
    sources: &'a Vec<crate::kind::asset::SourceRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    desired_sets: &'a Vec<DesiredSetEntry>,
    auto_sync: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    sync: &'a Option<SyncSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resync: &'a Option<SyncSpec>,
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
    pub sources: Vec<crate::kind::asset::SourceRef>,
    #[serde(default)]
    pub desired_sets: Vec<DesiredSetEntry>,
    #[serde(default = "default_true")]
    pub auto_sync: bool,
    pub sync: Option<SyncSpec>,
    pub resync: Option<SyncSpec>,
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
                sources: &asset.spec.sources,
                desired_sets: &asset.spec.desired_sets,
                auto_sync: asset.spec.auto_sync,
                sync: &asset.sync,
                resync: &asset.resync,
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

    const SOURCE_RAW_SALES: &str = "\
apiVersion: nagi.io/v1alpha1
kind: Source
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
kind: DesiredGroup
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

    // ── resolve tests ─────────────────────────────────────────────────────

    #[test]
    fn resolve_minimal_asset() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - name: check
      type: SQL
      query: \"SELECT true\"",
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        assert_eq!(output.assets[0].metadata.name, "daily-sales");
        assert_eq!(output.graph.nodes.len(), 1);
        assert!(output.graph.edges.is_empty());
    }

    #[test]
    fn resolve_expands_desired_group_ref() {
        let resources = parse(&yaml_docs(&[
            DESIRED_GROUP_DAILY_SLA,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - ref: daily-sla
    - name: check
      type: SQL
      query: \"SELECT true\"",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets[0].spec.desired_sets.len(), 2);
        assert!(matches!(
            &output.assets[0].spec.desired_sets[0],
            DesiredSetEntry::Inline(DesiredCondition::Freshness { .. })
        ));
        assert!(matches!(
            &output.assets[0].spec.desired_sets[1],
            DesiredSetEntry::Inline(DesiredCondition::SQL { .. })
        ));
    }

    #[test]
    fn resolve_rejects_unresolved_source_ref() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sources:
    - ref: nonexistent-source",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Source" && name == "nonexistent-source"));
    }

    #[test]
    fn resolve_rejects_unresolved_sync_ref() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: nonexistent-sync",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Sync" && name == "nonexistent-sync"));
    }

    #[test]
    fn resolve_rejects_unresolved_desired_group_ref() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - ref: nonexistent-group",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "DesiredGroup" && name == "nonexistent-group"));
    }

    #[test]
    fn resolve_rejects_duplicate_asset() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets: []
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets: []",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::DuplicateName { kind, name }
            if kind == "Asset" && name == "daily-sales"));
    }

    #[test]
    fn resolve_builds_dependency_graph() {
        let resources = parse(&yaml_docs(&[
            CONNECTION_MY_BQ,
            SOURCE_RAW_SALES,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  tags: [finance]
  sources:
    - ref: raw-sales
  desiredSets:
    - name: check
      type: SQL
      query: \"SELECT true\"",
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
    fn resolve_rejects_duplicate_conditions_after_expansion() {
        let resources = parse(&yaml_docs(&[
            "\
apiVersion: nagi.io/v1alpha1
kind: DesiredGroup
metadata:
  name: my-checks
spec:
  - name: check
    type: SQL
    query: \"SELECT true\"",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - ref: my-checks
    - name: check
      type: SQL
      query: \"SELECT true\"",
        ]));
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::Kind(_)));
    }

    #[test]
    fn resolve_validates_sync_and_resync_refs() {
        let resources = parse(&yaml_docs(&[
            "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-default
spec:
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--select\", \"{{ asset.name }}\"]",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: dbt-default
  resync:
    ref: dbt-default",
        ]));
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        assert_eq!(
            output.assets[0].spec.sync.as_ref().unwrap().ref_name,
            "dbt-default"
        );
        let resolved = &output.assets[0];
        assert_eq!(
            resolved.sync.as_ref().unwrap().run.args,
            vec!["dbt", "run", "--select", "daily-sales"]
        );
        assert_eq!(
            resolved.resync.as_ref().unwrap().run.args,
            vec!["dbt", "run", "--select", "daily-sales"]
        );
    }

    #[test]
    fn resolve_rejects_unresolved_resync_ref() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  resync:
    ref: nonexistent-sync",
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Sync" && name == "nonexistent-sync"));
    }

    // ── sync template expansion tests ──────────────────────────────────────

    #[test]
    fn resolve_expands_asset_name_in_sync() {
        let resources = parse(&yaml_docs(&[
            SYNC_DBT_RUN,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: dbt-run",
        ]));
        let output = resolve(resources).unwrap();
        let resolved = &output.assets[0];
        assert_eq!(
            resolved.sync.as_ref().unwrap().run.args,
            vec!["dbt", "run", "--select", "daily-sales"]
        );
    }

    #[test]
    fn resolve_expands_with_variables_in_sync() {
        let resources = parse(&yaml_docs(&[
            "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-run
spec:
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--select\", \"{{ sync.selector }}\"]",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: dbt-run
    with:
      selector: \"+daily_sales\"",
        ]));
        let output = resolve(resources).unwrap();
        let resolved = &output.assets[0];
        assert_eq!(
            resolved.sync.as_ref().unwrap().run.args,
            vec!["dbt", "run", "--select", "+daily_sales"]
        );
    }

    #[test]
    fn resolve_expands_templates_in_all_steps() {
        let resources = parse(&yaml_docs(&[
            "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: full-sync
spec:
  pre:
    type: Command
    args: [\"echo\", \"pre-{{ asset.name }}\"]
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--select\", \"{{ asset.name }}\"]
  post:
    type: Command
    args: [\"echo\", \"post-{{ asset.name }}\"]",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: full-sync",
        ]));
        let output = resolve(resources).unwrap();
        let resolved = &output.assets[0];
        let sync = resolved.sync.as_ref().unwrap();
        assert_eq!(
            sync.pre.as_ref().unwrap().args,
            vec!["echo", "pre-daily-sales"]
        );
        assert_eq!(sync.run.args, vec!["dbt", "run", "--select", "daily-sales"]);
        assert_eq!(
            sync.post.as_ref().unwrap().args,
            vec!["echo", "post-daily-sales"]
        );
    }

    #[test]
    fn resolve_expands_resync_separately() {
        let resources = parse(&yaml_docs(&[
            SYNC_DBT_RUN,
            SYNC_DBT_FULL,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: dbt-run
  resync:
    ref: dbt-full",
        ]));
        let output = resolve(resources).unwrap();
        let resolved = &output.assets[0];
        assert_eq!(
            resolved.sync.as_ref().unwrap().run.args,
            vec!["dbt", "run", "--select", "daily-sales"]
        );
        assert_eq!(
            resolved.resync.as_ref().unwrap().run.args,
            vec!["dbt", "run", "--full-refresh", "--select", "daily-sales"]
        );
    }

    #[test]
    fn resolve_no_sync_no_resolved_syncs() {
        let resources = parse(
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - name: check
      type: SQL
      query: \"SELECT true\"",
        );
        let output = resolve(resources).unwrap();
        assert!(output.assets[0].sync.is_none());
        assert!(output.assets[0].resync.is_none());
    }

    #[test]
    fn resolve_combines_asset_name_and_with_variables() {
        let resources = parse(&yaml_docs(&[
            "\
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-run
spec:
  run:
    type: Command
    args: [\"dbt\", \"run\", \"--select\", \"{{ sync.selector }}\", \"--vars\", \"name={{ asset.name }}\"]",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: dbt-run
    with:
      selector: \"+daily_sales\"",
        ]));
        let output = resolve(resources).unwrap();
        let args = &output.assets[0].sync.as_ref().unwrap().run.args;
        assert_eq!(args[3], "+daily_sales");
        assert_eq!(args[5], "name=daily-sales");
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
spec:
  desiredSets:
    - name: check
      type: SQL
      query: \"SELECT true\"",
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
            SOURCE_RAW_SALES,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sources:
    - ref: raw-sales",
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
    fn write_output_embeds_resolved_sync_in_asset_yaml() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(&yaml_docs(&[
            SYNC_DBT_RUN,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: dbt-run",
        ]));
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let content = std::fs::read_to_string(target.join("assets/daily-sales.yaml")).unwrap();
        let value: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let sync_args = &value["spec"]["sync"]["run"]["args"];
        let args: Vec<String> = serde_yaml::from_value(sync_args.clone()).unwrap();
        assert_eq!(args, vec!["dbt", "run", "--select", "daily-sales"]);
        // No separate syncs/ directory.
        assert!(!target.join("syncs").exists());
    }

    // ── load_resources tests ──────────────────────────────────────────────

    #[test]
    fn load_resources_reads_subdirectories() {
        let tmp = TempDir::new().unwrap();
        let assets = tmp.path().join("assets");
        let subdir = assets.join("subdir");
        std::fs::create_dir_all(&subdir).unwrap();

        write_yaml(
            &subdir,
            "asset.yaml",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: nested-asset
spec:
  desiredSets:
    - name: check
      type: SQL
      query: \"SELECT true\"",
        );

        let resources = load_resources(&assets).unwrap();
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
        let assets = tmp.path().join("assets");
        std::fs::create_dir_all(&assets).unwrap();

        write_yaml(
            &assets,
            "asset.yaml",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-asset
spec:
  desiredSets: []",
        );

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&assets, assets.join("loop")).unwrap();
            let resources = load_resources(&assets).unwrap();
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
  defaultSync:
    ref: dbt-run";

    fn manifests_for(origin_name: &str) -> HashMap<String, String> {
        HashMap::from([(origin_name.to_string(), MANIFEST_JSON.to_string())])
    }

    #[test]
    fn expand_origins_generates_resources_from_manifest() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]));
        let manifests = manifests_for("my-dbt");
        let expanded = expand_origins(resources, &manifests).unwrap();

        let assets: Vec<_> = expanded.iter().filter(|r| r.kind() == "Asset").collect();
        assert_eq!(assets.len(), 2);

        let sources: Vec<_> = expanded.iter().filter(|r| r.kind() == "Source").collect();
        assert_eq!(sources.len(), 1);

        let syncs: Vec<_> = expanded.iter().filter(|r| r.kind() == "Sync").collect();
        // dbt-run (user) + dbt-tag-finance (auto)
        assert_eq!(syncs.len(), 2);
    }

    #[test]
    fn expand_origins_noop_without_origin() {
        let resources = parse(&yaml_docs(&[CONNECTION_MY_BQ, SOURCE_RAW_SALES]));
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

        assert_eq!(output.assets.len(), 2);
        let customer_asset = output
            .assets
            .iter()
            .find(|a| a.metadata.name == "customers")
            .unwrap();
        assert!(customer_asset.sync.is_some());
        assert!(!customer_asset.spec.desired_sets.is_empty());
    }

    #[test]
    fn compile_with_origin_writes_target() {
        let tmp = TempDir::new().unwrap();
        let assets_dir = tmp.path().join("assets");
        let target_dir = tmp.path().join("nagi_target");
        std::fs::create_dir_all(&assets_dir).unwrap();

        write_yaml(
            &assets_dir,
            "infra.yaml",
            &yaml_docs(&[CONNECTION_MY_BQ, SYNC_DBT_RUN, ORIGIN_YAML]),
        );

        let resources = load_resources(&assets_dir).unwrap();
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

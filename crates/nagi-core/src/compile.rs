use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::kind::asset::{
    validate_no_duplicate_conditions, AssetSpec, DesiredCondition, DesiredSetEntry,
};
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
    pub assets: Vec<(Metadata, AssetSpec)>,
    pub graph: DependencyGraph,
}

/// Compiles all YAML resources from `assets_dir` and writes resolved output to `target_dir`.
pub fn compile(assets_dir: &Path, target_dir: &Path) -> Result<CompileOutput, CompileError> {
    let resources = load_resources(assets_dir)?;
    let output = resolve(resources)?;
    write_output(&output, target_dir)?;
    Ok(output)
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

struct CategorizedResources {
    sources: HashSet<String>,
    desired_groups: HashMap<String, Vec<DesiredCondition>>,
    syncs: HashSet<String>,
    assets: Vec<(Metadata, AssetSpec)>,
}

fn categorize(resources: Vec<NagiKind>) -> Result<CategorizedResources, CompileError> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut result = CategorizedResources {
        sources: HashSet::new(),
        desired_groups: HashMap::new(),
        syncs: HashSet::new(),
        assets: Vec::new(),
    };

    for resource in resources {
        let kind = resource.kind().to_string();
        let name = resource.metadata().name.clone();
        if !seen.insert((kind.clone(), name.clone())) {
            return Err(CompileError::DuplicateName { kind, name });
        }
        match resource {
            NagiKind::Connection { .. } => {}
            NagiKind::Source { .. } => {
                result.sources.insert(name);
            }
            NagiKind::DesiredGroup { spec, .. } => {
                result.desired_groups.insert(name, spec.0.clone());
            }
            NagiKind::Sync { .. } => {
                result.syncs.insert(name);
            }
            NagiKind::Asset { metadata, spec } => {
                result.assets.push((metadata, spec));
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

/// Resolves all references and builds the dependency graph.
pub fn resolve(resources: Vec<NagiKind>) -> Result<CompileOutput, CompileError> {
    let CategorizedResources {
        sources,
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
        // Both sync and resync reference kind: Sync resources.
        if let Some(sync_ref) = &spec.sync {
            require_ref(&syncs, "Sync", &sync_ref.ref_name)?;
        }
        if let Some(resync_ref) = &spec.resync {
            require_ref(&syncs, "Sync", &resync_ref.ref_name)?;
        }

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
        validate_no_duplicate_conditions(&all_conditions)?;

        resolved_assets.push((metadata, spec));
    }

    let graph = build_graph(&resolved_assets, &sources)?;
    detect_cycles(&graph)?;

    Ok(CompileOutput {
        assets: resolved_assets,
        graph,
    })
}

fn build_graph(
    assets: &[(Metadata, AssetSpec)],
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

    for (metadata, spec) in assets {
        nodes.push(GraphNode {
            name: metadata.name.clone(),
            kind: "Asset".to_string(),
            tags: spec.tags.clone(),
        });
        for source_ref in &spec.sources {
            edges.push(GraphEdge {
                from: source_ref.ref_name.clone(),
                to: metadata.name.clone(),
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

fn write_output(output: &CompileOutput, target_dir: &Path) -> Result<(), CompileError> {
    let assets_dir = target_dir.join("assets");
    std::fs::create_dir_all(&assets_dir)?;

    for (metadata, spec) in &output.assets {
        let resource = NagiKind::Asset {
            metadata: metadata.clone(),
            spec: spec.clone(),
        };
        let yaml = serde_yaml::to_string(&resource).map_err(KindError::YamlParse)?;
        std::fs::write(assets_dir.join(format!("{}.yaml", metadata.name)), yaml)?;
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
            r#"
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - type: SQL
      query: "SELECT true"
"#,
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        assert_eq!(output.assets[0].0.name, "daily-sales");
        assert_eq!(output.graph.nodes.len(), 1);
        assert!(output.graph.edges.is_empty());
    }

    #[test]
    fn resolve_expands_desired_group_ref() {
        let resources = parse(
            r#"
kind: DesiredGroup
metadata:
  name: daily-sla
spec:
  - type: Freshness
    maxAge: 24h
    interval: 6h
---
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - ref: daily-sla
    - type: SQL
      query: "SELECT true"
"#,
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets[0].1.desired_sets.len(), 2);
        assert!(matches!(
            &output.assets[0].1.desired_sets[0],
            DesiredSetEntry::Inline(DesiredCondition::Freshness { .. })
        ));
        assert!(matches!(
            &output.assets[0].1.desired_sets[1],
            DesiredSetEntry::Inline(DesiredCondition::SQL { .. })
        ));
    }

    #[test]
    fn resolve_rejects_unresolved_source_ref() {
        let resources = parse(
            r#"
kind: Asset
metadata:
  name: daily-sales
spec:
  sources:
    - ref: nonexistent-source
"#,
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Source" && name == "nonexistent-source"));
    }

    #[test]
    fn resolve_rejects_unresolved_sync_ref() {
        let resources = parse(
            r#"
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: nonexistent-sync
"#,
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Sync" && name == "nonexistent-sync"));
    }

    #[test]
    fn resolve_rejects_unresolved_desired_group_ref() {
        let resources = parse(
            r#"
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - ref: nonexistent-group
"#,
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "DesiredGroup" && name == "nonexistent-group"));
    }

    #[test]
    fn resolve_rejects_duplicate_asset() {
        let resources = parse(
            r#"
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets: []
---
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets: []
"#,
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::DuplicateName { kind, name }
            if kind == "Asset" && name == "daily-sales"));
    }

    #[test]
    fn resolve_builds_dependency_graph() {
        let resources = parse(
            r#"
kind: Connection
metadata:
  name: my-bq
spec:
  dbtProfile:
    profile: my_project
---
kind: Source
metadata:
  name: raw-sales
spec:
  connection: my-bq
---
kind: Asset
metadata:
  name: daily-sales
spec:
  tags: [finance]
  sources:
    - ref: raw-sales
  desiredSets:
    - type: SQL
      query: "SELECT true"
"#,
        );
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
        let resources = parse(
            r#"
kind: DesiredGroup
metadata:
  name: my-checks
spec:
  - type: SQL
    query: "SELECT true"
---
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - ref: my-checks
    - type: SQL
      query: "SELECT true"
"#,
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::Kind(_)));
    }

    #[test]
    fn resolve_validates_sync_and_resync_refs() {
        let resources = parse(
            r#"
kind: Sync
metadata:
  name: dbt-default
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
---
kind: Asset
metadata:
  name: daily-sales
spec:
  sync:
    ref: dbt-default
  resync:
    ref: dbt-default
"#,
        );
        let output = resolve(resources).unwrap();
        assert_eq!(output.assets.len(), 1);
        assert_eq!(
            output.assets[0].1.sync.as_ref().unwrap().ref_name,
            "dbt-default"
        );
    }

    #[test]
    fn resolve_rejects_unresolved_resync_ref() {
        let resources = parse(
            r#"
kind: Asset
metadata:
  name: daily-sales
spec:
  resync:
    ref: nonexistent-sync
"#,
        );
        let err = resolve(resources).unwrap_err();
        assert!(matches!(err, CompileError::UnresolvedRef { kind, name }
            if kind == "Sync" && name == "nonexistent-sync"));
    }

    // ── write_output tests ────────────────────────────────────────────────

    #[test]
    fn write_output_creates_asset_yaml() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");

        let resources = parse(
            r#"
kind: Asset
metadata:
  name: daily-sales
spec:
  desiredSets:
    - type: SQL
      query: "SELECT true"
"#,
        );
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let yaml_path = target.join("assets/daily-sales.yaml");
        assert!(yaml_path.exists());

        // Verify the written YAML is parseable and contains the resolved asset.
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

        let resources = parse(
            r#"
kind: Source
metadata:
  name: raw-sales
spec:
  connection: my-bq
---
kind: Asset
metadata:
  name: daily-sales
spec:
  sources:
    - ref: raw-sales
"#,
        );
        let output = resolve(resources).unwrap();
        write_output(&output, &target).unwrap();

        let graph_content = std::fs::read_to_string(target.join("graph.json")).unwrap();
        let graph: DependencyGraph = serde_json::from_str(&graph_content).unwrap();
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].from, "raw-sales");
        assert_eq!(graph.edges[0].to, "daily-sales");
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
            r#"
kind: Asset
metadata:
  name: nested-asset
spec:
  desiredSets:
    - type: SQL
      query: "SELECT true"
"#,
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
            r#"
kind: Asset
metadata:
  name: my-asset
spec:
  desiredSets: []
"#,
        );

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&assets, assets.join("loop")).unwrap();
            let resources = load_resources(&assets).unwrap();
            assert_eq!(resources.len(), 1);
            assert_eq!(resources[0].metadata().name, "my-asset");
        }
    }
}

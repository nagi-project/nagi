use std::path::Path;

use crate::runtime::compile::{CompileError, DependencyGraph};

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
        let selected = crate::interface::select::select_assets(&graph, selectors).map_err(|e| {
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

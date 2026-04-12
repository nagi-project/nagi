use std::collections::HashSet;
use std::path::Path;

use crate::runtime::kind::{self, NagiKind};

use super::{into_result, CompileError, DependencyGraph};

pub fn load_resources(dir: &Path) -> Result<Vec<NagiKind>, CompileError> {
    if !dir.exists() {
        return Err(CompileError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("assets directory not found: {}", dir.display()),
        )));
    }
    let mut resources = Vec::new();
    let mut visited = HashSet::new();
    let mut errors = Vec::new();
    load_resources_recursive(dir, &mut resources, &mut visited, &mut errors)?;
    into_result(errors)?;
    Ok(resources)
}

fn load_resources_recursive(
    dir: &Path,
    resources: &mut Vec<NagiKind>,
    visited: &mut HashSet<std::path::PathBuf>,
    errors: &mut Vec<CompileError>,
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
            load_resources_recursive(&path, resources, visited, errors)?;
        } else if is_yaml_file(&path) {
            parse_yaml_file(&path, resources, errors)?;
        }
    }
    Ok(())
}

fn is_yaml_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("yaml") | Some("yml")
    )
}

fn parse_yaml_file(
    path: &Path,
    resources: &mut Vec<NagiKind>,
    errors: &mut Vec<CompileError>,
) -> Result<(), CompileError> {
    let content = std::fs::read_to_string(path)?;
    match kind::parse_kinds(&content) {
        Ok(kinds) => resources.extend(kinds),
        Err(e) => errors.push(CompileError::Kind(e)),
    }
    Ok(())
}

/// Resolves asset names from compiled output.
///
/// When selectors are provided, uses the graph to filter.
/// Otherwise, lists all `.yaml` files in `assets_path`.
fn resolve_asset_names(
    graph_json: &str,
    selectors: &[&str],
    excludes: &[&str],
    assets_path: &Path,
) -> Result<Vec<String>, CompileError> {
    if !selectors.is_empty() || !excludes.is_empty() {
        let graph: DependencyGraph = serde_json::from_str(graph_json)
            .map_err(|e| CompileError::ManifestParse(e.to_string()))?;
        let selected =
            crate::runtime::select::select_assets(&graph, selectors, excludes).map_err(|e| {
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
pub(crate) fn load_graph(target_dir: &Path) -> Result<DependencyGraph, CompileError> {
    let graph_path = target_dir.join("graph.json");
    let graph_json = std::fs::read_to_string(&graph_path).map_err(CompileError::Io)?;
    serde_json::from_str(&graph_json).map_err(|e| CompileError::ManifestParse(e.to_string()))
}

/// Resolves asset names from a compiled target directory.
pub(crate) fn resolve_compiled_asset_names(
    target_dir: &Path,
    selectors: &[&str],
    excludes: &[&str],
) -> Result<Vec<String>, CompileError> {
    let assets_path = target_dir.join("assets");
    let graph_path = target_dir.join("graph.json");
    let graph_json = std::fs::read_to_string(&graph_path).map_err(CompileError::Io)?;
    resolve_asset_names(&graph_json, selectors, excludes, &assets_path)
}

/// Resolves asset names from selectors or directory listing, then reads each
/// compiled YAML file. Returns `Vec<(name, yaml_content)>`.
pub(crate) fn load_compiled_assets(
    target_dir: &Path,
    selectors: &[&str],
    excludes: &[&str],
) -> Result<Vec<(String, String)>, CompileError> {
    let names = resolve_compiled_asset_names(target_dir, selectors, excludes)?;
    let assets_path = target_dir.join("assets");
    let mut result = Vec::with_capacity(names.len());
    for name in names {
        let yaml_path = assets_path.join(format!("{name}.yaml"));
        let content = std::fs::read_to_string(&yaml_path).map_err(CompileError::Io)?;
        result.push((name, content));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_yaml(dir: &Path, filename: &str, content: &str) {
        std::fs::write(dir.join(filename), content).unwrap();
    }

    // ── is_yaml_file ───────────────────────────────────────────────────

    macro_rules! is_yaml_file_test {
        ($($name:ident: $path:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(is_yaml_file(Path::new($path)), $expected);
                }
            )*
        };
    }

    is_yaml_file_test! {
        yaml_extension: "foo.yaml" => true;
        yml_extension: "foo.yml" => true;
        non_yaml_extension: "foo.json" => false;
        no_extension: "foo" => false;
    }

    // ── parse_yaml_file ────────────────────────────────────────────────

    #[test]
    fn parse_yaml_file_adds_resources_on_valid_yaml() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("asset.yaml");
        std::fs::write(
            &path,
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: test-asset
spec: {}",
        )
        .unwrap();

        let mut resources = Vec::new();
        let mut errors = Vec::new();
        parse_yaml_file(&path, &mut resources, &mut errors).unwrap();
        assert_eq!(resources.len(), 1);
        assert!(errors.is_empty());
    }

    #[test]
    fn parse_yaml_file_pushes_error_on_invalid_yaml() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad.yaml");
        std::fs::write(&path, "not: valid: yaml: [").unwrap();

        let mut resources = Vec::new();
        let mut errors = Vec::new();
        parse_yaml_file(&path, &mut resources, &mut errors).unwrap();
        assert!(resources.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(matches!(&errors[0], CompileError::Kind(_)));
    }

    #[test]
    fn parse_yaml_file_returns_io_error_on_missing_file() {
        let mut resources = Vec::new();
        let mut errors = Vec::new();
        let err = parse_yaml_file(
            Path::new("/nonexistent/file.yaml"),
            &mut resources,
            &mut errors,
        )
        .unwrap_err();
        assert!(matches!(err, CompileError::Io(_)));
        assert!(errors.is_empty());
    }

    // ── load_resources ──────────────────────────────────────────────────

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

    #[test]
    fn load_resources_accumulates_yaml_parse_errors() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("resources");
        std::fs::create_dir_all(&dir).unwrap();

        write_yaml(
            &dir,
            "good.yaml",
            "\
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: valid-asset
spec: {}",
        );
        write_yaml(&dir, "bad1.yaml", "not: valid: yaml: content: [");
        write_yaml(&dir, "bad2.yaml", "also: invalid: [yaml");

        let err = load_resources(&dir).unwrap_err();
        match err {
            CompileError::Multiple(errors) => {
                assert_eq!(errors.len(), 2);
            }
            other => panic!("expected Multiple, got: {other}"),
        }
    }
}

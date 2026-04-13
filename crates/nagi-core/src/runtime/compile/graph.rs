use std::collections::{HashMap, HashSet, VecDeque};

use super::{CompileError, DependencyGraph, GraphEdge, GraphNode, ResolvedAsset};

pub(super) fn build_graph(assets: &[ResolvedAsset]) -> Result<DependencyGraph, CompileError> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for asset in assets {
        nodes.push(GraphNode {
            name: asset.metadata.name.clone(),
            kind: "Asset".to_string(),
            labels: asset.metadata.labels.clone(),
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

/// Collects all nodes involved in dependency cycles using Kahn's algorithm.
pub(super) fn collect_cycle_errors(graph: &DependencyGraph) -> Vec<CompileError> {
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

    while let Some(node) = queue.pop_front() {
        for &neighbor in &adjacency[node] {
            let deg = in_degree.get_mut(neighbor).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(neighbor);
            }
        }
    }

    let mut cycle_nodes: Vec<_> = in_degree
        .into_iter()
        .filter(|(_, deg)| *deg > 0)
        .map(|(name, _)| name.to_string())
        .collect();
    cycle_nodes.sort();
    cycle_nodes
        .into_iter()
        .map(|name| CompileError::CycleDetected { name })
        .collect()
}

pub(super) fn collect_unresolved_upstream_errors(
    upstreams: &[String],
    asset_names: &HashSet<String>,
) -> Vec<CompileError> {
    upstreams
        .iter()
        .filter(|name| !asset_names.contains(*name))
        .map(|name| CompileError::UnresolvedRef {
            kind: "Asset".to_string(),
            name: name.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    // ── collect_unresolved_upstream_errors ──────────────────────────────

    #[test]
    fn collect_unresolved_upstream_errors_all_valid() {
        let names: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let upstreams = vec!["a".to_string(), "b".to_string()];
        assert!(collect_unresolved_upstream_errors(&upstreams, &names).is_empty());
    }

    #[test]
    fn collect_unresolved_upstream_errors_multiple_missing() {
        let names: HashSet<String> = HashSet::new();
        let upstreams = vec!["x".to_string(), "y".to_string()];
        let errors = collect_unresolved_upstream_errors(&upstreams, &names);
        assert_eq!(errors.len(), 2);
        assert!(errors
            .iter()
            .all(|e| matches!(e, CompileError::UnresolvedRef { kind, .. } if kind == "Asset")));
    }

    #[test]
    fn collect_unresolved_upstream_errors_partial_match() {
        let names: HashSet<String> = ["a"].iter().map(|s| s.to_string()).collect();
        let upstreams = vec!["a".to_string(), "missing".to_string()];
        let errors = collect_unresolved_upstream_errors(&upstreams, &names);
        assert_eq!(errors.len(), 1);
        assert!(
            matches!(&errors[0], CompileError::UnresolvedRef { name, .. } if name == "missing")
        );
    }

    // ── collect_cycle_errors ───────────────────────────────────────────

    #[test]
    fn collect_cycle_errors_empty_graph() {
        let graph = DependencyGraph {
            nodes: vec![],
            edges: vec![],
        };
        assert!(collect_cycle_errors(&graph).is_empty());
    }

    #[test]
    fn collect_cycle_errors_no_cycle() {
        let graph = DependencyGraph {
            nodes: vec![
                GraphNode {
                    name: "a".into(),
                    kind: "Asset".into(),
                    labels: BTreeMap::default(),
                },
                GraphNode {
                    name: "b".into(),
                    kind: "Asset".into(),
                    labels: BTreeMap::default(),
                },
            ],
            edges: vec![GraphEdge {
                from: "a".into(),
                to: "b".into(),
            }],
        };
        assert!(collect_cycle_errors(&graph).is_empty());
    }

    #[test]
    fn collect_cycle_errors_reports_all_cycle_nodes() {
        let graph = DependencyGraph {
            nodes: vec![
                GraphNode {
                    name: "a".into(),
                    kind: "Asset".into(),
                    labels: BTreeMap::default(),
                },
                GraphNode {
                    name: "b".into(),
                    kind: "Asset".into(),
                    labels: BTreeMap::default(),
                },
                GraphNode {
                    name: "c".into(),
                    kind: "Asset".into(),
                    labels: BTreeMap::default(),
                },
            ],
            edges: vec![
                GraphEdge {
                    from: "a".into(),
                    to: "b".into(),
                },
                GraphEdge {
                    from: "b".into(),
                    to: "a".into(),
                },
            ],
        };
        let errors = collect_cycle_errors(&graph);
        assert_eq!(errors.len(), 2);
        let names: Vec<String> = errors
            .iter()
            .map(|e| match e {
                CompileError::CycleDetected { name } => name.clone(),
                _ => panic!("expected CycleDetected"),
            })
            .collect();
        assert!(names.contains(&"a".to_string()));
        assert!(names.contains(&"b".to_string()));
    }

    #[test]
    fn collect_cycle_errors_self_cycle() {
        let graph = DependencyGraph {
            nodes: vec![GraphNode {
                name: "a".into(),
                kind: "Asset".into(),
                labels: BTreeMap::default(),
            }],
            edges: vec![GraphEdge {
                from: "a".into(),
                to: "a".into(),
            }],
        };
        let errors = collect_cycle_errors(&graph);
        assert_eq!(errors.len(), 1);
        assert!(matches!(&errors[0], CompileError::CycleDetected { name } if name == "a"));
    }
}

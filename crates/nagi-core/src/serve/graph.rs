use std::collections::HashMap;

use crate::compile::{DependencyGraph, GraphEdge};

/// Detects connected components in the dependency graph using Union-Find.
/// Returns groups of Asset names (Source nodes are excluded from output).
pub fn connected_components(graph: &DependencyGraph) -> Vec<Vec<String>> {
    let mut name_to_id: HashMap<&str, usize> = HashMap::new();
    for (i, node) in graph.nodes.iter().enumerate() {
        name_to_id.insert(&node.name, i);
    }

    let n = graph.nodes.len();
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }

    fn union(parent: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent[rb] = ra;
        }
    }

    for edge in &graph.edges {
        if let (Some(&a), Some(&b)) = (
            name_to_id.get(edge.from.as_str()),
            name_to_id.get(edge.to.as_str()),
        ) {
            union(&mut parent, a, b);
        }
    }

    let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, node) in graph.nodes.iter().enumerate() {
        if node.kind == "Asset" {
            let root = find(&mut parent, i);
            groups.entry(root).or_default().push(node.name.clone());
        }
    }

    let mut result: Vec<Vec<String>> = groups.into_values().collect();
    result.sort_by(|a, b| a[0].cmp(&b[0]));
    for group in &mut result {
        group.sort();
    }
    result
}

pub fn build_downstream_map(edges: &[GraphEdge]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges {
        map.entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{DependencyGraph, GraphEdge, GraphNode};

    fn asset_node(name: &str) -> GraphNode {
        GraphNode {
            name: name.to_string(),
            kind: "Asset".to_string(),
            tags: vec![],
        }
    }

    fn source_node(name: &str) -> GraphNode {
        GraphNode {
            name: name.to_string(),
            kind: "Source".to_string(),
            tags: vec![],
        }
    }

    fn edge(from: &str, to: &str) -> GraphEdge {
        GraphEdge {
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    macro_rules! connected_components_test {
        ($($name:ident: $graph:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let result = connected_components(&$graph);
                    assert_eq!(result, $expected);
                }
            )*
        };
    }

    connected_components_test! {
        single_asset_no_edges: DependencyGraph {
            nodes: vec![asset_node("a")],
            edges: vec![],
        } => vec![vec!["a".to_string()]];

        two_independent_assets: DependencyGraph {
            nodes: vec![asset_node("a"), asset_node("b")],
            edges: vec![],
        } => vec![vec!["a".to_string()], vec!["b".to_string()]];

        chain_via_source: DependencyGraph {
            nodes: vec![source_node("s"), asset_node("a"), asset_node("b")],
            edges: vec![edge("s", "a"), edge("s", "b")],
        } => vec![vec!["a".to_string(), "b".to_string()]];

        two_separate_chains: DependencyGraph {
            nodes: vec![
                source_node("s1"), asset_node("a1"),
                source_node("s2"), asset_node("a2"),
            ],
            edges: vec![edge("s1", "a1"), edge("s2", "a2")],
        } => vec![vec!["a1".to_string()], vec!["a2".to_string()]];

        three_assets_one_component: DependencyGraph {
            nodes: vec![
                source_node("raw"), asset_node("daily"), asset_node("monthly"), asset_node("raw-asset"),
            ],
            edges: vec![edge("raw", "daily"), edge("raw", "monthly"), edge("raw", "raw-asset")],
        } => vec![vec!["daily".to_string(), "monthly".to_string(), "raw-asset".to_string()]];

        empty_graph: DependencyGraph {
            nodes: vec![],
            edges: vec![],
        } => Vec::<Vec<String>>::new();
    }

    #[test]
    fn downstream_map_basic() {
        let edges = vec![edge("a", "b"), edge("a", "c"), edge("b", "c")];
        let map = build_downstream_map(&edges);
        assert_eq!(
            map.get("a").unwrap(),
            &vec!["b".to_string(), "c".to_string()]
        );
        assert_eq!(map.get("b").unwrap(), &vec!["c".to_string()]);
        assert!(map.get("c").is_none());
    }

    #[test]
    fn downstream_map_empty() {
        let map = build_downstream_map(&[]);
        assert!(map.is_empty());
    }
}

use std::collections::HashMap;

use crate::runtime::compile::{DependencyGraph, GraphEdge};

/// Disjoint-set (Union-Find) with path-splitting for near-O(1) find/union.
/// Used to group graph nodes into connected components.
struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent[rb] = ra;
        }
    }
}

fn build_name_index(graph: &DependencyGraph) -> HashMap<&str, usize> {
    graph
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| (node.name.as_str(), i))
        .collect()
}

fn group_assets_by_root(graph: &DependencyGraph, uf: &mut UnionFind) -> Vec<Vec<String>> {
    let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, node) in graph.nodes.iter().enumerate() {
        if node.kind == "Asset" {
            let root = uf.find(i);
            groups.entry(root).or_default().push(node.name.clone());
        }
    }
    let mut result: Vec<Vec<String>> = groups.into_values().collect();
    for group in &mut result {
        group.sort();
    }
    result.sort_by(|a, b| a[0].cmp(&b[0]));
    result
}

/// Detects connected components in the dependency graph using Union-Find.
/// Returns groups of Asset names (Source nodes are excluded from output).
pub fn connected_components(graph: &DependencyGraph) -> Vec<Vec<String>> {
    let name_to_id = build_name_index(graph);
    let mut uf = UnionFind::new(graph.nodes.len());

    for edge in &graph.edges {
        if let (Some(&a), Some(&b)) = (
            name_to_id.get(edge.from.as_str()),
            name_to_id.get(edge.to.as_str()),
        ) {
            uf.union(a, b);
        }
    }

    group_assets_by_root(graph, &mut uf)
}

pub struct EdgeMaps {
    pub downstream: HashMap<String, Vec<String>>,
    pub upstream: HashMap<String, Vec<String>>,
}

pub fn build_edge_maps(edges: &[GraphEdge]) -> EdgeMaps {
    let mut downstream: HashMap<String, Vec<String>> = HashMap::new();
    let mut upstream: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges {
        downstream
            .entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
        upstream
            .entry(edge.to.clone())
            .or_default()
            .push(edge.from.clone());
    }
    EdgeMaps {
        downstream,
        upstream,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::compile::{DependencyGraph, GraphEdge, GraphNode};

    fn asset_node(name: &str) -> GraphNode {
        GraphNode {
            name: name.to_string(),
            kind: "Asset".to_string(),
            labels: Default::default(),
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

        chain_via_upstream: DependencyGraph {
            nodes: vec![asset_node("a"), asset_node("b"), asset_node("s")],
            edges: vec![edge("s", "a"), edge("s", "b")],
        } => vec![vec!["a".to_string(), "b".to_string(), "s".to_string()]];

        two_separate_chains: DependencyGraph {
            nodes: vec![
                asset_node("a1"), asset_node("s1"),
                asset_node("a2"), asset_node("s2"),
            ],
            edges: vec![edge("s1", "a1"), edge("s2", "a2")],
        } => vec![vec!["a1".to_string(), "s1".to_string()], vec!["a2".to_string(), "s2".to_string()]];

        three_assets_one_component: DependencyGraph {
            nodes: vec![
                asset_node("raw"), asset_node("daily"), asset_node("monthly"), asset_node("raw-asset"),
            ],
            edges: vec![edge("raw", "daily"), edge("raw", "monthly"), edge("raw", "raw-asset")],
        } => vec![vec!["daily".to_string(), "monthly".to_string(), "raw".to_string(), "raw-asset".to_string()]];

        empty_graph: DependencyGraph {
            nodes: vec![],
            edges: vec![],
        } => Vec::<Vec<String>>::new();
    }

    #[test]
    fn edge_maps_basic() {
        let edges = vec![edge("a", "b"), edge("a", "c"), edge("b", "c")];
        let maps = build_edge_maps(&edges);
        assert_eq!(
            maps.downstream.get("a").unwrap(),
            &vec!["b".to_string(), "c".to_string()]
        );
        assert_eq!(maps.downstream.get("b").unwrap(), &vec!["c".to_string()]);
        assert!(!maps.downstream.contains_key("c"));

        assert_eq!(maps.upstream.get("b").unwrap(), &vec!["a".to_string()]);
        assert_eq!(
            maps.upstream.get("c").unwrap(),
            &vec!["a".to_string(), "b".to_string()]
        );
        assert!(!maps.upstream.contains_key("a"));
    }

    #[test]
    fn union_find_initially_disjoint() {
        let mut uf = UnionFind::new(3);
        assert_ne!(uf.find(0), uf.find(1));
        assert_ne!(uf.find(1), uf.find(2));
    }

    #[test]
    fn union_find_merges_sets() {
        let mut uf = UnionFind::new(4);
        uf.union(0, 1);
        uf.union(2, 3);
        assert_eq!(uf.find(0), uf.find(1));
        assert_eq!(uf.find(2), uf.find(3));
        assert_ne!(uf.find(0), uf.find(2));
    }

    #[test]
    fn union_find_transitive() {
        let mut uf = UnionFind::new(3);
        uf.union(0, 1);
        uf.union(1, 2);
        assert_eq!(uf.find(0), uf.find(2));
    }

    #[test]
    fn build_name_index_maps_nodes_to_positions() {
        let graph = DependencyGraph {
            nodes: vec![asset_node("x"), asset_node("y")],
            edges: vec![],
        };
        let idx = build_name_index(&graph);
        assert_eq!(idx["x"], 0);
        assert_eq!(idx["y"], 1);
    }
}

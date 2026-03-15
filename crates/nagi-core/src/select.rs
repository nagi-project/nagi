use std::collections::{HashMap, HashSet, VecDeque};

use thiserror::Error;

use crate::compile::DependencyGraph;

#[derive(Debug, Error)]
pub enum SelectError {
    #[error("asset '{name}' not found in graph")]
    NotFound { name: String },

    #[error("invalid selector: {message}")]
    InvalidSelector { message: String },
}

/// Selects asset names from the graph based on a selector expression.
///
/// Follows dbt's `--select` syntax:
/// - `name`       — exact match
/// - `+name`      — name + all ancestors (upstream)
/// - `name+`      — name + all descendants (downstream)
/// - `+name+`     — name + ancestors + descendants
/// - `tag:value`  — all nodes with the given tag
/// - `+tag:value` — tag match + all ancestors
///
/// Multiple selectors separated by spaces are combined as union (OR).
pub fn select_assets(
    graph: &DependencyGraph,
    selectors: &[&str],
) -> Result<Vec<String>, SelectError> {
    let mut result: HashSet<String> = HashSet::new();

    for selector in selectors {
        let selected = select_single(graph, selector)?;
        result.extend(selected);
    }

    let mut sorted: Vec<String> = result.into_iter().collect();
    sorted.sort();
    Ok(sorted)
}

fn select_single(graph: &DependencyGraph, selector: &str) -> Result<HashSet<String>, SelectError> {
    if selector.is_empty() {
        return Err(SelectError::InvalidSelector {
            message: "selector must not be empty".to_string(),
        });
    }

    let (upstream, downstream, pattern) = parse_selector(selector)?;
    let seed_names = resolve_pattern(graph, pattern)?;

    // Build adjacency lists only for needed directions.
    let upstream_adj = if upstream {
        Some(build_adjacency(graph, Direction::Upstream))
    } else {
        None
    };
    let downstream_adj = if downstream {
        Some(build_adjacency(graph, Direction::Downstream))
    } else {
        None
    };

    let mut result: HashSet<String> = HashSet::new();
    for name in &seed_names {
        result.insert(name.clone());
        if let Some(adj) = &upstream_adj {
            traverse(adj, name, &mut result);
        }
        if let Some(adj) = &downstream_adj {
            traverse(adj, name, &mut result);
        }
    }

    Ok(result)
}

fn parse_selector(selector: &str) -> Result<(bool, bool, &str), SelectError> {
    let upstream = selector.starts_with('+');
    let downstream = selector.ends_with('+');

    let inner = if upstream { &selector[1..] } else { selector };
    let pattern = if downstream && !inner.is_empty() {
        &inner[..inner.len() - 1]
    } else {
        inner
    };

    if pattern.is_empty() {
        return Err(SelectError::InvalidSelector {
            message: "selector pattern must not be empty".to_string(),
        });
    }

    Ok((upstream, downstream, pattern))
}

fn resolve_pattern(graph: &DependencyGraph, pattern: &str) -> Result<Vec<String>, SelectError> {
    if let Some(tag) = pattern.strip_prefix("tag:") {
        if tag.is_empty() {
            return Err(SelectError::InvalidSelector {
                message: "tag value must not be empty".to_string(),
            });
        }
        let matched: Vec<String> = graph
            .nodes
            .iter()
            .filter(|n| n.tags.contains(&tag.to_string()))
            .map(|n| n.name.clone())
            .collect();
        if matched.is_empty() {
            return Err(SelectError::NotFound {
                name: format!("tag:{tag}"),
            });
        }
        Ok(matched)
    } else {
        if !graph.nodes.iter().any(|n| n.name == pattern) {
            return Err(SelectError::NotFound {
                name: pattern.to_string(),
            });
        }
        Ok(vec![pattern.to_string()])
    }
}

enum Direction {
    Upstream,
    Downstream,
}

/// Build an adjacency list from the graph edges for the given direction.
/// O(E) construction, enabling O(V+E) traversal.
fn build_adjacency(graph: &DependencyGraph, direction: Direction) -> HashMap<String, Vec<String>> {
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &graph.edges {
        let (key, value) = match direction {
            Direction::Upstream => (edge.to.clone(), edge.from.clone()),
            Direction::Downstream => (edge.from.clone(), edge.to.clone()),
        };
        adj.entry(key).or_default().push(value);
    }
    adj
}

fn traverse(adj: &HashMap<String, Vec<String>>, start: &str, visited: &mut HashSet<String>) {
    let mut queue = VecDeque::new();
    queue.push_back(start.to_string());

    while let Some(current) = queue.pop_front() {
        if let Some(neighbors) = adj.get(&current) {
            for neighbor in neighbors {
                if visited.insert(neighbor.clone()) {
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{GraphEdge, GraphNode};

    // raw-sales → daily-sales → monthly-report
    // raw-logs  → access-stats
    fn test_graph() -> DependencyGraph {
        DependencyGraph {
            nodes: vec![
                GraphNode {
                    name: "raw-sales".to_string(),
                    kind: "Source".to_string(),
                    tags: vec![],
                },
                GraphNode {
                    name: "daily-sales".to_string(),
                    kind: "Asset".to_string(),
                    tags: vec!["finance".to_string(), "daily".to_string()],
                },
                GraphNode {
                    name: "monthly-report".to_string(),
                    kind: "Asset".to_string(),
                    tags: vec!["finance".to_string()],
                },
                GraphNode {
                    name: "raw-logs".to_string(),
                    kind: "Source".to_string(),
                    tags: vec![],
                },
                GraphNode {
                    name: "access-stats".to_string(),
                    kind: "Asset".to_string(),
                    tags: vec!["ops".to_string()],
                },
            ],
            edges: vec![
                GraphEdge {
                    from: "raw-sales".to_string(),
                    to: "daily-sales".to_string(),
                },
                GraphEdge {
                    from: "daily-sales".to_string(),
                    to: "monthly-report".to_string(),
                },
                GraphEdge {
                    from: "raw-logs".to_string(),
                    to: "access-stats".to_string(),
                },
            ],
        }
    }

    #[test]
    fn exact_name() {
        let result = select_assets(&test_graph(), &["daily-sales"]).unwrap();
        assert_eq!(result, vec!["daily-sales"]);
    }

    #[test]
    fn upstream() {
        let result = select_assets(&test_graph(), &["+daily-sales"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "raw-sales"]);
    }

    #[test]
    fn downstream() {
        let result = select_assets(&test_graph(), &["daily-sales+"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report"]);
    }

    #[test]
    fn upstream_and_downstream() {
        let result = select_assets(&test_graph(), &["+daily-sales+"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report", "raw-sales"]);
    }

    #[test]
    fn tag_selector() {
        let result = select_assets(&test_graph(), &["tag:finance"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report"]);
    }

    #[test]
    fn tag_with_upstream() {
        let result = select_assets(&test_graph(), &["+tag:finance"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report", "raw-sales"]);
    }

    #[test]
    fn tag_with_downstream() {
        let result = select_assets(&test_graph(), &["tag:finance+"]).unwrap();
        // daily-sales and monthly-report match tag:finance.
        // daily-sales+ adds monthly-report (already included).
        // monthly-report+ adds nothing.
        assert_eq!(result, vec!["daily-sales", "monthly-report"]);
    }

    #[test]
    fn tag_with_upstream_and_downstream() {
        let result = select_assets(&test_graph(), &["+tag:finance+"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report", "raw-sales"]);
    }

    #[test]
    fn multiple_selectors_union() {
        let result = select_assets(&test_graph(), &["daily-sales", "access-stats"]).unwrap();
        assert_eq!(result, vec!["access-stats", "daily-sales"]);
    }

    #[test]
    fn source_with_downstream() {
        let result = select_assets(&test_graph(), &["raw-sales+"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report", "raw-sales"]);
    }

    #[test]
    fn not_found() {
        let err = select_assets(&test_graph(), &["nonexistent"]).unwrap_err();
        assert!(matches!(err, SelectError::NotFound { name } if name == "nonexistent"));
    }

    #[test]
    fn tag_not_found() {
        let err = select_assets(&test_graph(), &["tag:nonexistent"]).unwrap_err();
        assert!(matches!(err, SelectError::NotFound { name } if name == "tag:nonexistent"));
    }

    #[test]
    fn empty_selector() {
        let err = select_assets(&test_graph(), &[""]).unwrap_err();
        assert!(matches!(err, SelectError::InvalidSelector { .. }));
    }

    #[test]
    fn plus_only() {
        let err = select_assets(&test_graph(), &["+"]).unwrap_err();
        assert!(matches!(err, SelectError::InvalidSelector { .. }));
    }

    #[test]
    fn empty_tag_value() {
        let err = select_assets(&test_graph(), &["tag:"]).unwrap_err();
        assert!(matches!(err, SelectError::InvalidSelector { .. }));
    }

    #[test]
    fn double_plus() {
        let err = select_assets(&test_graph(), &["++"]).unwrap_err();
        assert!(matches!(err, SelectError::InvalidSelector { .. }));
    }

    #[test]
    fn upstream_on_leaf_node() {
        // monthly-report has no downstream; +monthly-report traverses upstream only.
        let result = select_assets(&test_graph(), &["+monthly-report"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report", "raw-sales"]);
    }

    #[test]
    fn downstream_on_root_node() {
        // monthly-report has no downstream edges.
        let result = select_assets(&test_graph(), &["monthly-report+"]).unwrap();
        assert_eq!(result, vec!["monthly-report"]);
    }

    #[test]
    fn multiple_selectors_dedup() {
        // daily-sales appears in both selectors' results; should appear once.
        let result = select_assets(&test_graph(), &["daily-sales", "+monthly-report"]).unwrap();
        assert_eq!(result, vec!["daily-sales", "monthly-report", "raw-sales"]);
    }

    // --- parse_selector unit tests ---

    #[test]
    fn parse_selector_exact() {
        let (up, down, pattern) = parse_selector("daily-sales").unwrap();
        assert!(!up);
        assert!(!down);
        assert_eq!(pattern, "daily-sales");
    }

    #[test]
    fn parse_selector_upstream() {
        let (up, down, pattern) = parse_selector("+daily-sales").unwrap();
        assert!(up);
        assert!(!down);
        assert_eq!(pattern, "daily-sales");
    }

    #[test]
    fn parse_selector_downstream() {
        let (up, down, pattern) = parse_selector("daily-sales+").unwrap();
        assert!(!up);
        assert!(down);
        assert_eq!(pattern, "daily-sales");
    }

    #[test]
    fn parse_selector_both() {
        let (up, down, pattern) = parse_selector("+daily-sales+").unwrap();
        assert!(up);
        assert!(down);
        assert_eq!(pattern, "daily-sales");
    }

    #[test]
    fn parse_selector_tag() {
        let (up, down, pattern) = parse_selector("+tag:finance+").unwrap();
        assert!(up);
        assert!(down);
        assert_eq!(pattern, "tag:finance");
    }

    // --- build_adjacency unit tests ---

    #[test]
    fn build_adjacency_upstream() {
        let graph = test_graph();
        let adj = build_adjacency(&graph, Direction::Upstream);
        // Upstream: edge.to → edge.from
        assert_eq!(adj.get("daily-sales").unwrap(), &vec!["raw-sales"]);
        assert_eq!(adj.get("monthly-report").unwrap(), &vec!["daily-sales"]);
        assert!(adj.get("raw-sales").is_none());
    }

    #[test]
    fn build_adjacency_downstream() {
        let graph = test_graph();
        let adj = build_adjacency(&graph, Direction::Downstream);
        // Downstream: edge.from → edge.to
        assert_eq!(adj.get("raw-sales").unwrap(), &vec!["daily-sales"]);
        assert_eq!(adj.get("daily-sales").unwrap(), &vec!["monthly-report"]);
        assert!(adj.get("monthly-report").is_none());
    }
}

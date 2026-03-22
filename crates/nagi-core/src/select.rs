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
/// - `N+name`     — name + N levels upstream
/// - `name+N`     — name + N levels downstream
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

/// Parses a selector expression into (upstream, downstream, pattern).
///
/// Supported forms:
/// - `name`        — exact match
/// - `+name`       — all upstream
/// - `name+`       — all downstream
/// - `+name+`      — both
/// - `2+name`      — N levels upstream (N-plus)
/// - `name+1`      — N levels downstream
/// - `2+name+3`    — N levels both directions
fn parse_selector(selector: &str) -> Result<(bool, bool, &str), SelectError> {
    // Strip upstream prefix: `+name` or `2+name`
    let (upstream, rest) = if let Some(pos) = selector.find('+') {
        let prefix = &selector[..pos];
        if prefix.is_empty() || prefix.chars().all(|c| c.is_ascii_digit()) {
            (true, &selector[pos + 1..])
        } else {
            (false, selector)
        }
    } else {
        (false, selector)
    };

    // Strip downstream suffix: `name+` or `name+1`
    let (downstream, pattern) = if let Some(pos) = rest.rfind('+') {
        let suffix = &rest[pos + 1..];
        if suffix.is_empty() || suffix.chars().all(|c| c.is_ascii_digit()) {
            (true, &rest[..pos])
        } else {
            (false, rest)
        }
    } else {
        (false, rest)
    };

    if pattern.is_empty() {
        return Err(SelectError::InvalidSelector {
            message: "selector pattern must not be empty".to_string(),
        });
    }

    Ok((upstream, downstream, pattern))
}

/// Extracts a model name from a selector expression, stripping `+` markers.
/// Returns `None` for tag selectors (`tag:value`) or other non-model patterns.
pub fn extract_model_name(selector: &str) -> Option<String> {
    let Ok((_upstream, _downstream, pattern)) = parse_selector(selector) else {
        return None;
    };
    // Tag or other qualified selectors are not model names.
    if pattern.contains(':') {
        return None;
    }
    Some(pattern.to_string())
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

    macro_rules! select_ok {
        ($($name:ident: $selectors:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let result = select_assets(&test_graph(), &$selectors).unwrap();
                    assert_eq!(result, $expected);
                }
            )*
        };
    }

    select_ok! {
        exact_name: ["daily-sales"] => vec!["daily-sales"];
        upstream: ["+daily-sales"] => vec!["daily-sales", "raw-sales"];
        downstream: ["daily-sales+"] => vec!["daily-sales", "monthly-report"];
        upstream_and_downstream: ["+daily-sales+"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        tag_selector: ["tag:finance"] => vec!["daily-sales", "monthly-report"];
        tag_with_upstream: ["+tag:finance"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        tag_with_downstream: ["tag:finance+"] => vec!["daily-sales", "monthly-report"];
        tag_with_upstream_and_downstream: ["+tag:finance+"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        multiple_selectors_union: ["daily-sales", "access-stats"] => vec!["access-stats", "daily-sales"];
        source_with_downstream: ["raw-sales+"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        upstream_on_leaf_node: ["+monthly-report"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        downstream_on_root_node: ["monthly-report+"] => vec!["monthly-report"];
        multiple_selectors_dedup: ["daily-sales", "+monthly-report"] => vec!["daily-sales", "monthly-report", "raw-sales"];
    }

    macro_rules! select_not_found {
        ($($name:ident: $selectors:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(matches!(
                        select_assets(&test_graph(), &$selectors).unwrap_err(),
                        SelectError::NotFound { .. }
                    ));
                }
            )*
        };
    }

    select_not_found! {
        not_found: ["nonexistent"];
        tag_not_found: ["tag:nonexistent"];
    }

    macro_rules! select_invalid {
        ($($name:ident: $selectors:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(matches!(
                        select_assets(&test_graph(), &$selectors).unwrap_err(),
                        SelectError::InvalidSelector { .. }
                    ));
                }
            )*
        };
    }

    select_invalid! {
        empty_selector: [""];
        plus_only: ["+"];
        empty_tag_value: ["tag:"];
        double_plus: ["++"];
    }

    // --- parse_selector unit tests ---

    macro_rules! parse_selector_test {
        ($($name:ident: $input:expr => ($up:expr, $down:expr, $pattern:expr);)*) => {
            $(
                #[test]
                fn $name() {
                    let (up, down, pattern) = parse_selector($input).unwrap();
                    assert_eq!(up, $up);
                    assert_eq!(down, $down);
                    assert_eq!(pattern, $pattern);
                }
            )*
        };
    }

    parse_selector_test! {
        parse_selector_exact: "daily-sales" => (false, false, "daily-sales");
        parse_selector_upstream: "+daily-sales" => (true, false, "daily-sales");
        parse_selector_downstream: "daily-sales+" => (false, true, "daily-sales");
        parse_selector_both: "+daily-sales+" => (true, true, "daily-sales");
        parse_selector_tag: "+tag:finance+" => (true, true, "tag:finance");
    }

    // --- build_adjacency unit tests ---

    #[test]
    fn build_adjacency_upstream() {
        let graph = test_graph();
        let adj = build_adjacency(&graph, Direction::Upstream);
        // Upstream: edge.to → edge.from
        assert_eq!(adj.get("daily-sales").unwrap(), &vec!["raw-sales"]);
        assert_eq!(adj.get("monthly-report").unwrap(), &vec!["daily-sales"]);
        assert!(!adj.contains_key("raw-sales"));
    }

    #[test]
    fn build_adjacency_downstream() {
        let graph = test_graph();
        let adj = build_adjacency(&graph, Direction::Downstream);
        // Downstream: edge.from → edge.to
        assert_eq!(adj.get("raw-sales").unwrap(), &vec!["daily-sales"]);
        assert_eq!(adj.get("daily-sales").unwrap(), &vec!["monthly-report"]);
        assert!(!adj.contains_key("monthly-report"));
    }

    // ── parse_selector ──────────────────────────────────────────────────

    macro_rules! parse_selector_test {
        ($($name:ident: $input:expr => ($up:expr, $down:expr, $pat:expr);)*) => {
            $(
                #[test]
                fn $name() {
                    let (up, down, pat) = parse_selector($input).unwrap();
                    assert_eq!((up, down, pat), ($up, $down, $pat), "input: {}", $input);
                }
            )*
        };
    }

    parse_selector_test! {
        parse_plain_name: "daily-sales" => (false, false, "daily-sales");
        parse_upstream: "+daily-sales" => (true, false, "daily-sales");
        parse_downstream: "daily-sales+" => (false, true, "daily-sales");
        parse_both: "+daily-sales+" => (true, true, "daily-sales");
        parse_tag: "tag:finance" => (false, false, "tag:finance");
        parse_upstream_tag: "+tag:finance" => (true, false, "tag:finance");
        parse_n_plus_upstream: "2+daily-sales" => (true, false, "daily-sales");
        parse_n_plus_downstream: "daily-sales+1" => (false, true, "daily-sales");
        parse_n_plus_both: "2+daily-sales+3" => (true, true, "daily-sales");
        parse_1_plus: "1+daily-sales" => (true, false, "daily-sales");
    }

    #[test]
    fn parse_selector_empty_is_error() {
        assert!(parse_selector("").is_err());
        assert!(parse_selector("+").is_err());
    }

    // ── extract_model_name ──────────────────────────────────────────────

    macro_rules! extract_model_test {
        ($($name:ident: $input:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(extract_model_name($input), $expected, "input: {}", $input);
                }
            )*
        };
    }

    extract_model_test! {
        extract_plain: "daily-sales" => Some("daily-sales".to_string());
        extract_upstream: "+daily-sales" => Some("daily-sales".to_string());
        extract_n_plus: "2+daily-sales" => Some("daily-sales".to_string());
        extract_downstream_n: "daily-sales+1" => Some("daily-sales".to_string());
        extract_tag_returns_none: "tag:finance" => None;
        extract_upstream_tag_returns_none: "+tag:finance" => None;
    }
}

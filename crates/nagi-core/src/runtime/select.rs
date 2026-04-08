use std::collections::{HashMap, HashSet, VecDeque};

use thiserror::Error;

use crate::runtime::compile::DependencyGraph;

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
/// - `label:key=value` — all nodes with the given label key-value pair
/// - `label:key`        — all nodes that have the given label key (any value)
/// - `+label:key`       — label match + all ancestors
///
/// Multiple selectors separated by spaces are combined as union (OR).
/// Assets matching any exclude selector are removed from the result.
pub fn select_assets(
    graph: &DependencyGraph,
    selectors: &[&str],
    excludes: &[&str],
) -> Result<Vec<String>, SelectError> {
    let mut result = union_selectors(graph, selectors)?;
    exclude_selectors(graph, &mut result, excludes)?;

    let mut sorted: Vec<String> = result.into_iter().collect();
    sorted.sort();
    Ok(sorted)
}

/// When selectors is empty, returns all nodes in the graph.
fn union_selectors(
    graph: &DependencyGraph,
    selectors: &[&str],
) -> Result<HashSet<String>, SelectError> {
    if selectors.is_empty() {
        return Ok(graph.nodes.iter().map(|n| n.name.clone()).collect());
    }
    let mut result: HashSet<String> = HashSet::new();
    for selector in selectors {
        result.extend(resolve_selector(graph, selector)?);
    }
    Ok(result)
}

fn exclude_selectors(
    graph: &DependencyGraph,
    result: &mut HashSet<String>,
    excludes: &[&str],
) -> Result<(), SelectError> {
    for exclude in excludes {
        for name in resolve_selector(graph, exclude)? {
            result.remove(&name);
        }
    }
    Ok(())
}

fn resolve_selector(
    graph: &DependencyGraph,
    selector: &str,
) -> Result<HashSet<String>, SelectError> {
    if selector.is_empty() {
        return Err(SelectError::InvalidSelector {
            message: "selector must not be empty".to_string(),
        });
    }

    let (upstream_depth, downstream_depth, pattern) = parse_selector(selector)?;
    let roots = intersect_patterns(graph, pattern)?;

    // Build adjacency lists only for needed directions.
    let upstream_adj = if upstream_depth.is_some() {
        Some(build_adjacency(graph, Direction::Upstream))
    } else {
        None
    };
    let downstream_adj = if downstream_depth.is_some() {
        Some(build_adjacency(graph, Direction::Downstream))
    } else {
        None
    };

    let mut result: HashSet<String> = HashSet::new();
    for root in &roots {
        result.insert(root.clone());
        if let Some(adj) = &upstream_adj {
            traverse(adj, root, &mut result, upstream_depth);
        }
        if let Some(adj) = &downstream_adj {
            traverse(adj, root, &mut result, downstream_depth);
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
// Traversal depth: None = no traversal, Some(None) = unlimited, Some(Some(n)) = n levels.
type Depth = Option<Option<usize>>;

/// Parses a depth marker (the text on one side of `+`).
/// Empty string means unlimited, digits mean a specific level, anything else means no traversal.
fn parse_depth_marker(marker: &str) -> Option<Depth> {
    if marker.is_empty() {
        Some(Some(None))
    } else if marker.chars().all(|c| c.is_ascii_digit()) {
        let n: usize = marker.parse().unwrap_or(0);
        Some(Some(Some(n)))
    } else {
        None
    }
}

fn parse_selector(selector: &str) -> Result<(Depth, Depth, &str), SelectError> {
    let (upstream_depth, rest) = match selector.find('+') {
        Some(pos) => parse_depth_marker(&selector[..pos])
            .map(|d| (d, &selector[pos + 1..]))
            .unwrap_or((None, selector)),
        None => (None, selector),
    };

    let (downstream_depth, pattern) = match rest.rfind('+') {
        Some(pos) => parse_depth_marker(&rest[pos + 1..])
            .map(|d| (d, &rest[..pos]))
            .unwrap_or((None, rest)),
        None => (None, rest),
    };

    if pattern.is_empty() {
        return Err(SelectError::InvalidSelector {
            message: "selector pattern must not be empty".to_string(),
        });
    }

    Ok((upstream_depth, downstream_depth, pattern))
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

/// Resolves a possibly comma-separated pattern into asset names.
/// Comma-separated parts are intersected (AND).
fn intersect_patterns(graph: &DependencyGraph, pattern: &str) -> Result<Vec<String>, SelectError> {
    let parts: Vec<&str> = pattern.split(',').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(SelectError::InvalidSelector {
            message: "selector must not contain empty parts".to_string(),
        });
    }
    if parts.len() == 1 {
        return resolve_pattern(graph, pattern);
    }
    // Resolve each part into a set of asset names.
    let sets: Vec<HashSet<String>> = parts
        .iter()
        .map(|part| resolve_pattern(graph, part).map(|v| v.into_iter().collect()))
        .collect::<Result<_, _>>()?;
    // Intersect all sets to keep only assets matching every part.
    let result = sets
        .into_iter()
        .reduce(|a, b| a.intersection(&b).cloned().collect())
        .unwrap_or_default();
    let mut names: Vec<String> = result.into_iter().collect();
    names.sort();
    Ok(names)
}

fn resolve_pattern(graph: &DependencyGraph, pattern: &str) -> Result<Vec<String>, SelectError> {
    if let Some(label_expr) = pattern.strip_prefix("label:") {
        if label_expr.is_empty() {
            return Err(SelectError::InvalidSelector {
                message: "label selector must not be empty".to_string(),
            });
        }
        let matched: Vec<String> = if let Some((key, value)) = label_expr.split_once('=') {
            // Equality match: label:key=value
            graph
                .nodes
                .iter()
                .filter(|n| n.labels.get(key).is_some_and(|v| v == value))
                .map(|n| n.name.clone())
                .collect()
        } else {
            // Existence check: label:key
            graph
                .nodes
                .iter()
                .filter(|n| n.labels.contains_key(label_expr))
                .map(|n| n.name.clone())
                .collect()
        };
        if matched.is_empty() {
            return Err(SelectError::NotFound {
                name: format!("label:{label_expr}"),
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

fn traverse(
    adj: &HashMap<String, Vec<String>>,
    start: &str,
    visited: &mut HashSet<String>,
    max_depth: Option<Option<usize>>,
) {
    let limit = match max_depth {
        Some(Some(n)) => n,
        Some(None) => usize::MAX, // unlimited
        None => return,           // no traversal
    };

    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    queue.push_back((start.to_string(), 0));

    while let Some((current, depth)) = queue.pop_front() {
        if depth >= limit {
            continue;
        }
        let new_neighbors = adj
            .get(&current)
            .into_iter()
            .flatten()
            .filter(|n| visited.insert((*n).clone()));
        for neighbor in new_neighbors {
            queue.push_back((neighbor.clone(), depth + 1));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::runtime::compile::{GraphEdge, GraphNode};

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // raw-sales → daily-sales → monthly-report
    // raw-logs  → access-stats
    fn test_graph() -> DependencyGraph {
        DependencyGraph {
            nodes: vec![
                GraphNode {
                    name: "raw-sales".to_string(),
                    kind: "Asset".to_string(),
                    labels: BTreeMap::new(),
                },
                GraphNode {
                    name: "daily-sales".to_string(),
                    kind: "Asset".to_string(),
                    labels: labels(&[("dbt/finance", ""), ("dbt/daily", "")]),
                },
                GraphNode {
                    name: "monthly-report".to_string(),
                    kind: "Asset".to_string(),
                    labels: labels(&[("dbt/finance", "")]),
                },
                GraphNode {
                    name: "raw-logs".to_string(),
                    kind: "Asset".to_string(),
                    labels: BTreeMap::new(),
                },
                GraphNode {
                    name: "access-stats".to_string(),
                    kind: "Asset".to_string(),
                    labels: labels(&[("dbt/ops", "")]),
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
                    let result = select_assets(&test_graph(), &$selectors, &[]).unwrap();
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
        label_selector: ["label:dbt/finance"] => vec!["daily-sales", "monthly-report"];
        label_with_upstream: ["+label:dbt/finance"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        label_with_downstream: ["label:dbt/finance+"] => vec!["daily-sales", "monthly-report"];
        label_with_upstream_and_downstream: ["+label:dbt/finance+"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        multiple_selectors_union: ["daily-sales", "access-stats"] => vec!["access-stats", "daily-sales"];
        source_with_downstream: ["raw-sales+"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        upstream_on_leaf_node: ["+monthly-report"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        downstream_on_root_node: ["monthly-report+"] => vec!["monthly-report"];
        multiple_selectors_dedup: ["daily-sales", "+monthly-report"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        depth_1_upstream: ["1+monthly-report"] => vec!["daily-sales", "monthly-report"];
        depth_1_downstream: ["raw-sales+1"] => vec!["daily-sales", "raw-sales"];
        depth_2_upstream: ["2+monthly-report"] => vec!["daily-sales", "monthly-report", "raw-sales"];
        depth_0_upstream: ["0+monthly-report"] => vec!["monthly-report"];
        and_two_labels: ["label:dbt/finance,label:dbt/daily"] => vec!["daily-sales"];
        and_label_with_upstream: ["+label:dbt/finance,label:dbt/daily"] => vec!["daily-sales", "raw-sales"];
        and_union_combined: ["label:dbt/finance,label:dbt/daily", "access-stats"] => vec!["access-stats", "daily-sales"];
        label_equality: ["label:dbt/finance="] => vec!["daily-sales", "monthly-report"];
    }

    macro_rules! exclude_ok {
        ($($name:ident: selectors=$selectors:expr, excludes=$excludes:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let result = select_assets(&test_graph(), &$selectors, &$excludes).unwrap();
                    assert_eq!(result, $expected);
                }
            )*
        };
    }

    exclude_ok! {
        exclude_single: selectors=["label:dbt/finance"], excludes=["monthly-report"] => vec!["daily-sales"];
        exclude_by_label: selectors=["label:dbt/finance"], excludes=["label:dbt/daily"] => vec!["monthly-report"];
        exclude_all: selectors=["label:dbt/finance"], excludes=["label:dbt/finance"] => Vec::<String>::new();
        exclude_with_upstream: selectors=["+monthly-report"], excludes=["raw-sales"] => vec!["daily-sales", "monthly-report"];
    }

    macro_rules! select_not_found {
        ($($name:ident: $selectors:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(matches!(
                        select_assets(&test_graph(), &$selectors, &[]).unwrap_err(),
                        SelectError::NotFound { .. }
                    ));
                }
            )*
        };
    }

    select_not_found! {
        not_found: ["nonexistent"];
        label_not_found: ["label:nonexistent"];
    }

    macro_rules! select_invalid {
        ($($name:ident: $selectors:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(matches!(
                        select_assets(&test_graph(), &$selectors, &[]).unwrap_err(),
                        SelectError::InvalidSelector { .. }
                    ));
                }
            )*
        };
    }

    select_invalid! {
        empty_selector: [""];
        plus_only: ["+"];
        empty_label_value: ["label:"];
        double_plus: ["++"];
        empty_comma_part: ["label:dbt/finance,"];
    }

    // ── union_selectors ──────────────────────────────────────────────────

    #[test]
    fn union_selectors_single() {
        let result = union_selectors(&test_graph(), &["daily-sales"]).unwrap();
        assert_eq!(result, HashSet::from(["daily-sales".to_string()]));
    }

    #[test]
    fn union_selectors_multiple() {
        let result = union_selectors(&test_graph(), &["daily-sales", "access-stats"]).unwrap();
        assert_eq!(
            result,
            HashSet::from(["daily-sales".to_string(), "access-stats".to_string()])
        );
    }

    #[test]
    fn union_selectors_empty_returns_all_nodes() {
        let result = union_selectors(&test_graph(), &[]).unwrap();
        assert_eq!(result.len(), 5);
    }

    // ── exclude_selectors ───────────────────────────────────────────────

    #[test]
    fn exclude_selectors_removes_matching() {
        let mut set = HashSet::from([
            "daily-sales".to_string(),
            "monthly-report".to_string(),
            "access-stats".to_string(),
        ]);
        exclude_selectors(&test_graph(), &mut set, &["monthly-report"]).unwrap();
        assert_eq!(
            set,
            HashSet::from(["daily-sales".to_string(), "access-stats".to_string()])
        );
    }

    #[test]
    fn exclude_selectors_by_tag() {
        let mut set = HashSet::from([
            "daily-sales".to_string(),
            "monthly-report".to_string(),
            "access-stats".to_string(),
        ]);
        exclude_selectors(&test_graph(), &mut set, &["label:dbt/finance"]).unwrap();
        assert_eq!(set, HashSet::from(["access-stats".to_string()]));
    }

    #[test]
    fn exclude_selectors_empty_is_noop() {
        let mut set = HashSet::from(["daily-sales".to_string()]);
        exclude_selectors(&test_graph(), &mut set, &[]).unwrap();
        assert_eq!(set, HashSet::from(["daily-sales".to_string()]));
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

    // ── intersect_patterns ───────────────────────────────────────────────

    macro_rules! intersect_patterns_test {
        ($($name:ident: $input:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let result = intersect_patterns(&test_graph(), $input).unwrap();
                    assert_eq!(result, $expected, "input: {}", $input);
                }
            )*
        };
    }

    intersect_patterns_test! {
        intersect_single_label: "label:dbt/finance" => vec!["daily-sales", "monthly-report"];
        intersect_single_name: "daily-sales" => vec!["daily-sales"];
        intersect_two_labels: "label:dbt/finance,label:dbt/daily" => vec!["daily-sales"];
        intersect_disjoint_labels: "label:dbt/finance,label:dbt/ops" => Vec::<String>::new();
    }

    #[test]
    fn intersect_patterns_empty_part_is_error() {
        assert!(matches!(
            intersect_patterns(&test_graph(), "label:dbt/finance,"),
            Err(SelectError::InvalidSelector { .. })
        ));
    }

    #[test]
    fn intersect_patterns_not_found() {
        assert!(matches!(
            intersect_patterns(&test_graph(), "label:dbt/finance,label:nonexistent"),
            Err(SelectError::NotFound { .. })
        ));
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
        parse_plain_name: "daily-sales" => (None, None, "daily-sales");
        parse_upstream: "+daily-sales" => (Some(None), None, "daily-sales");
        parse_downstream: "daily-sales+" => (None, Some(None), "daily-sales");
        parse_both: "+daily-sales+" => (Some(None), Some(None), "daily-sales");
        parse_tag: "tag:finance" => (None, None, "tag:finance");
        parse_upstream_tag: "+tag:finance" => (Some(None), None, "tag:finance");
        parse_n_plus_upstream: "2+daily-sales" => (Some(Some(2)), None, "daily-sales");
        parse_n_plus_downstream: "daily-sales+1" => (None, Some(Some(1)), "daily-sales");
        parse_n_plus_both: "2+daily-sales+3" => (Some(Some(2)), Some(Some(3)), "daily-sales");
        parse_1_plus: "1+daily-sales" => (Some(Some(1)), None, "daily-sales");
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

    #[test]
    fn parse_depth_marker_empty_is_unlimited() {
        assert_eq!(parse_depth_marker(""), Some(Some(None)));
    }

    #[test]
    fn parse_depth_marker_digits_is_bounded() {
        assert_eq!(parse_depth_marker("3"), Some(Some(Some(3))));
    }

    #[test]
    fn parse_depth_marker_zero() {
        assert_eq!(parse_depth_marker("0"), Some(Some(Some(0))));
    }

    #[test]
    fn parse_depth_marker_non_digit_is_none() {
        assert_eq!(parse_depth_marker("abc"), None);
    }

    #[test]
    fn parse_depth_marker_mixed_is_none() {
        assert_eq!(parse_depth_marker("2a"), None);
    }
}

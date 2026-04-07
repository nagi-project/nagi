use std::fmt::Write;

/// Formats a JSON value as a table with column-aligned output.
///
/// Expects a JSON array of objects. Each object becomes a row.
/// Columns are derived from `columns` (display header, JSON key path).
pub fn format_table(value: &serde_json::Value, columns: &[(&str, &str)]) -> Result<String, String> {
    let rows = value
        .as_array()
        .ok_or_else(|| "expected JSON array".to_string())?;

    let headers: Vec<&str> = columns.iter().map(|(h, _)| *h).collect();
    let mut col_widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

    let cell_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, (_, key))| {
                    let cell = extract_cell(row, key);
                    col_widths[i] = col_widths[i].max(cell.len());
                    cell
                })
                .collect()
        })
        .collect();

    let mut out = String::new();

    // Header
    for (i, header) in headers.iter().enumerate() {
        if i > 0 {
            out.push_str("  ");
        }
        let _ = write!(out, "{:<width$}", header, width = col_widths[i]);
    }
    out.push('\n');

    // Rows
    for cells in &cell_rows {
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                out.push_str("  ");
            }
            let _ = write!(out, "{:<width$}", cell, width = col_widths[i]);
        }
        out.push('\n');
    }

    Ok(out)
}

/// Extracts a display string from a JSON value using a dot-separated key path.
fn extract_cell(value: &serde_json::Value, key: &str) -> String {
    let mut current = value;
    for part in key.split('.') {
        current = match current.get(part) {
            Some(v) => v,
            None => return "null".to_string(),
        };
    }
    value_to_display(current)
}

fn value_to_display(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(value_to_display).collect();
            items.join(", ")
        }
        serde_json::Value::Object(_) => "...".to_string(),
    }
}

// ── Column definitions per command ──────────────────────────────────────────

pub const EVALUATE_COLUMNS: &[(&str, &str)] = &[("ASSET", "assetName"), ("READY", "ready")];

pub const STATUS_COLUMNS: &[(&str, &str)] = &[
    ("ASSET", "asset"),
    ("READY", "evaluation.ready"),
    ("SUSPENDED", "suspended"),
];

pub const LS_ASSET_COLUMNS: &[(&str, &str)] = &[
    ("NAME", "name"),
    ("TAGS", "tags"),
    ("UPSTREAMS", "upstreams"),
    ("AUTO SYNC", "autoSync"),
];

pub const LS_CONNECTION_COLUMNS: &[(&str, &str)] = &[("NAME", "name")];

pub const LS_CONDITIONS_COLUMNS: &[(&str, &str)] =
    &[("NAME", "name"), ("CONDITIONS", "conditionNames")];

pub const LS_SYNC_COLUMNS: &[(&str, &str)] = &[("NAME", "name")];

/// Formats a JSON string as text using the given column definitions.
/// Parses the JSON, then calls `format_table`.
pub fn json_to_text(json_str: &str, columns: &[(&str, &str)]) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| format!("invalid JSON: {e}"))?;
    format_table(&value, columns)
}

type TableSection = (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
);

/// Formats ls output as text with sections per resource kind.
pub fn ls_to_text(json_str: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| format!("invalid JSON: {e}"))?;

    let mut out = String::new();
    let sections: &[TableSection] = &[
        ("Assets", "assets", LS_ASSET_COLUMNS),
        ("Connections", "connections", LS_CONNECTION_COLUMNS),
        ("Conditions", "conditions", LS_CONDITIONS_COLUMNS),
        ("Syncs", "syncs", LS_SYNC_COLUMNS),
    ];

    let mut first = true;
    for (title, key, columns) in sections {
        if let Some(arr) = value.get(key) {
            if let Some(items) = arr.as_array() {
                if items.is_empty() {
                    continue;
                }
                if !first {
                    out.push('\n');
                }
                out.push_str(title);
                out.push('\n');
                out.push_str(&format_table(arr, columns)?);
                first = false;
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn format_table_basic() {
        let data = json!([
            {"name": "daily-sales", "ready": true},
            {"name": "monthly-report", "ready": false},
        ]);
        let result = format_table(&data, &[("NAME", "name"), ("READY", "ready")]).unwrap();
        let lines: Vec<&str> = result.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("NAME"));
        assert!(lines[1].contains("daily-sales"));
        assert!(lines[2].contains("monthly-report"));
    }

    #[test]
    fn format_table_missing_key_shows_null() {
        let data = json!([{"name": "a"}]);
        let result = format_table(&data, &[("NAME", "name"), ("TAGS", "tags")]).unwrap();
        assert!(result.contains("null"));
    }

    #[test]
    fn format_table_array_value_joins_with_comma() {
        let data = json!([{"name": "a", "tags": ["x", "y"]}]);
        let result = format_table(&data, &[("NAME", "name"), ("TAGS", "tags")]).unwrap();
        assert!(result.contains("x, y"));
    }

    #[test]
    fn format_table_non_array_returns_error() {
        let data = json!({"name": "a"});
        assert!(format_table(&data, &[("NAME", "name")]).is_err());
    }

    #[test]
    fn extract_cell_nested_key() {
        let data = json!({"a": {"b": "value"}});
        assert_eq!(extract_cell(&data, "a.b"), "value");
    }

    #[test]
    fn extract_cell_missing_nested_key() {
        let data = json!({"a": {"c": "value"}});
        assert_eq!(extract_cell(&data, "a.b"), "null");
    }

    #[test]
    fn value_to_display_null() {
        assert_eq!(value_to_display(&json!(null)), "null");
    }

    #[test]
    fn value_to_display_object() {
        assert_eq!(value_to_display(&json!({"key": "val"})), "...");
    }

    #[test]
    fn json_to_text_parses_and_formats() {
        let json_str = r#"[{"name": "a", "ready": true}]"#;
        let result = json_to_text(json_str, &[("NAME", "name"), ("READY", "ready")]).unwrap();
        assert!(result.contains("NAME"));
        assert!(result.contains("a"));
    }

    #[test]
    fn json_to_text_invalid_json_returns_error() {
        assert!(json_to_text("not json", &[("NAME", "name")]).is_err());
    }

    #[test]
    fn ls_to_text_renders_sections() {
        let json_str = json!({
            "assets": [{"name": "daily-sales", "tags": ["finance"], "upstreams": [], "autoSync": true}],
            "connections": [{"name": "my-bq"}],
            "conditions": [],
            "syncs": [],
        })
        .to_string();
        let result = ls_to_text(&json_str).unwrap();
        assert!(result.contains("Assets"));
        assert!(result.contains("daily-sales"));
        assert!(result.contains("Connections"));
        assert!(result.contains("my-bq"));
        // Empty sections are omitted
        assert!(!result.contains("Conditions"));
        assert!(!result.contains("Syncs"));
    }

    #[test]
    fn ls_to_text_all_empty_returns_empty() {
        let json_str = json!({
            "assets": [],
            "connections": [],
            "conditions": [],
            "syncs": [],
        })
        .to_string();
        let result = ls_to_text(&json_str).unwrap();
        assert!(result.is_empty());
    }

    // ── Column definition integration tests ─────────────────────────────

    #[test]
    fn evaluate_columns_match_asset_eval_result() {
        use crate::runtime::evaluate::{AssetEvalResult, ConditionResult, ConditionStatus};
        let result = AssetEvalResult {
            asset_name: "daily-sales".to_string(),
            ready: false,
            conditions: vec![ConditionResult {
                condition_name: "freshness".to_string(),
                condition_type: "Freshness".to_string(),
                status: ConditionStatus::Drifted {
                    reason: "stale".to_string(),
                },
            }],
            evaluation_id: None,
        };
        let json_str = serde_json::to_string(&[result]).unwrap();
        let text = json_to_text(&json_str, EVALUATE_COLUMNS).unwrap();
        assert!(text.contains("daily-sales"));
        assert!(text.contains("false"));
    }

    #[test]
    fn status_columns_match_asset_status() {
        use crate::runtime::evaluate::{AssetEvalResult, ConditionResult, ConditionStatus};
        use crate::runtime::status::AssetStatus;
        let status = AssetStatus {
            asset: "daily-sales".to_string(),
            evaluation: Some(AssetEvalResult {
                asset_name: "daily-sales".to_string(),
                ready: true,
                conditions: vec![ConditionResult {
                    condition_name: "freshness".to_string(),
                    condition_type: "Freshness".to_string(),
                    status: ConditionStatus::Ready,
                }],
                evaluation_id: None,
            }),
            last_sync: None,
            suspended: None,
        };
        let json_str = serde_json::to_string(&[status]).unwrap();
        let text = json_to_text(&json_str, STATUS_COLUMNS).unwrap();
        assert!(text.contains("daily-sales"));
        assert!(text.contains("true"));
    }

    #[test]
    fn status_columns_null_evaluation() {
        use crate::runtime::status::AssetStatus;
        let status = AssetStatus {
            asset: "no-eval".to_string(),
            evaluation: None,
            last_sync: None,
            suspended: None,
        };
        let json_str = serde_json::to_string(&[status]).unwrap();
        let text = json_to_text(&json_str, STATUS_COLUMNS).unwrap();
        assert!(text.contains("no-eval"));
        assert!(text.contains("null"));
    }

    #[test]
    fn ls_columns_match_ls_output() {
        use crate::interface::ls::{LsAsset, LsConnection, LsOnDriftEntry, LsOutput};
        let output = LsOutput {
            assets: vec![LsAsset {
                name: "daily-sales".to_string(),
                tags: vec!["finance".to_string(), "daily".to_string()],
                upstreams: vec!["raw-sales".to_string()],
                auto_sync: true,
                on_drift: vec![LsOnDriftEntry {
                    conditions: "freshness".to_string(),
                    sync: "dbt-run".to_string(),
                }],
            }],
            connections: vec![LsConnection {
                name: "my-bq".to_string(),
            }],
            conditions: vec![],
            syncs: vec![],
        };
        let json_str = serde_json::to_string(&output).unwrap();
        let text = ls_to_text(&json_str).unwrap();
        assert!(text.contains("daily-sales"));
        assert!(text.contains("finance, daily"));
        assert!(text.contains("raw-sales"));
        assert!(text.contains("true"));
        assert!(text.contains("my-bq"));
    }
}

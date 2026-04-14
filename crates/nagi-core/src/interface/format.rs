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
        serde_json::Value::Object(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| match v.as_str() {
                    Some("") => k.clone(),
                    Some(s) => format!("{k}={s}"),
                    None => format!("{k}={}", value_to_display(v)),
                })
                .collect();
            items.join(", ")
        }
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
    ("LABELS", "labels"),
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

/// Formats inspection JSON (array of SyncInspection) as human-readable text.
pub fn inspect_to_text(json_str: &str) -> Result<String, String> {
    let inspections: Vec<serde_json::Value> =
        serde_json::from_str(json_str).map_err(|e| e.to_string())?;

    if inspections.is_empty() {
        return Ok("No inspections found.".to_string());
    }

    let mut out = String::new();
    for insp in &inspections {
        let asset = insp["asset_name"].as_str().unwrap_or("");
        let exec_id = insp["execution_id"].as_str().unwrap_or("");
        writeln!(out, "=== {asset}  execution_id: {exec_id} ===").unwrap();
        writeln!(out).unwrap();

        for (key, label) in [("before_sync", "Before sync"), ("after_sync", "After sync")] {
            let snapshot = &insp[key];
            writeln!(out, "  {label}:").unwrap();

            if let Some(evals) = snapshot["evaluations"].as_array() {
                for ev in evals {
                    let name = ev["name"].as_str().unwrap_or("");
                    let status = &ev["status"];
                    let status_str = if let Some(state) = status["state"].as_str() {
                        match status["reason"].as_str() {
                            Some(reason) if !reason.is_empty() => {
                                format!("{state}: {reason}")
                            }
                            _ => state.to_string(),
                        }
                    } else {
                        status.to_string()
                    };
                    writeln!(out, "    {name:30} {status_str}").unwrap();
                }
            }

            if let Some(phys) = snapshot["physical_object"].as_object() {
                let obj_type = phys
                    .get("object_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let metrics = phys
                    .get("metrics")
                    .and_then(|v| v.as_object())
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                writeln!(out, "    [{obj_type}] {metrics}").unwrap();
            }
            writeln!(out).unwrap();
        }

        if let Some(jobs) = insp["destination_jobs"].as_array() {
            if !jobs.is_empty() {
                writeln!(out, "  Destination jobs:").unwrap();
                for job in jobs {
                    let job_id = job["job_id"].as_str().unwrap_or("");
                    let stmt = job["statement_type"].as_str().unwrap_or("");
                    writeln!(out, "    {job_id:30} {stmt}").unwrap();
                }
                writeln!(out).unwrap();
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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
        let result = format_table(&data, &[("NAME", "name"), ("LABELS", "labels")]).unwrap();
        assert!(result.contains("null"));
    }

    #[test]
    fn format_table_object_value_shows_labels() {
        let data = json!([{"name": "a", "labels": {"dbt/x": "", "dbt/y": ""}}]);
        let result = format_table(&data, &[("NAME", "name"), ("LABELS", "labels")]).unwrap();
        assert!(result.contains("dbt/x, dbt/y"));
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
        assert_eq!(value_to_display(&json!({"key": "val"})), "key=val");
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
            "assets": [{"name": "daily-sales", "labels": {"dbt/finance": ""}, "upstreams": [], "autoSync": true}],
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
                labels: BTreeMap::from([
                    ("dbt/finance".to_string(), String::new()),
                    ("dbt/daily".to_string(), String::new()),
                ]),
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
        assert!(text.contains("dbt/daily, dbt/finance"));
        assert!(text.contains("raw-sales"));
        assert!(text.contains("true"));
        assert!(text.contains("my-bq"));
    }

    // ── inspect_to_text ─────────────────────────────────────────────

    #[test]
    fn inspect_to_text_empty() {
        let text = inspect_to_text("[]").unwrap();
        assert_eq!(text, "No inspections found.");
    }

    #[test]
    fn inspect_to_text_with_data() {
        let json = serde_json::json!([{
            "schema_version": 1,
            "execution_id": "exec-001",
            "asset_name": "daily-sales",
            "before_sync": {
                "evaluations": [{
                    "name": "freshness-24h",
                    "status": {"state": "drifted", "reason": "age 30h"},
                    "detail": {}
                }],
                "physical_object": {
                    "object_type": "BASE TABLE",
                    "metrics": {"row_count": 1000}
                }
            },
            "after_sync": {
                "evaluations": [{
                    "name": "freshness-24h",
                    "status": {"state": "ready"},
                    "detail": {}
                }],
                "physical_object": {
                    "object_type": "BASE TABLE",
                    "metrics": {"row_count": 1500}
                }
            },
            "destination_jobs": []
        }]);
        let text = inspect_to_text(&json.to_string()).unwrap();
        assert!(text.contains("daily-sales"));
        assert!(text.contains("exec-001"));
        assert!(text.contains("drifted: age 30h"));
        assert!(text.contains("ready"));
        assert!(text.contains("BASE TABLE"));
        assert!(text.contains("row_count=1000"));
        assert!(text.contains("row_count=1500"));
    }
}

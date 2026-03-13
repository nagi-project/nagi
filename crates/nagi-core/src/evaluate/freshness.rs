use std::time::SystemTime;

use serde_json::Value;

use super::{ConditionStatus, EvaluateError};


pub(super) fn evaluate_freshness(
    value: Value,
    max_age: std::time::Duration,
) -> Result<ConditionStatus, EvaluateError> {
    // BigQuery returns timestamps as RFC3339 strings or numeric milliseconds.
    let last_updated = parse_timestamp(&value).ok_or_else(|| {
        EvaluateError::UnexpectedResult(format!(
            "Freshness condition must return a timestamp (RFC3339 string or Unix epoch seconds), got: {value}"
        ))
    })?;
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let age_secs = now - last_updated;
    if age_secs <= max_age.as_secs_f64() {
        Ok(ConditionStatus::Ready)
    } else {
        Ok(ConditionStatus::NotReady {
            reason: format!(
                "last updated {:.0}s ago, max age is {}s",
                age_secs,
                max_age.as_secs()
            ),
        })
    }
}

/// Parses a timestamp value returned by BigQuery.
/// Accepts: RFC3339 string, Unix epoch float string, or numeric.
fn parse_timestamp(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => {
            // Try RFC3339 first.
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return Some(dt.timestamp() as f64);
            }
            // Fall back to numeric string (BigQuery sometimes returns epoch seconds as string).
            s.parse::<f64>().ok()
        }
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}


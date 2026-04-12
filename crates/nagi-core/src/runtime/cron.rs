use std::borrow::Cow;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A validated cron expression in standard 5-field format (e.g. "0 3 * * *").
/// Fails at deserialization time if the expression is invalid.
#[derive(Debug, Clone, PartialEq)]
pub struct CronSchedule(String);

impl JsonSchema for CronSchedule {
    fn schema_name() -> Cow<'static, str> {
        "CronSchedule".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "cron",
            "description": "A cron expression in standard 5-field format (e.g. \"0 3 * * *\")."
        })
    }
}

/// Validates a 5-field cron expression (minute hour day month weekday).
fn validate_cron(expr: &str) -> Result<(), String> {
    const FIELD_RANGES: [(u32, u32); 5] = [
        (0, 59), // minute
        (0, 23), // hour
        (1, 31), // day of month
        (1, 12), // month
        (0, 6),  // day of week
    ];
    const FIELD_NAMES: [&str; 5] = ["minute", "hour", "day", "month", "weekday"];

    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(format!("expected 5 fields, got {}", fields.len()));
    }

    for (i, field) in fields.iter().enumerate() {
        let (min, max) = FIELD_RANGES[i];
        validate_field(field, min, max, FIELD_NAMES[i])?;
    }
    Ok(())
}

fn validate_field(field: &str, min: u32, max: u32, name: &str) -> Result<(), String> {
    field.split(',').try_for_each(|item| {
        let (range_part, step) = split_step(item, name)?;
        if range_part == "*" {
            validate_step(step, max, name)
        } else {
            validate_range(range_part, min, max, name)
        }
    })
}

fn split_step<'a>(item: &'a str, name: &str) -> Result<(&'a str, Option<u32>), String> {
    match item.split_once('/') {
        Some((range, s)) => {
            let step: u32 = s
                .parse()
                .map_err(|_| format!("invalid step '{s}' in {name}"))?;
            if step == 0 {
                return Err(format!("step must be > 0 in {name}"));
            }
            Ok((range, Some(step)))
        }
        None => Ok((item, None)),
    }
}

fn validate_step(step: Option<u32>, max: u32, name: &str) -> Result<(), String> {
    match step {
        Some(st) if st > max => Err(format!("step {st} exceeds max {max} for {name}")),
        _ => Ok(()),
    }
}

fn validate_range(range_part: &str, min: u32, max: u32, name: &str) -> Result<(), String> {
    match range_part.split_once('-') {
        Some((lo_s, hi_s)) => {
            let lo: u32 = lo_s
                .parse()
                .map_err(|_| format!("invalid value '{lo_s}' in {name}"))?;
            let hi: u32 = hi_s
                .parse()
                .map_err(|_| format!("invalid value '{hi_s}' in {name}"))?;
            if lo < min || hi > max || lo > hi {
                return Err(format!(
                    "range {lo}-{hi} out of bounds {min}-{max} for {name}"
                ));
            }
        }
        None => {
            let val: u32 = range_part
                .parse()
                .map_err(|_| format!("invalid value '{range_part}' in {name}"))?;
            if val < min || val > max {
                return Err(format!("value {val} out of range {min}-{max} for {name}"));
            }
        }
    }
    Ok(())
}

impl<'de> Deserialize<'de> for CronSchedule {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        validate_cron(&s)
            .map(|_| CronSchedule(s))
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for CronSchedule {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    impl CronSchedule {
        pub fn as_str(&self) -> &str {
            &self.0
        }
    }

    #[derive(Deserialize, Serialize)]
    struct Wrapper {
        cron: CronSchedule,
    }

    #[test]
    fn parse_valid_cron() {
        let w: Wrapper = serde_yaml::from_str("cron: \"0 3 * * *\"").unwrap();
        assert_eq!(w.cron.as_str(), "0 3 * * *");
    }

    #[test]
    fn rejects_invalid_cron() {
        let result: Result<Wrapper, _> = serde_yaml::from_str("cron: \"invalid\"");
        assert!(result.is_err());
    }

    #[test]
    fn rejects_out_of_range_field() {
        let result: Result<Wrapper, _> = serde_yaml::from_str("cron: \"60 3 * * *\"");
        assert!(result.is_err());
    }

    #[test]
    fn serialize_roundtrip() {
        let w = Wrapper {
            cron: CronSchedule("0 3 * * *".to_string()),
        };
        let yaml = serde_yaml::to_string(&w).unwrap();
        let w2: Wrapper = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(w.cron, w2.cron);
    }

    // ── split_step ──────────────────────────────────────────────────────

    macro_rules! split_step_test {
        ($($name:ident: $input:expr => $range:expr, $step:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let (range, step) = split_step($input, "test").unwrap();
                    assert_eq!(range, $range, "input: {}", $input);
                    assert_eq!(step, $step, "input: {}", $input);
                }
            )*
        };
    }

    split_step_test! {
        split_step_no_step: "5" => "5", None;
        split_step_wildcard: "*" => "*", None;
        split_step_with_step: "*/5" => "*", Some(5);
        split_step_range_with_step: "1-10/2" => "1-10", Some(2);
        split_step_value_with_step: "5/10" => "5", Some(10);
    }

    #[test]
    fn split_step_rejects_zero_step() {
        let err = split_step("*/0", "minute").unwrap_err();
        assert!(err.contains("step must be > 0"), "got: {err}");
    }

    #[test]
    fn split_step_rejects_non_numeric_step() {
        let err = split_step("*/abc", "minute").unwrap_err();
        assert!(err.contains("invalid step"), "got: {err}");
    }

    // ── validate_step ───────────────────────────────────────────────────

    #[test]
    fn validate_step_at_max() {
        assert!(validate_step(Some(59), 59, "minute").is_ok());
    }

    #[test]
    fn validate_step_exceeds_max() {
        let err = validate_step(Some(60), 59, "minute").unwrap_err();
        assert!(err.contains("step 60 exceeds max 59"), "got: {err}");
    }

    // ── validate_range ──────────────────────────────────────────────────

    macro_rules! validate_range_ok_test {
        ($($name:ident: $input:expr, $min:expr, $max:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(validate_range($input, $min, $max, "test").is_ok(),
                        "expected ok for '{}' in {}-{}", $input, $min, $max);
                }
            )*
        };
    }

    validate_range_ok_test! {
        validate_range_single_value: "5", 0, 59;
        validate_range_at_min: "0", 0, 59;
        validate_range_at_max: "59", 0, 59;
        validate_range_span: "1-5", 0, 59;
        validate_range_single_point_range: "3-3", 0, 59;
        validate_range_day_min: "1", 1, 31;
    }

    macro_rules! validate_range_err_test {
        ($($name:ident: $input:expr, $min:expr, $max:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    let err = validate_range($input, $min, $max, "test").unwrap_err();
                    assert!(err.contains($expected),
                        "expected '{}' in error for '{}': {}", $expected, $input, err);
                }
            )*
        };
    }

    validate_range_err_test! {
        validate_range_below_min: "0", 1, 31 => "out of range";
        validate_range_above_max: "60", 0, 59 => "out of range";
        validate_range_lo_below_min: "0-5", 1, 31 => "out of bounds";
        validate_range_hi_above_max: "50-60", 0, 59 => "out of bounds";
        validate_range_inverted: "10-5", 0, 59 => "out of bounds";
        validate_range_non_numeric: "abc", 0, 59 => "invalid value";
    }
}

use serde_json::Value;

use super::{ConditionStatus, EvaluateError};

pub(super) fn evaluate_boolean(value: Value) -> Result<ConditionStatus, EvaluateError> {
    match &value {
        Value::Bool(true) => Ok(ConditionStatus::Ready),
        Value::Bool(false) => Ok(ConditionStatus::NotReady {
            reason: "condition returned false".to_string(),
        }),
        // BigQuery returns booleans as strings "true"/"false" in query results.
        Value::String(s) if s.eq_ignore_ascii_case("true") => Ok(ConditionStatus::Ready),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Ok(ConditionStatus::NotReady {
            reason: "condition returned false".to_string(),
        }),
        other => Err(EvaluateError::UnexpectedResult(format!(
            "SQL condition must return a scalar boolean (true/false), got: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_true_is_ready() {
        assert_eq!(evaluate_boolean(Value::Bool(true)).unwrap(), ConditionStatus::Ready);
    }

    #[test]
    fn bool_false_is_not_ready() {
        assert!(matches!(
            evaluate_boolean(Value::Bool(false)).unwrap(),
            ConditionStatus::NotReady { .. }
        ));
    }

    #[test]
    fn string_true_is_ready() {
        assert_eq!(
            evaluate_boolean(Value::String("true".to_string())).unwrap(),
            ConditionStatus::Ready
        );
    }

    #[test]
    fn string_false_is_not_ready() {
        assert!(matches!(
            evaluate_boolean(Value::String("false".to_string())).unwrap(),
            ConditionStatus::NotReady { .. }
        ));
    }

    #[test]
    fn unexpected_value_returns_error() {
        assert!(matches!(
            evaluate_boolean(Value::Null),
            Err(EvaluateError::UnexpectedResult(_))
        ));
    }
}

use serde_json::Value;

use super::{ConditionStatus, EvaluateError};

pub(super) fn evaluate_boolean(value: Value) -> Result<ConditionStatus, EvaluateError> {
    match &value {
        Value::Bool(true) => Ok(ConditionStatus::Ready),
        Value::Bool(false) => Ok(ConditionStatus::Drifted {
            reason: "condition returned false".to_string(),
        }),
        // BigQuery returns booleans as strings "true"/"false" in query results.
        Value::String(s) if s.eq_ignore_ascii_case("true") => Ok(ConditionStatus::Ready),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Ok(ConditionStatus::Drifted {
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

    macro_rules! boolean_ready {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(evaluate_boolean($input).unwrap(), ConditionStatus::Ready);
                }
            )*
        };
    }

    boolean_ready! {
        bool_true_is_ready: Value::Bool(true);
        string_true_is_ready: Value::String("true".to_string());
    }

    macro_rules! boolean_not_ready {
        ($($name:ident: $input:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert!(matches!(
                        evaluate_boolean($input).unwrap(),
                        ConditionStatus::Drifted { .. }
                    ));
                }
            )*
        };
    }

    boolean_not_ready! {
        bool_false_is_not_ready: Value::Bool(false);
        string_false_is_not_ready: Value::String("false".to_string());
    }

    #[test]
    fn unexpected_value_returns_error() {
        assert!(matches!(
            evaluate_boolean(Value::Null),
            Err(EvaluateError::UnexpectedResult(_))
        ));
    }
}

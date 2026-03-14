use crate::db::Connection;
use crate::kind::asset::DesiredCondition;

use super::{boolean, command, freshness, ConditionResult, EvaluateError};

pub(super) async fn evaluate_condition(
    index: usize,
    asset_name: &str,
    condition: &DesiredCondition,
    conn: &dyn Connection,
) -> Result<ConditionResult, EvaluateError> {
    let (condition_type, status) = match condition {
        DesiredCondition::Freshness {
            max_age, column, ..
        } => {
            let sql = conn.freshness_sql(asset_name, column.as_deref());
            let value = conn.query_scalar(&sql).await?;
            let status = freshness::evaluate_freshness(value, max_age.as_std())?;
            ("Freshness".to_string(), status)
        }
        DesiredCondition::SQL { query } => {
            let value = conn.query_scalar(query).await?;
            let status = boolean::evaluate_boolean(value)?;
            ("SQL".to_string(), status)
        }
        DesiredCondition::Command { run } => {
            let status = command::evaluate_command(run).await?;
            ("Command".to_string(), status)
        }
    };
    Ok(ConditionResult {
        index,
        condition_type,
        status,
    })
}

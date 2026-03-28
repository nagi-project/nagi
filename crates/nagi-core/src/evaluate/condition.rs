use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::kind::asset::DesiredCondition;
use crate::kind::connection::Connection;

use super::{boolean, command, freshness, ConditionResult, EvaluateError};

/// Parses the query with the connection's SQL dialect and rejects anything
/// other than a single SELECT statement.
fn require_select_only(query: &str, dialect: &dyn Dialect) -> Result<(), EvaluateError> {
    let stmts = Parser::new(dialect)
        .try_with_sql(query)
        .and_then(|mut p| p.parse_statements())
        .map_err(|e| EvaluateError::ReadOnlyViolation(e.to_string()))?;

    match stmts.as_slice() {
        [sqlparser::ast::Statement::Query(_)] => Ok(()),
        _ => Err(EvaluateError::ReadOnlyViolation(query.to_string())),
    }
}

pub(super) async fn evaluate_condition(
    name: &str,
    asset_name: &str,
    condition: &DesiredCondition,
    conn: Option<&dyn Connection>,
) -> Result<ConditionResult, EvaluateError> {
    // Require a connection for SQL-based conditions; Command conditions need none.
    macro_rules! require_conn {
        () => {
            conn.ok_or_else(|| EvaluateError::NoConnection {
                condition_name: name.to_string(),
            })?
        };
    }

    let (condition_type, status) = match condition {
        DesiredCondition::Freshness {
            max_age, column, ..
        } => {
            let c = require_conn!();
            let sql = c.freshness_sql(asset_name, column.as_deref());
            let value = c.query_scalar(&sql).await?;
            let status = freshness::evaluate_freshness(value, max_age.as_std())?;
            ("Freshness".to_string(), status)
        }
        DesiredCondition::SQL { query, .. } => {
            let c = require_conn!();
            require_select_only(query, &*c.sql_dialect())?;
            let value = c.query_scalar(query).await?;
            let status = boolean::evaluate_boolean(value)?;
            ("SQL".to_string(), status)
        }
        DesiredCondition::Command { run, env, .. } => {
            let status = command::evaluate_command(run, env).await?;
            ("Command".to_string(), status)
        }
    };
    Ok(ConditionResult {
        condition_name: name.to_string(),
        condition_type,
        status,
    })
}

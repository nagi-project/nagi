use crate::evaluate::{AssetEvalResult, ConditionStatus};

use super::{parse_date, LogError, LogStore};

impl LogStore {
    /// Records an evaluation result into evaluate_logs.
    pub fn write_evaluate_log(
        &self,
        evaluation_id: &str,
        result: &AssetEvalResult,
        started_at: &str,
        finished_at: &str,
    ) -> Result<(), LogError> {
        let date = parse_date(started_at)?;
        let tx = self.conn.unchecked_transaction()?;
        for cond in &result.conditions {
            let (result_str, detail) = match &cond.status {
                ConditionStatus::Ready => ("Ready", String::new()),
                ConditionStatus::NotReady { reason } => ("NotReady", reason.clone()),
            };
            tx.execute(
                "INSERT INTO evaluate_logs
                 (evaluation_id, condition_name, asset_name, condition_type,
                  started_at, finished_at, result, detail, date)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                rusqlite::params![
                    evaluation_id,
                    cond.condition_name,
                    result.asset_name,
                    cond.condition_type,
                    started_at,
                    finished_at,
                    result_str,
                    detail,
                    date,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluate::ConditionResult;

    fn make_eval_result(asset_name: &str) -> AssetEvalResult {
        AssetEvalResult {
            asset_name: asset_name.to_string(),
            ready: false,
            conditions: vec![
                ConditionResult {
                    condition_name: "freshness-check".to_string(),
                    condition_type: "Freshness".to_string(),
                    status: ConditionStatus::Ready,
                },
                ConditionResult {
                    condition_name: "row-count".to_string(),
                    condition_type: "SQL".to_string(),
                    status: ConditionStatus::NotReady {
                        reason: "count is 0".to_string(),
                    },
                },
            ],
        }
    }

    #[test]
    fn write_and_read_evaluate_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let result = make_eval_result("my-asset");
        store
            .write_evaluate_log(
                "eval-001",
                &result,
                "2026-03-16T10:00:00+09:00",
                "2026-03-16T10:00:02+09:00",
            )
            .unwrap();

        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM evaluate_logs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn evaluate_log_stores_correct_fields() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let result = make_eval_result("my-asset");
        store
            .write_evaluate_log(
                "eval-002",
                &result,
                "2026-03-16T10:00:00+09:00",
                "2026-03-16T10:00:02+09:00",
            )
            .unwrap();

        let (cond_name, asset, cond_type, res, detail, date): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = store
            .conn
            .query_row(
                "SELECT condition_name, asset_name, condition_type, result, detail, date
                 FROM evaluate_logs WHERE condition_name = 'row-count'",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(cond_name, "row-count");
        assert_eq!(asset, "my-asset");
        assert_eq!(cond_type, "SQL");
        assert_eq!(res, "NotReady");
        assert_eq!(detail, "count is 0");
        assert_eq!(date, "2026-03-16");
    }

    #[test]
    fn duplicate_evaluate_log_pk_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open_in_memory(dir.path()).unwrap();

        let result = make_eval_result("my-asset");
        store
            .write_evaluate_log(
                "eval-dup",
                &result,
                "2026-03-16T10:00:00+09:00",
                "2026-03-16T10:00:02+09:00",
            )
            .unwrap();

        let err = store
            .write_evaluate_log(
                "eval-dup",
                &result,
                "2026-03-16T11:00:00+09:00",
                "2026-03-16T11:00:02+09:00",
            )
            .unwrap_err();
        assert!(matches!(err, LogError::Sqlite(_)));
    }
}

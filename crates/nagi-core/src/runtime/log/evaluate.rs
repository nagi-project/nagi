use serde::Serialize;

use crate::runtime::evaluate::{AssetEvalResult, ConditionStatus};

use super::{parse_date, LogError, LogStore};

/// A single row in evaluate_logs.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateLogEntry {
    /// Unique identifier for this evaluation run.
    pub evaluation_id: String,
    /// Name of the evaluated condition.
    pub condition_name: String,
    /// Name of the Asset that was evaluated.
    pub asset_name: String,
    /// Type of the condition (e.g. `SQL`, `Freshness`).
    pub condition_type: String,
    /// RFC 3339 timestamp when evaluation started.
    pub started_at: String,
    /// RFC 3339 timestamp when evaluation finished.
    pub finished_at: String,
    /// Evaluation result: `Ready` or `Drifted`.
    pub result: String,
    /// Human-readable reason when result is `Drifted`. Empty when `Ready`.
    pub detail: String,
    /// Date partition key (YYYY-MM-DD) derived from `started_at`.
    pub date: String,
}

impl EvaluateLogEntry {
    /// Constructs entries from an `AssetEvalResult`, one per condition.
    pub fn from_eval_result(
        evaluation_id: &str,
        result: &AssetEvalResult,
        started_at: &str,
        finished_at: &str,
        date: &str,
    ) -> Vec<Self> {
        result
            .conditions
            .iter()
            .map(|cond| {
                let (result_str, detail) = match &cond.status {
                    ConditionStatus::Ready => ("Ready".to_string(), String::new()),
                    ConditionStatus::Drifted { reason } => ("Drifted".to_string(), reason.clone()),
                };
                EvaluateLogEntry {
                    evaluation_id: evaluation_id.to_string(),
                    condition_name: cond.condition_name.clone(),
                    asset_name: result.asset_name.clone(),
                    condition_type: cond.condition_type.clone(),
                    started_at: started_at.to_string(),
                    finished_at: finished_at.to_string(),
                    result: result_str,
                    detail,
                    date: date.to_string(),
                }
            })
            .collect()
    }
}

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
        let entries = EvaluateLogEntry::from_eval_result(
            evaluation_id,
            result,
            started_at,
            finished_at,
            date,
        );
        let tx = self.conn.unchecked_transaction()?;
        for entry in &entries {
            tx.execute(
                "INSERT INTO evaluate_logs
                 (evaluation_id, condition_name, asset_name, condition_type,
                  started_at, finished_at, result, detail, date)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                rusqlite::params![
                    entry.evaluation_id,
                    entry.condition_name,
                    entry.asset_name,
                    entry.condition_type,
                    entry.started_at,
                    entry.finished_at,
                    entry.result,
                    entry.detail,
                    entry.date,
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
    use crate::runtime::evaluate::{ConditionResult, ConditionStatus as CS};

    fn make_eval_result(asset_name: &str) -> AssetEvalResult {
        AssetEvalResult {
            asset_name: asset_name.to_string(),
            ready: false,
            conditions: vec![
                ConditionResult {
                    condition_name: "freshness-check".to_string(),
                    condition_type: "Freshness".to_string(),
                    status: CS::Ready,
                },
                ConditionResult {
                    condition_name: "row-count".to_string(),
                    condition_type: "SQL".to_string(),
                    status: CS::Drifted {
                        reason: "count is 0".to_string(),
                    },
                },
            ],
            evaluation_id: None,
        }
    }

    #[test]
    fn from_eval_result_maps_ready_condition() {
        let result = AssetEvalResult {
            asset_name: "asset-a".to_string(),
            ready: true,
            conditions: vec![ConditionResult {
                condition_name: "check".to_string(),
                condition_type: "SQL".to_string(),
                status: CS::Ready,
            }],
            evaluation_id: None,
        };
        let entries = EvaluateLogEntry::from_eval_result(
            "eval-x",
            &result,
            "2026-03-22T10:00:00Z",
            "2026-03-22T10:00:01Z",
            "2026-03-22",
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].evaluation_id, "eval-x");
        assert_eq!(entries[0].asset_name, "asset-a");
        assert_eq!(entries[0].condition_name, "check");
        assert_eq!(entries[0].result, "Ready");
        assert_eq!(entries[0].detail, "");
        assert_eq!(entries[0].date, "2026-03-22");
    }

    #[test]
    fn from_eval_result_maps_not_ready_condition() {
        let result = AssetEvalResult {
            asset_name: "asset-b".to_string(),
            ready: false,
            conditions: vec![ConditionResult {
                condition_name: "row-count".to_string(),
                condition_type: "SQL".to_string(),
                status: CS::Drifted {
                    reason: "count is 0".to_string(),
                },
            }],
            evaluation_id: None,
        };
        let entries = EvaluateLogEntry::from_eval_result(
            "eval-y",
            &result,
            "2026-03-22T10:00:00Z",
            "2026-03-22T10:00:01Z",
            "2026-03-22",
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].result, "Drifted");
        assert_eq!(entries[0].detail, "count is 0");
    }

    #[test]
    fn from_eval_result_produces_one_entry_per_condition() {
        let result = make_eval_result("multi");
        let entries = EvaluateLogEntry::from_eval_result(
            "eval-z",
            &result,
            "2026-03-22T10:00:00Z",
            "2026-03-22T10:00:01Z",
            "2026-03-22",
        );
        assert_eq!(entries.len(), 2);
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
        assert_eq!(res, "Drifted");
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

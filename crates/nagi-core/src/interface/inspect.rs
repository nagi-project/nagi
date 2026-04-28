use std::path::Path;
use std::time::Duration;

use crate::runtime::compile::{load_compiled_assets, CompiledAsset};
use crate::runtime::inspect::{bigquery, InspectionStore, SyncInspection};
use crate::runtime::kind::connection::{Connection, ResolvedConnection};

/// Backfills empty `jobs` in inspections by querying
/// BigQuery `INFORMATION_SCHEMA.JOBS_BY_PROJECT`.
///
/// For each inspection with empty jobs, resolves the asset's
/// Connection from compiled YAML, fetches jobs, updates the inspection,
/// and writes it back to the store.
///
/// Failures are logged as warnings and do not propagate.
pub async fn backfill_jobs(
    store: &InspectionStore,
    inspections: &mut [SyncInspection],
    target_dir: &Path,
    asset_name: &str,
    default_timeout: Duration,
) {
    let needs_backfill = inspections.iter().any(|i| i.jobs.is_empty());
    if !needs_backfill {
        return;
    }

    let conn_info = match resolve_bq_connection(target_dir, asset_name, default_timeout) {
        Some(info) => info,
        None => return,
    };

    backfill_with_connection(store, inspections, &conn_info).await;
}

async fn backfill_with_connection(
    store: &InspectionStore,
    inspections: &mut [SyncInspection],
    conn_info: &BqConnInfo,
) {
    for inspection in inspections.iter_mut() {
        if !inspection.jobs.is_empty() {
            continue;
        }
        fetch_and_save_jobs(store, inspection, conn_info).await;
    }
}

async fn fetch_and_save_jobs(
    store: &InspectionStore,
    inspection: &mut SyncInspection,
    conn_info: &BqConnInfo,
) {
    let jobs = match bigquery::fetch_jobs(
        conn_info.conn.as_ref(),
        &conn_info.project,
        conn_info.location.as_deref(),
        &inspection.execution_id,
    )
    .await
    {
        Ok(jobs) => jobs,
        Err(e) => {
            tracing::warn!(
                execution_id = %inspection.execution_id,
                error = %e,
                "backfill: failed to fetch destination jobs"
            );
            return;
        }
    };

    if jobs.is_empty() {
        return;
    }

    inspection.jobs = jobs;
    if let Err(e) = store.write(inspection) {
        tracing::warn!(error = %e, "backfill: failed to write inspection");
    }
}

struct BqConnInfo {
    conn: Box<dyn Connection>,
    project: String,
    location: Option<String>,
}

fn resolve_bq_connection(
    target_dir: &Path,
    asset_name: &str,
    default_timeout: Duration,
) -> Option<BqConnInfo> {
    let assets = load_compiled_assets(target_dir, &[asset_name], &[]).ok()?;
    let (_, yaml) = assets.into_iter().next()?;
    let compiled: CompiledAsset = serde_yaml::from_str(&yaml).ok()?;
    let resolved = compiled.connection.as_ref()?;

    #[cfg(feature = "bigquery")]
    if let ResolvedConnection::BigQuery {
        project, location, ..
    } = resolved
    {
        let conn = resolved.connect(default_timeout).ok()?;
        return Some(BqConnInfo {
            conn,
            project: project.clone(),
            location: location.clone(),
        });
    }

    let _ = resolved; // suppress unused warning when bigquery feature is off
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::inspect::{InspectionStore, SyncInspection, SyncJob};
    use crate::runtime::kind::connection::{Connection, ConnectionError};
    use async_trait::async_trait;
    use std::collections::HashMap;

    /// Mock Connection that returns a fixed set of jobs from query_rows.
    struct MockConn {
        jobs: Vec<serde_json::Value>,
    }

    impl MockConn {
        fn with_jobs(jobs: Vec<serde_json::Value>) -> Self {
            Self { jobs }
        }

        fn empty() -> Self {
            Self { jobs: vec![] }
        }
    }

    #[async_trait]
    impl Connection for MockConn {
        async fn query_scalar(&self, _sql: &str) -> Result<serde_json::Value, ConnectionError> {
            unimplemented!()
        }
        fn freshness_sql(&self, _: &str, _: Option<&str>) -> Result<String, ConnectionError> {
            unimplemented!()
        }
        fn sql_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
            Box::new(sqlparser::dialect::BigQueryDialect {})
        }
        async fn execute_sql(&self, _: &str) -> Result<(), ConnectionError> {
            unimplemented!()
        }
        async fn load_jsonl(
            &self,
            _: &str,
            _: &str,
            _: &std::path::Path,
        ) -> Result<(), ConnectionError> {
            unimplemented!()
        }
        async fn query_rows(&self, _sql: &str) -> Result<Vec<serde_json::Value>, ConnectionError> {
            Ok(self.jobs.clone())
        }
    }

    const TEST_FINISHED_AT: &str = "2026-04-16T09:30:00.000Z";

    fn new_inspection(execution_id: &str, asset_name: &str) -> SyncInspection {
        SyncInspection::new(
            execution_id.to_string(),
            asset_name.to_string(),
            TEST_FINISHED_AT.to_string(),
        )
    }

    fn conn_info_with(conn: MockConn) -> BqConnInfo {
        BqConnInfo {
            conn: Box::new(conn),
            project: "p".to_string(),
            location: Some("us".to_string()),
        }
    }

    // ── backfill_jobs ────────────────────────────────────

    #[tokio::test]
    async fn backfill_skips_when_all_have_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let mut inspections = vec![{
            let mut i = new_inspection("exec-001", "a");
            i.jobs = vec![SyncJob {
                job_id: "existing".to_string(),
                statement_type: None,
                details: HashMap::new(),
            }];
            i
        }];
        backfill_jobs(
            &store,
            &mut inspections,
            Path::new("/nonexistent"),
            "a",
            std::time::Duration::from_secs(30),
        )
        .await;
        assert_eq!(inspections[0].jobs.len(), 1);
        assert_eq!(inspections[0].jobs[0].job_id, "existing");
    }

    #[tokio::test]
    async fn backfill_skips_when_connection_not_resolved() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let mut inspections = vec![new_inspection("exec-001", "a")];
        backfill_jobs(
            &store,
            &mut inspections,
            dir.path(),
            "a",
            std::time::Duration::from_secs(30),
        )
        .await;
        assert!(inspections[0].jobs.is_empty());
    }

    // ── backfill_with_connection ─────────────────────────────────────

    #[tokio::test]
    async fn backfill_with_connection_fills_empty_inspections() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let mut inspections = vec![
            new_inspection("exec-001", "my-asset"),
            new_inspection("exec-002", "my-asset"),
        ];
        for i in &inspections {
            store.write(i).unwrap();
        }

        let conn_info = conn_info_with(MockConn::with_jobs(vec![serde_json::json!({
            "job_id": "bqjob_filled",
            "statement_type": "INSERT"
        })]));
        backfill_with_connection(&store, &mut inspections, &conn_info).await;

        for i in &inspections {
            assert_eq!(i.jobs.len(), 1);
            assert_eq!(i.jobs[0].job_id, "bqjob_filled");
        }
        // Verify files were updated
        for id in ["exec-001", "exec-002"] {
            let loaded = store.read("my-asset", id).unwrap();
            assert_eq!(loaded.jobs.len(), 1);
        }
    }

    #[tokio::test]
    async fn backfill_with_connection_skips_already_filled() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let mut filled = new_inspection("exec-001", "my-asset");
        filled.jobs = vec![SyncJob {
            job_id: "original".to_string(),
            statement_type: None,
            details: HashMap::new(),
        }];
        let empty = new_inspection("exec-002", "my-asset");
        store.write(&empty).unwrap();
        let mut inspections = vec![filled, empty];

        let conn_info = conn_info_with(MockConn::with_jobs(vec![serde_json::json!({
            "job_id": "bqjob_new",
            "statement_type": "MERGE"
        })]));
        backfill_with_connection(&store, &mut inspections, &conn_info).await;

        assert_eq!(inspections[0].jobs[0].job_id, "original");
        assert_eq!(inspections[1].jobs[0].job_id, "bqjob_new");
    }

    // ── fetch_and_save_jobs ─────────────────────────────────────────

    #[tokio::test]
    async fn fetch_and_save_jobs_writes_to_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let mut inspection = new_inspection("exec-001", "my-asset");
        store.write(&inspection).unwrap();

        let conn_info = conn_info_with(MockConn::with_jobs(vec![serde_json::json!({
            "job_id": "bqjob_fetched",
            "statement_type": "MERGE"
        })]));
        fetch_and_save_jobs(&store, &mut inspection, &conn_info).await;

        assert_eq!(inspection.jobs.len(), 1);
        assert_eq!(inspection.jobs[0].job_id, "bqjob_fetched");

        let loaded = store.read("my-asset", "exec-001").unwrap();
        assert_eq!(loaded.jobs.len(), 1);
        assert_eq!(loaded.jobs[0].job_id, "bqjob_fetched");
    }

    #[tokio::test]
    async fn fetch_and_save_jobs_does_not_write_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = InspectionStore::new(dir.path());
        let mut inspection = new_inspection("exec-001", "my-asset");

        let conn_info = conn_info_with(MockConn::empty());
        fetch_and_save_jobs(&store, &mut inspection, &conn_info).await;

        assert!(inspection.jobs.is_empty());
        assert!(store.read("my-asset", "exec-001").is_err());
    }
}

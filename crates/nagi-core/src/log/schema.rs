use rusqlite::Connection;

use super::LogError;

pub fn initialize(conn: &Connection) -> Result<(), LogError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS evaluate_logs (
            evaluation_id  TEXT NOT NULL,
            condition_name TEXT NOT NULL,
            asset_name     TEXT NOT NULL,
            condition_type TEXT NOT NULL,
            started_at     TEXT NOT NULL,
            finished_at    TEXT NOT NULL,
            result         TEXT NOT NULL,
            detail         TEXT NOT NULL DEFAULT '',
            date           TEXT NOT NULL,
            PRIMARY KEY (evaluation_id, condition_name)
        );

        CREATE TABLE IF NOT EXISTS sync_logs (
            execution_id TEXT    NOT NULL,
            stage        TEXT    NOT NULL,
            asset_name   TEXT    NOT NULL,
            sync_type    TEXT    NOT NULL,
            started_at   TEXT    NOT NULL,
            finished_at  TEXT    NOT NULL,
            exit_code    INTEGER NOT NULL,
            stdout_path  TEXT    NOT NULL,
            stderr_path  TEXT    NOT NULL,
            date         TEXT    NOT NULL,
            PRIMARY KEY (execution_id, stage)
        );

        CREATE TABLE IF NOT EXISTS sync_evaluations (
            execution_id  TEXT NOT NULL,
            evaluation_id TEXT NOT NULL,
            PRIMARY KEY (execution_id, evaluation_id)
        );",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_creates_tables() {
        let conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();

        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"evaluate_logs".to_string()));
        assert!(tables.contains(&"sync_logs".to_string()));
        assert!(tables.contains(&"sync_evaluations".to_string()));
    }

    #[test]
    fn initialize_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        initialize(&conn).unwrap();
        initialize(&conn).unwrap(); // Should not error.
    }
}

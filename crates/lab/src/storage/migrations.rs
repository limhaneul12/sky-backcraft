use crate::contracts::{ContentHash, LabError};
use rusqlite::{Connection, OptionalExtension, params};

const MIGRATIONS: &[(i64, &str, &str)] = &[
    (
        1,
        "0001_initial",
        include_str!("migrations/0001_initial.sql"),
    ),
    (
        2,
        "0002_jobs_runs",
        include_str!("migrations/0002_jobs_runs.sql"),
    ),
    (
        3,
        "0003_collection_raw_objects",
        include_str!("migrations/0003_collection_raw_objects.sql"),
    ),
    (
        4,
        "0004_fill_liquidity_source",
        include_str!("migrations/0004_fill_liquidity_source.sql"),
    ),
    (
        5,
        "0005_dataset_derivations",
        include_str!("migrations/0005_dataset_derivations.sql"),
    ),
    (
        6,
        "0006_run_scoped_fact_ids",
        include_str!("migrations/0006_run_scoped_fact_ids.sql"),
    ),
    (
        7,
        "0007_policy_registry_history",
        include_str!("migrations/0007_policy_registry_history.sql"),
    ),
    (
        8,
        "0008_hard_delete",
        include_str!("migrations/0008_hard_delete.sql"),
    ),
    (
        9,
        "0009_run_ledger_sealing",
        include_str!("migrations/0009_run_ledger_sealing.sql"),
    ),
    (
        10,
        "0010_research_automation",
        include_str!("migrations/0010_research_automation.sql"),
    ),
    (
        11,
        "0011_portfolio_research",
        include_str!("migrations/0011_portfolio_research.sql"),
    ),
    (
        12,
        "0012_schedule_recovery",
        include_str!("migrations/0012_schedule_recovery.sql"),
    ),
    (
        13,
        "0013_portfolio_projections",
        include_str!("migrations/0013_portfolio_projections.sql"),
    ),
];

pub(super) fn apply(connection: &mut Connection) -> Result<(), LabError> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (\
             version INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, \
             checksum TEXT NOT NULL, applied_at TEXT NOT NULL \
             DEFAULT CURRENT_TIMESTAMP) STRICT;",
        )
        .map_err(sql_error)?;

    let newest = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .map_err(sql_error)?
        .unwrap_or(0);
    let known = MIGRATIONS.last().map_or(0, |migration| migration.0);
    if newest > known {
        return Err(LabError::DataCorrupt(format!(
            "database schema version {newest} is newer than supported version {known}"
        )));
    }

    for &(version, name, sql) in MIGRATIONS {
        let checksum = ContentHash::of_bytes(sql.as_bytes()).to_string();
        let applied = connection
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [version],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        match applied {
            Some(found) if found != checksum => {
                return Err(LabError::DataCorrupt(format!(
                    "migration {version} checksum mismatch: expected {checksum}, found {found}"
                )));
            }
            Some(_) => continue,
            None => {}
        }
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction.execute_batch(sql).map_err(sql_error)?;
        transaction
            .execute(
                "INSERT INTO schema_migrations(version, name, checksum) VALUES (?1, ?2, ?3)",
                params![version, name, checksum],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
    }
    let violation = connection
        .query_row(
            "SELECT \"table\",rowid,parent,fkid FROM pragma_foreign_key_check LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    if let Some((table, rowid, parent, key)) = violation {
        return Err(LabError::DataCorrupt(format!(
            "foreign key check failed: table={table} rowid={rowid} parent={parent} key={key}"
        )));
    }
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn sql_error(error: rusqlite::Error) -> LabError {
    LabError::Internal(format!("sqlite migration: {error}"))
}

use super::MIGRATIONS;
use crate::contracts::ContentHash;
use crate::storage::Store;
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-decision-deferred-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("create migration fixture root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

const LEGACY_EVENT_KINDS: &[&str] = &[
    "STARTED",
    "PAUSED",
    "RESUMED",
    "STOPPED",
    "CANDLE_OBSERVED",
    "SIGNAL_OBSERVED",
    "INTENT_REJECTED",
    "ORDER_ADMITTED",
    "ORDER_SEND_CLAIMED",
    "ORDER_ACKNOWLEDGED",
    "ORDER_CANCEL_REQUESTED",
    "ORDER_RECONCILED",
    "FILL_RECORDED",
    "NEEDS_RECONCILIATION",
    "RECOVERY_BLOCKED",
    "BASELINE_RECORDED",
    "BASELINE_BLOCKED",
];

type EventRow = (i64, String, String, String);
type ForeignKeyRow = (i64, i64, String, String, String, String, String, String);
type EventSchemaObject = (String, String, String);
type IndexColumn = (i64, i64, Option<String>);

#[derive(Debug, PartialEq, Eq)]
struct IndexRow {
    name: String,
    unique: i64,
    origin: String,
    partial: i64,
    columns: Vec<IndexColumn>,
    sql: Option<String>,
}

struct LegacyEventSnapshot {
    rows: Vec<EventRow>,
    foreign_keys: Vec<ForeignKeyRow>,
    indexes: Vec<IndexRow>,
    schema_objects: Vec<EventSchemaObject>,
    checksums: Vec<(i64, String)>,
}

fn legacy_schema_25(path: &Path) -> Connection {
    let mut connection = Connection::open(path).expect("open schema 25 fixture");
    connection
        .execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE schema_migrations (
                 version INTEGER PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 checksum TEXT NOT NULL,
                 applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             ) STRICT;",
        )
        .expect("create migration ledger with foreign keys enabled");
    for &(version, name, sql) in MIGRATIONS.iter().take(25) {
        let checksum = ContentHash::of_bytes(sql.as_bytes()).to_string();
        let transaction = connection.transaction().expect("begin legacy migration");
        transaction
            .execute_batch(sql)
            .expect("apply historical migration");
        transaction
            .execute(
                "INSERT INTO schema_migrations(version,name,checksum) VALUES (?1,?2,?3)",
                params![version, name, checksum],
            )
            .expect("record historical migration");
        transaction.commit().expect("commit historical migration");
    }
    connection
}

fn event_rows(connection: &Connection) -> Vec<EventRow> {
    let mut statement = connection
        .prepare(
            "SELECT sequence,kind,at_utc,detail_json FROM automation_session_events \
             WHERE session_id='session-old' ORDER BY sequence",
        )
        .expect("prepare event readback");
    statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .expect("read event rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect event rows")
}

fn event_foreign_keys(connection: &Connection) -> Vec<ForeignKeyRow> {
    let mut statement = connection
        .prepare("PRAGMA foreign_key_list('automation_session_events')")
        .expect("prepare event foreign-key readback");
    statement
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        })
        .expect("read event foreign keys")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect event foreign keys")
}

fn event_indexes(connection: &Connection) -> Vec<IndexRow> {
    let metadata = {
        let mut statement = connection
            .prepare("PRAGMA index_list('automation_session_events')")
            .expect("prepare event index readback");
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .expect("read event indexes")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect event indexes")
    };
    metadata
        .into_iter()
        .map(|(name, unique, origin, partial)| {
            let columns = {
                let mut statement = connection
                    .prepare("SELECT seqno,cid,name FROM pragma_index_info(?1) ORDER BY seqno")
                    .expect("prepare event index columns");
                statement
                    .query_map([&name], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .expect("read event index columns")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("collect event index columns")
            };
            let sql = connection
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?1",
                    [&name],
                    |row| row.get(0),
                )
                .expect("read event index SQL");
            IndexRow {
                name,
                unique,
                origin,
                partial,
                columns,
                sql,
            }
        })
        .collect()
}

fn event_schema_objects(connection: &Connection) -> Vec<EventSchemaObject> {
    let mut statement = connection
        .prepare(
            "SELECT type,name,sql FROM sqlite_schema \
             WHERE tbl_name='automation_session_events' AND type IN ('index','trigger') \
             AND sql IS NOT NULL ORDER BY type,name",
        )
        .expect("prepare event index and trigger readback");
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .expect("read explicit event indexes and triggers")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect explicit event indexes and triggers")
}

fn deferred_lookup_uses_partial_index(connection: &Connection) -> bool {
    let mut statement = connection
        .prepare(
            "EXPLAIN QUERY PLAN SELECT detail_json FROM automation_session_events \
             INDEXED BY automation_events_deferred_lookup \
             WHERE session_id=?1 AND kind='DECISION_DEFERRED' \
             ORDER BY sequence DESC LIMIT 1",
        )
        .expect("prepare deferred-fact query plan");
    statement
        .query_map(["session-old"], |row| row.get::<_, String>(3))
        .expect("read deferred-fact query plan")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect deferred-fact query plan")
        .iter()
        .any(|detail| detail.contains("automation_events_deferred_lookup"))
}

fn migration_checksums_through_25(connection: &Connection) -> Vec<(i64, String)> {
    let mut statement = connection
        .prepare(
            "SELECT version,checksum FROM schema_migrations \
             WHERE version BETWEEN 1 AND 25 ORDER BY version",
        )
        .expect("prepare historical checksum readback");
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("read historical checksums")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect historical checksums")
}

fn maximum_migration_version(connection: &Connection) -> Option<i64> {
    connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .expect("read maximum migration version")
}

fn create_schema_25_with_legacy_events(path: &Path) -> LegacyEventSnapshot {
    let connection = legacy_schema_25(path);
    connection
        .execute(
            "INSERT INTO automation_sessions(\
                 session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                 frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
             ) VALUES ('session-old','request-old','mock:upbit-default','MOCK','STOPPED',1,'{}','{}',\
                       'KRW-BTC','h1','{}','{}','2026-10-23T00:00:00Z','2026-10-23T00:00:00Z')",
            [],
        )
        .expect("insert prior automation session");
    for (index, kind) in LEGACY_EVENT_KINDS.iter().enumerate() {
        let sequence = i64::try_from(index + 1).expect("fixture sequence fits SQLite");
        let at_utc = format!("2026-10-23T00:00:{sequence:02}Z");
        let detail_json = format!("{{\"legacy_kind\":\"{kind}\"}}");
        connection
            .execute(
                "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
                 VALUES ('session-old',?1,?2,?3,?4)",
                params![sequence, kind, at_utc, detail_json],
            )
            .expect("insert prior event kind");
    }
    assert!(connection
        .execute(
            "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
             VALUES ('session-old',18,'DECISION_DEFERRED','2026-10-23T00:00:18Z','{}')",
            [],
        )
        .is_err());
    LegacyEventSnapshot {
        rows: event_rows(&connection),
        foreign_keys: event_foreign_keys(&connection),
        indexes: event_indexes(&connection),
        schema_objects: event_schema_objects(&connection),
        checksums: migration_checksums_through_25(&connection),
    }
}

fn assert_legacy_event_history(
    connection: &Connection,
    snapshot: &LegacyEventSnapshot,
    expected_rows: &[EventRow],
) {
    assert_eq!(event_rows(connection), expected_rows);
    assert_eq!(event_foreign_keys(connection), snapshot.foreign_keys);
    let indexes = event_indexes(connection);
    for previous in &snapshot.indexes {
        let preserved = indexes
            .iter()
            .find(|index| index.name == previous.name)
            .expect("prior event index is preserved");
        assert_eq!(preserved, previous);
    }
    assert_eq!(indexes.len(), snapshot.indexes.len() + 1);
    let deferred_index = indexes
        .iter()
        .find(|index| index.name == "automation_events_deferred_lookup")
        .expect("deferred-fact index metadata exists");
    assert_eq!(deferred_index.unique, 0);
    assert_eq!(deferred_index.origin, "c");
    assert_eq!(deferred_index.partial, 1);
    assert_eq!(
        deferred_index.columns,
        vec![
            (0, 0, Some("session_id".into())),
            (1, 1, Some("sequence".into()))
        ]
    );
    assert!(deferred_index.sql.as_deref().is_some_and(|sql| {
        sql.to_ascii_uppercase()
            .contains("WHERE KIND = 'DECISION_DEFERRED'")
    }));
    let schema_objects = event_schema_objects(connection);
    for previous in &snapshot.schema_objects {
        assert!(
            schema_objects.contains(previous),
            "prior event object is preserved"
        );
    }
    assert_eq!(schema_objects.len(), snapshot.schema_objects.len() + 1);
    let deferred_index = schema_objects
        .iter()
        .find(|(_, name, _)| name == "automation_events_deferred_lookup")
        .expect("deferred-fact index exists");
    assert!(
        deferred_index
            .2
            .to_ascii_uppercase()
            .contains("WHERE KIND = 'DECISION_DEFERRED'")
    );
    assert!(deferred_lookup_uses_partial_index(connection));
    assert_eq!(
        migration_checksums_through_25(connection),
        snapshot.checksums
    );
}

#[test]
fn decision_deferred_migration_preserves_schema_25_history_across_restart_and_backup() {
    let root = TempRoot::new();
    let database_path = root.0.join("lab.sqlite");
    let snapshot = create_schema_25_with_legacy_events(&database_path);

    let upgraded = Store::open(&root.0).expect("upgrade schema 25 database");
    assert_eq!(maximum_migration_version(&upgraded.connection), Some(26));
    assert_legacy_event_history(&upgraded.connection, &snapshot, &snapshot.rows);
    let migration_checksum: String = upgraded
        .connection
        .query_row(
            "SELECT checksum FROM schema_migrations WHERE version=26",
            [],
            |row| row.get(0),
        )
        .expect("read version 26 checksum");
    assert_eq!(
        migration_checksum,
        ContentHash::of_bytes(
            include_str!("0026_decision_deferred_automation_events.sql").as_bytes()
        )
        .as_str()
    );
    let foreign_key_violations: i64 = upgraded
        .connection
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .expect("check migrated event foreign keys");
    assert_eq!(foreign_key_violations, 0);

    upgraded
        .connection
        .execute(
            "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
             VALUES ('session-old',18,'DECISION_DEFERRED','2026-10-23T00:00:18Z','{}')",
            [],
        )
        .expect("insert new event kind after migration");
    let mut expected_rows = snapshot.rows.clone();
    expected_rows.push((
        18,
        "DECISION_DEFERRED".into(),
        "2026-10-23T00:00:18Z".into(),
        "{}".into(),
    ));
    drop(upgraded);

    let restarted = Store::open(&root.0).expect("restart after schema upgrade");
    assert_eq!(maximum_migration_version(&restarted.connection), Some(26));
    assert_legacy_event_history(&restarted.connection, &snapshot, &expected_rows);
    let backup_path = root.0.join("backup");
    restarted
        .backup(&backup_path)
        .expect("backup upgraded database");
    drop(restarted);

    let restored_root = root.0.join("restored");
    let restored = Store::restore(&backup_path, &restored_root).expect("restore upgraded backup");
    assert_eq!(maximum_migration_version(&restored.connection), Some(26));
    assert_legacy_event_history(&restored.connection, &snapshot, &expected_rows);
    drop(restored);

    let reopened_restore = Store::open(&restored_root).expect("reopen restored database");
    assert_eq!(
        maximum_migration_version(&reopened_restore.connection),
        Some(26)
    );
    assert_legacy_event_history(&reopened_restore.connection, &snapshot, &expected_rows);
}

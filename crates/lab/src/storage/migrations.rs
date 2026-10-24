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
    (
        14,
        "0014_trading_control",
        include_str!("migrations/0014_trading_control.sql"),
    ),
    (
        15,
        "0015_paper_sessions",
        include_str!("migrations/0015_paper_sessions.sql"),
    ),
    (
        16,
        "0016_strategy_automation",
        include_str!("migrations/0016_strategy_automation.sql"),
    ),
    (
        17,
        "0017_nullable_market_buy_remaining_volume",
        include_str!("migrations/0017_nullable_market_buy_remaining_volume.sql"),
    ),
    (
        18,
        "0018_account_day_baseline_contract",
        include_str!("migrations/0018_account_day_baseline_contract.sql"),
    ),
    (
        19,
        "0019_policy_delete_requests",
        include_str!("migrations/0019_policy_delete_requests.sql"),
    ),
    (
        20,
        "0020_live_activation_references",
        include_str!("migrations/0020_live_activation_references.sql"),
    ),
    (
        21,
        "0021_live_order_control",
        include_str!("migrations/0021_live_order_control.sql"),
    ),
    (
        22,
        "0022_private_stream_observations",
        include_str!("migrations/0022_private_stream_observations.sql"),
    ),
    (
        23,
        "0023_oauth_public_clients",
        include_str!("migrations/0023_oauth_public_clients.sql"),
    ),
    (
        24,
        "0024_public_feed_recovery",
        include_str!("migrations/0024_public_feed_recovery.sql"),
    ),
    (
        25,
        "0025_paper_provider_proofs_index",
        include_str!("migrations/0025_paper_provider_proofs_index.sql"),
    ),
    (
        26,
        "0026_decision_deferred_automation_events",
        include_str!("migrations/0026_decision_deferred_automation_events.sql"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_market_buy_upgrade_preserves_existing_order_and_fill() {
        let mut connection = Connection::open_in_memory().expect("open migration fixture");
        connection
            .execute_batch(
                "PRAGMA foreign_keys=ON; \
                 CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, checksum TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP) STRICT;",
            )
            .expect("create migration ledger");
        for &(version, name, sql) in &MIGRATIONS[..16] {
            let checksum = ContentHash::of_bytes(sql.as_bytes()).to_string();
            let transaction = connection.transaction().expect("begin legacy migration");
            transaction
                .execute_batch(sql)
                .expect("apply legacy migration");
            transaction
                .execute(
                    "INSERT INTO schema_migrations(version,name,checksum) VALUES (?1,?2,?3)",
                    params![version, name, checksum],
                )
                .expect("record legacy migration");
            transaction.commit().expect("commit legacy migration");
        }

        connection
            .execute(
                "INSERT INTO automation_sessions(\
                     session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                     frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                 ) VALUES ('session-1','request-1','mock:upbit-default','MOCK','STOPPED',1,'{}','{}',\
                           'KRW-BTC','h1','{}','{}','2026-10-14T00:00:00Z','2026-10-14T00:00:00Z')",
                [],
            )
            .expect("insert preserved session");
        connection
            .execute(
                "INSERT INTO automation_candles(session_id,observation_id,close_time_utc,content_digest,candle_json,checkpoint_json)\
                 VALUES ('session-1','observation-1','2026-10-14T01:00:00Z','digest','{}','{}')",
                [],
            )
            .expect("insert preserved decision candle");
        connection
            .execute(
                "INSERT INTO automation_intents(intent_id,session_id,observation_id,decision_time_utc,action,state,evaluation_json,created_at_utc)\
                 VALUES ('intent-1','session-1','observation-1','2026-10-14T01:00:00Z','BUY','ORDER_RECONCILED','{}','2026-10-14T01:00:00Z')",
                [],
            )
            .expect("insert preserved intent");
        connection
            .execute(
                "INSERT INTO automation_orders(\
                     client_identifier,intent_id,session_id,account_identity,order_request_json,rules_snapshot_json,\
                     request_digest,market,side,state,requested_notional_krw,decision_mark_krw,reserve_quote_krw,\
                     reserve_asset,reserved_asset_volume,reserved_fee_krw,remaining_fee_krw,executed_volume,\
                     remaining_volume,paid_fee_krw,locked_amount,locked_currency,trades_count,created_at_utc,updated_at_utc\
                 ) VALUES ('order-1','intent-1','session-1','mock:upbit-default','{}','{}','digest',\
                           'KRW-BTC','BID','FILLED','9000','1000','9004.5','BTC','0','4.5','0','9',\
                           '0','4.5','0','KRW',1,'2026-10-14T01:00:00Z','2026-10-14T01:00:00Z')",
                [],
            )
            .expect("insert legacy market-buy order");
        connection
            .execute(
                "INSERT INTO automation_fills(trade_uuid,client_identifier,session_id,market,side,price_krw,volume,funds_krw,trade_time_utc,source_digest,created_at_utc)\
                 VALUES ('trade-1','order-1','session-1','KRW-BTC','BID','1000','9','9000',\
                         '2026-10-14T01:00:00Z','fill-digest','2026-10-14T01:00:00Z')",
                [],
            )
            .expect("insert preserved fill");

        apply(&mut connection).expect("apply nullable-volume upgrade");
        let (nullable, remaining): (i64, String) = connection
            .query_row(
                "SELECT (SELECT \"notnull\" FROM pragma_table_info('automation_orders') WHERE name='remaining_volume'),\
                        remaining_volume FROM automation_orders WHERE client_identifier='order-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read upgraded order");
        assert_eq!(nullable, 0);
        assert_eq!(remaining, "0");
        let fill_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM automation_fills", [], |row| {
                row.get(0)
            })
            .expect("count preserved fills");
        assert_eq!(fill_count, 1);

        connection
            .execute(
                "INSERT INTO automation_session_requests(
                     request_id,session_id,account_identity,action,payload_digest,request_json,state,created_at_utc
                 ) VALUES ('req-baseline-1','session-1','mock:upbit-default','RECORD_BASELINE','digest','{}','APPLIED','2026-10-14T00:00:00Z')",
                [],
            )
            .expect("insert RECORD_BASELINE request after 0018");
        connection
            .execute(
                "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json)
                 VALUES ('session-1',1,'BASELINE_RECORDED','2026-10-14T00:00:00Z','{}'),
                        ('session-1',2,'BASELINE_BLOCKED','2026-10-14T00:00:01Z','{}')",
                [],
            )
            .expect("insert baseline events after 0018");
    }
}

#[cfg(test)]
#[path = "migrations/decision_deferred_tests.rs"]
mod decision_deferred_tests;

use super::*;
use crate::contracts::{
    CandleInterval, CandleObservation, MarketDataOrigin, MarketId, ObservationId, PriceKrw,
    UtcTimestamp, observation_digest,
};
use crate::market_data::decode_candles;
use crate::storage::RawObjectInput;
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let sequence = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-provider-absence-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("create isolated recovery store");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

fn time(at: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(at).expect("fixture UTC instant")
}

fn candle_json(opens: &[chrono::DateTime<chrono::Utc>]) -> Vec<u8> {
    let rows = opens
        .iter()
        .rev()
        .map(|open| {
            serde_json::json!({
                "market": "KRW-BTC",
                "candle_date_time_utc": open.format("%Y-%m-%dT%H:%M:%S").to_string(),
                "opening_price": 100,
                "high_price": 101,
                "low_price": 99,
                "trade_price": 100,
                "candle_acc_trade_volume": 1,
                "candle_acc_trade_price": 100,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&rows).expect("encode valid Upbit candle page")
}

fn publish_page(
    store: &mut Store,
    body: Vec<u8>,
    fetched_at: UtcTimestamp,
) -> (crate::contracts::RawObjectRef, Vec<CandleObservation>) {
    publish_page_to(store, body, fetched_at, None)
}

fn publish_page_to(
    store: &mut Store,
    body: Vec<u8>,
    fetched_at: UtcTimestamp,
    requested_to: Option<UtcTimestamp>,
) -> (crate::contracts::RawObjectRef, Vec<CandleObservation>) {
    let cursor = requested_to.map_or_else(String::new, |to| format!("&to={}", to.to_rfc3339()));
    let published = store
        .publish_raw(RawObjectInput {
            source_url: format!(
                "https://api.upbit.com/v1/candles/minutes/5?market=KRW-BTC&count=200{cursor}"
            ),
            fetched_at,
            persisted_at: fetched_at,
            http_status: 200,
            remaining_req: Some("group=candle; sec=9".into()),
            origin: MarketDataOrigin::ExchangeObserved,
            body,
        })
        .expect("publish original REST page");
    store
        .catalog_raw(&published.object)
        .expect("catalog original REST page");
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let candles = decode_candles(&published.body, &market, CandleInterval::M5, fetched_at)
        .expect("decode the same raw page used by production");
    let observations = candles
        .into_iter()
        .filter(|candle| candle.completed)
        .rev()
        .map(|candle| {
            let content_digest = observation_digest(&candle).expect("observation digest");
            CandleObservation {
                id: ObservationId::from_seed(content_digest.as_str()),
                candle,
                content_digest,
                raw_object_ids: vec![published.object.id.clone()],
                constituent_ids: Vec::new(),
            }
        })
        .collect();
    (published.object, observations)
}

fn candle_opens_from(start: &str, count: usize) -> Vec<chrono::DateTime<chrono::Utc>> {
    let start = chrono::DateTime::parse_from_rfc3339(start)
        .expect("fixture candle start")
        .to_utc();
    (0..count)
        .map(|index| {
            start + chrono::Duration::minutes(5 * i64::try_from(index).expect("small page"))
        })
        .collect()
}

#[test]
fn exact_provider_absence_needs_two_verified_independent_reads_and_stays_durable() {
    let root = TempRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated recovery store");
    let interval = CandleInterval::M5;
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let pre_open = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .expect("fixture pre anchor")
        .to_utc();
    let post_open = pre_open + chrono::Duration::minutes(20);
    let body = candle_json(&[pre_open, post_open]);
    let first_at = time("2025-01-01T01:00:02.123456Z");
    let second_at = time("2025-01-01T01:00:03.123456Z");
    let (first_raw, first_rows) = publish_page(&mut store, body.clone(), first_at);
    let (second_raw, second_rows) = publish_page(&mut store, body, second_at);

    let first = store
        .observe_provider_absence_snapshot(&first_raw, &market, interval, &first_rows, second_at)
        .expect("persist the first independently observed raw page");
    assert!(
        first.confirmed_spans.is_empty(),
        "one read cannot confirm source absence"
    );
    assert!(first.pending_confirmation);
    assert!(
        store
            .load_provider_absence_spans(
                &market,
                interval,
                time("2025-01-01T00:05:00Z"),
                time("2025-01-01T00:20:00Z"),
            )
            .expect("query confirmed spans only")
            .is_empty()
    );

    let confirmed = store
        .observe_provider_absence_snapshot(&second_raw, &market, interval, &second_rows, second_at)
        .expect("confirm only the unchanged exact gap");
    assert_eq!(confirmed.confirmed_spans.len(), 1);
    assert!(!confirmed.pending_confirmation);
    let confirmed_spans = confirmed.confirmed_spans;
    assert_eq!(confirmed_spans[0].start_utc, time("2025-01-01T00:05:00Z"));
    assert_eq!(confirmed_spans[0].end_utc, time("2025-01-01T00:20:00Z"));
    assert_eq!(
        confirmed_spans[0].pre_anchor.content_digest,
        first_rows[0].content_digest
    );
    assert_eq!(
        confirmed_spans[0].post_anchor.content_digest,
        first_rows[1].content_digest
    );
    assert_eq!(
        confirmed_spans[0].snapshots[0].fetched_at_utc,
        first_raw.fetched_at
    );
    assert_eq!(confirmed_spans[0].snapshots[0].raw_object_id, first_raw.id);
    assert_eq!(confirmed_spans[0].snapshots[1].raw_object_id, second_raw.id);

    drop(store);
    let reopened = Store::open(&root.0).expect("reopen same durable store");
    let readback = reopened
        .load_provider_absence_spans(
            &market,
            interval,
            time("2025-01-01T00:00:00Z"),
            time("2025-01-01T00:25:00Z"),
        )
        .expect("read proof after restart");
    assert_eq!(readback, confirmed_spans);
}

#[test]
fn late_inner_candle_conflicts_with_a_previously_confirmed_absence_span() {
    let root = TempRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated recovery store");
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let interval = CandleInterval::M5;
    let pre_open = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .expect("fixture pre anchor")
        .to_utc();
    let post_open = pre_open + chrono::Duration::minutes(20);
    let first_body = candle_json(&[pre_open, post_open]);
    for fetched_at in ["2025-01-01T01:00:02Z", "2025-01-01T01:00:03Z"] {
        let (raw, rows) = publish_page(&mut store, first_body.clone(), time(fetched_at));
        store
            .observe_provider_absence_snapshot(
                &raw,
                &market,
                interval,
                &rows,
                time("2025-01-01T01:00:03Z"),
            )
            .expect("record source page");
    }

    let late_open = pre_open + chrono::Duration::minutes(10);
    let (late_raw, late_rows) = publish_page(
        &mut store,
        candle_json(&[pre_open, late_open, post_open]),
        time("2025-01-01T01:00:04Z"),
    );
    assert!(matches!(
        store.observe_provider_absence_snapshot(
            &late_raw,
            &market,
            interval,
            &late_rows,
            time("2025-01-01T01:00:04Z"),
        ),
        Err(LabError::Conflict(_))
    ));
    assert!(matches!(
        store.load_provider_absence_spans(
            &market,
            interval,
            time("2025-01-01T00:05:00Z"),
            time("2025-01-01T00:20:00Z"),
        ),
        Err(LabError::Conflict(_))
    ));
}

#[test]
fn late_inner_candle_before_confirmation_cannot_confirm_the_old_pending_gap() {
    let root = TempRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated recovery store");
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let interval = CandleInterval::M5;
    let pre_open = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .expect("fixture pre anchor")
        .to_utc();
    let post_open = pre_open + chrono::Duration::minutes(20);
    let (first_raw, first_rows) = publish_page(
        &mut store,
        candle_json(&[pre_open, post_open]),
        time("2025-01-01T01:00:02Z"),
    );
    assert!(
        store
            .observe_provider_absence_snapshot(
                &first_raw,
                &market,
                interval,
                &first_rows,
                time("2025-01-01T01:00:02Z"),
            )
            .expect("store pending first read")
            .confirmed_spans
            .is_empty()
    );

    let late_open = pre_open + chrono::Duration::minutes(10);
    let (second_raw, second_rows) = publish_page(
        &mut store,
        candle_json(&[pre_open, late_open, post_open]),
        time("2025-01-01T01:00:03Z"),
    );
    assert!(matches!(
        store.observe_provider_absence_snapshot(
            &second_raw,
            &market,
            interval,
            &second_rows,
            time("2025-01-01T01:00:03Z"),
        ),
        Err(LabError::Conflict(_))
    ));
    assert!(
        store
            .load_provider_absence_spans(
                &market,
                interval,
                time("2025-01-01T00:05:00Z"),
                time("2025-01-01T00:20:00Z"),
            )
            .expect("no confirmed proof after a changed second read")
            .is_empty()
    );
}

#[test]
fn observation_economics_must_decode_from_the_same_verified_raw_page() {
    let root = TempRoot::new();
    let mut store = Store::open(&root.0).expect("open isolated recovery store");
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let interval = CandleInterval::M5;
    let pre_open = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .expect("fixture pre anchor")
        .to_utc();
    let post_open = pre_open + chrono::Duration::minutes(20);
    let (raw, mut rows) = publish_page(
        &mut store,
        candle_json(&[pre_open, post_open]),
        time("2025-01-01T01:00:02Z"),
    );

    rows[0].candle.close = PriceKrw::new(Decimal::from(999)).expect("fixture forged price");
    rows[0].content_digest = observation_digest(&rows[0].candle).expect("forged digest");
    rows[0].id = ObservationId::from_seed(rows[0].content_digest.as_str());
    assert!(matches!(
        store.observe_provider_absence_snapshot(
            &raw,
            &market,
            interval,
            &rows,
            time("2025-01-01T01:00:02Z"),
        ),
        Err(LabError::DataCorrupt(_))
    ));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one deterministic three-page journey proves restart, page overlap, and the hard historical boundary"
)]
fn backwards_cursor_survives_restart_and_reaches_an_accepted_boundary_older_than_latest_page() {
    let root = TempRoot::new();
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let interval = CandleInterval::M5;
    let accepted_open = time("2024-12-31T00:00:00Z");
    let accepted_anchor = ProviderAbsenceAnchor {
        observation_id: ObservationId::from_seed("canonical-consumer-anchor"),
        open_time_utc: accepted_open,
        content_digest: ContentHash::of_bytes(b"canonical consumer economics"),
    };
    let first_at = time("2025-01-02T00:00:01Z");
    let first_body = candle_json(&candle_opens_from("2025-01-01T00:00:00Z", 200));
    let first_next_to = time("2025-01-01T00:05:00Z");
    let started_at;
    let prior_source_tip;
    {
        let mut store = Store::open(&root.0).expect("open isolated recovery store");
        let (raw, rows) = publish_page_to(&mut store, first_body, first_at, None);
        store
            .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
                raw: raw.clone(),
                market: market.clone(),
                interval,
                requested_to: None,
                seed_anchor: None,
                target_count: 199,
                observations: rows.clone(),
                pending_absence_confirmation: false,
                as_of_utc: first_at,
            })
            .expect("seed prior completed source epoch");
        let tip = rows.last().expect("prior confirmed actual source tip");
        prior_source_tip = ProviderAbsenceAnchor {
            observation_id: tip.id.clone(),
            open_time_utc: tip.candle.open_time_utc,
            content_digest: tip.content_digest.clone(),
        };
        store
            .confirm_provider_feed_recovery_window(
                &market,
                interval,
                std::slice::from_ref(tip),
                first_at,
            )
            .expect("persist prior accepted watermark before old consumer backfill");
        let cursor = store
            .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
                raw,
                market: market.clone(),
                interval,
                requested_to: None,
                seed_anchor: Some(accepted_anchor.clone()),
                target_count: 199,
                observations: rows,
                pending_absence_confirmation: false,
                as_of_utc: time("2025-01-02T00:00:02Z"),
            })
            .expect("persist first bounded page");
        assert_eq!(cursor.progress.pages_scanned, 1);
        assert_eq!(cursor.progress.rows_scanned, 200);
        assert_eq!(cursor.progress.next_page_to_utc, Some(first_next_to));
        assert!(!cursor.is_complete());
        assert_eq!(
            cursor.last_recovered_tip,
            Some(prior_source_tip.clone()),
            "starting an incomplete consumer backfill must retain the durable source watermark"
        );
        started_at = cursor.started_at_utc;
    }

    // Reopening the owner recovers the exact cursor and raw-page chain.
    let mut store = Store::open(&root.0).expect("reopen recovery store");
    let cursor = store
        .provider_feed_recovery_cursor(&market, interval)
        .expect("read restarted cursor")
        .expect("durable cursor");
    assert_eq!(cursor.started_at_utc, started_at);
    assert_eq!(
        cursor.last_recovered_tip,
        Some(prior_source_tip.clone()),
        "owner reopen restores the previous source watermark during backfill"
    );
    assert_eq!(
        cursor.progress.accepted_anchor,
        Some(accepted_anchor.clone())
    );
    assert_eq!(cursor.progress.next_page_to_utc, Some(first_next_to));
    assert_eq!(
        store
            .provider_feed_recovery_observations(&market, interval, started_at)
            .expect("read exact first raw page after restart")
            .len(),
        200
    );

    let second_to = cursor
        .progress
        .next_page_to_utc
        .expect("second page cursor");
    let (second_raw, second_rows) = publish_page_to(
        &mut store,
        candle_json(&candle_opens_from("2024-12-31T07:25:00Z", 200)),
        time("2025-01-02T00:00:03Z"),
        Some(second_to),
    );
    let second_cursor = store
        .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
            raw: second_raw,
            market: market.clone(),
            interval,
            requested_to: Some(second_to),
            seed_anchor: Some(accepted_anchor.clone()),
            target_count: 199,
            observations: second_rows,
            pending_absence_confirmation: false,
            as_of_utc: time("2025-01-02T00:00:04Z"),
        })
        .expect("persist second backwards page with one-row overlap");
    let third_to = second_cursor
        .progress
        .next_page_to_utc
        .expect("third page cursor");
    assert_eq!(
        second_cursor.last_recovered_tip,
        Some(prior_source_tip.clone())
    );
    assert_eq!(third_to, time("2024-12-31T07:30:00Z"));

    let (third_raw, third_rows) = publish_page_to(
        &mut store,
        candle_json(&candle_opens_from("2024-12-30T14:50:00Z", 200)),
        time("2025-01-02T00:00:05Z"),
        Some(third_to),
    );
    let completed = store
        .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
            raw: third_raw,
            market: market.clone(),
            interval,
            requested_to: Some(third_to),
            seed_anchor: Some(accepted_anchor),
            target_count: 199,
            observations: third_rows,
            pending_absence_confirmation: false,
            as_of_utc: time("2025-01-02T00:00:06Z"),
        })
        .expect("reach accepted open despite changed provider identity");
    assert!(completed.is_complete());
    assert_eq!(completed.last_recovered_tip, Some(prior_source_tip));
    assert_eq!(completed.progress.pages_scanned, 3);
    assert_eq!(completed.progress.rows_scanned, 600);
    let recovered = store
        .provider_feed_recovery_observations(&market, interval, started_at)
        .expect("revalidate all bounded pages against exact raw IDs");
    assert_eq!(
        recovered.len(),
        598,
        "each page-edge overlap is deduplicated"
    );
    assert!(
        recovered
            .iter()
            .any(|row| row.candle.open_time_utc == accepted_open)
    );
    assert!(
        recovered
            .iter()
            .any(|row| row.candle.open_time_utc == time("2025-01-01T00:00:00Z"))
    );
}

#[test]
fn confirmed_source_tip_is_durable_without_an_active_paper_consumer() {
    let root = TempRoot::new();
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let interval = CandleInterval::M5;
    let fetched_at = time("2025-01-01T00:10:01Z");
    let (tip_anchor, started_at);
    {
        let mut store = Store::open(&root.0).expect("open isolated recovery store");
        let (raw, rows) = publish_page(
            &mut store,
            candle_json(&candle_opens_from("2025-01-01T00:00:00Z", 1)),
            fetched_at,
        );
        let cursor = store
            .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
                raw,
                market: market.clone(),
                interval,
                requested_to: None,
                seed_anchor: None,
                target_count: 1,
                observations: rows.clone(),
                pending_absence_confirmation: false,
                as_of_utc: time("2025-01-01T00:10:02Z"),
            })
            .expect("complete one-page source bootstrap");
        assert!(cursor.is_complete());
        let tip = rows.last().expect("one observed candle");
        store
            .confirm_provider_feed_recovery_window(
                &market,
                interval,
                &rows,
                time("2025-01-01T00:10:03Z"),
            )
            .expect("atomically persist source tip and recovery watermark");
        tip_anchor = ProviderAbsenceAnchor {
            observation_id: tip.id.clone(),
            open_time_utc: tip.candle.open_time_utc,
            content_digest: tip.content_digest.clone(),
        };
        started_at = cursor.started_at_utc;
    }

    let store = Store::open(&root.0).expect("reopen source recovery store");
    let cursor = store
        .provider_feed_recovery_cursor(&market, interval)
        .expect("read recovered source cursor")
        .expect("persisted source cursor");
    assert_eq!(cursor.last_recovered_tip, Some(tip_anchor.clone()));
    assert_eq!(cursor.started_at_utc, started_at);
    assert_eq!(
        store
            .provider_feed_accepted_observation(&market, interval, &tip_anchor)
            .expect("load exact immutable source-owned tip")
            .id,
        tip_anchor.observation_id
    );
    let stored_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM candle_observations WHERE id=?1 AND content_digest=?2",
            rusqlite::params![
                tip_anchor.observation_id.as_str(),
                tip_anchor.content_digest.as_str(),
            ],
            |row| row.get(0),
        )
        .expect("count source-owned durable row");
    assert_eq!(stored_count, 1);
}

#[test]
#[allow(clippy::too_many_lines)]
fn legacy_same_tip_recovery_repairs_the_full_admitted_window_after_restart() {
    let root = TempRoot::new();
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let interval = CandleInterval::M5;
    let first_at = time("2025-01-01T06:00:02Z");
    let second_at = time("2025-01-01T06:00:03Z");
    let (started_at, accepted_anchor, tip_anchor);
    {
        let mut store = Store::open(&root.0).expect("open isolated recovery store");
        let opens = [
            time("2025-01-01T00:00:00Z").0,
            time("2025-01-01T05:00:00Z").0,
            time("2025-01-01T05:05:00Z").0,
            time("2025-01-01T05:10:00Z").0,
        ];
        let body = candle_json(&opens);
        let (first_raw, first_rows) = publish_page(&mut store, body.clone(), first_at);
        let (second_raw, rows) = publish_page(&mut store, body, second_at);
        store
            .observe_provider_absence_snapshot(
                &first_raw,
                &market,
                interval,
                &first_rows,
                second_at,
            )
            .expect("persist first absence read");
        let confirmed = store
            .observe_provider_absence_snapshot(&second_raw, &market, interval, &rows, second_at)
            .expect("confirm the exact gap from a second read");
        assert_eq!(confirmed.confirmed_spans.len(), 1);

        let pre = rows.first().expect("pre-gap observed candle");
        let accepted = ProviderAbsenceAnchor {
            observation_id: pre.id.clone(),
            open_time_utc: pre.candle.open_time_utc,
            content_digest: pre.content_digest.clone(),
        };
        let cursor = store
            .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
                raw: second_raw,
                market: market.clone(),
                interval,
                requested_to: None,
                seed_anchor: Some(accepted.clone()),
                target_count: 4,
                observations: rows.clone(),
                pending_absence_confirmation: false,
                as_of_utc: time("2025-01-01T06:00:04Z"),
            })
            .expect("admit the complete source recovery window");
        assert!(cursor.is_complete());
        started_at = cursor.started_at_utc;
        accepted_anchor = accepted;

        // Reproduce the pre-repair durable shape: only the confirmed latest tip
        // made it into canonical observations, while the complete raw page chain
        // and proof remained archived.
        let tip = rows.last().expect("latest observed candle");
        let tip_value = ProviderAbsenceAnchor {
            observation_id: tip.id.clone(),
            open_time_utc: tip.candle.open_time_utc,
            content_digest: tip.content_digest.clone(),
        };
        let transaction = store.connection.transaction().expect("legacy transaction");
        insert_observation(&transaction, tip).expect("persist legacy tip only");
        transaction
            .execute(
                "UPDATE provider_feed_recovery_cursors SET last_recovered_tip_json=?1 \
                 WHERE market=?2 AND interval=?3",
                rusqlite::params![
                    serde_json::to_string(&tip_value).expect("tip JSON"),
                    market.code(),
                    enum_text(&interval).expect("interval text"),
                ],
            )
            .expect("persist prior tip watermark");
        transaction.commit().expect("commit legacy source state");
        tip_anchor = tip_value;
    }

    let mut reopened = Store::open(&root.0).expect("cold reopen source owner");
    let repaired = reopened
        .repair_provider_feed_recovery_window(
            &market,
            interval,
            started_at,
            time("2025-01-01T06:00:05Z"),
        )
        .expect("repair the durable source window")
        .expect("missing canonical rows require one repair");
    assert_eq!(repaired.len(), 4);
    assert_eq!(repaired[0].id, accepted_anchor.observation_id);

    let cursor = reopened
        .provider_feed_recovery_cursor(&market, interval)
        .expect("read repaired cursor")
        .expect("complete cursor remains present");
    assert_eq!(cursor.last_recovered_tip, Some(tip_anchor.clone()));
    let stored_count: i64 = reopened
        .connection
        .query_row(
            "SELECT COUNT(*) FROM candle_observations WHERE market=?1 AND interval=?2",
            rusqlite::params![market.code(), enum_text(&interval).expect("interval text")],
            |row| row.get(0),
        )
        .expect("count canonical source rows");
    assert_eq!(
        stored_count, 4,
        "only provider-observed candles are published"
    );
    assert!(
        reopened
            .repair_provider_feed_recovery_window(
                &market,
                interval,
                started_at,
                time("2025-01-01T06:00:06Z"),
            )
            .expect("verify idempotent cold repair")
            .is_none()
    );
    assert_eq!(
        reopened
            .provider_feed_accepted_observation(&market, interval, &accepted_anchor)
            .expect("read exact canonical accepted anchor")
            .id,
        accepted_anchor.observation_id
    );
}

#[test]
fn canonical_window_survives_more_historical_raw_origins_than_a_recovery_page_budget() {
    let root = TempRoot::new();
    let mut store = Store::open(&root.0).expect("isolated canonical window");
    let market = MarketId::parse_upbit("KRW-BTC").expect("market");
    let interval = CandleInterval::M5;
    let now = time("2025-01-01T06:00:02Z");
    let start = time("2025-01-01T00:00:00Z").0;
    let opens = (0..67)
        .map(|index| start + chrono::Duration::minutes(index * 5))
        .collect::<Vec<_>>();
    let mut canonical = Vec::new();
    for open in &opens {
        let (_, mut rows) = publish_page(&mut store, candle_json(&[*open]), now);
        let row = rows.pop().expect("one actual completed candle");
        let transaction = store.connection.transaction().expect("fixture transaction");
        insert_observation(&transaction, &row).expect("persist admitted canonical candle");
        transaction.commit().expect("commit canonical candle");
        canonical.push(row);
    }
    let (raw, rows) = publish_page(&mut store, candle_json(&opens), now);
    let first = canonical.first().expect("canonical anchor");
    store
        .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
            raw,
            market: market.clone(),
            interval,
            requested_to: None,
            seed_anchor: Some(ProviderAbsenceAnchor {
                observation_id: first.id.clone(),
                open_time_utc: first.candle.open_time_utc,
                content_digest: first.content_digest.clone(),
            }),
            target_count: 67,
            observations: rows,
            pending_absence_confirmation: false,
            as_of_utc: now,
        })
        .expect("complete bounded recovery epoch");
    store
        .confirm_provider_feed_recovery_window(&market, interval, &canonical, now)
        .expect("old admitted candles do not exhaust the new recovery raw-page budget");
    let restored = store
        .canonical_provider_feed_observations(&market, interval, &canonical)
        .expect("read exact canonical history");
    assert_eq!(
        serde_json::to_value(&restored).expect("canonical JSON"),
        serde_json::to_value(&canonical).expect("original JSON")
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn archived_new_tip_waits_for_confirmation_without_repair_publication_or_watermark_loss() {
    let root = TempRoot::new();
    let market = MarketId::parse_upbit("KRW-BTC").expect("market");
    let interval = CandleInterval::M5;
    let first_at = time("2025-01-01T01:00:01Z");
    let old_tip;
    let staged_rows;
    let staged_epoch;
    {
        let mut store = Store::open(&root.0).expect("isolated pending tip owner");
        let (raw, rows) = publish_page(
            &mut store,
            candle_json(&candle_opens_from("2025-01-01T00:00:00Z", 4)),
            first_at,
        );
        let anchor = ProviderAbsenceAnchor {
            observation_id: rows[0].id.clone(),
            open_time_utc: rows[0].candle.open_time_utc,
            content_digest: rows[0].content_digest.clone(),
        };
        store
            .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
                raw,
                market: market.clone(),
                interval,
                requested_to: None,
                seed_anchor: Some(anchor.clone()),
                target_count: 4,
                observations: rows.clone(),
                pending_absence_confirmation: false,
                as_of_utc: first_at,
            })
            .expect("seed completed legacy epoch");
        store
            .confirm_provider_feed_recovery_window(&market, interval, &rows[3..], first_at)
            .expect("persist only the prior independently confirmed watermark");
        old_tip = store
            .provider_feed_recovery_cursor(&market, interval)
            .expect("cursor")
            .expect("prior cursor")
            .last_recovered_tip
            .expect("prior watermark");
        let second_at = time("2025-01-01T01:00:02Z");
        let (raw, rows) = publish_page(
            &mut store,
            candle_json(&candle_opens_from("2025-01-01T00:00:00Z", 5)),
            second_at,
        );
        let cursor = store
            .record_provider_feed_recovery_page(crate::storage::PersistedProviderRecoveryPage {
                raw,
                market: market.clone(),
                interval,
                requested_to: None,
                seed_anchor: Some(anchor),
                target_count: 4,
                observations: rows.clone(),
                pending_absence_confirmation: false,
                as_of_utc: second_at,
            })
            .expect("stage complete coverage while its new tip awaits independent confirmation");
        assert!(cursor.is_complete());
        assert_eq!(cursor.last_recovered_tip, Some(old_tip.clone()));
        staged_epoch = cursor.started_at_utc;
        staged_rows = rows;
    }
    let mut store = Store::open(&root.0).expect("restart with staged newer tip");
    assert!(
        store
            .repair_provider_feed_recovery_window(
                &market,
                interval,
                staged_epoch,
                time("2025-01-01T01:00:03Z")
            )
            .expect("pending tip is not a corrupt archive")
            .is_none()
    );
    assert_eq!(
        store
            .provider_feed_recovery_cursor(&market, interval)
            .expect("cursor")
            .expect("cursor remains")
            .last_recovered_tip,
        Some(old_tip)
    );
    assert_eq!(
        store
            .canonical_provider_feed_observations(&market, interval, &staged_rows)
            .expect("read canonical rows before confirmation")
            .len(),
        1,
        "cold repair cannot publish the unconfirmed new window"
    );
    store
        .confirm_provider_feed_recovery_window(
            &market,
            interval,
            &staged_rows,
            time("2025-01-01T01:00:04Z"),
        )
        .expect("normal confirmation publishes the full verified window");
    assert_eq!(
        store
            .canonical_provider_feed_observations(&market, interval, &staged_rows)
            .expect("read all actually observed rows after confirmation")
            .len(),
        5
    );
    assert_eq!(
        store
            .provider_feed_recovery_cursor(&market, interval)
            .expect("cursor")
            .expect("confirmed cursor")
            .last_recovered_tip
            .expect("new watermark")
            .observation_id,
        staged_rows[4].id
    );
}

#[test]
fn endpoint_revision_restarts_pending_reads_but_preserves_confirmed_absence() {
    for already_confirmed in [false, true] {
        let root = TempRoot::new();
        let mut store = Store::open(&root.0).expect("isolated revision store");
        let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
        let interval = CandleInterval::M5;
        let pre = time("2025-01-01T00:00:00Z").0;
        let post = pre + chrono::Duration::minutes(20);
        let old_body = candle_json(&[pre, post]);
        let first_at = time("2025-01-01T01:00:02Z");
        let (old_raw, old_rows) = publish_page(&mut store, old_body.clone(), first_at);
        store
            .observe_provider_absence_snapshot(&old_raw, &market, interval, &old_rows, first_at)
            .expect("first old read");
        let old_proof = if already_confirmed {
            let at = time("2025-01-01T01:00:03Z");
            let (raw, rows) = publish_page(&mut store, old_body, at);
            Some(
                store
                    .observe_provider_absence_snapshot(&raw, &market, interval, &rows, at)
                    .expect("second old read")
                    .confirmed_spans[0]
                    .clone(),
            )
        } else {
            None
        };
        let mut document: Vec<serde_json::Value> =
            serde_json::from_slice(&candle_json(&[pre, post])).expect("valid raw fixture");
        for row in &mut document {
            row["opening_price"] = serde_json::json!(110);
            row["high_price"] = serde_json::json!(111);
            row["low_price"] = serde_json::json!(109);
            row["trade_price"] = serde_json::json!(110);
            row["candle_acc_trade_price"] = serde_json::json!(110);
        }
        let revised_body = serde_json::to_vec(&document).expect("revised raw fixture");
        let at = time("2025-01-01T01:00:04Z");
        let (new_raw, new_rows) = publish_page(&mut store, revised_body.clone(), at);
        let outcome = store
            .observe_provider_absence_snapshot(&new_raw, &market, interval, &new_rows, at)
            .expect("endpoint economics are not an inner candle contradiction");
        assert_eq!(outcome.pending_confirmation, !already_confirmed);
        let at = time("2025-01-01T01:00:05Z");
        let (second_new_raw, second_new_rows) = publish_page(&mut store, revised_body, at);
        let proof = store
            .observe_provider_absence_snapshot(
                &second_new_raw,
                &market,
                interval,
                &second_new_rows,
                at,
            )
            .expect("stable independent revised read")
            .confirmed_spans[0]
            .clone();
        if let Some(old) = old_proof {
            assert_eq!(proof, old);
        } else {
            assert_eq!(proof.snapshots[0].raw_object_id, new_raw.id);
            assert_eq!(proof.snapshots[1].raw_object_id, second_new_raw.id);
        }
        assert!(store.raw.read_verified(&old_raw).is_ok());
        assert!(store.raw.read_verified(&new_raw).is_ok());
        assert!(store.raw.read_verified(&second_new_raw).is_ok());
        let at = time("2025-01-01T01:00:06Z");
        let (inner_raw, inner_rows) = publish_page(
            &mut store,
            candle_json(&[pre, pre + chrono::Duration::minutes(10), post]),
            at,
        );
        assert!(matches!(
            store.observe_provider_absence_snapshot(&inner_raw, &market, interval, &inner_rows, at),
            Err(LabError::Conflict(_))
        ));
        assert!(matches!(
            store.load_provider_absence_spans(
                &market,
                interval,
                time("2025-01-01T00:05:00Z"),
                time("2025-01-01T00:20:00Z")
            ),
            Err(LabError::Conflict(_))
        ));
    }
}

#[test]
fn submillisecond_first_read_cannot_round_up_the_independence_window() {
    let root = TempRoot::new();
    let mut store = Store::open(&root.0).expect("precision fixture store");
    let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
    let interval = CandleInterval::M5;
    let pre = time("2025-01-01T00:00:00Z").0;
    let body = candle_json(&[pre, pre + chrono::Duration::minutes(20)]);
    let at = time("2025-01-01T01:00:02.123456789Z");
    let (first, rows) = publish_page(&mut store, body.clone(), at);
    assert!(
        store
            .observe_provider_absence_snapshot(&first, &market, interval, &rows, at)
            .expect("first precise read")
            .pending_confirmation
    );
    let at = time("2025-01-01T01:00:03.123456788Z");
    let (too_early, rows) = publish_page(&mut store, body.clone(), at);
    let outcome = store
        .observe_provider_absence_snapshot(&too_early, &market, interval, &rows, at)
        .expect("read one nanosecond before the minimum");
    assert!(outcome.confirmed_spans.is_empty());
    assert!(outcome.pending_confirmation);
    let at = time("2025-01-01T01:00:03.123456789Z");
    let (confirmed, rows) = publish_page(&mut store, body, at);
    let outcome = store
        .observe_provider_absence_snapshot(&confirmed, &market, interval, &rows, at)
        .expect("read at the exact minimum");
    assert_eq!(outcome.confirmed_spans.len(), 1);
    assert_eq!(
        outcome.confirmed_spans[0].snapshots[0].fetched_at_utc,
        first.fetched_at
    );
    assert_eq!(
        outcome.confirmed_spans[0].snapshots[1].raw_object_id,
        confirmed.id
    );
}

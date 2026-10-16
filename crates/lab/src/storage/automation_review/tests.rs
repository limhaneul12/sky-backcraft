use super::*;
use crate::contracts::{
    AssetQuantity, CandleRecord, PolicyDefinition, PolicyId, PolicyOrigin, PolicyRevisionId,
    StrategyKind,
};
use crate::upbit::client::UpbitAccountBalance;
use rusqlite::params;
use rust_decimal::Decimal;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

static ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct ReviewFixture {
    root: std::path::PathBuf,
    store: Store,
    session_id: AutomationSessionId,
}

impl ReviewFixture {
    fn new() -> Result<Self, LabError> {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-review-{}-{}",
            std::process::id(),
            ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root)
            .map_err(|error| LabError::Internal(format!("create review test root: {error}")))?;
        let store = Store::open(&root)?;
        let (_, family, definition) = crate::contracts::builtin_policy_definitions()?
            .into_iter()
            .next()
            .ok_or_else(|| LabError::Internal("built-in policy test seed missing".into()))?;
        let frozen_policy = test_frozen_policy(family, definition)?;
        let session_id = AutomationSessionId::new("review-session")?;
        let created = time("2026-10-14T00:00:00Z")?;
        let risk = AutomationRiskPolicy {
            activation_loss_limit_krw: None,
            max_order_notional_krw: QuoteAmount::new(Decimal::from(50_000))?,
            max_daily_notional_krw: QuoteAmount::new(Decimal::from(500_000))?,
            max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_loss_krw: QuoteAmount::new(Decimal::from(10_000))?,
            max_open_orders: 2,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 3_600,
            max_feed_age_seconds: 3_600,
        };
        store
            .connection
            .execute(
                "INSERT INTO automation_sessions(\
                     session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                     frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                 ) VALUES (?1,'review-start','mock:review-account','MOCK','RUNNING',1,?2,?3,\
                           'KRW-BTC','h1',?4,'{}',?5,?5)",
                params![
                    session_id.as_str(),
                    serde_json::to_string(&frozen_policy.reference).map_err(super::super::json_error)?,
                    serde_json::to_string(&frozen_policy).map_err(super::super::json_error)?,
                    serde_json::to_string(&risk).map_err(super::super::json_error)?,
                    created.to_rfc3339(),
                ],
            )
            .map_err(super::super::sql_error)?;
        Ok(Self {
            root,
            store,
            session_id,
        })
    }

    fn add_decision(
        &mut self,
        sequence: u32,
        action: &str,
        evaluation: &serde_json::Value,
    ) -> Result<AutomationIntentId, LabError> {
        let base = time("2026-10-14T00:00:00Z")?;
        let open_at = UtcTimestamp(base.0 + chrono::Duration::hours(i64::from(sequence)));
        self.add_decision_at(sequence, open_at, action, evaluation)
    }

    fn add_decision_at(
        &mut self,
        sequence: u32,
        open_at: UtcTimestamp,
        action: &str,
        evaluation: &serde_json::Value,
    ) -> Result<AutomationIntentId, LabError> {
        let close_at = UtcTimestamp(open_at.0 + CandleInterval::H1.duration());
        let open_text = open_at.0.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let close_text = close_at
            .0
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let observation = observation_at(
            &open_text,
            &close_text,
            Decimal::from(100 + i64::from(sequence)),
        )?;
        let intent_id = AutomationIntentId::from_seed(&format!("review-decision-{sequence}"));
        let received =
            UtcTimestamp(observation.candle.close_time_utc.0 + chrono::Duration::seconds(1));
        self.store.automation_record_decision(
            &self.session_id,
            &observation,
            "{}",
            &intent_id,
            action,
            evaluation,
            received,
        )?;
        Ok(intent_id)
    }

    fn add_prestart_decision(
        &mut self,
        action: &str,
        marker_key: &str,
        marker_value: &str,
    ) -> Result<AutomationIntentId, LabError> {
        self.store
            .connection
            .execute(
                "UPDATE automation_sessions SET state='NEEDS_RECONCILIATION', \
                 recovery_reason='PROCESS_RESTART_REQUIRES_ACCOUNT_AND_ORDER_READBACK' \
                 WHERE session_id=?1",
                [self.session_id.as_str()],
            )
            .map_err(super::super::sql_error)?;
        let candle = observation_at(
            "2026-01-01T22:00:00Z",
            "2026-01-01T23:00:00Z",
            Decimal::from(90),
        )?;
        let intent_id = AutomationIntentId::from_seed("review-prestart-intent");
        let mut evaluation = serde_json::json!({
            "decision_time": candle.candle.close_time_utc,
            "completed_bars_seen": 10,
        });
        evaluation
            .as_object_mut()
            .ok_or_else(|| LabError::Internal("warmup fixture evaluation is not an object".into()))?
            .insert(marker_key.to_owned(), serde_json::json!(marker_value));
        self.store.automation_record_decision(
            &self.session_id,
            &candle,
            "{}",
            &intent_id,
            action,
            &evaluation,
            time("2026-10-14T00:00:01Z")?,
        )?;
        Ok(intent_id)
    }

    fn add_warmup_candle(&mut self) -> Result<CandleObservation, LabError> {
        let candle = observation_at(
            "2026-10-13T23:00:00Z",
            "2026-10-14T00:00:00Z",
            Decimal::from(90),
        )?;
        assert!(self.store.automation_record_candle(
            &self.session_id,
            &candle,
            "{}",
            time("2026-10-14T00:00:01Z")?,
        )?);
        Ok(candle)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the synthetic partial order fixture seeds mutually linked order, fill, fee and event facts"
    )]
    fn add_partial_buy(
        &mut self,
        intent_id: &AutomationIntentId,
        opening_base: Option<Decimal>,
        opening_mode: &str,
        opening_observed_at: &str,
    ) -> Result<(), LabError> {
        let client_identifier = AutomationOrderIdentifier::from_intent(intent_id);
        let order_time = time("2026-10-14T02:00:02Z")?;
        if let Some(base_balance) = opening_base {
            let observed_at = time(opening_observed_at)?;
            let recorded_at = UtcTimestamp(observed_at.0 + chrono::Duration::seconds(1));
            let balances = vec![UpbitAccountBalance {
                currency: "BTC".into(),
                balance: base_balance,
                locked: Decimal::new(5, 2),
                avg_buy_price: Decimal::from(80),
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            }];
            self.store
                .connection
                .execute(
                    "INSERT INTO automation_account_snapshots(\
                         snapshot_sequence,account_identity,observed_at_utc,source_mode,\
                         balances_json,balances_digest,created_at_utc\
                     ) VALUES (1,'mock:review-account',?1,?2,?3,?4,?5)",
                    params![
                        observed_at.to_rfc3339(),
                        opening_mode,
                        serde_json::to_string(&balances).map_err(super::super::json_error)?,
                        ContentHash::of_value(&balances)?.as_str(),
                        recorded_at.to_rfc3339(),
                    ],
                )
                .map_err(super::super::sql_error)?;
        }
        self.store
            .connection
            .execute(
                "INSERT INTO automation_orders(\
                     client_identifier,intent_id,session_id,account_identity,order_request_json,\
                     rules_snapshot_json,request_digest,market,side,state,requested_notional_krw,\
                     decision_mark_krw,reserve_quote_krw,reserve_asset,reserved_asset_volume,\
                     reserved_fee_krw,remaining_fee_krw,executed_volume,remaining_volume,paid_fee_krw,\
                     locked_amount,locked_currency,trades_count,venue_created_at_utc,created_at_utc,updated_at_utc\
                 ) VALUES (?1,?2,?3,'mock:review-account','{}','{}',?4,'KRW-BTC','BID',\
                           'PARTIALLY_FILLED','1000','101','1001','BTC','0','1','0.8','2','3','0.2',\
                           '800','KRW',1,?5,?5,?6)",
                params![
                    client_identifier.as_str(),
                    intent_id.as_str(),
                    self.session_id.as_str(),
                    ContentHash::of_bytes(b"review-order").as_str(),
                    order_time.to_rfc3339(),
                    time("2026-10-14T02:02:00Z")?.to_rfc3339(),
                ],
            )
            .map_err(super::super::sql_error)?;
        self.store
            .connection
            .execute(
                "UPDATE automation_intents SET state='ORDER_SUBMITTED' WHERE intent_id=?1",
                [intent_id.as_str()],
            )
            .map_err(super::super::sql_error)?;
        self.store
            .connection
            .execute(
                "INSERT INTO automation_fills(\
                     trade_uuid,client_identifier,session_id,market,side,price_krw,volume,funds_krw,\
                     trade_time_utc,source_digest,created_at_utc\
                 ) VALUES ('review-partial-fill',?1,?2,'KRW-BTC','BID','101','2','202','\
                           2026-10-14T02:01:00Z',?3,'2026-10-14T02:02:00Z')",
                params![
                    client_identifier.as_str(),
                    self.session_id.as_str(),
                    ContentHash::of_bytes(b"review-fill-source").as_str(),
                ],
            )
            .map_err(super::super::sql_error)?;
        self.store
            .connection
            .execute(
                "INSERT INTO automation_fee_accruals(\
                     client_identifier,sequence,delta_paid_fee_krw,cumulative_paid_fee_krw,observed_at_utc\
                 ) VALUES (?1,1,'0.2','0.2','2026-10-14T02:02:00Z')",
                [client_identifier.as_str()],
            )
            .map_err(super::super::sql_error)?;
        let next_sequence: i64 = self
            .store
            .connection
            .query_row(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM automation_session_events WHERE session_id=?1",
                [self.session_id.as_str()],
                |row| row.get(0),
            )
            .map_err(super::super::sql_error)?;
        self.store
            .connection
            .execute(
                "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
                 VALUES (?1,?2,'ORDER_ADMITTED','2026-10-14T02:00:02Z',?3)",
                params![
                    self.session_id.as_str(),
                    next_sequence,
                    serde_json::json!({
                        "intent_id": intent_id,
                        "client_identifier": client_identifier,
                        "notional_krw": "1000",
                    })
                    .to_string(),
                ],
            )
            .map_err(super::super::sql_error)?;
        Ok(())
    }

    fn add_account_snapshot(
        &mut self,
        sequence: i64,
        observed_at: &str,
        recorded_at: &str,
        base_balance: Decimal,
        base_locked: Decimal,
    ) -> Result<(), LabError> {
        let balances = vec![UpbitAccountBalance {
            currency: "BTC".into(),
            balance: base_balance,
            locked: base_locked,
            avg_buy_price: Decimal::from(80),
            avg_buy_price_modified: false,
            unit_currency: "KRW".into(),
        }];
        self.store
            .connection
            .execute(
                "INSERT INTO automation_account_snapshots(\
                     snapshot_sequence,account_identity,observed_at_utc,source_mode,\
                     balances_json,balances_digest,created_at_utc\
                 ) VALUES (?1,'mock:review-account',?2,'MOCK',?3,?4,?5)",
                params![
                    sequence,
                    observed_at,
                    serde_json::to_string(&balances).map_err(super::super::json_error)?,
                    ContentHash::of_value(&balances)?.as_str(),
                    recorded_at,
                ],
            )
            .map_err(super::super::sql_error)?;
        Ok(())
    }

    fn close(self) -> Result<(), LabError> {
        self.store.close()?;
        fs::remove_dir_all(self.root)
            .map_err(|error| LabError::Internal(format!("remove review test root: {error}")))
    }
}

fn test_frozen_policy(
    family: StrategyKind,
    definition: PolicyDefinition,
) -> Result<FrozenPolicyRevision, LabError> {
    let definition_digest = ContentHash::of_value(&definition)?;
    Ok(FrozenPolicyRevision {
        reference: PolicyRevisionRef {
            policy_id: PolicyId::new("review-policy")?,
            revision_id: PolicyRevisionId::new("review-revision")?,
            definition_digest,
        },
        revision_number: 1,
        parent_revision_id: None,
        family,
        origin: PolicyOrigin::Builtin,
        definition,
    })
}

fn time(value: &str) -> Result<UtcTimestamp, LabError> {
    UtcTimestamp::parse_rfc3339(value)
}

fn observation_at(
    open_at: &str,
    close_at: &str,
    close_price: Decimal,
) -> Result<CandleObservation, LabError> {
    let open = time(open_at)?;
    let close_time = time(close_at)?;
    let close = PriceKrw::new(close_price)?;
    let candle = CandleRecord {
        market: "KRW-BTC".into(),
        interval: CandleInterval::H1,
        open_time_utc: open,
        close_time_utc: close_time,
        open: PriceKrw::new(close_price - Decimal::ONE)?,
        high: PriceKrw::new(close_price + Decimal::ONE)?,
        low: PriceKrw::new(close_price - Decimal::from(2))?,
        close,
        volume: AssetQuantity::new(Decimal::from(5))?,
        quote_turnover: QuoteAmount::new(close_price * Decimal::from(5))?,
        completed: true,
    };
    let content_digest = observation_digest(&candle)?;
    Ok(CandleObservation {
        id: ObservationId::from_seed(content_digest.as_str()),
        candle,
        content_digest,
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    })
}

fn evaluation(decision_time: &str, extra: &str) -> serde_json::Value {
    serde_json::json!({
        "decision_time_utc": decision_time,
        "reasons": [extra],
        "indicators": {"ema": "101.25", "volatility": "0.02"},
        "policy_trace": {"matched_rule_id": extra},
    })
}

#[test]
fn review_details_preserve_decisions_partial_fills_and_restart_identity() -> Result<(), LabError> {
    let mut fixture = ReviewFixture::new()?;
    let warmup = fixture.add_warmup_candle()?;
    let first = fixture.add_decision(
        0,
        "HOLD",
        &serde_json::json!({
            "recovery_disposition": "WARMUP_HOLD",
            "decision_time_utc": "2026-10-14T01:00:00Z",
            "policy_evaluation": null,
        }),
    )?;
    let second = fixture.add_decision(1, "BUY", &evaluation("2026-10-14T02:00:00Z", "BUY_RULE"))?;
    fixture.add_partial_buy(&second, None, "MOCK", "2026-10-14T01:59:55Z")?;
    let third =
        fixture.add_decision(2, "SELL", &evaluation("2026-10-14T03:00:00Z", "SELL_RULE"))?;
    fixture.store.automation_reject_intent(
        &third,
        "synthetic risk fixture rejection",
        time("2026-10-14T03:00:02Z")?,
    )?;

    let first_page = fixture
        .store
        .automation_review_decisions(&fixture.session_id, 0, 2)?;
    assert_eq!(first_page.total_count, 3);
    assert_eq!(first_page.returned_count, 2);
    assert_eq!(first_page.next_offset, Some(2));
    assert_eq!(first_page.items[0].intent_id, first);
    assert_eq!(first_page.items[0].action, AutomationReviewAction::Hold);
    assert_eq!(first_page.items[1].intent_id, second);

    let detail = fixture
        .store
        .automation_review_detail(&fixture.session_id, &third)?;
    assert_eq!(detail.decision.action, AutomationReviewAction::Sell);
    assert_eq!(detail.decision.evaluation["reasons"][0], "SELL_RULE");
    assert_eq!(detail.context_candles.len(), 4);
    assert_eq!(detail.context_candles[0].id, warmup.id);
    assert_eq!(
        detail.context_candles.last().map(|candle| &candle.id),
        Some(&detail.decision.observation_id)
    );
    assert!(
        detail
            .context_candles
            .iter()
            .all(|candle| candle.candle.close_time_utc <= detail.decision.decision_time_utc)
    );
    assert!(detail.events.iter().any(|event| {
        event.kind == AutomationEventKind::IntentRejected
            && event.detail["intent_id"] == third.as_str()
    }));

    let snapshot = fixture
        .store
        .automation_review_snapshot(&fixture.session_id)?;
    assert_eq!(snapshot.candles.len(), 4);
    assert!(snapshot.candles.iter().any(|candle| candle.id == warmup.id));
    assert_eq!(snapshot.orders.len(), 1);
    assert_eq!(snapshot.fills.len(), 1);
    assert_eq!(snapshot.fee_accruals.len(), 1);
    assert_eq!(
        snapshot.orders[0].order.state,
        crate::contracts::AutomationOrderState::PartiallyFilled
    );
    assert!(snapshot.opening_inventory.is_none());
    assert_eq!(snapshot.fills[0].fill.volume.get(), Decimal::from(2));
    assert_eq!(
        snapshot.fee_accruals[0].cumulative_paid_fee_krw.get(),
        Decimal::new(2, 1)
    );
    let manifest = crate::reporting::automation_review::export_manifest(&snapshot)?;
    assert_eq!(manifest.session_id, fixture.session_id);
    let snapshot_bytes = serde_json::to_vec(&snapshot).map_err(super::super::json_error)?;
    let ReviewFixture {
        root,
        store,
        session_id,
    } = fixture;
    store.close()?;
    let mut reopened = Store::open(&root)?;
    let after_reopen = reopened.automation_review_snapshot(&session_id)?;
    assert_eq!(
        serde_json::to_vec(&after_reopen).map_err(super::super::json_error)?,
        snapshot_bytes
    );
    reopened.close()?;
    fs::remove_dir_all(root)
        .map_err(|error| LabError::Internal(format!("remove review test root: {error}")))?;
    Ok(())
}

#[test]
fn decision_pages_preserve_order_counts_cursors_and_signal_ownership() -> Result<(), LabError> {
    const DECISION_COUNT: u32 = 205;
    const PAGE_LIMIT: u32 = 100;

    let mut fixture = ReviewFixture::new()?;
    let base = time("2026-10-14T00:00:00Z")?;
    let decision_capacity = usize::try_from(DECISION_COUNT)
        .map_err(|_| LabError::Internal("fixture decision count overflow".into()))?;
    let mut expected_rows = Vec::with_capacity(decision_capacity);
    for index in 0..DECISION_COUNT {
        let open_at = UtcTimestamp(base.0 + chrono::Duration::hours(i64::from(index)));
        let close_at = UtcTimestamp(open_at.0 + CandleInterval::H1.duration());
        let open_text = open_at.0.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let close_text = close_at
            .0
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let evaluation = evaluation(&close_text, "PAGE_HOLD");
        let expected_observation = observation_at(
            &open_text,
            &close_text,
            Decimal::from(100 + i64::from(index)),
        )?;
        let intent_id = fixture.add_decision_at(index, open_at, "HOLD", &evaluation)?;
        expected_rows.push((intent_id, expected_observation.id, close_at));
    }

    let mut actual_ids = Vec::with_capacity(expected_rows.len());
    for offset in [0, PAGE_LIMIT, PAGE_LIMIT * 2] {
        let page =
            fixture
                .store
                .automation_review_decisions(&fixture.session_id, offset, PAGE_LIMIT)?;
        let end = (offset + PAGE_LIMIT).min(DECISION_COUNT);
        let start_index = usize::try_from(offset)
            .map_err(|_| LabError::Internal("fixture page offset overflow".into()))?;
        let end_index = usize::try_from(end)
            .map_err(|_| LabError::Internal("fixture page end overflow".into()))?;
        assert_eq!(page.total_count, u64::from(DECISION_COUNT));
        assert_eq!(page.returned_count, u64::from(end - offset));
        assert_eq!(page.next_offset, (end < DECISION_COUNT).then_some(end));
        let page_ids = page
            .items
            .iter()
            .map(|item| item.intent_id.clone())
            .collect::<Vec<_>>();
        let expected_page = &expected_rows[start_index..end_index];
        assert_eq!(
            page_ids,
            expected_page
                .iter()
                .map(|(intent_id, _, _)| intent_id.clone())
                .collect::<Vec<_>>()
        );
        for (item, (intent_id, observation_id, decision_time)) in
            page.items.iter().zip(expected_page)
        {
            assert_eq!(&item.intent_id, intent_id);
            assert_eq!(&item.observation_id, observation_id);
            assert_eq!(item.decision_time_utc, *decision_time);
            assert_eq!(item.action, AutomationReviewAction::Hold);
        }
        actual_ids.extend(page_ids);
    }
    assert_eq!(
        actual_ids,
        expected_rows
            .iter()
            .map(|(intent_id, _, _)| intent_id.clone())
            .collect::<Vec<_>>()
    );
    fixture.close()?;
    Ok(())
}

#[test]
fn decision_pages_reject_missing_or_duplicate_signal_events() -> Result<(), LabError> {
    let mut missing = ReviewFixture::new()?;
    let intent_id = missing.add_decision(
        0,
        "HOLD",
        &evaluation("2026-10-14T01:00:00Z", "MISSING_SIGNAL"),
    )?;
    let (sequence, detail_json): (i64, String) = missing
        .store
        .connection
        .query_row(
            "SELECT sequence,detail_json FROM automation_session_events \
             WHERE session_id=?1 AND kind='SIGNAL_OBSERVED' \
               AND json_extract(detail_json,'$.intent_id')=?2",
            params![missing.session_id.as_str(), intent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(super::super::sql_error)?;
    let mut detail: serde_json::Value =
        serde_json::from_str(&detail_json).map_err(super::super::json_error)?;
    let unselected_id = AutomationIntentId::from_seed("unselected-page-signal");
    detail["intent_id"] = serde_json::json!(unselected_id.as_str());
    let detail_json = serde_json::to_string(&detail).map_err(super::super::json_error)?;
    missing
        .store
        .connection
        .execute(
            "UPDATE automation_session_events SET detail_json=?1 \
             WHERE session_id=?2 AND sequence=?3",
            params![detail_json, missing.session_id.as_str(), sequence],
        )
        .map_err(super::super::sql_error)?;
    assert!(matches!(
        missing
            .store
            .automation_review_decisions(&missing.session_id, 0, 100),
        Err(LabError::DataCorrupt(_))
    ));
    missing.close()?;

    let mut duplicate = ReviewFixture::new()?;
    let intent_id = duplicate.add_decision(
        0,
        "HOLD",
        &evaluation("2026-10-14T01:00:00Z", "DUPLICATE_SIGNAL"),
    )?;
    let (at_utc, detail_json): (String, String) = duplicate
        .store
        .connection
        .query_row(
            "SELECT at_utc,detail_json FROM automation_session_events \
             WHERE session_id=?1 AND kind='SIGNAL_OBSERVED' \
               AND json_extract(detail_json,'$.intent_id')=?2",
            params![duplicate.session_id.as_str(), intent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(super::super::sql_error)?;
    let next_sequence: i64 = duplicate
        .store
        .connection
        .query_row(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM automation_session_events WHERE session_id=?1",
            [duplicate.session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(super::super::sql_error)?;
    duplicate
        .store
        .connection
        .execute(
            "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
             VALUES (?1,?2,'SIGNAL_OBSERVED',?3,?4)",
            params![
                duplicate.session_id.as_str(),
                next_sequence,
                at_utc,
                detail_json,
            ],
        )
        .map_err(super::super::sql_error)?;
    assert!(matches!(
        duplicate
            .store
            .automation_review_decisions(&duplicate.session_id, 0, 100),
        Err(LabError::DataCorrupt(_))
    ));
    duplicate.close()?;
    Ok(())
}

#[test]
fn opening_inventory_requires_a_fresh_persisted_matching_mode_snapshot() -> Result<(), LabError> {
    let mut fixture = ReviewFixture::new()?;
    let _hold = fixture.add_decision(
        0,
        "HOLD",
        &serde_json::json!({
            "decision_time_utc": "2026-10-14T01:00:00Z",
            "policy_evaluation": null,
        }),
    )?;
    let buy = fixture.add_decision(1, "BUY", &evaluation("2026-10-14T02:00:00Z", "BUY_RULE"))?;
    fixture.add_partial_buy(
        &buy,
        Some(Decimal::new(25, 2)),
        "MOCK",
        "2026-10-14T01:59:55Z",
    )?;
    let snapshot = fixture
        .store
        .automation_review_snapshot(&fixture.session_id)?;
    let opening = snapshot.opening_inventory.ok_or_else(|| {
        LabError::DataCorrupt("fresh opening inventory evidence was omitted".into())
    })?;
    assert_eq!(opening.source_mode, AutomationSessionMode::Mock);
    assert_eq!(opening.base_quantity.get(), Decimal::new(30, 2));
    assert_eq!(opening.snapshot_sequence, 1);
    fixture.close()?;

    for (mode, observed_at) in [
        ("MOCK", "2026-10-14T00:30:00Z"),
        ("LIVE", "2026-10-14T01:59:55Z"),
    ] {
        let mut fixture = ReviewFixture::new()?;
        let _hold = fixture.add_decision(
            0,
            "HOLD",
            &serde_json::json!({
                "decision_time_utc": "2026-10-14T01:00:00Z",
                "policy_evaluation": null,
            }),
        )?;
        let buy =
            fixture.add_decision(1, "BUY", &evaluation("2026-10-14T02:00:00Z", "BUY_RULE"))?;
        fixture.add_partial_buy(&buy, Some(Decimal::new(25, 2)), mode, observed_at)?;
        let snapshot = fixture
            .store
            .automation_review_snapshot(&fixture.session_id)?;
        assert!(snapshot.opening_inventory.is_none());
        fixture.close()?;
    }
    Ok(())
}

#[test]
fn same_second_post_fill_snapshot_cannot_shadow_an_earlier_zero_inventory() -> Result<(), LabError>
{
    let mut fixture = ReviewFixture::new()?;
    let _hold = fixture.add_decision(
        0,
        "HOLD",
        &serde_json::json!({
            "decision_time_utc": "2026-10-14T01:00:00Z",
            "policy_evaluation": null,
        }),
    )?;
    let buy = fixture.add_decision(1, "BUY", &evaluation("2026-10-14T02:00:00Z", "BUY_RULE"))?;
    fixture.add_partial_buy(&buy, None, "MOCK", "2026-10-14T01:59:55Z")?;
    let client_identifier = AutomationOrderIdentifier::from_intent(&buy);
    fixture
        .store
        .connection
        .execute(
            "UPDATE automation_orders SET created_at_utc='2026-10-14T02:01:00Z', \
             venue_created_at_utc='2026-10-14T02:01:00Z' WHERE client_identifier=?1",
            [client_identifier.as_str()],
        )
        .map_err(super::super::sql_error)?;
    fixture.add_account_snapshot(
        1,
        "2026-10-14T01:59:00Z",
        "2026-10-14T01:59:01Z",
        Decimal::ZERO,
        Decimal::ZERO,
    )?;
    fixture.add_account_snapshot(
        2,
        "2026-10-14T02:01:00Z",
        "2026-10-14T02:01:00Z",
        Decimal::from(5),
        Decimal::ZERO,
    )?;
    let snapshot = fixture
        .store
        .automation_review_snapshot(&fixture.session_id)?;
    let opening = snapshot
        .opening_inventory
        .ok_or_else(|| LabError::DataCorrupt("earlier zero snapshot was omitted".into()))?;
    assert_eq!(opening.snapshot_sequence, 1);
    assert_eq!(opening.base_quantity.get(), Decimal::ZERO);
    fixture.close()?;

    let mut fixture = ReviewFixture::new()?;
    let _hold = fixture.add_decision(
        0,
        "HOLD",
        &serde_json::json!({
            "decision_time_utc": "2026-10-14T01:00:00Z",
            "policy_evaluation": null,
        }),
    )?;
    let buy = fixture.add_decision(1, "BUY", &evaluation("2026-10-14T02:00:00Z", "BUY_RULE"))?;
    fixture.add_partial_buy(&buy, None, "MOCK", "2026-10-14T01:59:55Z")?;
    let client_identifier = AutomationOrderIdentifier::from_intent(&buy);
    fixture
        .store
        .connection
        .execute(
            "UPDATE automation_orders SET created_at_utc='2026-10-14T02:01:00Z', \
             venue_created_at_utc='2026-10-14T02:01:00Z' WHERE client_identifier=?1",
            [client_identifier.as_str()],
        )
        .map_err(super::super::sql_error)?;
    fixture.add_account_snapshot(
        1,
        "2026-10-14T02:01:00Z",
        "2026-10-14T02:01:00Z",
        Decimal::from(5),
        Decimal::ZERO,
    )?;
    let snapshot = fixture
        .store
        .automation_review_snapshot(&fixture.session_id)?;
    assert!(snapshot.opening_inventory.is_none());
    fixture.close()?;
    Ok(())
}

#[test]
fn review_snapshot_rejects_changed_evaluation_and_missing_candle_links() -> Result<(), LabError> {
    let mut fixture = ReviewFixture::new()?;
    let intent_id =
        fixture.add_decision(0, "BUY", &evaluation("2026-10-14T01:00:00Z", "BUY_RULE"))?;
    fixture
        .store
        .connection
        .execute(
            "UPDATE automation_intents SET evaluation_json='{}' WHERE intent_id=?1",
            [intent_id.as_str()],
        )
        .map_err(super::super::sql_error)?;
    assert!(matches!(
        fixture
            .store
            .automation_review_detail(&fixture.session_id, &intent_id),
        Err(LabError::DataCorrupt(_))
    ));
    fixture.close()?;

    let mut fixture = ReviewFixture::new()?;
    let intent_id =
        fixture.add_decision(0, "HOLD", &evaluation("2026-10-14T01:00:00Z", "HOLD_RULE"))?;
    fixture
        .store
        .connection
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .map_err(super::super::sql_error)?;
    fixture
        .store
        .connection
        .execute(
            "UPDATE automation_intents SET observation_id='obs-missing' WHERE intent_id=?1",
            [intent_id.as_str()],
        )
        .map_err(super::super::sql_error)?;
    assert!(matches!(
        fixture
            .store
            .automation_review_detail(&fixture.session_id, &intent_id),
        Err(LabError::DataCorrupt(_))
    ));
    assert!(matches!(
        fixture
            .store
            .automation_review_decisions(&fixture.session_id, 0, 100),
        Err(LabError::DataCorrupt(_))
    ));
    fixture.close()?;
    Ok(())
}

#[test]
fn restart_replay_prestart_policy_warmup_hold_stays_reviewable_and_fail_closed()
-> Result<(), LabError> {
    let mut fixture = ReviewFixture::new()?;
    let warmup_intent = fixture.add_prestart_decision("HOLD", "phase", "POLICY_WARMUP")?;
    let snapshot = fixture
        .store
        .automation_review_snapshot(&fixture.session_id)?;
    assert_eq!(snapshot.decisions.len(), 1);
    assert_eq!(snapshot.decisions[0].intent_id, warmup_intent);
    assert_eq!(snapshot.decisions[0].action, AutomationReviewAction::Hold);
    assert!(snapshot.decisions[0].decision_time_utc < snapshot.session.created_at_utc);
    assert!(snapshot.decisions[0].recorded_at_utc >= snapshot.session.created_at_utc);
    fixture.close()?;

    let mut fixture = ReviewFixture::new()?;
    let warmup_intent = fixture.add_prestart_decision("HOLD", "kind", "POLICY_WARMUP")?;
    let snapshot = fixture
        .store
        .automation_review_snapshot(&fixture.session_id)?;
    assert_eq!(snapshot.decisions[0].intent_id, warmup_intent);
    assert_eq!(snapshot.decisions[0].action, AutomationReviewAction::Hold);
    fixture.close()?;

    let mut fixture = ReviewFixture::new()?;
    let _invalid_action = fixture.add_prestart_decision("BUY", "phase", "POLICY_WARMUP")?;
    assert!(matches!(
        fixture
            .store
            .automation_review_snapshot(&fixture.session_id),
        Err(LabError::DataCorrupt(_))
    ));
    fixture.close()?;

    let mut fixture = ReviewFixture::new()?;
    let _invalid_phase = fixture.add_prestart_decision("HOLD", "phase", "RECOVERY")?;
    assert!(matches!(
        fixture
            .store
            .automation_review_snapshot(&fixture.session_id),
        Err(LabError::DataCorrupt(_))
    ));
    fixture.close()?;
    Ok(())
}

#[test]
fn review_snapshot_fails_closed_at_fact_and_encoded_size_limits() -> Result<(), LabError> {
    let mut fixture = ReviewFixture::new()?;
    {
        let transaction = fixture
            .store
            .connection
            .transaction()
            .map_err(super::super::sql_error)?;
        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
                     VALUES (?1,?2,'PAUSED','2026-10-14T00:00:00Z','{}')",
                )
                .map_err(super::super::sql_error)?;
            for sequence in 1..=50_000_i64 {
                statement
                    .execute(params![fixture.session_id.as_str(), sequence])
                    .map_err(super::super::sql_error)?;
            }
        }
        transaction.commit().map_err(super::super::sql_error)?;
    }
    assert!(matches!(
        fixture
            .store
            .automation_review_snapshot(&fixture.session_id),
        Err(LabError::ResourceLimit(_))
    ));
    fixture.close()?;

    let mut fixture = ReviewFixture::new()?;
    let oversized_json =
        serde_json::Value::String("x".repeat(crate::contracts::MAX_AUTOMATION_REVIEW_BYTES + 1))
            .to_string();
    fixture
        .store
        .connection
        .execute(
            "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
             VALUES (?1,1,'PAUSED','2026-10-14T00:00:00Z',?2)",
            params![fixture.session_id.as_str(), oversized_json],
        )
        .map_err(super::super::sql_error)?;
    assert!(matches!(
        fixture
            .store
            .automation_review_snapshot(&fixture.session_id),
        Err(LabError::ResourceLimit(_))
    ));
    fixture.close()?;
    Ok(())
}

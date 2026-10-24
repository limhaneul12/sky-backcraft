use super::*;
use crate::contracts::{
    AutomationSessionMode, AutomationSessionWrite, BasisPoints, CandleInterval, ContentHash,
    CostPolicy, ExecutionPolicy, FrozenPolicyRevision, MarketDataOrigin, MarketId,
    PaperMarketRuleScenario, PaperMarketStart, PaperRecoveryCause, PaperSessionState,
    PaperSessionWrite, PolicyDefinition, PolicyIndicator, PolicyIndicatorKind, PolicyProgram,
    PolicySignalExpiry, PolicyTarget, PolicyWrite, QuoteAmount, RawObjectRef,
    RawWebSocketTransport, RequestId, RulesProgram, TickBand, UtcTimestamp, Weight,
};
use rust_decimal::Decimal;

fn realtime_fixture_time(value: &str) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
}

fn start_observer_session(
    store: &mut Store,
    policy_ref: &crate::contracts::PolicyRevisionRef,
    request_label: &str,
    market: MarketId,
    interval: CandleInterval,
    at: UtcTimestamp,
) -> crate::contracts::PaperSessionView {
    start_observer_session_for_markets(store, policy_ref, request_label, &[market], interval, at)
}

fn start_observer_session_for_markets(
    store: &mut Store,
    policy_ref: &crate::contracts::PolicyRevisionRef,
    request_label: &str,
    markets: &[MarketId],
    interval: CandleInterval,
    at: UtcTimestamp,
) -> crate::contracts::PaperSessionView {
    let zero = BasisPoints::new(Decimal::ZERO).expect("zero cost");
    store
        .paper_write(
            &PaperSessionWrite::Start {
                request_id: RequestId::new(format!("start-{request_label}"))
                    .expect("fixture request id"),
                policy_ref: policy_ref.clone(),
                decision_interval: interval,
                execution_resolution: interval,
                latency_ms: 0,
                execution: ExecutionPolicy::NextBarOpen {
                    participation_cap: Weight::new(Decimal::ONE).expect("unit participation"),
                },
                markets: markets
                    .iter()
                    .cloned()
                    .map(|market| PaperMarketStart {
                        market,
                        initial_cash: QuoteAmount::new(Decimal::from(100_000))
                            .expect("fixture cash"),
                        costs: CostPolicy {
                            buy_fee_bps: zero,
                            sell_fee_bps: zero,
                            maker_fee_bps: zero,
                            half_spread_bps: zero,
                            slippage_bps: zero,
                            impact_bps: zero,
                            assumption_label: "observer union fixture".into(),
                            dynamic: None,
                        },
                        market_rules: PaperMarketRuleScenario::SimulatedScenario {
                            min_notional: QuoteAmount::new(Decimal::from(10))
                                .expect("fixture minimum"),
                            quantity_step: AssetQuantity::new(Decimal::new(1, 4))
                                .expect("fixture quantity step"),
                            ticks: vec![TickBand {
                                lower_bound: QuoteAmount::new(Decimal::ZERO)
                                    .expect("fixture tick lower bound"),
                                tick: PriceKrw::new(Decimal::ONE).expect("fixture price tick"),
                            }],
                            assumption_label: "SIMULATED_SCENARIO fixture".into(),
                        },
                    })
                    .collect(),
            },
            at,
        )
        .expect("start valid observer fixture")
        .session
}

fn mark_gap_observer(
    store: &mut Store,
    view: &mut crate::contracts::PaperSessionView,
    cause: PaperRecoveryCause,
) {
    view.state = PaperSessionState::RecoveryRequired;
    view.recovery_cause = Some(cause);
    view.recovery_detail = Some("fixture durable data gap".into());
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET state='RECOVERY_REQUIRED',recovery_reason=?1,view_json=?2 \
             WHERE session_id=?3",
            rusqlite::params![
                format!("{cause:?}"),
                serde_json::to_string(view).expect("serialize recovery observer"),
                view.session_id.as_str(),
            ],
        )
        .expect("persist observation-only recovery state");
}

fn write_feed_observer_policy(
    store: &mut Store,
    request_id: &str,
    name: &str,
    source_interval: Option<CandleInterval>,
    at: UtcTimestamp,
) -> FrozenPolicyRevision {
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: name.into(),
        description: "synthetic active feed observer fixture".into(),
        program: PolicyProgram::Rules {
            program: RulesProgram {
                condition_roles: Vec::new(),
                indicators: source_interval
                    .into_iter()
                    .map(|source_interval| PolicyIndicator {
                        id: "source-close".into(),
                        indicator: PolicyIndicatorKind::Close,
                        source_interval: Some(source_interval),
                    })
                    .collect(),
                states: Vec::new(),
                rules: Vec::new(),
                fallback: PolicyTarget::Hold,
                signal_expiry: PolicySignalExpiry::EndOfRange,
            },
        },
    };
    store
        .write_policy(
            &PolicyWrite::Create {
                request_id: RequestId::new(request_id).expect("policy request ID"),
                definition,
            },
            at,
        )
        .expect("write synthetic feed observer policy")
        .snapshot
}

fn realtime_input_state_digest(store: &Store) -> String {
    let paper_sessions = store
        .connection
        .prepare(
            "SELECT session_id,state,recovery_reason,view_json FROM paper_sessions ORDER BY session_id",
        )
        .expect("prepare paper state snapshot")
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .expect("query paper state snapshot")
        .collect::<Result<Vec<_>, _>>()
        .expect("read paper state snapshot");
    let checkpoints = store
        .connection
        .prepare(
            "SELECT session_id,market_order,schema_version,engine_version,checkpoint_digest,checkpoint_json \
             FROM paper_market_checkpoints ORDER BY session_id,market_order",
        )
        .expect("prepare checkpoint snapshot")
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .expect("query checkpoint snapshot")
        .collect::<Result<Vec<_>, _>>()
        .expect("read checkpoint snapshot");
    let automation_sessions = store
        .connection
        .prepare(
            "SELECT session_id,state,frozen_policy_json FROM automation_sessions ORDER BY session_id",
        )
        .expect("prepare automation state snapshot")
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .expect("query automation state snapshot")
        .collect::<Result<Vec<_>, _>>()
        .expect("read automation state snapshot");
    let bytes = serde_json::to_vec(&(paper_sessions, checkpoints, automation_sessions))
        .expect("serialize realtime input state");
    ContentHash::of_bytes(&bytes).to_string()
}

#[test]
fn websocket_raw_provenance_round_trips_and_legacy_json_stays_unchanged() {
    let root = TempRoot::new("websocket-raw-provenance");
    let mut store = Store::open(&root.0).expect("open isolated raw store");
    let received_at =
        UtcTimestamp::parse_rfc3339("2026-10-17T00:00:01Z").expect("fixture UTC instant");
    let transport = RawWebSocketTransport {
        connection_epoch: 1,
        message_sequence: 7,
        provider_at: Some(received_at),
        received_at,
    };
    let input = || RawObjectInput {
        source_url: "wss://api.upbit.com/websocket/v1".into(),
        fetched_at: received_at,
        persisted_at: received_at,
        http_status: 101,
        remaining_req: None,
        origin: MarketDataOrigin::ExchangeObserved,
        body: br#"{"type":"candle.1m","code":"KRW-BTC"}"#.to_vec(),
    };
    let first = store
        .publish_websocket_raw(input(), transport.clone())
        .expect("publish actual websocket frame provenance");
    store
        .catalog_raw(&first.object)
        .expect("catalog websocket raw");
    let replay = store
        .publish_websocket_raw(input(), transport)
        .expect("idempotent websocket publication");
    assert_eq!(first.object.id, replay.object.id);
    assert!(first.object.transport.is_some());
    let bytes = store
        .raw_objects()
        .read_verified(&first.object)
        .expect("read and verify websocket raw");
    assert_eq!(bytes, first.body);
    let mut legacy = first.object.clone();
    legacy.transport = None;
    let json = serde_json::to_value(&legacy).expect("serialize legacy raw shape");
    assert!(json.get("transport").is_none());
    let decoded: RawObjectRef = serde_json::from_value(json).expect("read legacy shape");
    assert!(decoded.transport.is_none());
}

#[test]
fn websocket_raw_rejects_credentials_and_fabricated_upgrade_metadata() {
    let root = TempRoot::new("websocket-raw-security");
    let store = Store::open(&root.0).expect("open isolated raw store");
    let received_at =
        UtcTimestamp::parse_rfc3339("2026-10-17T00:00:01Z").expect("fixture UTC instant");
    for (url, status) in [
        (
            "wss://api.upbit.com/websocket/v1?authorization=not-a-credential",
            101,
        ),
        ("wss://api.upbit.com/websocket/v1", 200),
    ] {
        let result = store.publish_websocket_raw(
            RawObjectInput {
                source_url: url.into(),
                fetched_at: received_at,
                persisted_at: received_at,
                http_status: status,
                remaining_req: None,
                origin: MarketDataOrigin::ExchangeObserved,
                body: b"{}".to_vec(),
            },
            RawWebSocketTransport {
                connection_epoch: 1,
                message_sequence: 1,
                provider_at: None,
                received_at,
            },
        );
        assert!(matches!(result, Err(LabError::InvalidConfig(_))));
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one isolated journal journey verifies replay and identity, provenance, cursor and payload corruption"
)]
fn private_stream_journal_deduplicates_facts_and_detects_conflicts_and_corruption() {
    use crate::contracts::{PrivateStreamObservation, PrivateStreamObservationKind};
    let root = TempRoot::new("private-stream-journal");
    let mut store = Store::open(&root.0).expect("open isolated journal");
    let at = UtcTimestamp::parse_rfc3339("2026-10-17T00:00:01Z").expect("fixture clock");
    let mut event = PrivateStreamObservation {
        account_identity: "isolated-account".into(),
        generation: 1,
        kind: PrivateStreamObservationKind::OrderObserved,
        event_key: "observed-order-one".into(),
        provider_at_utc: Some(at),
        received_at_utc: at,
        transport: Some(RawWebSocketTransport {
            connection_epoch: 1,
            message_sequence: 1,
            provider_at: Some(at),
            received_at: at,
        }),
        payload: serde_json::json!({"order_uuid":"fixture-order", "volume":"0.25"}),
    };
    let first = store
        .record_private_stream_observation(&event)
        .expect("record provider facts");
    event.generation = 2;
    event.received_at_utc =
        UtcTimestamp::parse_rfc3339("2026-10-17T00:00:02Z").expect("later receipt");
    event.transport = Some(RawWebSocketTransport {
        connection_epoch: event.generation,
        message_sequence: 2,
        provider_at: event.provider_at_utc,
        received_at: event.received_at_utc,
    });
    assert_eq!(
        store
            .record_private_stream_observation(&event)
            .expect("replay provider facts"),
        first
    );
    let recorded = store
        .private_stream_observation_by_key("isolated-account", &event.event_key)
        .expect("exact durable readback")
        .expect("record exists");
    assert_eq!(recorded.observation.generation, 1);
    assert_eq!(recorded.observation.received_at_utc, at);
    assert!(
        store
            .private_stream_observations("different-account", 0, 100)
            .expect("isolated history scope")
            .is_empty()
    );
    assert!(
        store
            .private_stream_latest_reconciliation("isolated-account")
            .expect("empty recovery coverage")
            .is_none()
    );
    let mut coverage = event.clone();
    coverage.kind = PrivateStreamObservationKind::RestReconciled;
    coverage.transport = None;
    coverage.event_key = "coverage-one".into();
    coverage.payload = serde_json::json!({"covered_through":"2026-10-17T00:00:01Z"});
    store
        .record_private_stream_observation(&coverage)
        .expect("durable recovery coverage");
    assert_eq!(
        store
            .private_stream_latest_reconciliation("isolated-account")
            .expect("verified account watermark")
            .expect("coverage exists")
            .observation
            .event_key,
        coverage.event_key
    );
    assert!(
        store
            .private_stream_latest_reconciliation("different-account")
            .expect("isolated coverage")
            .is_none()
    );
    coverage.event_key = "transport-mismatch".into();
    coverage.transport = Some(RawWebSocketTransport {
        connection_epoch: coverage.generation + 1,
        message_sequence: 1,
        provider_at: coverage.provider_at_utc,
        received_at: coverage.received_at_utc,
    });
    assert!(matches!(
        store.record_private_stream_observation(&coverage),
        Err(LabError::InvalidConfig(_))
    ));
    event.payload["volume"] = "0.5".into();
    assert!(matches!(
        store.record_private_stream_observation(&event),
        Err(LabError::DataCorrupt(_))
    ));
    let mut missing_provenance = event.clone();
    missing_provenance.event_key = "missing-provenance".into();
    missing_provenance.transport = None;
    assert!(matches!(
        store.record_private_stream_observation(&missing_provenance),
        Err(LabError::InvalidConfig(_))
    ));
    store
        .connection
        .execute(
            "UPDATE private_stream_observations SET event_key='altered-key' WHERE sequence=?1",
            [i64::try_from(first).expect("SQL sequence")],
        )
        .expect("tamper isolated identity");
    assert!(matches!(
        store.private_stream_observations("isolated-account", 0, 100),
        Err(LabError::DataCorrupt(_))
    ));
    store
        .connection
        .execute(
            "UPDATE private_stream_observations SET event_key=?1 WHERE sequence=?2",
            rusqlite::params![event.event_key, i64::try_from(first).expect("SQL sequence")],
        )
        .expect("restore isolated identity");
    let original_sequence = i64::try_from(first).expect("SQL sequence");
    let altered_sequence = original_sequence + 1_000_000;
    store
        .connection
        .execute(
            "UPDATE private_stream_observations SET sequence=?1 WHERE sequence=?2",
            rusqlite::params![altered_sequence, original_sequence],
        )
        .expect("tamper isolated journal order");
    assert!(matches!(
        store.private_stream_observations("isolated-account", 0, 100),
        Err(LabError::DataCorrupt(_))
    ));
    store
        .connection
        .execute(
            "UPDATE private_stream_observations SET sequence=?1 WHERE sequence=?2",
            rusqlite::params![original_sequence, altered_sequence],
        )
        .expect("restore isolated journal order");
    event.event_key = "credential-field-rejected".into();
    event.payload = serde_json::json!({"Authorization":"forbidden-field"});
    assert!(matches!(
        store.record_private_stream_observation(&event),
        Err(LabError::InvalidConfig(_))
    ));
    store
        .connection
        .execute(
            "UPDATE private_stream_observations SET payload_json='{}' WHERE sequence=?1",
            [i64::try_from(first).expect("SQL sequence")],
        )
        .expect("tamper isolated test row");
    assert!(matches!(
        store.private_stream_observations("isolated-account", 0, 100),
        Err(LabError::DataCorrupt(_))
    ));
    assert!(
        store
            .active_realtime_market_requirements()
            .expect("empty session requirements")
            .is_empty()
    );
}

#[test]
fn websocket_raw_uppercase_scheme_cannot_bypass_provenance() {
    let root = TempRoot::new("websocket-uppercase-scheme");
    let store = Store::open(&root.0).expect("open isolated raw store");
    let now = UtcTimestamp::now();
    let result = store.publish_raw(RawObjectInput {
        source_url: "WSS://api.upbit.com/websocket/v1".into(),
        fetched_at: now,
        persisted_at: now,
        http_status: 101,
        remaining_req: None,
        origin: MarketDataOrigin::ExchangeObserved,
        body: b"{}".to_vec(),
    });
    assert!(matches!(result, Err(LabError::InvalidConfig(_))));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one durable store journey proves the exact union of strategy-running and gap-observer feed keys"
)]
fn feed_union_retains_running_feed_gap_and_provider_gap_observers_without_resuming_them() {
    use crate::contracts::{PolicyProgram, RulesProgram};

    let root = TempRoot::new("feed-gap-observer-union");
    let mut store = Store::open(&root.0).expect("open isolated paper database");
    let definition = PolicyDefinition {
        schema_version: "1.0".into(),
        name: "feed_observer_fixture".into(),
        description: "synthetic feed observer union test".into(),
        program: PolicyProgram::Rules {
            program: RulesProgram {
                condition_roles: Vec::new(),
                indicators: Vec::new(),
                states: Vec::new(),
                rules: Vec::new(),
                fallback: PolicyTarget::Hold,
                signal_expiry: PolicySignalExpiry::EndOfRange,
            },
        },
    };
    let revision = store
        .write_policy(
            &PolicyWrite::Create {
                request_id: RequestId::new("feed-observer-policy").expect("policy request"),
                definition,
            },
            realtime_fixture_time("2025-01-01T00:00:00Z"),
        )
        .expect("create immutable fixture policy");
    let policy_ref = revision.snapshot.reference;
    let at = realtime_fixture_time("2025-01-01T00:01:00Z");

    let running = start_observer_session(
        &mut store,
        &policy_ref,
        "running",
        MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
        CandleInterval::M1,
        at,
    );
    let mut feed_gap = start_observer_session(
        &mut store,
        &policy_ref,
        "feed-gap",
        MarketId::parse_upbit("KRW-ETH").expect("ETH market"),
        CandleInterval::H1,
        at,
    );
    mark_gap_observer(&mut store, &mut feed_gap, PaperRecoveryCause::FeedGap);
    let mut provider_gap = start_observer_session(
        &mut store,
        &policy_ref,
        "provider-gap",
        MarketId::parse_upbit("KRW-XRP").expect("XRP market"),
        CandleInterval::M5,
        at,
    );
    mark_gap_observer(
        &mut store,
        &mut provider_gap,
        PaperRecoveryCause::ProviderGap,
    );
    let paused = start_observer_session(
        &mut store,
        &policy_ref,
        "paused",
        MarketId::parse_upbit("KRW-DOGE").expect("DOGE market"),
        CandleInterval::M15,
        at,
    );
    store
        .paper_write(
            &PaperSessionWrite::Pause {
                request_id: RequestId::new("pause-observer-fixture").expect("pause request"),
                session_id: paused.session_id,
                expected_sequence: paused.sequence,
            },
            realtime_fixture_time("2025-01-01T00:02:00Z"),
        )
        .expect("pause unrelated session");
    let stopped = start_observer_session(
        &mut store,
        &policy_ref,
        "stopped",
        MarketId::parse_upbit("KRW-SOL").expect("SOL market"),
        CandleInterval::D1,
        at,
    );
    store
        .paper_write(
            &PaperSessionWrite::Stop {
                request_id: RequestId::new("stop-observer-fixture").expect("stop request"),
                session_id: stopped.session_id,
                expected_sequence: stopped.sequence,
            },
            realtime_fixture_time("2025-01-01T00:02:00Z"),
        )
        .expect("stop unrelated session");

    let requirements = store
        .active_realtime_market_requirements()
        .expect("read bounded observer union");
    assert_eq!(
        requirements,
        [
            (
                MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
                CandleInterval::M1,
            ),
            (
                MarketId::parse_upbit("KRW-ETH").expect("ETH market"),
                CandleInterval::H1,
            ),
            (
                MarketId::parse_upbit("KRW-XRP").expect("XRP market"),
                CandleInterval::M5,
            ),
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        requirements.iter().cloned().collect::<Vec<_>>(),
        vec![
            (
                MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
                CandleInterval::M1,
            ),
            (
                MarketId::parse_upbit("KRW-ETH").expect("ETH market"),
                CandleInterval::H1,
            ),
            (
                MarketId::parse_upbit("KRW-XRP").expect("XRP market"),
                CandleInterval::M5,
            ),
        ]
    );
    let persisted: Vec<(String, String)> = store
        .connection
        .prepare("SELECT state,view_json FROM paper_sessions ORDER BY session_id")
        .expect("prepare session state readback")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query session states")
        .collect::<Result<_, _>>()
        .expect("read session states");
    assert_eq!(
        store
            .paper_get_session(&running.session_id, at)
            .expect("unchanged running observer")
            .session
            .state,
        PaperSessionState::Running
    );
    assert!(persisted.iter().any(|(state, view)| {
        state == "RECOVERY_REQUIRED"
            && serde_json::from_str::<crate::contracts::PaperSessionView>(view)
                .is_ok_and(|view| view.recovery_cause == Some(PaperRecoveryCause::FeedGap))
    }));
    assert!(persisted.iter().any(|(state, view)| {
        state == "RECOVERY_REQUIRED"
            && serde_json::from_str::<crate::contracts::PaperSessionView>(view)
                .is_ok_and(|view| view.recovery_cause == Some(PaperRecoveryCause::ProviderGap))
    }));
    assert!(persisted.iter().any(|(state, _)| state == "PAUSED"));
    assert!(persisted.iter().any(|(state, _)| state == "STOPPED"));
}

#[test]
fn feed_union_preserves_database_view_mismatches() {
    let root = TempRoot::new("feed-view-mismatch");
    let mut store = Store::open(&root.0).expect("open isolated paper database");
    let at = realtime_fixture_time("2025-01-01T00:01:00Z");
    let policy = write_feed_observer_policy(
        &mut store,
        "feed-view-mismatch-policy",
        "feed_view_mismatch_fixture",
        None,
        realtime_fixture_time("2025-01-01T00:00:00Z"),
    );

    let mut running = start_observer_session(
        &mut store,
        &policy.reference,
        "running-view-paused",
        MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
        CandleInterval::M1,
        at,
    );
    running.state = PaperSessionState::Paused;
    running.paused_at = Some(at);
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET view_json=?1 WHERE session_id=?2",
            rusqlite::params![
                serde_json::to_string(&running).expect("serialize mismatched view"),
                running.session_id.as_str(),
            ],
        )
        .expect("persist running row with a paused view");

    let mut recovery = start_observer_session(
        &mut store,
        &policy.reference,
        "recovery-view-running",
        MarketId::parse_upbit("KRW-ETH").expect("ETH market"),
        CandleInterval::H1,
        at,
    );
    mark_gap_observer(&mut store, &mut recovery, PaperRecoveryCause::FeedGap);
    recovery.state = PaperSessionState::Running;
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET view_json=?1 WHERE session_id=?2",
            rusqlite::params![
                serde_json::to_string(&recovery).expect("serialize mismatched recovery view"),
                recovery.session_id.as_str(),
            ],
        )
        .expect("persist recovery row with a running view");

    let state_before = realtime_input_state_digest(&store);
    let requirements = store
        .active_realtime_market_requirements()
        .expect("mismatched active rows remain readable");
    assert_eq!(
        requirements.iter().cloned().collect::<Vec<_>>(),
        vec![(
            MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
            CandleInterval::M1,
        )]
    );
    assert_eq!(realtime_input_state_digest(&store), state_before);
}

#[test]
fn feed_union_parses_running_sql_ids_before_malformed_views() {
    let root = TempRoot::new("feed-invalid-running-id-order");
    let mut store = Store::open(&root.0).expect("open isolated paper database");
    let at = realtime_fixture_time("2025-01-01T00:01:00Z");
    let policy = write_feed_observer_policy(
        &mut store,
        "invalid-running-id-policy",
        "invalid_running_id_fixture",
        None,
        realtime_fixture_time("2025-01-01T00:00:00Z"),
    );
    let running = start_observer_session(
        &mut store,
        &policy.reference,
        "invalid-running-id-order",
        MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
        CandleInterval::M1,
        at,
    );
    assert!(crate::contracts::PaperSessionId::new("").is_err());
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET view_json='{}' WHERE session_id=?1",
            [running.session_id.as_str()],
        )
        .expect("malform valid-ID active view");
    store
        .connection
        .execute(
            "INSERT INTO paper_sessions( \
                 session_id,origin_request_id,state,sequence,schema_version,engine_version, \
                 policy_id,policy_revision_id,policy_ref_json,decision_interval,execution_resolution, \
                 latency_ms,execution_json,markets_json,created_at_utc,started_at_utc,paused_at_utc, \
                 stopped_at_utc,last_evaluated_at_utc,event_count,recovery_reason,view_json \
             ) SELECT '','invalid-running-session-id',state,sequence,schema_version,engine_version, \
                 policy_id,policy_revision_id,policy_ref_json,decision_interval,execution_resolution, \
                 latency_ms,execution_json,markets_json,created_at_utc,started_at_utc,paused_at_utc, \
                 stopped_at_utc,last_evaluated_at_utc,event_count,recovery_reason,'{}' \
               FROM paper_sessions WHERE session_id=?1",
            [running.session_id.as_str()],
        )
        .expect("insert malformed active row with an invalid SQL session ID");

    assert!(matches!(
        store.active_realtime_market_requirements(),
        Err(LabError::InvalidConfig(_))
    ));
}

#[test]
fn feed_union_preserves_sql_view_session_id_redirection() {
    let root = TempRoot::new("feed-sql-view-session-redirection");
    let mut store = Store::open(&root.0).expect("open isolated paper database");
    let at = realtime_fixture_time("2025-01-01T00:01:00Z");
    let policy = write_feed_observer_policy(
        &mut store,
        "feed-session-redirection-policy",
        "feed_session_redirection_fixture",
        None,
        realtime_fixture_time("2025-01-01T00:00:00Z"),
    );
    let running = start_observer_session(
        &mut store,
        &policy.reference,
        "sql-session-btc",
        MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
        CandleInterval::M1,
        at,
    );
    let redirected = start_observer_session(
        &mut store,
        &policy.reference,
        "view-session-eth",
        MarketId::parse_upbit("KRW-ETH").expect("ETH market"),
        CandleInterval::H1,
        at,
    );
    let redirected_view = serde_json::to_string(&redirected).expect("serialize redirected view");
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET state='PAUSED' WHERE session_id=?1",
            [redirected.session_id.as_str()],
        )
        .expect("hide redirected target row from active SQL scans");
    store
        .connection
        .execute(
            "UPDATE paper_sessions SET view_json=?1 WHERE session_id=?2",
            rusqlite::params![redirected_view, running.session_id.as_str()],
        )
        .expect("redirect running SQL row to another valid session view");

    let state_before = realtime_input_state_digest(&store);
    let requirements = store
        .active_realtime_market_requirements()
        .expect("read requirements across a valid SQL/view ID redirection");
    assert_eq!(
        requirements.iter().cloned().collect::<Vec<_>>(),
        vec![
            (
                MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
                CandleInterval::M1,
            ),
            (
                MarketId::parse_upbit("KRW-ETH").expect("ETH market"),
                CandleInterval::H1,
            ),
        ]
    );
    assert_eq!(realtime_input_state_digest(&store), state_before);
}

#[test]
fn inactive_paper_sessions_ignore_hidden_corruption() {
    let root = TempRoot::new("hidden-paper-session-corruption");
    let mut store = Store::open(&root.0).expect("open isolated paper database");
    let at = realtime_fixture_time("2025-01-01T00:01:00Z");
    let policy = write_feed_observer_policy(
        &mut store,
        "hidden-corruption-policy",
        "hidden_corruption_fixture",
        None,
        realtime_fixture_time("2025-01-01T00:00:00Z"),
    );

    let _running = start_observer_session(
        &mut store,
        &policy.reference,
        "visible-running",
        MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
        CandleInterval::M1,
        at,
    );
    let paused = start_observer_session(
        &mut store,
        &policy.reference,
        "hidden-paused",
        MarketId::parse_upbit("KRW-DOGE").expect("DOGE market"),
        CandleInterval::M15,
        at,
    );
    store
        .paper_write(
            &PaperSessionWrite::Pause {
                request_id: RequestId::new("pause-hidden-fixture").expect("pause request"),
                session_id: paused.session_id.clone(),
                expected_sequence: paused.sequence,
            },
            realtime_fixture_time("2025-01-01T00:02:00Z"),
        )
        .expect("pause hidden observer");
    let stopped = start_observer_session(
        &mut store,
        &policy.reference,
        "hidden-stopped",
        MarketId::parse_upbit("KRW-SOL").expect("SOL market"),
        CandleInterval::D1,
        at,
    );
    store
        .paper_write(
            &PaperSessionWrite::Stop {
                request_id: RequestId::new("stop-hidden-fixture").expect("stop request"),
                session_id: stopped.session_id.clone(),
                expected_sequence: stopped.sequence,
            },
            realtime_fixture_time("2025-01-01T00:02:00Z"),
        )
        .expect("stop hidden observer");

    let malformed_digest = ContentHash::of_bytes(b"{}").to_string();
    for session_id in [&paused.session_id, &stopped.session_id] {
        store
            .connection
            .execute(
                "UPDATE paper_sessions SET view_json='{}' WHERE session_id=?1",
                [session_id.as_str()],
            )
            .expect("malform hidden session view");
        store
            .connection
            .execute(
                "UPDATE paper_market_checkpoints SET checkpoint_json='{}' ,checkpoint_digest=?1 \
                 WHERE session_id=?2",
                rusqlite::params![malformed_digest, session_id.as_str()],
            )
            .expect("malform hidden checkpoint with a matching digest");
    }

    let state_before = realtime_input_state_digest(&store);
    assert_eq!(
        store
            .active_realtime_market_requirements()
            .expect("hidden inactive corruption remains unread")
            .into_iter()
            .collect::<Vec<_>>(),
        vec![(
            MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
            CandleInterval::M1,
        )]
    );
    assert_eq!(realtime_input_state_digest(&store), state_before);
}

#[test]
fn active_mtf_automation_contributes_decision_and_source_intervals_in_order() {
    use crate::contracts::{AutomationRiskPolicy, AutomationSessionState};

    let root = TempRoot::new("feed-mtf-automation-requirements");
    let mut store = Store::open(&root.0).expect("open isolated automation database");
    let at = realtime_fixture_time("2025-01-01T00:01:00Z");
    let policy = write_feed_observer_policy(
        &mut store,
        "feed-mtf-automation-policy",
        "feed_mtf_automation_fixture",
        Some(CandleInterval::M5),
        realtime_fixture_time("2025-01-01T00:00:00Z"),
    );
    let start = AutomationSessionWrite::Start {
        request_id: RequestId::new("feed-mtf-automation-start").expect("automation request"),
        policy_ref: policy.reference.clone(),
        market: MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
        decision_interval: CandleInterval::M1,
        mode: AutomationSessionMode::Mock,
        risk: AutomationRiskPolicy {
            max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000)).expect("order limit"),
            max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))
                .expect("daily limit"),
            max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))
                .expect("exposure limit"),
            max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000)).expect("loss limit"),
            activation_loss_limit_krw: None,
            max_open_orders: 1,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 3_600,
            max_feed_age_seconds: 3_600,
        },
    };
    let response = store
        .automation_write(&start, Some(policy), Some("mock:upbit-default".into()), at)
        .expect("start synthetic MOCK automation only in the isolated database");
    assert_eq!(response.session.state, AutomationSessionState::Running);

    let state_before = realtime_input_state_digest(&store);
    let requirements = store
        .active_realtime_market_requirements()
        .expect("load active MTF automation requirements");
    assert_eq!(
        requirements.iter().cloned().collect::<Vec<_>>(),
        vec![
            (
                MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
                CandleInterval::M1,
            ),
            (
                MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
                CandleInterval::M5,
            ),
        ]
    );
    assert_eq!(realtime_input_state_digest(&store), state_before);

    store
        .connection
        .execute(
            "UPDATE automation_sessions SET frozen_policy_json='{}' WHERE session_id=?1",
            [response.session.session_id.as_str()],
        )
        .expect("malform active frozen policy");
    assert!(matches!(
        store.active_realtime_market_requirements(),
        Err(LabError::Internal(_))
    ));
}

#[test]
fn active_paper_observer_errors_keep_view_before_checkpoint_order() {
    let root = TempRoot::new("feed-active-observer-error-order");
    let mut store = Store::open(&root.0).expect("open isolated paper database");
    let at = realtime_fixture_time("2025-01-01T00:01:00Z");
    let policy = write_feed_observer_policy(
        &mut store,
        "feed-active-error-policy",
        "feed_active_error_fixture",
        None,
        realtime_fixture_time("2025-01-01T00:00:00Z"),
    );
    let running = start_observer_session(
        &mut store,
        &policy.reference,
        "active-error-order",
        MarketId::parse_upbit("KRW-BTC").expect("BTC market"),
        CandleInterval::M1,
        at,
    );
    let original_view = serde_json::to_string(&running).expect("serialize active view");
    let original_engine: String = store
        .connection
        .query_row(
            "SELECT engine_version FROM paper_market_checkpoints WHERE session_id=?1",
            [running.session_id.as_str()],
            |row| row.get(0),
        )
        .expect("read active checkpoint fixture");

    store
        .connection
        .execute(
            "UPDATE paper_sessions SET view_json='{}' WHERE session_id=?1",
            [running.session_id.as_str()],
        )
        .expect("malform active view");
    store
        .connection
        .execute(
            "UPDATE paper_market_checkpoints SET engine_version='future-engine' WHERE session_id=?1",
            [running.session_id.as_str()],
        )
        .expect("corrupt active checkpoint after view");
    assert!(matches!(
        store.active_realtime_market_requirements(),
        Err(LabError::Internal(_))
    ));

    store
        .connection
        .execute(
            "UPDATE paper_sessions SET view_json=?1 WHERE session_id=?2",
            rusqlite::params![original_view, running.session_id.as_str()],
        )
        .expect("restore active view");
    assert!(matches!(
        store.active_realtime_market_requirements(),
        Err(LabError::DataCorrupt(_))
    ));

    store
        .connection
        .execute(
            "UPDATE paper_market_checkpoints SET engine_version=?1,checkpoint_json='{}' ,checkpoint_digest=?2 \
             WHERE session_id=?3",
            rusqlite::params![
                original_engine,
                ContentHash::of_bytes(b"{}").to_string(),
                running.session_id.as_str(),
            ],
        )
        .expect("malform active checkpoint with a matching digest");
    assert!(matches!(
        store.active_realtime_market_requirements(),
        Err(LabError::Internal(_))
    ));
}

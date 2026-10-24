use super::tests::{
    baseline_test_risk_policy, completed_test_observation, seed_reserved_test_order,
    seed_running_automation_session,
};
use super::*;
use crate::contracts::{
    AdmissionDeferredFact, AdmissionDeferredPhase, AdmissionDeferredReason,
    AdmissionDeferredTarget, AutomationIntentId, AutomationSessionId, CandleInterval,
    DecisionPreparation, DecisionPreparationStatus, LabError, MarketId, ObservationId,
    UtcTimestamp,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

type MutationSnapshot = (i64, String, Option<String>, String, String, String, String);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "sky-backcraft-admission-deferred-{label}-{}-{}",
            std::process::id(),
            TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&root).expect("create isolated automation admission test root");
        Self(root)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn time(value: &str) -> Result<UtcTimestamp, LabError> {
    UtcTimestamp::parse_rfc3339(value)
}

fn running_store(
    root: &TestRoot,
    session_name: &str,
    market: &str,
) -> Result<(Store, AutomationSessionId), LabError> {
    let mut store = Store::open(&root.0)?;
    let session_id = seed_running_automation_session(
        &mut store,
        session_name,
        market,
        "mock:upbit-default",
        &baseline_test_risk_policy()?,
        time("2026-10-14T00:30:00Z")?,
    )?;
    Ok((store, session_id))
}

fn pending_intent(
    store: &mut Store,
    session_id: &AutomationSessionId,
    suffix: &str,
) -> Result<(AutomationIntentId, crate::contracts::CandleObservation), LabError> {
    let observation = completed_test_observation("2026-10-14T00:00:00Z", "2026-10-14T01:00:00Z")?;
    let intent_id = AutomationIntentId::from_seed(&format!("admission-deferred:{suffix}"));
    store.automation_record_decision(
        session_id,
        &observation,
        "{}",
        &intent_id,
        "BUY",
        &serde_json::json!({"decision_time_utc":"2026-10-14T01:00:00Z"}),
        time("2026-10-14T01:00:01Z")?,
    )?;
    Ok((intent_id, observation))
}

fn mutation_snapshot(
    store: &Store,
    session_id: &AutomationSessionId,
) -> Result<MutationSnapshot, LabError> {
    let session = store
        .connection
        .query_row(
            "SELECT sequence,checkpoint_json,last_finalized_candle_at_utc \
             FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(sql_error)?;
    Ok((
        session.0,
        session.1,
        session.2,
        ordered_rows(
            store,
            session_id,
            "SELECT intent_id || char(31) || observation_id || char(31) || \
                    decision_time_utc || char(31) || action || char(31) || state || char(31) || \
                    evaluation_json \
             FROM automation_intents WHERE session_id=?1 ORDER BY intent_id",
        )?,
        ordered_rows(
            store,
            session_id,
            "SELECT observation_id || char(31) || close_time_utc || char(31) || content_digest || \
                    char(31) || candle_json || char(31) || checkpoint_json \
             FROM automation_candles WHERE session_id=?1 ORDER BY observation_id",
        )?,
        ordered_rows(
            store,
            session_id,
            "SELECT client_identifier || char(31) || intent_id || char(31) || account_identity || \
                    char(31) || order_request_json || char(31) || rules_snapshot_json || char(31) || \
                    request_digest || char(31) || market || char(31) || side || char(31) || state || \
                    char(31) || coalesce(venue_order_uuid,'') || char(31) || requested_notional_krw || \
                    char(31) || decision_mark_krw || char(31) || reserve_quote_krw || char(31) || \
                    reserve_asset || char(31) || reserved_asset_volume || char(31) || \
                    reserved_fee_krw || char(31) || remaining_fee_krw || char(31) || \
                    executed_volume || char(31) || remaining_volume || char(31) || paid_fee_krw || \
                    char(31) || locked_amount || char(31) || locked_currency || char(31) || \
                    trades_count || char(31) || coalesce(venue_created_at_utc,'') || char(31) || \
                    created_at_utc || char(31) || updated_at_utc || char(31) || coalesce(detail_json,'') \
             FROM automation_orders WHERE session_id=?1 ORDER BY client_identifier",
        )?,
        ordered_rows(
            store,
            session_id,
            "SELECT r.client_identifier || char(31) || r.account_identity || char(31) || \
                    r.market || char(31) || r.quote_reserved_krw || char(31) || r.asset_reserved || \
                    char(31) || r.asset_reserved_volume || char(31) || r.order_notional_krw || \
                    char(31) || r.order_day_utc || char(31) || r.state || char(31) || \
                    r.created_at_utc || char(31) || coalesce(r.released_at_utc,'') \
             FROM automation_risk_reservations AS r \
             JOIN automation_orders AS o USING(client_identifier) \
             WHERE o.session_id=?1 ORDER BY r.client_identifier",
        )?,
    ))
}

fn ordered_rows(
    store: &Store,
    session_id: &AutomationSessionId,
    query: &str,
) -> Result<String, LabError> {
    let mut statement = store.connection.prepare(query).map_err(sql_error)?;
    let rows = statement
        .query_map([session_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    let values = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    Ok(values.join("\n"))
}

fn deferred_count(store: &Store, session_id: &AutomationSessionId) -> Result<i64, LabError> {
    store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM automation_session_events \
             WHERE session_id=?1 AND kind='DECISION_DEFERRED'",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn latest_deferred_fact(
    store: &Store,
    session_id: &AutomationSessionId,
) -> Result<(serde_json::Value, AdmissionDeferredFact), LabError> {
    let detail_json: String = store
        .connection
        .query_row(
            "SELECT detail_json FROM automation_session_events \
             WHERE session_id=?1 AND kind='DECISION_DEFERRED' \
             ORDER BY sequence DESC LIMIT 1",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let detail: serde_json::Value = serde_json::from_str(&detail_json)
        .map_err(|_| LabError::DataCorrupt("deferred event detail is invalid".into()))?;
    let fact: AdmissionDeferredFact = serde_json::from_value(
        detail
            .get("admission_deferral")
            .cloned()
            .ok_or_else(|| LabError::DataCorrupt("admission deferral fact is missing".into()))?,
    )
    .map_err(|_| LabError::DataCorrupt("admission deferral fact is invalid".into()))?;
    Ok((detail, fact))
}

#[test]
fn pending_deferral_links_original_fact_deduplicates_consecutive_and_preserves_state()
-> Result<(), LabError> {
    let root = TestRoot::new("pending");
    let (mut store, session_id) = running_store(&root, "pending-deferral", "KRW-BTC")?;
    let (intent_id, observation) = pending_intent(&mut store, &session_id, "pending")?;
    let target = AdmissionDeferredTarget::PendingIntent(intent_id.clone());
    let before = mutation_snapshot(&store, &session_id)?;
    let now = time("2026-10-14T01:00:02Z")?;
    let first_detail = "private account recovery is still required";

    assert!(store.automation_record_admission_deferral(
        &session_id,
        &target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some(first_detail),
        now,
    )?);
    assert_eq!(mutation_snapshot(&store, &session_id)?, before);
    assert_eq!(deferred_count(&store, &session_id)?, 1);
    let (detail, first_fact) = latest_deferred_fact(&store, &session_id)?;
    assert_eq!(first_fact.phase, AdmissionDeferredPhase::BeforeAdmission);
    assert_eq!(
        first_fact.reason,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired
    );
    assert_eq!(first_fact.intent_id, intent_id);
    assert_eq!(first_fact.observation_id, observation.id);
    assert_eq!(first_fact.client_identifier, None);
    assert_eq!(first_fact.reason_detail.as_deref(), Some(first_detail));
    assert_eq!(detail["intent_id"], first_fact.intent_id.as_str());
    assert_eq!(detail["observation_id"], first_fact.observation_id.as_str());
    assert_eq!(detail["reason"], "PRIVATE_ACCOUNT_RECOVERY_REQUIRED");
    assert!(detail.get("client_identifier").is_none());

    assert!(!store.automation_record_admission_deferral(
        &session_id,
        &target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some(first_detail),
        now,
    )?);
    let second_detail = "a newer readback is needed";
    assert!(store.automation_record_admission_deferral(
        &session_id,
        &target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some(second_detail),
        now,
    )?);
    assert!(store.automation_record_admission_deferral(
        &session_id,
        &target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some(first_detail),
        now,
    )?);
    assert!(!store.automation_record_admission_deferral(
        &session_id,
        &target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some(first_detail),
        now,
    )?);
    assert_eq!(deferred_count(&store, &session_id)?, 3);
    assert_eq!(mutation_snapshot(&store, &session_id)?, before);

    let other_session = seed_running_automation_session(
        &mut store,
        "other-owner",
        "KRW-ETH",
        "mock:upbit-default",
        &baseline_test_risk_policy()?,
        time("2026-10-14T00:30:00Z")?,
    )?;
    assert!(matches!(
        store.automation_record_admission_deferral(
            &other_session,
            &target,
            AdmissionDeferredReason::PrivateAccountRecoveryRequired,
            None,
            now,
        ),
        Err(LabError::Conflict(_))
    ));
    assert_eq!(deferred_count(&store, &session_id)?, 3);

    store
        .connection
        .execute(
            "UPDATE automation_intents SET state='REJECTED' WHERE intent_id=?1",
            [intent_id.as_str()],
        )
        .map_err(sql_error)?;
    let after_terminal = mutation_snapshot(&store, &session_id)?;
    assert!(!store.automation_record_admission_deferral(
        &session_id,
        &target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some(first_detail),
        now,
    )?);
    assert_eq!(mutation_snapshot(&store, &session_id)?, after_terminal);
    assert_eq!(deferred_count(&store, &session_id)?, 3);
    store.close()?;
    Ok(())
}

#[test]
fn preparation_and_admission_deferrals_interleave_and_deduplicate_by_latest_variant()
-> Result<(), LabError> {
    let root = TestRoot::new("interleaved");
    let (mut store, session_id) = running_store(&root, "interleaved-deferral", "KRW-BTC")?;
    let (intent_id, _) = pending_intent(&mut store, &session_id, "interleaved")?;
    let before = mutation_snapshot(&store, &session_id)?;
    let preparation = DecisionPreparation {
        status: DecisionPreparationStatus::PrivateReadinessBlocked,
        market: MarketId::parse_upbit("KRW-BTC")?,
        interval: CandleInterval::H1,
        observation_id: ObservationId::from_seed("next-candidate-02:00"),
        decision_time: time("2026-10-14T02:00:00Z")?,
        completed_bars_seen: 1,
        required_bars: 2,
    };

    assert!(store.automation_record_preparation(
        &session_id,
        &preparation,
        time("2026-10-14T02:00:01Z")?,
    )?);
    assert!(store.automation_record_admission_deferral(
        &session_id,
        &AdmissionDeferredTarget::PendingIntent(intent_id),
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        None,
        time("2026-10-14T02:00:02Z")?,
    )?);
    // The latest event is an admission fact, so the same preparation is a new
    // variant transition; its following replay is the one that deduplicates.
    assert!(store.automation_record_preparation(
        &session_id,
        &preparation,
        time("2026-10-14T02:00:03Z")?,
    )?);
    assert!(!store.automation_record_preparation(
        &session_id,
        &preparation,
        time("2026-10-14T02:00:04Z")?,
    )?);
    assert_eq!(deferred_count(&store, &session_id)?, 3);
    assert_eq!(mutation_snapshot(&store, &session_id)?, before);
    store.close()?;
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one real-Store scenario checks both order phases, linked facts, stale transition, ownership, and unchanged effect state"
)]
fn reserved_and_claimed_order_deferrals_link_without_changing_effect_state() -> Result<(), LabError>
{
    let root = TestRoot::new("orders");
    let (mut store, session_id) = running_store(&root, "order-deferral", "KRW-BTC")?;
    let now = time("2026-10-14T02:00:00Z")?;
    let reserved = seed_reserved_test_order(&mut store, &session_id, "reserved", now)?;
    let before_reserved = mutation_snapshot(&store, &session_id)?;
    let reservation_before: (String, String, String) = store
        .connection
        .query_row(
            "SELECT o.state,i.state,r.state FROM automation_orders o \
             JOIN automation_intents i USING(intent_id) \
             JOIN automation_risk_reservations r USING(client_identifier) \
             WHERE o.client_identifier=?1",
            [reserved.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(sql_error)?;
    assert_eq!(
        reservation_before,
        ("RESERVED".into(), "ORDER_RESERVED".into(), "ACTIVE".into())
    );
    let reserved_target = AdmissionDeferredTarget::ReservedOrder(reserved.clone());
    assert!(store.automation_record_admission_deferral(
        &session_id,
        &reserved_target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some("private account snapshot is unavailable"),
        now,
    )?);
    let (_, reserved_fact) = latest_deferred_fact(&store, &session_id)?;
    assert_eq!(reserved_fact.phase, AdmissionDeferredPhase::BeforeSendClaim);
    assert_eq!(
        reserved_fact.reason,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired
    );
    assert_eq!(reserved_fact.client_identifier, Some(reserved.clone()));
    assert_eq!(mutation_snapshot(&store, &session_id)?, before_reserved);
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT o.state,i.state,r.state FROM automation_orders o \
                 JOIN automation_intents i USING(intent_id) \
                 JOIN automation_risk_reservations r USING(client_identifier) \
                 WHERE o.client_identifier=?1",
                [reserved.as_str()],
                |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?
                )),
            )
            .map_err(sql_error)?,
        reservation_before
    );
    assert!(!store.automation_record_admission_deferral(
        &session_id,
        &reserved_target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some("private account snapshot is unavailable"),
        now,
    )?);
    assert!(matches!(
        store.automation_record_admission_deferral(
            &session_id,
            &reserved_target,
            AdmissionDeferredReason::EffectAuthorizationBlocked,
            None,
            now,
        ),
        Err(LabError::InvalidConfig(_))
    ));

    let other_session = seed_running_automation_session(
        &mut store,
        "other-order-owner",
        "KRW-ETH",
        "mock:upbit-default",
        &baseline_test_risk_policy()?,
        time("2026-10-14T00:30:00Z")?,
    )?;
    assert!(matches!(
        store.automation_record_admission_deferral(
            &other_session,
            &reserved_target,
            AdmissionDeferredReason::PrivateAccountRecoveryRequired,
            None,
            now,
        ),
        Err(LabError::Conflict(_))
    ));

    let reserved_intent: String = store
        .connection
        .query_row(
            "SELECT intent_id FROM automation_orders WHERE client_identifier=?1",
            [reserved.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    store
        .connection
        .execute(
            "UPDATE automation_intents SET state='NO_ACTION' WHERE intent_id=?1",
            [reserved_intent.as_str()],
        )
        .map_err(sql_error)?;
    let before_phase_mismatch = mutation_snapshot(&store, &session_id)?;
    assert!(!store.automation_record_admission_deferral(
        &session_id,
        &reserved_target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        None,
        now,
    )?);
    assert_eq!(
        mutation_snapshot(&store, &session_id)?,
        before_phase_mismatch
    );
    store
        .connection
        .execute(
            "UPDATE automation_intents SET state='ORDER_RESERVED' WHERE intent_id=?1",
            [reserved_intent.as_str()],
        )
        .map_err(sql_error)?;

    store
        .connection
        .execute(
            "UPDATE automation_orders SET state='SUBMITTED_UNKNOWN' WHERE client_identifier=?1",
            [reserved.as_str()],
        )
        .map_err(sql_error)?;
    store
        .connection
        .execute(
            "UPDATE automation_intents SET state='ORDER_SUBMITTED' WHERE intent_id=?1",
            [reserved_intent.as_str()],
        )
        .map_err(sql_error)?;
    let event_count_after_transition = deferred_count(&store, &session_id)?;
    assert!(!store.automation_record_admission_deferral(
        &session_id,
        &reserved_target,
        AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        Some("private account snapshot is unavailable"),
        now,
    )?);
    assert_eq!(
        deferred_count(&store, &session_id)?,
        event_count_after_transition
    );

    let claimed_now = time("2026-10-14T03:00:00Z")?;
    let claimed = seed_reserved_test_order(&mut store, &session_id, "claimed", claimed_now)?;
    let claimed_intent: String = store
        .connection
        .query_row(
            "SELECT intent_id FROM automation_orders WHERE client_identifier=?1",
            [claimed.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    store
        .connection
        .execute(
            "UPDATE automation_orders SET state='SUBMITTED_UNKNOWN' WHERE client_identifier=?1",
            [claimed.as_str()],
        )
        .map_err(sql_error)?;
    store
        .connection
        .execute(
            "UPDATE automation_intents SET state='ORDER_SUBMITTED' WHERE intent_id=?1",
            [claimed_intent.as_str()],
        )
        .map_err(sql_error)?;
    let before_claimed = mutation_snapshot(&store, &session_id)?;
    let claimed_target = AdmissionDeferredTarget::ClaimedOrder(claimed.clone());
    assert!(store.automation_record_admission_deferral(
        &session_id,
        &claimed_target,
        AdmissionDeferredReason::EffectAuthorizationBlocked,
        Some("effect authorization is blocked"),
        claimed_now,
    )?);
    let (detail, claimed_fact) = latest_deferred_fact(&store, &session_id)?;
    assert_eq!(claimed_fact.phase, AdmissionDeferredPhase::AfterSendClaim);
    assert_eq!(
        claimed_fact.reason,
        AdmissionDeferredReason::EffectAuthorizationBlocked
    );
    assert_eq!(claimed_fact.client_identifier, Some(claimed.clone()));
    assert_eq!(detail["intent_id"], claimed_intent.as_str());
    assert_eq!(
        detail["observation_id"],
        claimed_fact.observation_id.as_str()
    );
    assert_eq!(detail["client_identifier"], claimed.as_str());
    assert_eq!(detail["reason"], "EFFECT_AUTHORIZATION_BLOCKED");
    assert_eq!(mutation_snapshot(&store, &session_id)?, before_claimed);
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT o.state,i.state,r.state FROM automation_orders o \
                 JOIN automation_intents i USING(intent_id) \
                 JOIN automation_risk_reservations r USING(client_identifier) \
                 WHERE o.client_identifier=?1",
                [claimed.as_str()],
                |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?
                )),
            )
            .map_err(sql_error)?,
        (
            "SUBMITTED_UNKNOWN".into(),
            "ORDER_SUBMITTED".into(),
            "ACTIVE".into()
        )
    );
    store.close()?;
    Ok(())
}

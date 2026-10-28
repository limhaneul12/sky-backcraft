//! Bounded, consistent reads of persisted strategy-automation review facts.

use super::{Store, automation_store};
use crate::contracts::{
    AssetQuantity, AutomationDecisionDetail, AutomationDecisionPage, AutomationDecisionRecord,
    AutomationDecisionSummary, AutomationEvent, AutomationEventKind, AutomationFillView,
    AutomationIntentId, AutomationOrderIdentifier, AutomationOrderView, AutomationReviewAction,
    AutomationReviewFee, AutomationReviewFill, AutomationReviewOpeningInventory,
    AutomationReviewOrder, AutomationReviewSession, AutomationReviewSnapshot, AutomationRiskPolicy,
    AutomationSessionId, AutomationSessionMode, AutomationSessionState, CandleInterval,
    CandleObservation, ContentHash, FrozenPolicyRevision, LabError, MarketId, ObservationId,
    PolicyRevisionRef, PriceKrw, QuoteAmount, UtcTimestamp, observation_digest,
    validate_frozen_policy,
};
use rusqlite::{
    Connection, OptionalExtension, TransactionBehavior, params, params_from_iter,
    types::Value as SqlValue,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};

impl Store {
    pub(crate) fn automation_review_decisions(
        &mut self,
        session_id: &AutomationSessionId,
        offset: u32,
        limit: u32,
    ) -> Result<AutomationDecisionPage, LabError> {
        if !(1..=100).contains(&limit) {
            return Err(LabError::InvalidConfig(
                "automation decision page limit must be 1..=100".into(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(super::sql_error)?;
        check_decision_page_source_limits(&transaction, session_id, offset, limit)?;
        check_event_read_limits(&transaction, session_id)?;
        let (session, frozen_policy) = load_review_session(&transaction, session_id)?;
        let total_count = count_rows(
            &transaction,
            "SELECT COUNT(*) FROM automation_intents WHERE session_id=?1",
            session_id,
        )?;
        let decisions = load_review_decision_rows(
            &transaction,
            session_id,
            &session,
            &frozen_policy,
            Some((offset, limit)),
        )?;
        let intent_ids = decisions
            .iter()
            .map(|decision| decision.intent_id.clone())
            .collect::<Vec<_>>();
        let signal_events = load_signal_events_for_intents(&transaction, session_id, &intent_ids)?;
        for decision in &decisions {
            validate_signal_event(
                decision,
                signal_events
                    .get(&decision.intent_id)
                    .map_or(&[], Vec::as_slice),
            )?;
        }
        let items = decisions
            .into_iter()
            .map(|decision| AutomationDecisionSummary {
                intent_id: decision.intent_id,
                observation_id: decision.observation_id,
                decision_time_utc: decision.decision_time_utc,
                action: decision.action,
                state: decision.state,
            })
            .collect::<Vec<_>>();
        let returned_count = u64::try_from(items.len())
            .map_err(|_| LabError::ResourceLimit("automation decision count overflow".into()))?;
        let next_offset = if u64::from(offset) + returned_count < total_count {
            Some(offset.checked_add(limit).ok_or_else(|| {
                LabError::ResourceLimit("automation decision page offset overflow".into())
            })?)
        } else {
            None
        };
        let page = AutomationDecisionPage {
            session_id: session_id.clone(),
            mode: session.mode,
            policy_ref: frozen_policy.reference,
            items,
            returned_count,
            total_count,
            next_offset,
        };
        transaction.commit().map_err(super::sql_error)?;
        Ok(page)
    }

    pub(crate) fn automation_review_detail(
        &mut self,
        session_id: &AutomationSessionId,
        intent_id: &AutomationIntentId,
    ) -> Result<AutomationDecisionDetail, LabError> {
        let snapshot = self.automation_review_snapshot(session_id)?;
        decision_detail_from_snapshot(snapshot, intent_id)
    }

    /// Read one complete, immutable-from-the-caller's-view fact set under a single SQLite read
    /// transaction. Dynamic account-risk projections and current time are deliberately absent.
    pub(crate) fn automation_review_snapshot(
        &mut self,
        session_id: &AutomationSessionId,
    ) -> Result<AutomationReviewSnapshot, LabError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(super::sql_error)?;
        let snapshot = load_review_snapshot(&transaction, session_id)?;
        transaction.commit().map_err(super::sql_error)?;
        Ok(snapshot)
    }
}

fn load_review_session(
    connection: &Connection,
    session_id: &AutomationSessionId,
) -> Result<(AutomationReviewSession, FrozenPolicyRevision), LabError> {
    let row = connection
        .query_row(
            "SELECT mode,state,policy_ref_json,frozen_policy_json,market,decision_interval,\
                    risk_json,created_at_utc,updated_at_utc \
             FROM automation_sessions WHERE session_id=?1",
            [session_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()
        .map_err(super::sql_error)?
        .ok_or_else(|| {
            LabError::InvalidConfig(format!("unknown automation session {session_id}"))
        })?;
    let mode = parse_mode(&row.0)?;
    let policy_ref: PolicyRevisionRef = serde_json::from_str(&row.2).map_err(super::json_error)?;
    let frozen_policy: FrozenPolicyRevision =
        serde_json::from_str(&row.3).map_err(super::json_error)?;
    if frozen_policy.reference != policy_ref {
        return Err(LabError::DataCorrupt(
            "automation session policy reference differs from its frozen policy".into(),
        ));
    }
    validate_frozen_policy(&frozen_policy)?;
    let market = MarketId::parse_upbit(&row.4)?;
    let decision_interval = CandleInterval::parse_code(&row.5)?;
    let risk: AutomationRiskPolicy = serde_json::from_str(&row.6).map_err(super::json_error)?;
    risk.validate_for_mode(mode)?;
    let state = parse_session_state(&row.1)?;
    let created_at_utc = UtcTimestamp::parse_rfc3339(&row.7)?;
    let updated_at_utc = UtcTimestamp::parse_rfc3339(&row.8)?;
    if created_at_utc > updated_at_utc {
        return Err(LabError::DataCorrupt(
            "automation session update predates creation".into(),
        ));
    }
    Ok((
        AutomationReviewSession {
            session_id: session_id.clone(),
            mode,
            state,
            policy_ref,
            market,
            decision_interval,
            risk,
            created_at_utc,
            updated_at_utc,
        },
        frozen_policy,
    ))
}

const REVIEW_DECISION_SELECT: &str = "SELECT i.intent_id,i.observation_id,i.decision_time_utc,i.action,i.state,i.evaluation_json,\
            i.created_at_utc,c.observation_id,c.close_time_utc,c.content_digest,c.candle_json \
     FROM automation_intents i LEFT JOIN automation_candles c \
       ON c.session_id=i.session_id AND c.observation_id=i.observation_id \
     WHERE i.session_id=?1 ORDER BY i.decision_time_utc,i.intent_id";

type StoredReviewDecisionRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn read_review_decision_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredReviewDecisionRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
    ))
}

fn load_review_decision_rows(
    connection: &Connection,
    session_id: &AutomationSessionId,
    session: &AutomationReviewSession,
    frozen_policy: &FrozenPolicyRevision,
    page: Option<(u32, u32)>,
) -> Result<Vec<AutomationDecisionRecord>, LabError> {
    let query = if page.is_some() {
        format!("{REVIEW_DECISION_SELECT} LIMIT ?2 OFFSET ?3")
    } else {
        REVIEW_DECISION_SELECT.to_owned()
    };
    let mut statement = connection.prepare(&query).map_err(super::sql_error)?;
    let mut rows = match page {
        Some((offset, limit)) => statement
            .query(params![
                session_id.as_str(),
                i64::from(limit),
                i64::from(offset)
            ])
            .map_err(super::sql_error)?,
        None => statement
            .query([session_id.as_str()])
            .map_err(super::sql_error)?,
    };
    let mut decisions = Vec::new();
    while let Some(row) = rows.next().map_err(super::sql_error)? {
        decisions.push(decode_review_decision(
            read_review_decision_row(row).map_err(super::sql_error)?,
            session,
            frozen_policy,
            session_id,
        )?);
    }
    Ok(decisions)
}

fn decode_review_decision(
    row: StoredReviewDecisionRow,
    session: &AutomationReviewSession,
    frozen_policy: &FrozenPolicyRevision,
    session_id: &AutomationSessionId,
) -> Result<AutomationDecisionRecord, LabError> {
    let (
        raw_intent_id,
        raw_observation_id,
        raw_decision_time,
        raw_action,
        state,
        evaluation_json,
        raw_recorded_at,
        candle_observation_id,
        candle_close_time,
        candle_digest,
        candle_json,
    ) = row;
    let intent_id = AutomationIntentId::new(raw_intent_id)?;
    let observation_id = ObservationId::new(raw_observation_id)?;
    let decision_time_utc = UtcTimestamp::parse_rfc3339(&raw_decision_time)?;
    let action = parse_action(&raw_action)?;
    validate_intent_state(&state)?;
    let evaluation: serde_json::Value =
        serde_json::from_str(&evaluation_json).map_err(super::json_error)?;
    let evaluation_digest = ContentHash::of_value(&evaluation)?;
    let recorded_at_utc = UtcTimestamp::parse_rfc3339(&raw_recorded_at)?;
    let candle_row = candle_observation_id
        .zip(candle_close_time)
        .zip(candle_digest)
        .zip(candle_json)
        .map(|(((observation, close), digest), candle)| (observation, close, digest, candle))
        .ok_or_else(|| LabError::DataCorrupt("automation decision has no linked candle".into()))?;
    if candle_row.0 != observation_id.as_str() {
        return Err(LabError::DataCorrupt(
            "automation intent observation differs from its linked candle".into(),
        ));
    }
    let stored_close = UtcTimestamp::parse_rfc3339(&candle_row.1)?;
    let stored_digest = ContentHash::try_from(candle_row.2)?;
    let candle: CandleObservation =
        serde_json::from_str(&candle_row.3).map_err(super::json_error)?;
    validate_candle_link(
        &candle,
        &observation_id,
        &stored_digest,
        stored_close,
        &session.market,
        session.decision_interval,
    )?;
    if decision_time_utc != stored_close || decision_time_utc > recorded_at_utc {
        return Err(LabError::DataCorrupt(
            "automation decision time differs from its source candle or receipt".into(),
        ));
    }
    validate_prestart_policy_warmup(
        action,
        &state,
        &evaluation,
        decision_time_utc,
        recorded_at_utc,
        session.created_at_utc,
    )?;
    validate_evaluation(
        &evaluation,
        decision_time_utc,
        &frozen_policy.reference,
        action,
    )?;
    Ok(AutomationDecisionRecord {
        session_id: session_id.clone(),
        intent_id,
        observation_id,
        decision_time_utc,
        recorded_at_utc,
        action,
        state,
        candle,
        evaluation,
        evaluation_digest,
    })
}

fn validate_prestart_policy_warmup(
    action: AutomationReviewAction,
    state: &str,
    evaluation: &serde_json::Value,
    decision_time: UtcTimestamp,
    recorded_at: UtcTimestamp,
    session_created_at: UtcTimestamp,
) -> Result<(), LabError> {
    let phase = evaluation.get("phase");
    let kind = evaluation.get("kind");
    let is_warmup = [phase, kind]
        .into_iter()
        .flatten()
        .any(|marker| marker.as_str() == Some("POLICY_WARMUP"));
    let marker_values_match = [phase, kind]
        .into_iter()
        .flatten()
        .all(|marker| marker.as_str() == Some("POLICY_WARMUP"));
    if is_warmup {
        let warmup_time = evaluation
            .get("decision_time")
            .cloned()
            .map(serde_json::from_value::<UtcTimestamp>)
            .transpose()
            .map_err(super::json_error)?;
        if !marker_values_match
            || action != AutomationReviewAction::Hold
            || state != "NO_ACTION"
            || warmup_time != Some(decision_time)
        {
            return Err(LabError::DataCorrupt(
                "saved POLICY_WARMUP evidence is not a matching HOLD decision".into(),
            ));
        }
    }
    if decision_time < session_created_at
        && (!is_warmup || !marker_values_match || recorded_at < session_created_at)
    {
        return Err(LabError::DataCorrupt(
            "pre-start automation decision is not a recorded POLICY_WARMUP HOLD".into(),
        ));
    }
    Ok(())
}

fn load_review_candles(
    connection: &Connection,
    session_id: &AutomationSessionId,
    session: &AutomationReviewSession,
) -> Result<Vec<CandleObservation>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT observation_id,close_time_utc,content_digest,candle_json \
             FROM automation_candles WHERE session_id=?1 \
             ORDER BY close_time_utc,observation_id",
        )
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map([session_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    rows.into_iter()
        .map(|(raw_id, raw_close, raw_digest, raw_candle)| {
            let observation_id = ObservationId::new(raw_id)?;
            let close_time = UtcTimestamp::parse_rfc3339(&raw_close)?;
            let content_digest = ContentHash::try_from(raw_digest)?;
            let observation: CandleObservation =
                serde_json::from_str(&raw_candle).map_err(super::json_error)?;
            validate_candle_link(
                &observation,
                &observation_id,
                &content_digest,
                close_time,
                &session.market,
                session.decision_interval,
            )?;
            if close_time > session.updated_at_utc {
                return Err(LabError::DataCorrupt(
                    "persisted automation candle is later than its session record".into(),
                ));
            }
            Ok(observation)
        })
        .collect()
}

fn load_review_snapshot(
    connection: &Connection,
    session_id: &AutomationSessionId,
) -> Result<AutomationReviewSnapshot, LabError> {
    check_review_fact_and_source_byte_limits(connection, session_id)?;
    let (session, frozen_policy) = load_review_session(connection, session_id)?;
    let candles = load_review_candles(connection, session_id, &session)?;
    let decisions = load_review_decisions(connection, session_id, &session, &frozen_policy)?;
    let orders = load_review_orders(connection, session_id, &session, &decisions)?;
    let (fills, fees) = load_review_fills_and_fees(connection, session_id, &orders)?;
    let events = load_review_events(connection, session_id, &candles, &decisions, &orders)?;
    let opening_inventory = load_opening_inventory(connection, &session, &orders)?;
    let snapshot = AutomationReviewSnapshot {
        session,
        frozen_policy,
        candles,
        decisions,
        orders,
        fills,
        fee_accruals: fees,
        events,
        opening_inventory,
    };
    validate_snapshot_fact_limit(&snapshot)?;
    let encoded_len = super::serialized_json_len(&snapshot).map_err(super::json_error)?;
    if encoded_len > crate::contracts::MAX_AUTOMATION_REVIEW_BYTES {
        return Err(LabError::ResourceLimit(format!(
            "automation review snapshot is {encoded_len} bytes; limit is {}",
            crate::contracts::MAX_AUTOMATION_REVIEW_BYTES
        )));
    }
    Ok(snapshot)
}

fn load_review_decisions(
    connection: &Connection,
    session_id: &AutomationSessionId,
    session: &AutomationReviewSession,
    frozen_policy: &FrozenPolicyRevision,
) -> Result<Vec<AutomationDecisionRecord>, LabError> {
    load_review_decision_rows(connection, session_id, session, frozen_policy, None)
}

fn load_review_orders(
    connection: &Connection,
    session_id: &AutomationSessionId,
    session: &AutomationReviewSession,
    decisions: &[AutomationDecisionRecord],
) -> Result<Vec<AutomationReviewOrder>, LabError> {
    let decision_by_id = decisions
        .iter()
        .map(|decision| (decision.intent_id.clone(), decision))
        .collect::<BTreeMap<_, _>>();

    let order_count = count_rows(
        connection,
        "SELECT COUNT(*) FROM automation_orders WHERE session_id=?1",
        session_id,
    )?;
    let orders = automation_store::load_orders(
        connection,
        session_id,
        u32::try_from(order_count).map_err(|_| {
            LabError::ResourceLimit("automation review order count overflow".into())
        })?,
        0,
    )?;
    let order_metadata = load_order_metadata(connection, session_id)?;
    let orders = orders
        .into_iter()
        .map(|order| {
            let (decision_mark_krw, venue_created_at_utc) = order_metadata
                .get(&order.client_identifier)
                .copied()
                .ok_or_else(|| {
                    LabError::DataCorrupt("automation order review metadata is missing".into())
                })?;
            let decision = decision_by_id.get(&order.intent_id).ok_or_else(|| {
                LabError::DataCorrupt("automation order has no decision in its session".into())
            })?;
            let expected_side = match decision.action {
                AutomationReviewAction::Buy => "BID",
                AutomationReviewAction::Sell => "ASK",
                AutomationReviewAction::Hold => {
                    return Err(LabError::DataCorrupt("HOLD decision owns an order".into()));
                }
            };
            if order.market != session.market
                || matches!(order.side, crate::contracts::AutomationSide::Bid)
                    != (expected_side == "BID")
                || decision_mark_krw.get() <= Decimal::ZERO
            {
                return Err(LabError::DataCorrupt(
                    "automation order differs from its frozen decision identity".into(),
                ));
            }
            Ok(AutomationReviewOrder {
                order,
                decision_mark_krw,
                venue_created_at_utc,
            })
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    if orders.len() != order_metadata.len() {
        return Err(LabError::DataCorrupt(
            "automation order metadata contains an unlinked order".into(),
        ));
    }
    Ok(orders)
}

fn load_review_fills_and_fees(
    connection: &Connection,
    session_id: &AutomationSessionId,
    orders: &[AutomationReviewOrder],
) -> Result<(Vec<AutomationReviewFill>, Vec<AutomationReviewFee>), LabError> {
    let fill_count = count_rows(
        connection,
        "SELECT COUNT(*) FROM automation_fills WHERE session_id=?1",
        session_id,
    )?;
    let fills = load_review_fills(connection, session_id, fill_count)?;
    let order_by_identifier = orders
        .iter()
        .map(|review| (&review.order.client_identifier, &review.order))
        .collect::<BTreeMap<_, _>>();
    for review_fill in &fills {
        let order = order_by_identifier
            .get(&review_fill.fill.client_identifier)
            .ok_or_else(|| LabError::DataCorrupt("automation fill has no owned order".into()))?;
        if review_fill.fill.market != order.market || review_fill.fill.side != order.side {
            return Err(LabError::DataCorrupt(
                "automation fill market or side differs from its owned order".into(),
            ));
        }
    }
    let fees = load_review_fees(connection, session_id, &order_by_identifier)?;
    Ok((fills, fees))
}

fn load_review_events(
    connection: &Connection,
    session_id: &AutomationSessionId,
    candles: &[CandleObservation],
    decisions: &[AutomationDecisionRecord],
    orders: &[AutomationReviewOrder],
) -> Result<Vec<AutomationEvent>, LabError> {
    let decision_ids = decisions
        .iter()
        .map(|decision| decision.intent_id.clone())
        .collect::<BTreeSet<_>>();
    let client_ids = orders
        .iter()
        .map(|review| review.order.client_identifier.clone())
        .collect::<BTreeSet<_>>();
    let events = load_session_events(connection, session_id)?;
    validate_all_signal_events(decisions, &events)?;
    validate_candle_events(candles, &events)?;
    validate_event_links(&events, &decision_ids, &client_ids)?;
    Ok(events)
}

fn validate_snapshot_fact_limit(snapshot: &AutomationReviewSnapshot) -> Result<(), LabError> {
    let fact_count = [
        1_usize,
        snapshot.candles.len(),
        snapshot.decisions.len(),
        snapshot.orders.len(),
        snapshot.fills.len(),
        snapshot.fee_accruals.len(),
        snapshot.events.len(),
        usize::from(snapshot.opening_inventory.is_some()),
    ]
    .into_iter()
    .try_fold(0_usize, |total, count| {
        total
            .checked_add(count)
            .ok_or_else(|| LabError::ResourceLimit("automation review fact count overflow".into()))
    })?;
    if fact_count > crate::contracts::MAX_AUTOMATION_REVIEW_FACTS {
        return Err(LabError::ResourceLimit(format!(
            "automation review has {fact_count} facts; limit is {}",
            crate::contracts::MAX_AUTOMATION_REVIEW_FACTS
        )));
    }

    Ok(())
}

fn check_review_fact_and_source_byte_limits(
    connection: &Connection,
    session_id: &AutomationSessionId,
) -> Result<(), LabError> {
    let counts = connection
        .query_row(
            "SELECT \
               (SELECT COUNT(*) FROM automation_candles WHERE session_id=?1), \
               (SELECT COUNT(*) FROM automation_intents WHERE session_id=?1), \
               (SELECT COUNT(*) FROM automation_orders WHERE session_id=?1), \
               (SELECT COUNT(*) FROM automation_fills WHERE session_id=?1), \
               (SELECT COUNT(*) FROM automation_fee_accruals WHERE client_identifier IN \
                    (SELECT client_identifier FROM automation_orders WHERE session_id=?1)), \
               (SELECT COUNT(*) FROM automation_session_events WHERE session_id=?1)",
            [session_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .map_err(super::sql_error)?;
    let missing_decision_candles: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM automation_intents i LEFT JOIN automation_candles c \
               ON c.session_id=i.session_id AND c.observation_id=i.observation_id \
             WHERE i.session_id=?1 AND c.observation_id IS NULL",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(super::sql_error)?;
    if missing_decision_candles != 0 {
        return Err(LabError::DataCorrupt(
            "automation decision has no linked persisted candle".into(),
        ));
    }
    let fact_count = [counts.0, counts.1, counts.2, counts.3, counts.4, counts.5]
        .into_iter()
        .try_fold(1_u64, |total, count| {
            let count = u64::try_from(count).map_err(|_| {
                LabError::DataCorrupt("negative automation review fact count".into())
            })?;
            total.checked_add(count).ok_or_else(|| {
                LabError::ResourceLimit("automation review fact count overflow".into())
            })
        })?;
    if fact_count > crate::contracts::MAX_AUTOMATION_REVIEW_FACTS as u64 {
        return Err(LabError::ResourceLimit(format!(
            "automation review has {fact_count} facts; limit is {}",
            crate::contracts::MAX_AUTOMATION_REVIEW_FACTS
        )));
    }
    let source_bytes: i64 = connection
        .query_row(
            "SELECT COALESCE(SUM(payload_bytes),0) FROM ( \
               SELECT length(CAST(policy_ref_json AS BLOB))+length(CAST(frozen_policy_json AS BLOB))+length(CAST(risk_json AS BLOB))+length(CAST(market AS BLOB))+length(CAST(decision_interval AS BLOB)) AS payload_bytes \
                 FROM automation_sessions WHERE session_id=?1 \
               UNION ALL SELECT length(CAST(evaluation_json AS BLOB)) FROM automation_intents WHERE session_id=?1 \
               UNION ALL SELECT length(CAST(candle_json AS BLOB)) FROM automation_candles WHERE session_id=?1 \
               UNION ALL SELECT length(CAST(detail_json AS BLOB)) FROM automation_session_events WHERE session_id=?1 \
               UNION ALL SELECT length(CAST(order_request_json AS BLOB))+length(CAST(rules_snapshot_json AS BLOB))+length(CAST(decision_mark_krw AS BLOB))+length(CAST(requested_notional_krw AS BLOB))+length(CAST(reserved_fee_krw AS BLOB))+length(CAST(executed_volume AS BLOB))+length(CAST(paid_fee_krw AS BLOB)) \
                 FROM automation_orders WHERE session_id=?1 \
               UNION ALL SELECT length(CAST(price_krw AS BLOB))+length(CAST(volume AS BLOB))+length(CAST(funds_krw AS BLOB))+length(CAST(source_digest AS BLOB)) \
                 FROM automation_fills WHERE session_id=?1 \
               UNION ALL SELECT length(CAST(delta_paid_fee_krw AS BLOB))+length(CAST(cumulative_paid_fee_krw AS BLOB)) \
                 FROM automation_fee_accruals WHERE client_identifier IN \
                   (SELECT client_identifier FROM automation_orders WHERE session_id=?1) \
               UNION ALL SELECT payload_bytes FROM ( \
                   SELECT length(CAST(a.balances_json AS BLOB))+length(CAST(a.balances_digest AS BLOB)) AS payload_bytes \
                     FROM automation_account_snapshots a JOIN automation_sessions s ON s.session_id=?1 \
                    WHERE a.account_identity=s.account_identity AND a.source_mode=s.mode \
                      AND a.observed_at_utc>=s.created_at_utc \
                      AND a.observed_at_utc<(SELECT MIN(created_at_utc) FROM automation_orders WHERE session_id=?1) \
                      AND a.created_at_utc<(SELECT MIN(created_at_utc) FROM automation_orders WHERE session_id=?1) \
                    ORDER BY a.observed_at_utc DESC,a.snapshot_sequence DESC LIMIT 1 \
               ) \
             )",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(super::sql_error)?;
    let source_bytes = usize::try_from(source_bytes)
        .map_err(|_| LabError::DataCorrupt("negative automation review payload size".into()))?;
    if source_bytes > crate::contracts::MAX_AUTOMATION_REVIEW_BYTES {
        return Err(LabError::ResourceLimit(format!(
            "automation review source payload is {source_bytes} bytes; limit is {}",
            crate::contracts::MAX_AUTOMATION_REVIEW_BYTES
        )));
    }
    Ok(())
}

fn check_decision_page_source_limits(
    connection: &Connection,
    session_id: &AutomationSessionId,
    offset: u32,
    limit: u32,
) -> Result<(), LabError> {
    let bytes: Option<i64> = connection
        .query_row(
            "SELECT \
               (SELECT length(CAST(policy_ref_json AS BLOB))+length(CAST(frozen_policy_json AS BLOB))+length(CAST(risk_json AS BLOB)) \
                  FROM automation_sessions WHERE session_id=?1) + \
               COALESCE((SELECT SUM(payload_bytes) FROM ( \
                   SELECT length(CAST(i.evaluation_json AS BLOB))+length(CAST(c.candle_json AS BLOB)) AS payload_bytes \
                     FROM automation_intents i JOIN automation_candles c \
                       ON c.session_id=i.session_id AND c.observation_id=i.observation_id \
                    WHERE i.session_id=?1 ORDER BY i.decision_time_utc,i.intent_id LIMIT ?2 OFFSET ?3 \
               )),0)",
            params![session_id.as_str(), i64::from(limit), i64::from(offset)],
            |row| row.get(0),
        )
        .map_err(super::sql_error)?;
    let bytes = bytes.ok_or_else(|| {
        LabError::InvalidConfig(format!("unknown automation session {session_id}"))
    })?;
    let bytes = usize::try_from(bytes)
        .map_err(|_| LabError::DataCorrupt("negative automation decision page size".into()))?;
    if bytes > crate::contracts::MAX_AUTOMATION_REVIEW_BYTES {
        return Err(LabError::ResourceLimit(format!(
            "automation decision page source is {bytes} bytes; limit is {}",
            crate::contracts::MAX_AUTOMATION_REVIEW_BYTES
        )));
    }
    Ok(())
}

fn check_event_read_limits(
    connection: &Connection,
    session_id: &AutomationSessionId,
) -> Result<(), LabError> {
    let (count, bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*),COALESCE(SUM(length(CAST(detail_json AS BLOB))),0) \
             FROM automation_session_events WHERE session_id=?1",
            [session_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(super::sql_error)?;
    if u64::try_from(count)
        .map_err(|_| LabError::DataCorrupt("negative automation event count".into()))?
        > crate::contracts::MAX_AUTOMATION_REVIEW_FACTS as u64
        || usize::try_from(bytes)
            .map_err(|_| LabError::DataCorrupt("negative automation event byte count".into()))?
            > crate::contracts::MAX_AUTOMATION_REVIEW_BYTES
    {
        return Err(LabError::ResourceLimit(
            "automation decision event-link lookup exceeds review limits".into(),
        ));
    }
    Ok(())
}

fn load_order_metadata(
    connection: &Connection,
    session_id: &AutomationSessionId,
) -> Result<BTreeMap<AutomationOrderIdentifier, (PriceKrw, Option<UtcTimestamp>)>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT client_identifier,decision_mark_krw,venue_created_at_utc \
             FROM automation_orders WHERE session_id=?1 ORDER BY created_at_utc,client_identifier",
        )
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map([session_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    rows.into_iter()
        .map(|(identifier, mark, venue_created)| {
            let mark = Decimal::from_str_exact(&mark).map_err(|error| {
                LabError::DataCorrupt(format!("invalid stored order decision mark: {error}"))
            })?;
            let identifier = AutomationOrderIdentifier::new(identifier)?;
            let venue_created = venue_created
                .map(|value| UtcTimestamp::parse_rfc3339(&value))
                .transpose()?;
            if mark <= Decimal::ZERO {
                return Err(LabError::DataCorrupt(
                    "stored order decision mark must be positive".into(),
                ));
            }
            Ok((identifier, (PriceKrw::new(mark)?, venue_created)))
        })
        .collect()
}

struct OpeningSnapshotMetadata {
    sequence: u64,
    observed_at_utc: UtcTimestamp,
    payload_bytes: usize,
    balances_digest: ContentHash,
}

fn load_opening_inventory(
    connection: &Connection,
    session: &AutomationReviewSession,
    orders: &[AutomationReviewOrder],
) -> Result<Option<AutomationReviewOpeningInventory>, LabError> {
    let Some(first_order_at) = orders.iter().map(|order| order.order.created_at_utc).min() else {
        return Ok(None);
    };
    let account_identity: String = connection
        .query_row(
            "SELECT account_identity FROM automation_sessions WHERE session_id=?1",
            [session.session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(super::sql_error)?;
    let Some(metadata) =
        find_opening_snapshot(connection, session, &account_identity, first_order_at)?
    else {
        return Ok(None);
    };
    let balances = load_opening_balances(connection, &metadata)?;
    let base_quantity = opening_base_quantity(&balances, session.market.base.code())?;
    Ok(Some(AutomationReviewOpeningInventory {
        snapshot_sequence: metadata.sequence,
        source_mode: session.mode,
        observed_at_utc: metadata.observed_at_utc,
        base_quantity,
        balances_digest: metadata.balances_digest,
    }))
}

fn find_opening_snapshot(
    connection: &Connection,
    session: &AutomationReviewSession,
    account_identity: &str,
    first_order_at: UtcTimestamp,
) -> Result<Option<OpeningSnapshotMetadata>, LabError> {
    let row = connection
        .query_row(
            "SELECT snapshot_sequence,observed_at_utc,created_at_utc,length(CAST(balances_json AS BLOB)),balances_digest \
             FROM automation_account_snapshots WHERE account_identity=?1 AND source_mode=?2 \
               AND observed_at_utc>=?3 AND observed_at_utc<?4 AND created_at_utc<?4 \
             ORDER BY observed_at_utc DESC,snapshot_sequence DESC LIMIT 1",
            params![
                account_identity,
                mode_name(session.mode),
                session.created_at_utc.to_rfc3339(),
                first_order_at.to_rfc3339(),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()
        .map_err(super::sql_error)?;
    let Some((raw_sequence, raw_observed, raw_recorded, raw_bytes, raw_digest)) = row else {
        return Ok(None);
    };
    let sequence = u64::try_from(raw_sequence)
        .map_err(|_| LabError::DataCorrupt("negative account snapshot sequence".into()))?;
    let observed_at_utc = UtcTimestamp::parse_rfc3339(&raw_observed)?;
    let recorded_at_utc = UtcTimestamp::parse_rfc3339(&raw_recorded)?;
    if observed_at_utc < session.created_at_utc
        || observed_at_utc >= first_order_at
        || recorded_at_utc < observed_at_utc
        || recorded_at_utc >= first_order_at
    {
        return Err(LabError::DataCorrupt(
            "opening account snapshot timestamps are inconsistent".into(),
        ));
    }
    let age = first_order_at.0 - observed_at_utc.0;
    if age < chrono::Duration::zero()
        || age > chrono::Duration::seconds(i64::from(session.risk.max_balance_age_seconds))
    {
        return Ok(None);
    }
    let payload_bytes = usize::try_from(raw_bytes)
        .map_err(|_| LabError::DataCorrupt("negative account snapshot payload size".into()))?;
    if payload_bytes > crate::contracts::MAX_AUTOMATION_REVIEW_BYTES {
        return Err(LabError::ResourceLimit(format!(
            "opening account snapshot is {payload_bytes} bytes; limit is {}",
            crate::contracts::MAX_AUTOMATION_REVIEW_BYTES
        )));
    }
    Ok(Some(OpeningSnapshotMetadata {
        sequence,
        observed_at_utc,
        payload_bytes,
        balances_digest: ContentHash::try_from(raw_digest)?,
    }))
}

fn load_opening_balances(
    connection: &Connection,
    metadata: &OpeningSnapshotMetadata,
) -> Result<Vec<crate::upbit::client::UpbitAccountBalance>, LabError> {
    let raw_sequence = i64::try_from(metadata.sequence)
        .map_err(|_| LabError::ResourceLimit("account snapshot sequence exceeds SQLite".into()))?;
    let balances_json: String = connection
        .query_row(
            "SELECT balances_json FROM automation_account_snapshots WHERE snapshot_sequence=?1",
            [raw_sequence],
            |row| row.get(0),
        )
        .map_err(super::sql_error)?;
    if balances_json.len() != metadata.payload_bytes {
        return Err(LabError::DataCorrupt(
            "opening account snapshot payload changed inside its read transaction".into(),
        ));
    }
    let balances: Vec<crate::upbit::client::UpbitAccountBalance> =
        serde_json::from_str(&balances_json).map_err(super::json_error)?;
    if ContentHash::of_value(&balances)? != metadata.balances_digest {
        return Err(LabError::DataCorrupt(
            "opening account snapshot balances digest mismatch".into(),
        ));
    }
    Ok(balances)
}

fn opening_base_quantity(
    balances: &[crate::upbit::client::UpbitAccountBalance],
    base_currency: &str,
) -> Result<AssetQuantity, LabError> {
    let mut seen = BTreeSet::new();
    let mut base_quantity = Decimal::ZERO;
    for balance in balances {
        if !seen.insert(balance.currency.clone())
            || balance.balance < Decimal::ZERO
            || balance.locked < Decimal::ZERO
        {
            return Err(LabError::DataCorrupt(
                "opening account snapshot has duplicate or negative balances".into(),
            ));
        }
        if balance.currency == base_currency {
            base_quantity = balance.balance.checked_add(balance.locked).ok_or_else(|| {
                LabError::DataCorrupt("opening base balance exceeds Decimal".into())
            })?;
        }
    }
    AssetQuantity::new(base_quantity)
}

fn mode_name(mode: AutomationSessionMode) -> &'static str {
    match mode {
        AutomationSessionMode::Mock => "MOCK",
        AutomationSessionMode::Live => "LIVE",
    }
}

fn load_review_fills(
    connection: &Connection,
    session_id: &AutomationSessionId,
    count: u64,
) -> Result<Vec<AutomationReviewFill>, LabError> {
    let limit = i64::try_from(count).map_err(|_| {
        LabError::ResourceLimit("automation review fill count exceeds SQLite".into())
    })?;
    let mut statement = connection
        .prepare(
            "SELECT trade_uuid,client_identifier,market,side,price_krw,volume,funds_krw,\
                    trade_time_utc,source_digest,created_at_utc \
             FROM automation_fills WHERE session_id=?1 ORDER BY trade_time_utc,trade_uuid LIMIT ?2",
        )
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map(params![session_id.as_str(), limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
            ))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    rows.into_iter()
        .map(|row| {
            let fill = AutomationFillView {
                trade_uuid: row.0,
                client_identifier: AutomationOrderIdentifier::new(row.1)?,
                market: MarketId::parse_upbit(&row.2)?,
                side: parse_side(&row.3)?,
                price_krw: PriceKrw::new(parse_decimal(&row.4)?)?,
                volume: crate::contracts::AssetQuantity::new(parse_decimal(&row.5)?)?,
                funds_krw: QuoteAmount::new(parse_decimal(&row.6)?)?,
                observed_at_utc: UtcTimestamp::parse_rfc3339(&row.7)?,
            };
            let stored_trade_time_utc = UtcTimestamp::parse_rfc3339(&row.7)?;
            let recorded_at_utc = UtcTimestamp::parse_rfc3339(&row.9)?;
            if recorded_at_utc < stored_trade_time_utc {
                return Err(LabError::DataCorrupt(
                    "automation fill receipt predates its venue trade time".into(),
                ));
            }
            Ok(AutomationReviewFill {
                fill,
                stored_trade_time_utc,
                recorded_at_utc,
                source_digest: ContentHash::try_from(row.8)?,
            })
        })
        .collect()
}

fn load_review_fees(
    connection: &Connection,
    session_id: &AutomationSessionId,
    orders: &BTreeMap<&AutomationOrderIdentifier, &AutomationOrderView>,
) -> Result<Vec<AutomationReviewFee>, LabError> {
    let mut statement = connection
        .prepare(
            "SELECT f.client_identifier,f.sequence,f.delta_paid_fee_krw,f.cumulative_paid_fee_krw,f.observed_at_utc \
             FROM automation_fee_accruals f JOIN automation_orders o USING(client_identifier) \
             WHERE o.session_id=?1 ORDER BY f.client_identifier,f.sequence",
        )
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map([session_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    let mut previous = BTreeMap::<String, (u64, Decimal)>::new();
    let mut result = Vec::with_capacity(rows.len());
    for (raw_identifier, raw_sequence, raw_delta, raw_cumulative, observed) in rows {
        let identifier = AutomationOrderIdentifier::new(raw_identifier)?;
        let order = orders.get(&identifier).ok_or_else(|| {
            LabError::DataCorrupt("automation fee accrual has no session-owned order".into())
        })?;
        let sequence = u64::try_from(raw_sequence)
            .map_err(|_| LabError::DataCorrupt("negative automation fee sequence".into()))?;
        let delta = parse_decimal(&raw_delta)?;
        let cumulative = parse_decimal(&raw_cumulative)?;
        let expected_previous = previous
            .get(identifier.as_str())
            .copied()
            .unwrap_or((0, Decimal::ZERO));
        let expected_sequence = expected_previous
            .0
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("automation fee sequence overflow".into()))?;
        let expected_cumulative = expected_previous.1.checked_add(delta).ok_or_else(|| {
            LabError::DataCorrupt("automation cumulative fee exceeds Decimal".into())
        })?;
        if sequence != expected_sequence
            || delta < Decimal::ZERO
            || cumulative != expected_cumulative
            || cumulative > order.paid_fee_krw.get()
        {
            return Err(LabError::DataCorrupt(
                "automation fee accrual sequence or cumulative amount is inconsistent".into(),
            ));
        }
        previous.insert(identifier.to_string(), (sequence, cumulative));
        result.push(AutomationReviewFee {
            client_identifier: identifier,
            sequence,
            delta_paid_fee_krw: QuoteAmount::new(delta)?,
            cumulative_paid_fee_krw: QuoteAmount::new(cumulative)?,
            observed_at_utc: UtcTimestamp::parse_rfc3339(&observed)?,
        });
    }
    Ok(result)
}

fn load_session_events(
    connection: &Connection,
    session_id: &AutomationSessionId,
) -> Result<Vec<AutomationEvent>, LabError> {
    let event_count = count_rows(
        connection,
        "SELECT COUNT(*) FROM automation_session_events WHERE session_id=?1",
        session_id,
    )?;
    if event_count > crate::contracts::MAX_AUTOMATION_REVIEW_FACTS as u64 {
        return Err(LabError::ResourceLimit(format!(
            "automation session has {event_count} events; review limit is {}",
            crate::contracts::MAX_AUTOMATION_REVIEW_FACTS
        )));
    }
    let mut statement = connection
        .prepare(
            "SELECT sequence,kind,at_utc,detail_json FROM automation_session_events \
             WHERE session_id=?1 ORDER BY sequence",
        )
        .map_err(super::sql_error)?;
    let rows = statement
        .query_map([session_id.as_str()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(super::sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::sql_error)?;
    rows.into_iter()
        .map(|(sequence, kind, at, detail)| {
            let sequence = u64::try_from(sequence)
                .map_err(|_| LabError::DataCorrupt("negative automation event sequence".into()))?;
            if sequence == 0 {
                return Err(LabError::DataCorrupt(
                    "automation event sequence must be positive".into(),
                ));
            }
            let kind = parse_event_kind(&kind)?;
            let detail: serde_json::Value =
                serde_json::from_str(&detail).map_err(super::json_error)?;
            Ok(AutomationEvent {
                session_id: session_id.clone(),
                sequence,
                kind,
                at_utc: UtcTimestamp::parse_rfc3339(&at)?,
                detail,
            })
        })
        .collect()
}

fn load_signal_events_for_intents(
    connection: &Connection,
    session_id: &AutomationSessionId,
    intent_ids: &[AutomationIntentId],
) -> Result<BTreeMap<AutomationIntentId, Vec<AutomationEvent>>, LabError> {
    if intent_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let selected_ids = intent_ids.iter().cloned().collect::<BTreeSet<_>>();
    let placeholders = (2..=intent_ids.len() + 1)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "SELECT sequence,kind,at_utc,detail_json,\
                json_extract(detail_json,'$.intent_id') \
         FROM automation_session_events WHERE session_id=?1 \
           AND kind='SIGNAL_OBSERVED' \
           AND json_extract(detail_json,'$.intent_id') IN ({placeholders}) \
         ORDER BY sequence"
    );
    let mut values = Vec::with_capacity(intent_ids.len() + 1);
    values.push(SqlValue::Text(session_id.to_string()));
    values.extend(
        intent_ids
            .iter()
            .map(|intent_id| SqlValue::Text(intent_id.to_string())),
    );
    let mut statement = connection.prepare(&query).map_err(super::sql_error)?;
    let mut rows = statement
        .query(params_from_iter(values.iter()))
        .map_err(super::sql_error)?;
    let mut grouped = BTreeMap::<AutomationIntentId, Vec<AutomationEvent>>::new();
    while let Some(row) = rows.next().map_err(super::sql_error)? {
        let sequence = row.get::<_, i64>(0).map_err(super::sql_error)?;
        let kind = row.get::<_, String>(1).map_err(super::sql_error)?;
        let at = row.get::<_, String>(2).map_err(super::sql_error)?;
        let detail_json = row.get::<_, String>(3).map_err(super::sql_error)?;
        let raw_intent_id = row.get::<_, String>(4).map_err(super::sql_error)?;
        let intent_id = AutomationIntentId::new(raw_intent_id)?;
        if !selected_ids.contains(&intent_id) {
            return Err(LabError::DataCorrupt(
                "signal event query returned an unselected decision".into(),
            ));
        }
        let event = AutomationEvent {
            session_id: session_id.clone(),
            sequence: u64::try_from(sequence)
                .map_err(|_| LabError::DataCorrupt("negative automation signal sequence".into()))?,
            kind: parse_event_kind(&kind)?,
            at_utc: UtcTimestamp::parse_rfc3339(&at)?,
            detail: serde_json::from_str(&detail_json).map_err(super::json_error)?,
        };
        grouped.entry(intent_id).or_default().push(event);
    }
    Ok(grouped)
}

fn event_matches(
    detail: &serde_json::Value,
    intent_ids: &BTreeSet<AutomationIntentId>,
    client_ids: &BTreeSet<AutomationOrderIdentifier>,
) -> bool {
    detail
        .get("intent_id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| intent_ids.iter().any(|id| id.as_str() == value))
        || detail
            .get("client_identifier")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| client_ids.iter().any(|id| id.as_str() == value))
}

fn validate_all_signal_events(
    decisions: &[AutomationDecisionRecord],
    events: &[AutomationEvent],
) -> Result<(), LabError> {
    // Validate each event ID before lookup; borrow canonical ID text to avoid cloning the index.
    let decision_by_id = decisions
        .iter()
        .map(|decision| (decision.intent_id.as_str(), decision))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeMap::<&AutomationIntentId, usize>::new();
    for event in events {
        if event.kind == AutomationEventKind::SignalObserved {
            let raw_intent = event
                .detail
                .get("intent_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    LabError::DataCorrupt("signal event has no intent identity".into())
                })?;
            let intent_id = AutomationIntentId::new(raw_intent.to_owned())?;
            let decision = decision_by_id
                .get(intent_id.as_str())
                .copied()
                .ok_or_else(|| {
                    LabError::DataCorrupt("signal event has no stored decision".into())
                })?;
            validate_signal_event(decision, std::slice::from_ref(event))?;
            *seen.entry(&decision.intent_id).or_default() += 1;
        }
    }
    if decisions
        .iter()
        .any(|decision| seen.get(&decision.intent_id).copied() != Some(1))
    {
        return Err(LabError::DataCorrupt(
            "stored automation decision does not have exactly one matching signal event".into(),
        ));
    }
    Ok(())
}

fn validate_candle_events(
    candles: &[CandleObservation],
    events: &[AutomationEvent],
) -> Result<(), LabError> {
    let by_id = candles
        .iter()
        .map(|candle| (&candle.id, candle))
        .collect::<BTreeMap<_, _>>();
    let mut observed = BTreeSet::new();
    for event in events
        .iter()
        .filter(|event| event.kind == AutomationEventKind::CandleObserved)
    {
        let observation_id = event
            .detail
            .get("observation_id")
            .cloned()
            .map(serde_json::from_value::<ObservationId>)
            .transpose()
            .map_err(super::json_error)?
            .ok_or_else(|| {
                LabError::DataCorrupt("candle event has no observation identity".into())
            })?;
        let observation = by_id.get(&observation_id).ok_or_else(|| {
            LabError::DataCorrupt("candle event has no persisted observation".into())
        })?;
        let close_time = event
            .detail
            .get("close_time_utc")
            .cloned()
            .map(serde_json::from_value::<UtcTimestamp>)
            .transpose()
            .map_err(super::json_error)?;
        let digest = event
            .detail
            .get("content_digest")
            .cloned()
            .map(serde_json::from_value::<ContentHash>)
            .transpose()
            .map_err(super::json_error)?;
        if close_time != Some(observation.candle.close_time_utc)
            || digest.as_ref() != Some(&observation.content_digest)
            || !observed.insert(observation_id)
        {
            return Err(LabError::DataCorrupt(
                "candle event differs from its persisted observation or is duplicated".into(),
            ));
        }
    }
    if observed.len() != candles.len() {
        return Err(LabError::DataCorrupt(
            "persisted automation candle has no unique observation event".into(),
        ));
    }
    Ok(())
}

fn validate_signal_event(
    decision: &AutomationDecisionRecord,
    events: &[AutomationEvent],
) -> Result<(), LabError> {
    let matching = events
        .iter()
        .filter(|event| {
            event.kind == AutomationEventKind::SignalObserved
                && event
                    .detail
                    .get("intent_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(decision.intent_id.as_str())
        })
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err(LabError::DataCorrupt(
            "automation decision does not have exactly one signal event".into(),
        ));
    }
    let detail = &matching[0].detail;
    let action = detail.get("action").and_then(serde_json::Value::as_str);
    let decision_time = detail
        .get("decision_time_utc")
        .cloned()
        .map(serde_json::from_value::<UtcTimestamp>)
        .transpose()
        .map_err(super::json_error)?;
    let digest = detail
        .get("evaluation_digest")
        .cloned()
        .map(serde_json::from_value::<ContentHash>)
        .transpose()
        .map_err(super::json_error)?;
    if action != Some(action_name(decision.action))
        || decision_time != Some(decision.decision_time_utc)
        || digest.as_ref() != Some(&decision.evaluation_digest)
    {
        return Err(LabError::DataCorrupt(
            "signal event identity or evaluation digest differs from persisted decision".into(),
        ));
    }
    Ok(())
}

fn validate_event_links(
    events: &[AutomationEvent],
    intent_ids: &BTreeSet<AutomationIntentId>,
    client_ids: &BTreeSet<AutomationOrderIdentifier>,
) -> Result<(), LabError> {
    for event in events {
        if let Some(raw) = event
            .detail
            .get("intent_id")
            .and_then(serde_json::Value::as_str)
        {
            let id = AutomationIntentId::new(raw.to_owned())?;
            if !intent_ids.contains(&id) {
                return Err(LabError::DataCorrupt(
                    "automation event intent belongs to another or missing decision".into(),
                ));
            }
        }
        if let Some(raw) = event
            .detail
            .get("client_identifier")
            .and_then(serde_json::Value::as_str)
        {
            let id = AutomationOrderIdentifier::new(raw.to_owned())?;
            if !client_ids.contains(&id) {
                return Err(LabError::DataCorrupt(
                    "automation event order belongs to another or missing session order".into(),
                ));
            }
        }
    }
    Ok(())
}

fn decision_detail_from_snapshot(
    snapshot: AutomationReviewSnapshot,
    intent_id: &AutomationIntentId,
) -> Result<AutomationDecisionDetail, LabError> {
    if snapshot.frozen_policy.reference != snapshot.session.policy_ref {
        return Err(LabError::DataCorrupt(
            "automation decision detail session and frozen policy identity differ".into(),
        ));
    }
    let decision = snapshot
        .decisions
        .iter()
        .find(|decision| &decision.intent_id == intent_id)
        .cloned()
        .ok_or_else(|| LabError::InvalidConfig(format!("unknown automation intent {intent_id}")))?;
    if decision.session_id != snapshot.session.session_id {
        return Err(LabError::DataCorrupt(
            "automation decision detail belongs to another session".into(),
        ));
    }
    let mut context_candles = snapshot
        .candles
        .iter()
        .filter(|candle| candle.candle.close_time_utc <= decision.decision_time_utc)
        .cloned()
        .collect::<Vec<_>>();
    if context_candles.len() > 64 {
        context_candles.drain(..context_candles.len() - 64);
    }
    if context_candles.iter().any(|candle| {
        candle.candle.close_time_utc > decision.decision_time_utc
            || candle.candle.close_time_utc > decision.candle.candle.close_time_utc
    }) {
        return Err(LabError::DataCorrupt(
            "automation review context contains a non-prior candle".into(),
        ));
    }
    let orders: Vec<_> = snapshot
        .orders
        .iter()
        .filter(|order| order.order.intent_id == decision.intent_id)
        .map(|order| order.order.clone())
        .collect();
    let order_ids = snapshot
        .orders
        .iter()
        .filter(|order| order.order.intent_id == decision.intent_id)
        .map(|order| order.order.client_identifier.clone())
        .collect::<BTreeSet<_>>();
    let intent_ids = BTreeSet::from([decision.intent_id.clone()]);
    let selected_events = snapshot
        .events
        .iter()
        .filter(|event| {
            event_matches(&event.detail, &intent_ids, &order_ids)
                || event
                    .detail
                    .get("observation_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(decision.observation_id.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    let fills = snapshot
        .fills
        .iter()
        .filter(|fill| order_ids.contains(&fill.fill.client_identifier))
        .map(|fill| fill.fill.clone())
        .collect::<Vec<_>>();
    let recorded_decision = crate::reporting::decision_explanation::automation_recorded_decision(
        &snapshot.session.policy_ref,
        &decision,
        &orders,
        &fills,
        &selected_events,
    )?;
    let events = snapshot
        .events
        .into_iter()
        .filter(|event| {
            event_matches(&event.detail, &intent_ids, &order_ids)
                || event
                    .detail
                    .get("observation_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(decision.observation_id.as_str())
        })
        .collect();
    Ok(AutomationDecisionDetail {
        session: snapshot.session,
        frozen_policy: snapshot.frozen_policy,
        decision,
        context_candles,
        orders,
        events,
        recorded_decision: Some(recorded_decision),
    })
}

fn validate_candle_link(
    observation: &CandleObservation,
    expected_id: &ObservationId,
    expected_digest: &ContentHash,
    expected_close: UtcTimestamp,
    market: &MarketId,
    interval: CandleInterval,
) -> Result<(), LabError> {
    let candle = &observation.candle;
    if &observation.id != expected_id
        || observation.id != ObservationId::from_seed(observation.content_digest.as_str())
        || &observation.content_digest != expected_digest
        || observation.content_digest != observation_digest(candle)?
        || candle.market != market.code()
        || candle.interval != interval
        || candle.close_time_utc != expected_close
        || !candle.completed
        || candle.open_time_utc >= candle.close_time_utc
        || candle.close_time_utc.0 - candle.open_time_utc.0 != interval.duration()
    {
        return Err(LabError::DataCorrupt(
            "automation decision candle content or identity link is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_evaluation(
    evaluation: &serde_json::Value,
    decision_time: UtcTimestamp,
    policy_ref: &PolicyRevisionRef,
    action: AutomationReviewAction,
) -> Result<(), LabError> {
    let Some(object) = evaluation.as_object() else {
        return Err(LabError::DataCorrupt(
            "automation evaluation is not an object".into(),
        ));
    };
    if let Some(raw_time) = object.get("decision_time_utc") {
        let time: UtcTimestamp =
            serde_json::from_value(raw_time.clone()).map_err(super::json_error)?;
        if time != decision_time {
            return Err(LabError::DataCorrupt(
                "automation evaluation decision time differs from its candle".into(),
            ));
        }
    }
    if let Some(raw_ref) = object.get("policy_ref").filter(|value| !value.is_null()) {
        let embedded: PolicyRevisionRef =
            serde_json::from_value(raw_ref.clone()).map_err(super::json_error)?;
        if &embedded != policy_ref {
            return Err(LabError::DataCorrupt(
                "automation evaluation policy identity differs from the frozen revision".into(),
            ));
        }
    }
    if let Some(nested) = object.get("policy_evaluation") {
        match nested {
            serde_json::Value::Null if action == AutomationReviewAction::Hold => {}
            serde_json::Value::Null => {
                return Err(LabError::DataCorrupt(
                    "non-HOLD automation decision has no policy evaluation".into(),
                ));
            }
            value => validate_evaluation(value, decision_time, policy_ref, action)?,
        }
    }
    Ok(())
}

fn parse_mode(raw: &str) -> Result<AutomationSessionMode, LabError> {
    match raw {
        "MOCK" => Ok(AutomationSessionMode::Mock),
        "LIVE" => Ok(AutomationSessionMode::Live),
        _ => Err(LabError::DataCorrupt(
            "unknown automation review mode".into(),
        )),
    }
}

fn parse_session_state(raw: &str) -> Result<AutomationSessionState, LabError> {
    match raw {
        "RUNNING" => Ok(AutomationSessionState::Running),
        "PAUSED" => Ok(AutomationSessionState::Paused),
        "STOPPED" => Ok(AutomationSessionState::Stopped),
        "NEEDS_RECONCILIATION" => Ok(AutomationSessionState::NeedsReconciliation),
        "BLOCKED_APPROVAL" => Ok(AutomationSessionState::BlockedApproval),
        "FAULTED" => Ok(AutomationSessionState::Faulted),
        _ => Err(LabError::DataCorrupt(
            "unknown automation review session state".into(),
        )),
    }
}

fn parse_action(raw: &str) -> Result<AutomationReviewAction, LabError> {
    match raw {
        "BUY" => Ok(AutomationReviewAction::Buy),
        "SELL" => Ok(AutomationReviewAction::Sell),
        "HOLD" => Ok(AutomationReviewAction::Hold),
        _ => Err(LabError::DataCorrupt(
            "unknown automation decision action".into(),
        )),
    }
}

fn action_name(action: AutomationReviewAction) -> &'static str {
    match action {
        AutomationReviewAction::Buy => "BUY",
        AutomationReviewAction::Sell => "SELL",
        AutomationReviewAction::Hold => "HOLD",
    }
}

fn validate_intent_state(raw: &str) -> Result<(), LabError> {
    match raw {
        "NO_ACTION" | "PENDING" | "REJECTED" | "ORDER_RESERVED" | "ORDER_SUBMITTED"
        | "ORDER_UNKNOWN" | "ORDER_RECONCILED" => Ok(()),
        _ => Err(LabError::DataCorrupt(
            "unknown automation intent state".into(),
        )),
    }
}

fn parse_event_kind(raw: &str) -> Result<AutomationEventKind, LabError> {
    match raw {
        "STARTED" => Ok(AutomationEventKind::Started),
        "PAUSED" => Ok(AutomationEventKind::Paused),
        "RESUMED" => Ok(AutomationEventKind::Resumed),
        "STOPPED" => Ok(AutomationEventKind::Stopped),
        "CANDLE_OBSERVED" => Ok(AutomationEventKind::CandleObserved),
        "DECISION_DEFERRED" => Ok(AutomationEventKind::DecisionDeferred),
        "SIGNAL_OBSERVED" => Ok(AutomationEventKind::SignalObserved),
        "INTENT_REJECTED" => Ok(AutomationEventKind::IntentRejected),
        "ORDER_ADMITTED" => Ok(AutomationEventKind::OrderAdmitted),
        "ORDER_SEND_CLAIMED" => Ok(AutomationEventKind::OrderSendClaimed),
        "ORDER_ACKNOWLEDGED" => Ok(AutomationEventKind::OrderAcknowledged),
        "ORDER_CANCEL_REQUESTED" => Ok(AutomationEventKind::OrderCancelRequested),
        "ORDER_RECONCILED" => Ok(AutomationEventKind::OrderReconciled),
        "FILL_RECORDED" => Ok(AutomationEventKind::FillRecorded),
        "NEEDS_RECONCILIATION" => Ok(AutomationEventKind::NeedsReconciliation),
        "RECOVERY_BLOCKED" => Ok(AutomationEventKind::RecoveryBlocked),
        "BASELINE_RECORDED" => Ok(AutomationEventKind::BaselineRecorded),
        "BASELINE_BLOCKED" => Ok(AutomationEventKind::BaselineBlocked),
        _ => Err(LabError::DataCorrupt(
            "unknown automation event kind".into(),
        )),
    }
}

fn parse_side(raw: &str) -> Result<crate::contracts::AutomationSide, LabError> {
    match raw {
        "BID" => Ok(crate::contracts::AutomationSide::Bid),
        "ASK" => Ok(crate::contracts::AutomationSide::Ask),
        _ => Err(LabError::DataCorrupt("unknown automation fill side".into())),
    }
}

fn parse_decimal(raw: &str) -> Result<Decimal, LabError> {
    Decimal::from_str_exact(raw).map_err(|error| {
        LabError::DataCorrupt(format!("invalid stored automation decimal: {error}"))
    })
}

fn count_rows(
    connection: &Connection,
    sql: &str,
    session_id: &AutomationSessionId,
) -> Result<u64, LabError> {
    let count: i64 = connection
        .query_row(sql, [session_id.as_str()], |row| row.get(0))
        .map_err(super::sql_error)?;
    u64::try_from(count)
        .map_err(|_| LabError::DataCorrupt("negative automation review row count".into()))
}

#[cfg(test)]
mod tests;

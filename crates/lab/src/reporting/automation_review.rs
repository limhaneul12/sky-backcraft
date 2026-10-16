//! Pure private strategy review over saved facts, never current account guesses.

mod accounting;
mod export;
pub use export::{export_manifest, export_read};

use crate::contracts::{
    AutomationDecisionRecord, AutomationPerformanceReport, AutomationReviewAction,
    AutomationReviewMetric, AutomationReviewSnapshot, ContentHash, LabError,
    MAX_AUTOMATION_REVIEW_BYTES, MAX_AUTOMATION_REVIEW_FACTS, QuoteAmount, SignedAmount,
    UtcTimestamp, observation_digest, validate_frozen_policy,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};

struct PreparedReview {
    snapshot: AutomationReviewSnapshot,
    digest: ContentHash,
}

fn corrupt(message: &str) -> LabError {
    LabError::DataCorrupt(format!("automation review: {message}"))
}

fn overflow() -> LabError {
    LabError::ResourceLimit("automation review decimal/resource capacity exceeded".into())
}

fn count(value: usize) -> Result<u64, LabError> {
    u64::try_from(value).map_err(|_| overflow())
}

fn prepare(snapshot: &AutomationReviewSnapshot) -> Result<PreparedReview, LabError> {
    let total = [
        1,
        snapshot.candles.len(),
        snapshot.decisions.len(),
        snapshot.orders.len(),
        snapshot.fills.len(),
        snapshot.fee_accruals.len(),
        snapshot.events.len(),
        usize::from(snapshot.opening_inventory.is_some()),
    ]
    .into_iter()
    .try_fold(0_usize, |sum, n| sum.checked_add(n).ok_or_else(overflow))?;
    if total > MAX_AUTOMATION_REVIEW_FACTS {
        return Err(LabError::ResourceLimit(
            "automation review snapshot exceeds fact cap".into(),
        ));
    }
    validate_frozen_policy(&snapshot.frozen_policy)?;
    if snapshot.session.policy_ref != snapshot.frozen_policy.reference {
        return Err(corrupt("frozen policy identity mismatch"));
    }
    validate_decisions(snapshot)?;
    validate_ledger_links(snapshot)?;
    let mut saved = snapshot.clone();
    saved
        .candles
        .sort_by(|a, b| (a.candle.close_time_utc, &a.id).cmp(&(b.candle.close_time_utc, &b.id)));
    saved.decisions.sort_by(|a, b| {
        (a.decision_time_utc, &a.intent_id).cmp(&(b.decision_time_utc, &b.intent_id))
    });
    saved.orders.sort_by(|a, b| {
        (a.order.created_at_utc, &a.order.client_identifier)
            .cmp(&(b.order.created_at_utc, &b.order.client_identifier))
    });
    saved.fills.sort_by(|a, b| {
        (a.stored_trade_time_utc, &a.fill.trade_uuid)
            .cmp(&(b.stored_trade_time_utc, &b.fill.trade_uuid))
    });
    saved.fee_accruals.sort_by(|a, b| {
        (&a.client_identifier, a.sequence).cmp(&(&b.client_identifier, b.sequence))
    });
    saved.events.sort_by_key(|v| v.sequence);
    let bytes = serde_json::to_vec(&saved)?;
    if bytes.len() > MAX_AUTOMATION_REVIEW_BYTES {
        return Err(LabError::ResourceLimit(
            "automation review snapshot exceeds byte cap".into(),
        ));
    }
    Ok(PreparedReview {
        snapshot: saved,
        digest: ContentHash::of_bytes(&bytes),
    })
}

fn validate_decisions(snapshot: &AutomationReviewSnapshot) -> Result<(), LabError> {
    let snapshot_at = snapshot_time(snapshot);
    let expected_market = snapshot.session.market.code();
    let expected_policy = serde_json::to_value(&snapshot.session.policy_ref)?;
    let mut candles = BTreeMap::new();
    let mut candle_closes = BTreeSet::new();
    for observation in &snapshot.candles {
        let c = &observation.candle;
        if c.market != expected_market
            || c.interval != snapshot.session.decision_interval
            || !c.completed
            || c.open_time_utc >= c.close_time_utc
            || c.close_time_utc > snapshot_at
            || c.close_time_utc.0 - c.open_time_utc.0 != c.interval.duration()
            || observation_digest(c)? != observation.content_digest
            || observation.id
                != crate::contracts::ObservationId::from_seed(observation.content_digest.as_str())
            || candles.insert(&observation.id, observation).is_some()
            || !candle_closes.insert(c.close_time_utc)
        {
            return Err(corrupt(
                "saved candle identity, digest or completed interval mismatch",
            ));
        }
    }
    let mut intents = BTreeSet::new();
    let mut observations = BTreeSet::new();
    let mut closes = BTreeSet::new();
    for d in &snapshot.decisions {
        let c = &d.candle.candle;
        if d.session_id != snapshot.session.session_id
            || d.observation_id != d.candle.id
            || candles
                .get(&d.observation_id)
                .is_none_or(|saved| saved.content_digest != d.candle.content_digest)
            || c.market != expected_market
            || c.interval != snapshot.session.decision_interval
            || !c.completed
            || c.close_time_utc != d.decision_time_utc
            || c.open_time_utc >= c.close_time_utc
            || d.recorded_at_utc < d.decision_time_utc
            || c.close_time_utc.0 - c.open_time_utc.0 != c.interval.duration()
            || observation_digest(c)? != d.candle.content_digest
            || d.candle.id
                != crate::contracts::ObservationId::from_seed(d.candle.content_digest.as_str())
            || ContentHash::of_value(&d.evaluation)? != d.evaluation_digest
            || !intents.insert(&d.intent_id)
            || !observations.insert(&d.observation_id)
            || !closes.insert(d.decision_time_utc)
        {
            return Err(corrupt("decision/candle identity, time or digest mismatch"));
        }
        let e = saved_evaluation(d);
        if let Some(reference) = e.get("policy_ref")
            && !reference.is_null()
            && *reference != expected_policy
        {
            return Err(corrupt(
                "saved evaluation policy differs from frozen session",
            ));
        }
        if let Some(time) = e
            .get("decision_time_utc")
            .and_then(serde_json::Value::as_str)
            && UtcTimestamp::parse_rfc3339(time)? != d.decision_time_utc
        {
            return Err(corrupt("saved evaluation time differs from its candle"));
        }
    }
    Ok(())
}

fn validate_ledger_links(snapshot: &AutomationReviewSnapshot) -> Result<(), LabError> {
    if let Some(opening) = &snapshot.opening_inventory {
        let first_order = snapshot
            .orders
            .iter()
            .map(|o| o.order.created_at_utc)
            .min()
            .ok_or_else(|| corrupt("opening inventory reference has no first owned order"))?;
        if opening.source_mode != snapshot.session.mode
            || opening.snapshot_sequence == 0
            || opening.observed_at_utc < snapshot.session.created_at_utc
            || opening.observed_at_utc >= first_order
            || first_order.0 - opening.observed_at_utc.0
                > chrono::Duration::seconds(i64::from(
                    snapshot.session.risk.max_balance_age_seconds,
                ))
        {
            return Err(corrupt(
                "opening inventory reference is mismatched, stale or after first order",
            ));
        }
    }
    let intents: BTreeSet<_> = snapshot.decisions.iter().map(|d| &d.intent_id).collect();
    let mut orders = BTreeMap::new();
    for entry in &snapshot.orders {
        let o = &entry.order;
        if o.market != snapshot.session.market
            || !intents.contains(&o.intent_id)
            || orders.insert(&o.client_identifier, o).is_some()
        {
            return Err(corrupt("order has no unique owned decision"));
        }
    }
    let mut fills = BTreeSet::new();
    for row in &snapshot.fills {
        let f = &row.fill;
        let order = orders
            .get(&f.client_identifier)
            .ok_or_else(|| corrupt("fill has no owned order"))?;
        if f.market != snapshot.session.market
            || f.side != order.side
            || !fills.insert(&f.trade_uuid)
            || f.volume.get() <= Decimal::ZERO
            || f.funds_krw.get() <= Decimal::ZERO
        {
            return Err(corrupt("fill identity, side or amount mismatch"));
        }
    }
    let mut fees = BTreeSet::new();
    for row in &snapshot.fee_accruals {
        if !orders.contains_key(&row.client_identifier)
            || !fees.insert((&row.client_identifier, row.sequence))
        {
            return Err(corrupt("fee accrual has no unique owned order"));
        }
    }
    let mut events = BTreeSet::new();
    for event in &snapshot.events {
        if event.session_id != snapshot.session.session_id || !events.insert(event.sequence) {
            return Err(corrupt("event identity mismatch"));
        }
    }
    Ok(())
}

fn saved_evaluation(decision: &AutomationDecisionRecord) -> &serde_json::Value {
    decision
        .evaluation
        .get("policy_evaluation")
        .unwrap_or(&decision.evaluation)
}

fn is_warmup(decision: &AutomationDecisionRecord) -> bool {
    decision.action == AutomationReviewAction::Hold
        && ["phase", "kind"].iter().any(|key| {
            saved_evaluation(decision)
                .get(key)
                .and_then(serde_json::Value::as_str)
                == Some("POLICY_WARMUP")
        })
}

fn metric(
    value: Option<Decimal>,
    unit: &str,
    reason: &str,
) -> Result<AutomationReviewMetric, LabError> {
    Ok(AutomationReviewMetric {
        value: value.map(SignedAmount::new).transpose()?,
        unit: unit.into(),
        unavailable_reason: value.is_none().then(|| reason.into()),
    })
}

fn snapshot_time(snapshot: &AutomationReviewSnapshot) -> UtcTimestamp {
    snapshot
        .decisions
        .iter()
        .map(|d| d.recorded_at_utc)
        .chain(snapshot.orders.iter().map(|o| o.order.updated_at_utc))
        .chain(snapshot.fills.iter().map(|f| f.recorded_at_utc))
        .chain(snapshot.fee_accruals.iter().map(|f| f.observed_at_utc))
        .chain(snapshot.events.iter().map(|e| e.at_utc))
        .chain([snapshot.session.updated_at_utc])
        .max()
        .unwrap_or(snapshot.session.created_at_utc)
}

/// Calculate exact session-owned performance without assuming shared account capital.
/// # Errors
/// Rejects corrupt fact links, changed evidence, resource excess and decimal overflow.
pub fn build_performance(
    snapshot: &AutomationReviewSnapshot,
) -> Result<AutomationPerformanceReport, LabError> {
    let prepared = prepare(snapshot)?;
    performance(&prepared)
}

fn performance(prepared: &PreparedReview) -> Result<AutomationPerformanceReport, LabError> {
    let s = &prepared.snapshot;
    let result = accounting::calculate(s)?;
    let mark = result
        .mark_time
        .and_then(|time| s.candles.iter().find(|c| c.candle.close_time_utc == time));
    Ok(AutomationPerformanceReport {
        schema_version: "1.0".into(), session: s.session.clone(), snapshot_digest: prepared.digest.clone(),
        snapshot_at_utc: snapshot_time(s), decision_count: count(s.decisions.iter().filter(|d| !is_warmup(d)).count())?,
        warmup_record_count: count(s.decisions.iter().filter(|d| is_warmup(d)).count())?,
        buy_signal_count: count(s.decisions.iter().filter(|d| d.action==AutomationReviewAction::Buy).count())?,
        sell_signal_count: count(s.decisions.iter().filter(|d| d.action==AutomationReviewAction::Sell).count())?,
        hold_signal_count: count(s.decisions.iter().filter(|d| d.action==AutomationReviewAction::Hold && !is_warmup(d)).count())?,
        rejected_intent_count: count(s.decisions.iter().filter(|d| d.state=="REJECTED").count())?,
        order_count: count(s.orders.len())?, fill_count: count(s.fills.len())?,
        buy_funds_krw: QuoteAmount::new(result.buys)?, sell_funds_krw: QuoteAmount::new(result.sells)?,
        paid_fees_krw: QuoteAmount::new(result.fees)?, net_quote_cashflow_krw: SignedAmount::new(result.cashflow)?,
        realized_pnl_krw: metric(result.realized,"KRW",&result.reason)?,
        unrealized_pnl_krw: metric(result.unrealized,"KRW",&result.mark_reason)?,
        total_pnl_krw: metric(result.total,"KRW",&result.mark_reason)?,
        return_ratio: metric(None,"RATIO","NO_ATTRIBUTABLE_SESSION_CAPITAL_BASIS")?,
        max_drawdown_ratio: metric(None,"RATIO","NO_ATTRIBUTABLE_SESSION_EQUITY_SERIES")?,
        remaining_volume: result.volume.map(crate::contracts::AssetQuantity::new).transpose()?,
        remaining_cost_krw: result.cost.map(QuoteAmount::new).transpose()?,
        mark_price_krw: result.mark_price.map(crate::contracts::PriceKrw::new).transpose()?,
        mark_at_utc: result.mark_time,
        mark_observation_id: mark.map(|c|c.id.clone()),
        mark_content_digest: mark.map(|c|c.content_digest.clone()),
        opening_inventory: s.opening_inventory.clone(),
        methodology: "Session-owned fills; P&L attribution requires a verified zero opening-base reference before the first owned order; weighted-average buy cost including allocated order fees; sell proceeds net of allocated fees. Fees allocated by observed fill funds with final-fill residual. Net quote cashflow is not profit. Marks are saved completed candles strictly after the last stored fill time, not current quotes; equal timestamps are ambiguous. Shared account capital, external positions and account returns are not attributed to this session. Stored trade times retain their original receipt/provenance semantics.".into(),
        warnings: result.warnings,
    })
}

#[cfg(test)]
mod tests;

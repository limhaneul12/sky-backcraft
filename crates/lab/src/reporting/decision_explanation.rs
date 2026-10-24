//! Deterministic projections of saved decision and linked lifecycle facts.

use crate::contracts::{
    AutomationDecisionRecord, AutomationEvent, AutomationEventKind, AutomationFillView,
    AutomationOrderState, AutomationOrderView, DecisionPreparation, DecisionPreparationStatus,
    FillRecord, LabError, OrderRecord, OrderStatus, PolicyRevisionRef, PolicyTrace, ReasonCode,
    RecordedDecisionAction, RecordedDecisionExplanation, RecordedDecisionFill,
    RecordedDecisionOrder, RecordedDecisionReason, RecordedDecisionStage, RecordedExecutionState,
    SavedFactCaptureStatus, Side, SignalOutcome, SignalRecord, UtcTimestamp, Weight,
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::BTreeSet;

const DECISION_EXPLANATION_SCHEMA_VERSION: &str = "1.0";
const WARMUP_PHASE: &str = "POLICY_WARMUP";

/// Project one saved paper signal and its already cutoff-bounded lifecycle facts.
///
/// # Errors
/// Rejects lifecycle facts that do not belong to the selected signal.
pub(crate) fn paper_recorded_decision(
    signal: Option<&SignalRecord>,
    fallback_policy_ref: Option<&PolicyRevisionRef>,
    orders: &[OrderRecord],
    fills: &[FillRecord],
) -> Result<RecordedDecisionExplanation, LabError> {
    let Some(signal) = signal else {
        if !orders.is_empty() || !fills.is_empty() {
            return Err(corrupt(
                "paper lifecycle facts exist without a selected signal",
            ));
        }
        return Ok(empty_explanation(
            fallback_policy_ref.cloned(),
            None,
            None,
            SavedFactCaptureStatus::NotDecisionEvent,
            Vec::new(),
        ));
    };

    validate_paper_links(signal, orders, fills)?;
    let artificial_terminal = signal.reasons.contains(&ReasonCode::ArtificialTerminalExit);
    let policy_trace = (!artificial_terminal)
        .then_some(signal.policy_trace.as_ref())
        .flatten();
    let condition_trace = if artificial_terminal {
        Vec::new()
    } else {
        policy_trace
            .and_then(|trace| trace.condition_trace.as_ref())
            .or(signal.decision_trace.as_ref())
            .cloned()
            .unwrap_or_default()
    };
    let condition_trace_capture = if artificial_terminal {
        SavedFactCaptureStatus::NotCaptured
    } else if policy_trace
        .and_then(|trace| trace.condition_trace.as_ref())
        .is_some()
        || signal.decision_trace.is_some()
    {
        SavedFactCaptureStatus::Captured
    } else {
        SavedFactCaptureStatus::NotCaptured
    };

    let mut reasons = signal
        .reasons
        .iter()
        .copied()
        .map(reason_for_code)
        .collect::<Vec<_>>();
    if signal.outcome == SignalOutcome::OrderUnfilled && fills.is_empty() {
        push_reason(
            &mut reasons,
            stage_reason(
                RecordedDecisionStage::Execution,
                "ORDER_UNFILLED",
                "판단 시점 신호 결과가 ORDER_UNFILLED로 기록됨",
            ),
        );
    }
    for order in orders {
        push_reason(&mut reasons, reason_for_code(order.reason));
    }

    Ok(RecordedDecisionExplanation {
        schema_version: DECISION_EXPLANATION_SCHEMA_VERSION.into(),
        policy_ref: signal
            .policy_ref
            .clone()
            .or_else(|| fallback_policy_ref.cloned()),
        decision_time: Some(signal.decision_available_at),
        strategy_action: action_from_weights(
            Some(signal.raw_target_weight),
            signal.actual_weight_at_decision,
        ),
        constrained_action: action_from_constrained_facts(
            Some(signal.constrained_target_weight),
            signal.actual_weight_at_decision,
            signal.intended_side,
        ),
        execution_state: paper_execution_state(signal.outcome, orders, fills)?,
        actual_weight_at_decision: signal.actual_weight_at_decision,
        raw_target_weight: Some(signal.raw_target_weight),
        constrained_target_weight: Some(signal.constrained_target_weight),
        matched_rule_id: policy_trace.and_then(|trace| trace.matched_rule_id.clone()),
        condition_trace_capture,
        condition_trace,
        reasons,
        orders: paper_orders(orders)?,
        fills: paper_fills(fills),
    })
}

/// Project an explicit pre-evaluation preparation fact without inventing a HOLD.
#[must_use]
pub(crate) fn paper_preparation_explanation(
    policy_ref: &PolicyRevisionRef,
    preparation: &DecisionPreparation,
) -> RecordedDecisionExplanation {
    empty_explanation(
        Some(policy_ref.clone()),
        Some(preparation.decision_time),
        None,
        SavedFactCaptureStatus::NotDecisionEvent,
        vec![reason_for_preparation(preparation.status)],
    )
}

/// Project one saved automation intent and only its explicitly linked facts.
///
/// # Errors
/// Rejects mismatched session/intent/order/fill identities and malformed saved evaluation data.
pub(crate) fn automation_recorded_decision(
    policy_ref: &PolicyRevisionRef,
    decision: &AutomationDecisionRecord,
    orders: &[AutomationOrderView],
    fills: &[AutomationFillView],
    events: &[AutomationEvent],
) -> Result<RecordedDecisionExplanation, LabError> {
    validate_automation_links(decision, orders, fills, events)?;
    let evaluation = saved_evaluation(&decision.evaluation);
    let warmup = is_policy_warmup(&decision.evaluation);
    let reasons = automation_decision_reasons(decision, evaluation, warmup, orders, events)?;

    let policy_trace = saved_policy_trace(evaluation)?;
    let decision_trace = saved_decision_trace(evaluation)?;
    let (condition_trace_capture, condition_trace) = if warmup {
        (SavedFactCaptureStatus::NotDecisionEvent, Vec::new())
    } else if let Some(trace) = policy_trace
        .as_ref()
        .and_then(|trace| trace.condition_trace.as_ref())
    {
        (SavedFactCaptureStatus::Captured, trace.clone())
    } else if let Some(trace) = decision_trace {
        (SavedFactCaptureStatus::Captured, trace)
    } else {
        (SavedFactCaptureStatus::NotCaptured, Vec::new())
    };
    let actual_weight = optional_saved::<Weight>(evaluation, "actual_weight_at_decision")?;
    let raw_target = optional_saved::<Weight>(evaluation, "raw_target_weight")?;
    let constrained_target = optional_saved::<Weight>(evaluation, "constrained_target_weight")?;
    let intended_side = optional_saved::<Side>(evaluation, "intended_side")?;
    let strategy_action = if warmup {
        RecordedDecisionAction::NotEvaluated
    } else {
        action_from_weights(raw_target, actual_weight)
    };
    let constrained_action = if warmup {
        RecordedDecisionAction::NotEvaluated
    } else {
        action_from_constrained_facts(constrained_target, actual_weight, intended_side)
    };
    let execution_state = if warmup {
        RecordedExecutionState::NotEvaluated
    } else {
        automation_execution_state(decision, orders, fills)
    };
    let policy_ref_from_evaluation = optional_saved::<PolicyRevisionRef>(evaluation, "policy_ref")?;
    if policy_ref_from_evaluation
        .as_ref()
        .is_some_and(|saved| saved != policy_ref)
    {
        return Err(corrupt(
            "saved automation decision policy reference mismatch",
        ));
    }

    Ok(RecordedDecisionExplanation {
        schema_version: DECISION_EXPLANATION_SCHEMA_VERSION.into(),
        policy_ref: Some(policy_ref.clone()),
        decision_time: Some(decision.decision_time_utc),
        strategy_action,
        constrained_action,
        execution_state,
        actual_weight_at_decision: actual_weight,
        raw_target_weight: raw_target,
        constrained_target_weight: constrained_target,
        matched_rule_id: policy_trace.and_then(|trace| trace.matched_rule_id),
        condition_trace_capture,
        condition_trace,
        reasons,
        orders: automation_orders(orders, events)?,
        fills: automation_fills(fills),
    })
}

fn automation_decision_reasons(
    decision: &AutomationDecisionRecord,
    evaluation: &Value,
    warmup: bool,
    orders: &[AutomationOrderView],
    events: &[AutomationEvent],
) -> Result<Vec<RecordedDecisionReason>, LabError> {
    let mut reasons = saved_reasons(evaluation)?;
    let mut linked_event_reasons = Vec::new();
    for event in events {
        append_automation_event_reasons(event, decision, orders, &mut linked_event_reasons)?;
    }
    for reason in linked_event_reasons {
        if matches!(
            reason.code.as_str(),
            "PRIVATE_ACCOUNT_RECOVERY_REQUIRED" | "EFFECT_AUTHORIZATION_BLOCKED"
        ) {
            push_distinct_admission_deferred_reason(&mut reasons, reason);
        } else {
            push_reason(&mut reasons, reason);
        }
    }
    if !warmup
        && decision.state == "REJECTED"
        && !reasons
            .iter()
            .any(|reason| reason.stage == RecordedDecisionStage::Admission)
    {
        push_reason(
            &mut reasons,
            stage_reason(
                RecordedDecisionStage::Admission,
                "REJECTED",
                "저장된 intent가 거부 상태임",
            ),
        );
    }
    if warmup {
        push_reason(
            &mut reasons,
            reason_for_preparation(DecisionPreparationStatus::PolicyWarmup),
        );
    }
    for saved in [&decision.evaluation, evaluation] {
        if let Some(status) = saved
            .get("preparation")
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
        {
            push_reason(&mut reasons, reason_for_preparation_code(status));
        }
        if let Some(disposition) = saved.get("recovery_disposition").and_then(Value::as_str) {
            push_reason(&mut reasons, reason_for_preparation_code(disposition));
        }
    }
    Ok(reasons)
}

fn empty_explanation(
    policy_ref: Option<PolicyRevisionRef>,
    decision_time: Option<UtcTimestamp>,
    matched_rule_id: Option<String>,
    condition_trace_capture: SavedFactCaptureStatus,
    reasons: Vec<RecordedDecisionReason>,
) -> RecordedDecisionExplanation {
    RecordedDecisionExplanation {
        schema_version: DECISION_EXPLANATION_SCHEMA_VERSION.into(),
        policy_ref,
        decision_time,
        strategy_action: RecordedDecisionAction::NotEvaluated,
        constrained_action: RecordedDecisionAction::NotEvaluated,
        execution_state: RecordedExecutionState::NotEvaluated,
        actual_weight_at_decision: None,
        raw_target_weight: None,
        constrained_target_weight: None,
        matched_rule_id,
        condition_trace_capture,
        condition_trace: Vec::new(),
        reasons,
        orders: Vec::new(),
        fills: Vec::new(),
    }
}

fn action_from_weights(
    raw_target: Option<Weight>,
    actual_weight: Option<Weight>,
) -> RecordedDecisionAction {
    match raw_target.zip(actual_weight) {
        Some((target, actual)) if target.get() > actual.get() => RecordedDecisionAction::Buy,
        Some((target, actual)) if target.get() < actual.get() => RecordedDecisionAction::Sell,
        Some(_) => RecordedDecisionAction::Hold,
        None => RecordedDecisionAction::NotCaptured,
    }
}

fn action_from_side(side: Option<Side>) -> RecordedDecisionAction {
    match side {
        Some(Side::Buy) => RecordedDecisionAction::Buy,
        Some(Side::Sell) => RecordedDecisionAction::Sell,
        None => RecordedDecisionAction::NotCaptured,
    }
}

fn action_from_constrained_facts(
    constrained_target: Option<Weight>,
    actual_weight: Option<Weight>,
    intended_side: Option<Side>,
) -> RecordedDecisionAction {
    let Some((target, actual)) = constrained_target.zip(actual_weight) else {
        return action_from_side(intended_side);
    };
    let derived = action_from_weights(Some(target), Some(actual));
    let recorded_side = action_from_side(intended_side);
    if recorded_side == derived {
        recorded_side
    } else {
        derived
    }
}

fn paper_execution_state(
    outcome: SignalOutcome,
    orders: &[OrderRecord],
    fills: &[FillRecord],
) -> Result<RecordedExecutionState, LabError> {
    if orders.is_empty() {
        return Ok(match outcome {
            SignalOutcome::NoAction => RecordedExecutionState::NoOrder,
            SignalOutcome::SignalExpired => RecordedExecutionState::Expired,
            SignalOutcome::Executed
            | SignalOutcome::CapitalBlocked
            | SignalOutcome::RuleBlocked
            | SignalOutcome::EvidenceUnavailable
            | SignalOutcome::OrderUnfilled => RecordedExecutionState::NotCaptured,
        });
    }

    let statuses = orders.iter().map(|order| order.status).collect::<Vec<_>>();
    let filled_order_ids = fills
        .iter()
        .map(|fill| fill.order_id.as_str())
        .collect::<BTreeSet<_>>();
    for order in orders {
        let has_fill = filled_order_ids.contains(order.order_id.as_str());
        let has_quantity = order.cumulative_filled_qty.get() > Decimal::ZERO;
        if has_fill != has_quantity
            || (matches!(
                order.status,
                OrderStatus::Filled | OrderStatus::PartiallyFilled
            ) && !has_fill)
            || (order.status == OrderStatus::Rejected && has_fill)
        {
            return Err(corrupt(
                "paper order status and cutoff-bounded fills disagree",
            ));
        }
    }
    if statuses.contains(&OrderStatus::Rejected) {
        return Ok(RecordedExecutionState::Rejected);
    }
    if statuses.contains(&OrderStatus::PartiallyFilled)
        || (!fills.is_empty()
            && statuses
                .iter()
                .any(|status| matches!(status, OrderStatus::Cancelled | OrderStatus::Expired)))
    {
        return Ok(RecordedExecutionState::PartiallyFilled);
    }
    if statuses.iter().all(|status| *status == OrderStatus::Filled) {
        return Ok(RecordedExecutionState::Filled);
    }
    if statuses.contains(&OrderStatus::Created) {
        return Ok(RecordedExecutionState::Pending);
    }
    if statuses.contains(&OrderStatus::Cancelled) {
        return Ok(RecordedExecutionState::Cancelled);
    }
    if statuses.contains(&OrderStatus::Expired) {
        return Ok(RecordedExecutionState::Expired);
    }
    Ok(RecordedExecutionState::NotCaptured)
}

fn automation_execution_state(
    decision: &AutomationDecisionRecord,
    orders: &[AutomationOrderView],
    fills: &[AutomationFillView],
) -> RecordedExecutionState {
    if orders.is_empty() {
        return match decision.state.as_str() {
            "NO_ACTION" => RecordedExecutionState::NoOrder,
            "REJECTED" => RecordedExecutionState::Rejected,
            _ => RecordedExecutionState::NotCaptured,
        };
    }

    let states = orders.iter().map(|order| order.state).collect::<Vec<_>>();
    if states.contains(&AutomationOrderState::Rejected) {
        return RecordedExecutionState::Rejected;
    }
    if states.contains(&AutomationOrderState::PartiallyFilled)
        || (!fills.is_empty() && states.contains(&AutomationOrderState::Cancelled))
    {
        return RecordedExecutionState::PartiallyFilled;
    }
    if states
        .iter()
        .all(|state| *state == AutomationOrderState::Filled)
    {
        return RecordedExecutionState::Filled;
    }
    if states.iter().any(|state| {
        matches!(
            state,
            AutomationOrderState::Reserved | AutomationOrderState::Open
        )
    }) {
        return RecordedExecutionState::Pending;
    }
    if states.contains(&AutomationOrderState::Cancelled) {
        return RecordedExecutionState::Cancelled;
    }
    RecordedExecutionState::NotCaptured
}

fn validate_paper_links(
    signal: &SignalRecord,
    orders: &[OrderRecord],
    fills: &[FillRecord],
) -> Result<(), LabError> {
    let mut order_ids = BTreeSet::new();
    for order in orders {
        if order.parent_signal_id != signal.signal_id
            || order.context.run_id != signal.context.run_id
            || order.context.model_id != signal.context.model_id
            || order.context.market != signal.context.market
            || order.context.event_seq <= signal.context.event_seq
            || !order_ids.insert(order.order_id.as_str())
        {
            return Err(corrupt(
                "paper order identity does not link to selected signal",
            ));
        }
    }
    let mut fill_ids = BTreeSet::new();
    for fill in fills {
        if !order_ids.contains(fill.order_id.as_str())
            || fill.context.run_id != signal.context.run_id
            || fill.context.model_id != signal.context.model_id
            || fill.context.market != signal.context.market
            || !fill_ids.insert(fill.fill_id.as_str())
        {
            return Err(corrupt(
                "paper fill identity does not link through selected signal order",
            ));
        }
    }
    Ok(())
}

fn paper_orders(orders: &[OrderRecord]) -> Result<Vec<RecordedDecisionOrder>, LabError> {
    orders
        .iter()
        .map(|order| {
            Ok(RecordedDecisionOrder {
                order_id: order.order_id.to_string(),
                state: enum_name(&order.status)?,
                reason: Some(enum_name(&order.reason)?),
            })
        })
        .collect()
}

fn paper_fills(fills: &[FillRecord]) -> Vec<RecordedDecisionFill> {
    fills
        .iter()
        .map(|fill| RecordedDecisionFill {
            fill_id: fill.fill_id.to_string(),
            order_id: fill.order_id.to_string(),
        })
        .collect()
}

fn validate_automation_links(
    decision: &AutomationDecisionRecord,
    orders: &[AutomationOrderView],
    fills: &[AutomationFillView],
    events: &[AutomationEvent],
) -> Result<(), LabError> {
    let mut order_ids = BTreeSet::new();
    let mut order_by_id = std::collections::BTreeMap::new();
    for order in orders {
        if order.intent_id != decision.intent_id
            || order.market.code() != decision.candle.candle.market.as_str()
            || !order_ids.insert(order.client_identifier.as_str())
        {
            return Err(corrupt(
                "automation order identity does not link to selected intent",
            ));
        }
        order_by_id.insert(order.client_identifier.as_str(), order);
    }
    let mut fill_ids = BTreeSet::new();
    for fill in fills {
        let Some(order) = order_by_id.get(fill.client_identifier.as_str()) else {
            return Err(corrupt(
                "automation fill has no order linked to selected intent",
            ));
        };
        if fill.market != order.market
            || fill.side != order.side
            || !fill_ids.insert(fill.trade_uuid.as_str())
        {
            return Err(corrupt(
                "automation fill identity differs from its selected order",
            ));
        }
    }
    for event in events {
        let detail_intent = event.detail.get("intent_id").and_then(Value::as_str);
        let detail_order = event
            .detail
            .get("client_identifier")
            .and_then(Value::as_str);
        let detail_observation = event.detail.get("observation_id").and_then(Value::as_str);
        if event.session_id != decision.session_id
            || detail_intent.is_some_and(|id| id != decision.intent_id.as_str())
            || detail_order.is_some_and(|id| !order_ids.contains(id))
            || (detail_intent != Some(decision.intent_id.as_str())
                && !detail_order.is_some_and(|id| order_ids.contains(id))
                && detail_observation != Some(decision.observation_id.as_str()))
        {
            return Err(corrupt(
                "automation review event is not linked to selected intent",
            ));
        }
    }
    Ok(())
}

fn automation_orders(
    orders: &[AutomationOrderView],
    events: &[AutomationEvent],
) -> Result<Vec<RecordedDecisionOrder>, LabError> {
    orders
        .iter()
        .map(|order| {
            Ok(RecordedDecisionOrder {
                order_id: order.client_identifier.to_string(),
                state: enum_name(&order.state)?,
                reason: events
                    .iter()
                    .filter(|event| {
                        event
                            .detail
                            .get("client_identifier")
                            .and_then(Value::as_str)
                            == Some(order.client_identifier.as_str())
                    })
                    .filter(|event| event.detail.get("admission_deferral").is_none())
                    .filter_map(|event| event.detail.get("reason").and_then(Value::as_str))
                    .next_back()
                    .map(str::to_owned),
            })
        })
        .collect()
}

fn automation_fills(fills: &[AutomationFillView]) -> Vec<RecordedDecisionFill> {
    fills
        .iter()
        .map(|fill| RecordedDecisionFill {
            fill_id: fill.trade_uuid.clone(),
            order_id: fill.client_identifier.to_string(),
        })
        .collect()
}

fn saved_evaluation(evaluation: &Value) -> &Value {
    evaluation
        .get("policy_evaluation")
        .filter(|value| !value.is_null())
        .unwrap_or(evaluation)
}

fn is_policy_warmup(evaluation: &Value) -> bool {
    ["phase", "kind"]
        .iter()
        .any(|key| evaluation.get(key).and_then(Value::as_str) == Some(WARMUP_PHASE))
}

fn saved_policy_trace(evaluation: &Value) -> Result<Option<PolicyTrace>, LabError> {
    optional_saved(evaluation, "policy_trace")
}

fn saved_decision_trace(
    evaluation: &Value,
) -> Result<Option<Vec<crate::contracts::PolicyConditionTrace>>, LabError> {
    optional_saved(evaluation, "decision_trace")
}

fn saved_reasons(evaluation: &Value) -> Result<Vec<RecordedDecisionReason>, LabError> {
    let Some(reasons) = evaluation.get("reasons") else {
        return Ok(Vec::new());
    };
    let Some(reasons) = reasons.as_array() else {
        return Err(corrupt("saved automation reasons are not an array"));
    };
    let mut projected = Vec::with_capacity(reasons.len());
    for value in reasons {
        let Some(code) = value.as_str() else {
            return Err(corrupt("saved automation reason code is not a string"));
        };
        push_reason(&mut projected, reason_for_saved_code(code));
    }
    Ok(projected)
}

fn append_automation_event_reasons(
    event: &AutomationEvent,
    decision: &AutomationDecisionRecord,
    orders: &[AutomationOrderView],
    reasons: &mut Vec<RecordedDecisionReason>,
) -> Result<(), LabError> {
    let intent_link =
        event.detail.get("intent_id").and_then(Value::as_str) == Some(decision.intent_id.as_str());
    let observation_link = event.detail.get("observation_id").and_then(Value::as_str)
        == Some(decision.observation_id.as_str());
    let order_link = event
        .detail
        .get("client_identifier")
        .and_then(Value::as_str)
        .is_some_and(|identifier| {
            orders
                .iter()
                .any(|order| order.client_identifier.as_str() == identifier)
        });
    if !intent_link && !observation_link && !order_link {
        return Ok(());
    }
    let admission_deferral = event.detail.get("admission_deferral");
    if admission_deferral.is_some() && event.kind != AutomationEventKind::DecisionDeferred {
        return Err(corrupt(
            "automation admission deferral is attached to a non-deferred event",
        ));
    }
    if let Some(raw) = admission_deferral {
        let fact: crate::contracts::AdmissionDeferredFact = serde_json::from_value(raw.clone())
            .map_err(|_| corrupt("saved automation admission deferral is malformed"))?;
        validate_admission_deferred_links(event, decision, orders, &fact)?;
        push_distinct_admission_deferred_reason(reasons, admission_deferred_reason(&fact));
    }
    if matches!(
        event.kind,
        AutomationEventKind::IntentRejected
            | AutomationEventKind::RecoveryBlocked
            | AutomationEventKind::BaselineBlocked
    ) {
        if let Some(code) = event.detail.get("reason").and_then(Value::as_str) {
            let (stage, label) = match event.kind {
                AutomationEventKind::IntentRejected => (
                    RecordedDecisionStage::Admission,
                    "주문 전 승인 단계에서 거부됨",
                ),
                AutomationEventKind::RecoveryBlocked => (
                    RecordedDecisionStage::DataReadiness,
                    "복구 준비가 끝나지 않아 판단이 보류됨",
                ),
                AutomationEventKind::BaselineBlocked => (
                    RecordedDecisionStage::Admission,
                    "계정 기준선 조건이 충족되지 않아 진입이 차단됨",
                ),
                _ => return Ok(()),
            };
            push_reason(reasons, stage_reason(stage, code, label));
        } else {
            let code = match event.kind {
                AutomationEventKind::IntentRejected => "INTENT_REJECTED",
                AutomationEventKind::RecoveryBlocked => "RECOVERY_BLOCKED",
                AutomationEventKind::BaselineBlocked => "BASELINE_BLOCKED",
                _ => return Ok(()),
            };
            push_reason(
                reasons,
                stage_reason(
                    RecordedDecisionStage::Admission,
                    code,
                    "저장된 안전 단계에서 진행이 차단됨",
                ),
            );
        }
    }
    if let Some(raw) = event.detail.get("preparation") {
        let preparation: DecisionPreparation = serde_json::from_value(raw.clone())
            .map_err(|_| corrupt("saved automation preparation fact is malformed"))?;
        if preparation.market.code() != decision.candle.candle.market
            || preparation.interval != decision.candle.candle.interval
            || preparation.observation_id != decision.observation_id
            || preparation.decision_time != decision.decision_time_utc
        {
            return Err(corrupt(
                "automation preparation identity differs from selected decision",
            ));
        }
        push_reason(reasons, reason_for_preparation(preparation.status));
    }
    Ok(())
}

fn validate_admission_deferred_links(
    event: &AutomationEvent,
    decision: &AutomationDecisionRecord,
    orders: &[AutomationOrderView],
    fact: &crate::contracts::AdmissionDeferredFact,
) -> Result<(), LabError> {
    let detail_intent = required_event_text(&event.detail, "intent_id")?;
    let detail_observation = required_event_text(&event.detail, "observation_id")?;
    let detail_reason = required_event_text(&event.detail, "reason")?;
    let detail_client_identifier = optional_event_text(&event.detail, "client_identifier")?;
    if fact.intent_id != decision.intent_id
        || fact.observation_id != decision.observation_id
        || detail_intent != fact.intent_id.as_str()
        || detail_observation != fact.observation_id.as_str()
        || detail_reason != admission_deferred_reason_code(fact.reason)
    {
        return Err(corrupt(
            "automation admission deferral differs from selected intent facts",
        ));
    }
    match fact.client_identifier.as_ref() {
        Some(identifier) => {
            if detail_client_identifier != Some(identifier.as_str())
                || !orders
                    .iter()
                    .any(|order| &order.client_identifier == identifier)
            {
                return Err(corrupt(
                    "automation admission deferral order is not linked to selected intent",
                ));
            }
        }
        None if detail_client_identifier.is_none() => {}
        None => {
            return Err(corrupt(
                "automation admission deferral has an uncaptured order identity",
            ));
        }
    }
    Ok(())
}

fn required_event_text<'a>(detail: &'a Value, key: &str) -> Result<&'a str, LabError> {
    detail
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt("saved automation admission deferral link is malformed"))
}

fn optional_event_text<'a>(detail: &'a Value, key: &str) -> Result<Option<&'a str>, LabError> {
    match detail.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(corrupt(
            "saved automation admission deferral order link is malformed",
        )),
    }
}

fn admission_deferred_reason(
    fact: &crate::contracts::AdmissionDeferredFact,
) -> RecordedDecisionReason {
    let (stage, label) = match fact.phase {
        crate::contracts::AdmissionDeferredPhase::BeforeAdmission => {
            (RecordedDecisionStage::Admission, "주문승인보류됨")
        }
        crate::contracts::AdmissionDeferredPhase::BeforeSendClaim => {
            (RecordedDecisionStage::Execution, "전송전대기했음")
        }
        crate::contracts::AdmissionDeferredPhase::AfterSendClaim => (
            RecordedDecisionStage::Execution,
            "전송권한확보안되어대기했음",
        ),
    };
    stage_reason(stage, admission_deferred_reason_code(fact.reason), label)
}

fn admission_deferred_reason_code(
    reason: crate::contracts::AdmissionDeferredReason,
) -> &'static str {
    match reason {
        crate::contracts::AdmissionDeferredReason::PrivateAccountRecoveryRequired => {
            "PRIVATE_ACCOUNT_RECOVERY_REQUIRED"
        }
        crate::contracts::AdmissionDeferredReason::EffectAuthorizationBlocked => {
            "EFFECT_AUTHORIZATION_BLOCKED"
        }
    }
}

fn push_distinct_admission_deferred_reason(
    reasons: &mut Vec<RecordedDecisionReason>,
    reason: RecordedDecisionReason,
) {
    if !reasons.iter().any(|existing| {
        existing.stage == reason.stage
            && existing.code == reason.code
            && existing.label_ko == reason.label_ko
    }) {
        reasons.push(reason);
    }
}

fn parse_preparation_status(value: &str) -> Option<DecisionPreparationStatus> {
    serde_json::from_value(Value::String(value.to_owned())).ok()
}

fn reason_for_preparation(status: DecisionPreparationStatus) -> RecordedDecisionReason {
    let (code, label) = match status {
        DecisionPreparationStatus::PolicyWarmup => (
            "POLICY_WARMUP",
            "정책 초기화에 필요한 완료 봉이 아직 부족함",
        ),
        DecisionPreparationStatus::AwaitingPolicySources => (
            "AWAITING_POLICY_SOURCES",
            "정책 판단에 필요한 입력 봉을 기다리는 중",
        ),
        DecisionPreparationStatus::AwaitingExecutionBar => {
            ("AWAITING_EXECUTION_BAR", "다음 실행 봉을 기다리는 중")
        }
        DecisionPreparationStatus::PrivateReadinessBlocked => (
            "PRIVATE_READINESS_BLOCKED",
            "계정 준비 조건이 충족되지 않아 판단을 보류함",
        ),
        DecisionPreparationStatus::RecoveryRequired => {
            ("RECOVERY_REQUIRED", "복구가 끝나기 전 판단을 보류함")
        }
        DecisionPreparationStatus::CatchUpStateOnly => (
            "CATCH_UP_STATE_ONLY",
            "복구 구간에서 상태만 동기화하고 매수 판단은 하지 않음",
        ),
    };
    stage_reason(RecordedDecisionStage::DataReadiness, code, label)
}

fn reason_for_preparation_code(code: &str) -> RecordedDecisionReason {
    parse_preparation_status(code).map_or_else(
        || {
            stage_reason(
                RecordedDecisionStage::DataReadiness,
                code,
                "저장된 입력 준비 또는 복구 상태",
            )
        },
        reason_for_preparation,
    )
}

fn reason_for_saved_code(code: &str) -> RecordedDecisionReason {
    serde_json::from_value::<ReasonCode>(Value::String(code.to_owned())).map_or_else(
        |_| {
            stage_reason(
                RecordedDecisionStage::Strategy,
                code,
                "저장된 전략 평가 사유 코드",
            )
        },
        reason_for_code,
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "the exhaustive persisted-reason mapping is one fixed, reviewable table"
)]
fn reason_for_code(code: ReasonCode) -> RecordedDecisionReason {
    use ReasonCode as R;
    let (stage, code_name, label) = match code {
        R::StartFromCash => (
            RecordedDecisionStage::Strategy,
            "START_FROM_CASH",
            "현금 상태에서 진입 규칙을 평가함",
        ),
        R::BandEntry => (
            RecordedDecisionStage::Strategy,
            "BAND_ENTRY",
            "진입 밴드 조건 충족",
        ),
        R::BandExit => (
            RecordedDecisionStage::Strategy,
            "BAND_EXIT",
            "이탈 밴드 조건 충족",
        ),
        R::Breakout => (
            RecordedDecisionStage::Strategy,
            "BREAKOUT",
            "돌파 조건 충족",
        ),
        R::Breakdown => (
            RecordedDecisionStage::Strategy,
            "BREAKDOWN",
            "하락 이탈 조건 충족",
        ),
        R::HoldState => (
            RecordedDecisionStage::Strategy,
            "HOLD_STATE",
            "전략 상태 유지",
        ),
        R::TrendPullbackEntry => (
            RecordedDecisionStage::Strategy,
            "TREND_PULLBACK_ENTRY",
            "추세 되돌림 진입 조건 충족",
        ),
        R::TrendInvalidated => (
            RecordedDecisionStage::Strategy,
            "TREND_INVALIDATED",
            "추세 조건 무효",
        ),
        R::RsiRecovery => (
            RecordedDecisionStage::Strategy,
            "RSI_RECOVERY",
            "RSI 회복 조건 충족",
        ),
        R::HoldingTimeout => (
            RecordedDecisionStage::Strategy,
            "HOLDING_TIMEOUT",
            "최대 보유 기간 조건 도달",
        ),
        R::EvidenceUnavailable => (
            RecordedDecisionStage::DataReadiness,
            "EVIDENCE_UNAVAILABLE",
            "근거 데이터 미확보",
        ),
        R::EvidenceOverlay => (
            RecordedDecisionStage::Constraints,
            "EVIDENCE_OVERLAY",
            "근거 기반 비중 조정 적용",
        ),
        R::RebalanceBand => (
            RecordedDecisionStage::Constraints,
            "REBALANCE_BAND",
            "목표와 현재 비중 차이가 재조정 범위 안",
        ),
        R::NoAction => (
            RecordedDecisionStage::Strategy,
            "NO_ACTION",
            "추가 행동이 필요하지 않음",
        ),
        R::CapitalBlocked => (
            RecordedDecisionStage::Admission,
            "CAPITAL_BLOCKED",
            "자본 제한으로 진입 차단",
        ),
        R::RuleBlocked => (
            RecordedDecisionStage::Admission,
            "RULE_BLOCKED",
            "시장 규칙 제한으로 주문 차단",
        ),
        R::QuantityRoundedToZero => (
            RecordedDecisionStage::Constraints,
            "QUANTITY_ROUNDED_TO_ZERO",
            "수량 단위 적용 후 주문 수량이 0",
        ),
        R::SignalExpired => (
            RecordedDecisionStage::Execution,
            "SIGNAL_EXPIRED",
            "신호 유효 시간이 지나 만료",
        ),
        R::WouldTakeOrUnknown => (
            RecordedDecisionStage::Execution,
            "WOULD_TAKE_OR_UNKNOWN",
            "지정가가 즉시 체결 가능하거나 상태를 확인할 수 없음",
        ),
        R::PassiveNotPenetrated => (
            RecordedDecisionStage::Execution,
            "PASSIVE_NOT_PENETRATED",
            "지정가 체결 가격 조건 불충족",
        ),
        R::PassivePartial => (
            RecordedDecisionStage::Execution,
            "PASSIVE_PARTIAL",
            "지정가 주문 일부 체결",
        ),
        R::PassiveRemainderCancelled => (
            RecordedDecisionStage::Execution,
            "PASSIVE_REMAINDER_CANCELLED",
            "일부 체결 후 주문 잔량 취소",
        ),
        R::MarketFilled => (
            RecordedDecisionStage::Execution,
            "MARKET_FILLED",
            "시장가 체결 기록",
        ),
        R::VolumeCap => (
            RecordedDecisionStage::Constraints,
            "VOLUME_CAP",
            "유동성 참여 한도 적용",
        ),
        R::TerminalMark => (
            RecordedDecisionStage::Lifecycle,
            "TERMINAL_MARK",
            "마지막 보유분 평가 기록",
        ),
        R::ArtificialTerminalExit => (
            RecordedDecisionStage::Lifecycle,
            "ARTIFICIAL_TERMINAL_EXIT",
            "마지막 시점 시나리오 청산",
        ),
        R::Cancelled => (
            RecordedDecisionStage::Lifecycle,
            "CANCELLED",
            "주문 취소 기록",
        ),
        R::PolicyRule => (
            RecordedDecisionStage::Strategy,
            "POLICY_RULE",
            "저장된 정책 규칙이 선택됨",
        ),
        R::PolicyFallback => (
            RecordedDecisionStage::Strategy,
            "POLICY_FALLBACK",
            "저장된 정책 기본 경로가 선택됨",
        ),
    };
    stage_reason(stage, code_name, label)
}

fn stage_reason(
    stage: RecordedDecisionStage,
    code: &str,
    label_ko: &str,
) -> RecordedDecisionReason {
    RecordedDecisionReason {
        stage,
        code: code.to_owned(),
        label_ko: label_ko.to_owned(),
    }
}

fn push_reason(reasons: &mut Vec<RecordedDecisionReason>, reason: RecordedDecisionReason) {
    if !reasons
        .iter()
        .any(|existing| existing.stage == reason.stage && existing.code == reason.code)
    {
        reasons.push(reason);
    }
}

fn optional_saved<T: DeserializeOwned>(value: &Value, key: &str) -> Result<Option<T>, LabError> {
    let Some(raw) = value.get(key).filter(|raw| !raw.is_null()) else {
        return Ok(None);
    };
    serde_json::from_value(raw.clone())
        .map(Some)
        .map_err(|_| corrupt("saved automation decision fact has an invalid typed value"))
}

fn enum_name(value: &impl Serialize) -> Result<String, LabError> {
    let encoded = serde_json::to_value(value)?;
    encoded
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| corrupt("saved decision enum does not serialize as a string"))
}

fn corrupt(message: &str) -> LabError {
    LabError::DataCorrupt(format!("recorded decision explanation: {message}"))
}

#[cfg(test)]
#[path = "decision_explanation/tests.rs"]
mod tests;

use super::*;
use crate::contracts::{
    AssetQuantity, AutomationDecisionRecord, AutomationEvent, AutomationEventKind,
    AutomationFillView, AutomationIntentId, AutomationOrderIdentifier, AutomationOrderState,
    AutomationOrderView, AutomationReviewAction, AutomationSessionId, AutomationSide,
    CandleInterval, CandleObservation, CandleRecord, ContentHash, EventContext, IndicatorSnapshot,
    MarketId, ModelId, ObservationId, PolicyId, PolicyRevisionId, PositionState, PriceKrw,
    QuoteAmount, ReasonCode, RunId, Side, SignalId, SignalOutcome, StrategyKind, UtcTimestamp,
};
use rust_decimal::Decimal;

fn policy_ref() -> PolicyRevisionRef {
    PolicyRevisionRef {
        policy_id: PolicyId::new("decision-policy").expect("policy id"),
        revision_id: PolicyRevisionId::new("decision-revision").expect("revision id"),
        definition_digest: ContentHash::of_bytes(b"decision policy"),
    }
}

fn signal(
    outcome: SignalOutcome,
    reasons: Vec<ReasonCode>,
    actual_weight: Option<Weight>,
    intended_side: Option<Side>,
) -> SignalRecord {
    let decision_time = UtcTimestamp::parse_rfc3339("2026-01-01T00:00:00Z").expect("decision time");
    SignalRecord {
        context: EventContext {
            run_id: RunId::new("decision-run").expect("run id"),
            model_id: ModelId::new("decision-model").expect("model id"),
            market: MarketId::parse_upbit("KRW-BTC").expect("market"),
            event_seq: 1,
            accounting_event_time: decision_time,
        },
        signal_id: SignalId::new("decision-signal").expect("signal id"),
        strategy: StrategyKind::BuyAndHold,
        strategy_version: "fixture-v1".into(),
        policy_ref: Some(policy_ref()),
        policy_trace: None,
        decision_trace: None,
        actual_weight_at_decision: actual_weight,
        source_bar_ids: Vec::new(),
        signal_time: decision_time,
        decision_available_at: decision_time,
        valid_until: decision_time,
        state_before: PositionState::Cash,
        state_after: PositionState::Long,
        raw_target_weight: Weight::new(Decimal::new(8, 1)).expect("raw target"),
        constrained_target_weight: Weight::new(Decimal::new(8, 1)).expect("constrained target"),
        intended_side,
        reasons,
        indicators: IndicatorSnapshot {
            version: "fixture-v1".into(),
            ema: None,
            bar_sigma: None,
            annual_vol: None,
            rsi: None,
            entry_high: None,
            exit_low: None,
            completed_bars_seen: 1,
            policy_values: Vec::new(),
        },
        evidence_effect: None,
        outcome,
    }
}

#[test]
fn legacy_missing_position_weight_does_not_turn_saved_buy_intent_into_strategy_action() {
    let signal = signal(
        SignalOutcome::RuleBlocked,
        vec![ReasonCode::RuleBlocked],
        None,
        Some(Side::Buy),
    );

    let explanation =
        paper_recorded_decision(Some(&signal), None, &[], &[]).expect("saved signal projection");

    assert_eq!(
        explanation.strategy_action,
        RecordedDecisionAction::NotCaptured
    );
    assert_eq!(explanation.constrained_action, RecordedDecisionAction::Buy);
    assert_eq!(
        explanation.execution_state,
        RecordedExecutionState::NotCaptured
    );
    assert_eq!(
        explanation.condition_trace_capture,
        SavedFactCaptureStatus::NotCaptured
    );
    assert_eq!(explanation.reasons[0].code, "RULE_BLOCKED");
    assert_eq!(
        explanation.reasons[0].label_ko,
        "시장 규칙 제한으로 주문 차단"
    );
}

#[test]
fn explicit_no_action_is_distinct_from_a_missing_order_fact() {
    let signal = signal(
        SignalOutcome::NoAction,
        vec![ReasonCode::NoAction],
        Some(Weight::new(Decimal::new(8, 1)).expect("actual weight")),
        None,
    );

    let explanation =
        paper_recorded_decision(Some(&signal), None, &[], &[]).expect("saved no-action projection");

    assert_eq!(explanation.strategy_action, RecordedDecisionAction::Hold);
    assert_eq!(explanation.constrained_action, RecordedDecisionAction::Hold);
    assert_eq!(explanation.execution_state, RecordedExecutionState::NoOrder);
}

#[test]
fn artificial_terminal_exit_does_not_present_copied_policy_trace_as_a_new_decision() {
    let mut signal = signal(
        SignalOutcome::Executed,
        vec![ReasonCode::ArtificialTerminalExit],
        Some(Weight::new(Decimal::ZERO).expect("actual weight")),
        Some(Side::Sell),
    );
    signal.policy_trace = Some(PolicyTrace {
        matched_rule_id: Some("prior-rule".into()),
        state_before: Vec::new(),
        state_after: Vec::new(),
        evidence: None,
        condition_trace: Some(Vec::new()),
        source_provenance: None,
    });

    let explanation =
        paper_recorded_decision(Some(&signal), None, &[], &[]).expect("terminal exit projection");

    assert_eq!(
        explanation.condition_trace_capture,
        SavedFactCaptureStatus::NotCaptured
    );
    assert!(explanation.condition_trace.is_empty());
    assert_eq!(explanation.matched_rule_id, None);
    assert_eq!(
        explanation.reasons[0].stage,
        RecordedDecisionStage::Lifecycle
    );
}

fn automation_decision(
    action: AutomationReviewAction,
    state: &str,
    evaluation: serde_json::Value,
) -> AutomationDecisionRecord {
    let time = UtcTimestamp::parse_rfc3339("2026-01-01T01:00:00Z").expect("time");
    let candle = CandleObservation {
        id: ObservationId::new("automation-observation").expect("observation id"),
        candle: CandleRecord {
            market: "KRW-BTC".into(),
            interval: CandleInterval::H1,
            open_time_utc: UtcTimestamp(time.0 - chrono::Duration::hours(1)),
            close_time_utc: time,
            open: PriceKrw::new(Decimal::from(100)).expect("open"),
            high: PriceKrw::new(Decimal::from(101)).expect("high"),
            low: PriceKrw::new(Decimal::from(99)).expect("low"),
            close: PriceKrw::new(Decimal::from(100)).expect("close"),
            volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
            quote_turnover: QuoteAmount::new(Decimal::from(100)).expect("turnover"),
            completed: true,
        },
        content_digest: ContentHash::of_bytes(b"automation observation"),
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    };
    AutomationDecisionRecord {
        session_id: AutomationSessionId::new("automation-session").expect("session id"),
        intent_id: AutomationIntentId::new("automation-intent").expect("intent id"),
        observation_id: candle.id.clone(),
        decision_time_utc: time,
        recorded_at_utc: UtcTimestamp(time.0 + chrono::Duration::seconds(1)),
        action,
        state: state.into(),
        candle,
        evaluation_digest: ContentHash::of_value(&evaluation).expect("evaluation digest"),
        evaluation,
    }
}

fn automation_order(
    decision: &AutomationDecisionRecord,
    state: AutomationOrderState,
) -> AutomationOrderView {
    AutomationOrderView {
        client_identifier: AutomationOrderIdentifier::new("automation-order")
            .expect("order identifier"),
        intent_id: decision.intent_id.clone(),
        market: MarketId::parse_upbit("KRW-BTC").expect("market"),
        side: AutomationSide::Bid,
        state,
        venue_order_uuid: Some("venue-order".into()),
        requested_notional_krw: QuoteAmount::new(Decimal::from(100)).expect("notional"),
        reserved_fee_krw: QuoteAmount::new(Decimal::ONE).expect("fee reserve"),
        executed_volume: AssetQuantity::new(Decimal::ONE).expect("executed volume"),
        paid_fee_krw: QuoteAmount::new(Decimal::ONE).expect("paid fee"),
        locked_quote_krw: None,
        locked_base: None,
        trade_count: 1,
        created_at_utc: decision.decision_time_utc,
        updated_at_utc: decision.recorded_at_utc,
    }
}

fn admission_deferred_event(
    decision: &AutomationDecisionRecord,
    phase: crate::contracts::AdmissionDeferredPhase,
    reason: crate::contracts::AdmissionDeferredReason,
    client_identifier: Option<&AutomationOrderIdentifier>,
    reason_detail: Option<&str>,
) -> AutomationEvent {
    let reason_code = serde_json::to_value(reason).expect("admission deferral reason code");
    let fact = crate::contracts::AdmissionDeferredFact {
        phase,
        reason,
        intent_id: decision.intent_id.clone(),
        observation_id: decision.observation_id.clone(),
        client_identifier: client_identifier.cloned(),
        reason_detail: reason_detail.map(str::to_owned),
    };
    AutomationEvent {
        session_id: decision.session_id.clone(),
        sequence: 1,
        kind: AutomationEventKind::DecisionDeferred,
        at_utc: decision.decision_time_utc,
        detail: serde_json::json!({
            "intent_id": decision.intent_id.as_str(),
            "observation_id": decision.observation_id.as_str(),
            "client_identifier": client_identifier.map(AutomationOrderIdentifier::as_str),
            "reason": reason_code,
            "admission_deferral": fact,
        }),
    }
}

fn automation_fill(
    decision: &AutomationDecisionRecord,
    order: &AutomationOrderView,
) -> AutomationFillView {
    AutomationFillView {
        trade_uuid: "automation-fill".into(),
        client_identifier: order.client_identifier.clone(),
        market: order.market.clone(),
        side: order.side,
        price_krw: PriceKrw::new(Decimal::from(100)).expect("price"),
        volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
        funds_krw: QuoteAmount::new(Decimal::from(100)).expect("funds"),
        observed_at_utc: decision.recorded_at_utc,
    }
}

fn assert_admission_reason(
    explanation: &crate::contracts::RecordedDecisionExplanation,
    stage: RecordedDecisionStage,
    code: &str,
    label: &str,
) {
    assert!(explanation.reasons.iter().any(|reason| {
        reason.stage == stage && reason.code == code && reason.label_ko == label
    }));
}

#[test]
fn admission_deferral_phase_maps_to_history_without_rejection() -> Result<(), LabError> {
    let reference = policy_ref();
    let decision = automation_decision(
        AutomationReviewAction::Buy,
        "ORDER_SUBMITTED",
        serde_json::json!({"reasons": []}),
    );
    let before_admission = admission_deferred_event(
        &decision,
        crate::contracts::AdmissionDeferredPhase::BeforeAdmission,
        crate::contracts::AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        None,
        Some("private account recovery read is pending"),
    );
    let no_order = automation_recorded_decision(
        &reference,
        &decision,
        &[],
        &[],
        std::slice::from_ref(&before_admission),
    )?;
    assert_eq!(
        no_order.strategy_action,
        RecordedDecisionAction::NotCaptured
    );
    assert_eq!(
        no_order.execution_state,
        RecordedExecutionState::NotCaptured
    );
    assert_admission_reason(
        &no_order,
        RecordedDecisionStage::Admission,
        "PRIVATE_ACCOUNT_RECOVERY_REQUIRED",
        "주문승인보류됨",
    );

    let reserved_order = automation_order(&decision, AutomationOrderState::Reserved);
    let before_claim = admission_deferred_event(
        &decision,
        crate::contracts::AdmissionDeferredPhase::BeforeSendClaim,
        crate::contracts::AdmissionDeferredReason::EffectAuthorizationBlocked,
        Some(&reserved_order.client_identifier),
        Some("effect authorization is unavailable"),
    );
    let reserved = automation_recorded_decision(
        &reference,
        &decision,
        std::slice::from_ref(&reserved_order),
        &[],
        std::slice::from_ref(&before_claim),
    )?;
    assert_eq!(reserved.execution_state, RecordedExecutionState::Pending);
    assert_admission_reason(
        &reserved,
        RecordedDecisionStage::Execution,
        "EFFECT_AUTHORIZATION_BLOCKED",
        "전송전대기했음",
    );
    assert_eq!(reserved.orders[0].reason, None);
    Ok(())
}

#[test]
fn after_claim_deferral_preserves_fill_state_and_rejects_unrelated_same_cutoff_fact()
-> Result<(), LabError> {
    let reference = policy_ref();
    let decision = automation_decision(
        AutomationReviewAction::Buy,
        "ORDER_SUBMITTED",
        serde_json::json!({"reasons": []}),
    );
    let filled_order = automation_order(&decision, AutomationOrderState::Filled);
    let fill = automation_fill(&decision, &filled_order);
    let mut before_claim = admission_deferred_event(
        &decision,
        crate::contracts::AdmissionDeferredPhase::BeforeSendClaim,
        crate::contracts::AdmissionDeferredReason::EffectAuthorizationBlocked,
        Some(&filled_order.client_identifier),
        Some("authorization was initially unavailable"),
    );
    before_claim.sequence = 1;
    let mut after_claim = admission_deferred_event(
        &decision,
        crate::contracts::AdmissionDeferredPhase::AfterSendClaim,
        crate::contracts::AdmissionDeferredReason::EffectAuthorizationBlocked,
        Some(&filled_order.client_identifier),
        Some("effect authorization was not available after claim"),
    );
    after_claim.sequence = 2;
    after_claim.at_utc = decision.recorded_at_utc;
    let deferrals = [before_claim, after_claim.clone()];
    let filled = automation_recorded_decision(
        &reference,
        &decision,
        std::slice::from_ref(&filled_order),
        std::slice::from_ref(&fill),
        &deferrals,
    )?;
    assert_eq!(filled.strategy_action, RecordedDecisionAction::NotCaptured);
    assert_eq!(filled.execution_state, RecordedExecutionState::Filled);
    assert_eq!(filled.orders[0].reason, None);
    assert_admission_reason(
        &filled,
        RecordedDecisionStage::Execution,
        "EFFECT_AUTHORIZATION_BLOCKED",
        "전송전대기했음",
    );
    assert_admission_reason(
        &filled,
        RecordedDecisionStage::Execution,
        "EFFECT_AUTHORIZATION_BLOCKED",
        "전송권한확보안되어대기했음",
    );
    assert_eq!(
        after_claim.detail["admission_deferral"]["reason_detail"],
        "effect authorization was not available after claim"
    );

    let mut unrelated = decision.clone();
    unrelated.intent_id = AutomationIntentId::new("unrelated-intent").expect("intent id");
    unrelated.observation_id = ObservationId::new("unrelated-observation").expect("observation id");
    unrelated.candle.id = unrelated.observation_id.clone();
    let mut unrelated_event = admission_deferred_event(
        &unrelated,
        crate::contracts::AdmissionDeferredPhase::BeforeAdmission,
        crate::contracts::AdmissionDeferredReason::PrivateAccountRecoveryRequired,
        None,
        None,
    );
    unrelated_event.at_utc = after_claim.at_utc;
    assert_eq!(unrelated_event.at_utc, after_claim.at_utc);
    assert!(matches!(
        automation_recorded_decision(
            &reference,
            &decision,
            std::slice::from_ref(&filled_order),
            std::slice::from_ref(&fill),
            &[after_claim.clone(), unrelated_event],
        ),
        Err(LabError::DataCorrupt(_))
    ));

    let mut mismatched_fact = after_claim;
    mismatched_fact.detail["admission_deferral"]["intent_id"] =
        serde_json::json!(unrelated.intent_id.as_str());
    assert!(matches!(
        automation_recorded_decision(
            &reference,
            &decision,
            &[filled_order],
            &[fill],
            &[mismatched_fact],
        ),
        Err(LabError::DataCorrupt(_))
    ));
    Ok(())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the fixture keeps distinct rejection, warmup, preparation, fill, and legacy outcomes together"
)]
fn automation_rejection_fill_and_legacy_missing_facts_remain_distinct() {
    let reference = policy_ref();
    let old = automation_decision(
        AutomationReviewAction::Hold,
        "NO_ACTION",
        serde_json::json!({"reasons": []}),
    );
    let old_projection = automation_recorded_decision(&reference, &old, &[], &[], &[])
        .expect("legacy automation projection");
    assert_eq!(
        old_projection.strategy_action,
        RecordedDecisionAction::NotCaptured
    );
    assert_eq!(
        old_projection.constrained_action,
        RecordedDecisionAction::NotCaptured
    );
    assert_eq!(
        old_projection.execution_state,
        RecordedExecutionState::NoOrder
    );
    assert_eq!(
        old_projection.condition_trace_capture,
        SavedFactCaptureStatus::NotCaptured
    );

    let mut warmup_evaluation = serde_json::json!({"phase": "POLICY_WARMUP"});
    warmup_evaluation["decision_time"] =
        serde_json::to_value(old.decision_time_utc).expect("warmup decision time");
    let warmup = automation_decision(AutomationReviewAction::Hold, "NO_ACTION", warmup_evaluation);
    let warmup_projection = automation_recorded_decision(&reference, &warmup, &[], &[], &[])
        .expect("explicit warmup projection");
    assert_eq!(
        warmup_projection.strategy_action,
        RecordedDecisionAction::NotEvaluated
    );
    assert_eq!(
        warmup_projection.execution_state,
        RecordedExecutionState::NotEvaluated
    );
    assert_eq!(
        warmup_projection.condition_trace_capture,
        SavedFactCaptureStatus::NotDecisionEvent
    );

    let preparation = DecisionPreparation {
        status: DecisionPreparationStatus::CatchUpStateOnly,
        market: MarketId::parse_upbit("KRW-BTC").expect("market"),
        interval: CandleInterval::H1,
        observation_id: old.observation_id.clone(),
        decision_time: old.decision_time_utc,
        completed_bars_seen: 10,
        required_bars: 10,
    };
    let deferred = AutomationEvent {
        session_id: old.session_id.clone(),
        sequence: 1,
        kind: AutomationEventKind::DecisionDeferred,
        at_utc: old.recorded_at_utc,
        detail: serde_json::json!({
            "observation_id": old.observation_id,
            "preparation": preparation,
        }),
    };
    let deferred_projection = automation_recorded_decision(&reference, &old, &[], &[], &[deferred])
        .expect("linked catch-up preparation projection");
    assert_eq!(
        deferred_projection.strategy_action,
        RecordedDecisionAction::NotCaptured
    );
    assert_eq!(deferred_projection.reasons[0].code, "CATCH_UP_STATE_ONLY");

    let rejected = automation_decision(
        AutomationReviewAction::Buy,
        "REJECTED",
        serde_json::json!({"reasons": ["START_FROM_CASH"]}),
    );
    let rejection = AutomationEvent {
        session_id: rejected.session_id.clone(),
        sequence: 1,
        kind: AutomationEventKind::IntentRejected,
        at_utc: rejected.recorded_at_utc,
        detail: serde_json::json!({
            "intent_id": rejected.intent_id,
            "reason": "CAPITAL_BLOCKED",
        }),
    };
    let rejected_projection =
        automation_recorded_decision(&reference, &rejected, &[], &[], &[rejection])
            .expect("saved rejection projection");
    assert_eq!(
        rejected_projection.strategy_action,
        RecordedDecisionAction::NotCaptured
    );
    assert_eq!(
        rejected_projection.execution_state,
        RecordedExecutionState::Rejected
    );
    assert!(rejected_projection.reasons.iter().any(|reason| {
        reason.stage == RecordedDecisionStage::Admission && reason.code == "CAPITAL_BLOCKED"
    }));

    let actual = Weight::new(Decimal::ZERO).expect("actual weight");
    let target = Weight::new(Decimal::ONE).expect("target weight");
    let filled = automation_decision(
        AutomationReviewAction::Buy,
        "ORDER_SUBMITTED",
        serde_json::json!({
            "actual_weight_at_decision": serde_json::to_value(actual).expect("actual"),
            "raw_target_weight": serde_json::to_value(target).expect("target"),
            "constrained_target_weight": serde_json::to_value(target).expect("target"),
            "reasons": ["START_FROM_CASH"],
        }),
    );
    let order = automation_order(&filled, AutomationOrderState::Filled);
    let fill = AutomationFillView {
        trade_uuid: "automation-fill".into(),
        client_identifier: order.client_identifier.clone(),
        market: order.market.clone(),
        side: order.side,
        price_krw: PriceKrw::new(Decimal::from(100)).expect("price"),
        volume: AssetQuantity::new(Decimal::ONE).expect("volume"),
        funds_krw: QuoteAmount::new(Decimal::from(100)).expect("funds"),
        observed_at_utc: filled.recorded_at_utc,
    };
    let filled_projection =
        automation_recorded_decision(&reference, &filled, &[order], &[fill], &[])
            .expect("saved fill projection");
    assert_eq!(
        filled_projection.strategy_action,
        RecordedDecisionAction::Buy
    );
    assert_eq!(
        filled_projection.constrained_action,
        RecordedDecisionAction::Buy
    );
    assert_eq!(
        filled_projection.execution_state,
        RecordedExecutionState::Filled
    );
    assert_eq!(filled_projection.fills[0].fill_id, "automation-fill");
}

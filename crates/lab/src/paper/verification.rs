//! Independent verification of persisted paper-session accounting facts.
//!
//! This module folds saved typed events. It does not execute the policy,
//! evaluator, execution planner, or engine account transition methods.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::contracts::{
    AccountMark, CandleInterval, CandleObservation, ContentHash, EventContext, FillId, FillRecord,
    FillTiming, MarkKind, MarketId, ObservationId, OrderId, OrderRecord, OrderStatus,
    PaperEventKind, PaperProviderRecovery, PaperProviderRecoveryPhase, PaperSessionCommandAction,
    PaperSessionEvent, PaperSessionState, PaperSessionView, PaperVerification,
    PaperVerificationStatus, Side, SignalId, SignalRecord, SimulatedOrderType, UtcTimestamp,
    VerifiedProviderCoverage, observation_digest,
};

use super::PaperMarketCheckpoint;

pub(crate) const MAX_PAPER_SESSION_VERIFICATION_EVENTS: usize = 20_000;
pub(crate) const MAX_PAPER_SESSION_VERIFICATION_OBSERVATIONS: usize = 40_000;
pub(crate) const MAX_PAPER_SESSION_VERIFICATION_EVENT_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const MAX_PAPER_SESSION_VERIFICATION_OBSERVATION_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const MAX_PAPER_SESSION_VERIFICATION_JSON_BYTES: usize = 512 * 1024 * 1024;
const MAX_FINDINGS: usize = 8;
const VERIFICATION_SCOPE: &str = "SCOPE=ACCOUNTING+EVENT+REFERENCES+FACT_IDEMPOTENCE; NOT_VERIFIED=STRATEGY_LOGIC,PROVIDER_FIRST_SEEN,DYNAMIC_COST_PROVENANCE,BENCHMARK_ACCOUNTING,EVENT_HASH_CHAIN,REQUEST_RECEIPT_REPLAY,EPISODE_ANALYTICS";

// These tuples retain the SQL row metadata needed for parity checks. They are
// storage rows, not duplicate/normalized versions of the persisted DTOs.
pub(crate) struct StoredEventRow {
    pub(crate) sql_sequence: i64,
    pub(crate) sql_market: Option<String>,
    pub(crate) sql_kind: String,
    pub(crate) event: PaperSessionEvent,
}
pub(crate) type StoredCheckpointRow = (String, i64, PaperMarketCheckpoint);
pub(crate) type StoredSessionRow = (i64, i64);

pub(crate) fn unavailable_paper_verification(
    session: &PaperSessionView,
    code: &'static str,
) -> BTreeMap<MarketId, PaperVerification> {
    let findings = Findings {
        unavailable: vec![code],
        ..Findings::default()
    };
    all_markets_with(session, &findings)
}

pub(crate) fn failed_paper_verification(
    session: &PaperSessionView,
    code: &'static str,
) -> BTreeMap<MarketId, PaperVerification> {
    let findings = Findings {
        failed: vec![code],
        ..Findings::default()
    };
    all_markets_with(session, &findings)
}

pub(crate) fn verification_observation_ids(
    event_rows: &[StoredEventRow],
    checkpoint_rows: &[StoredCheckpointRow],
) -> BTreeSet<ObservationId> {
    let mut ids = BTreeSet::new();
    for row in event_rows {
        let event = &row.event;
        if let Some(candle) = &event.candle {
            ids.insert(candle.observation_id.clone());
        }
        for signal in &event.signals {
            ids.extend(signal.source_bar_ids.iter().cloned());
        }
        for fill in &event.fills {
            ids.insert(fill.source_bar_id.clone());
            ids.insert(fill.liquidity_source_bar_id.clone());
        }
        for mark in &event.account_marks {
            ids.insert(mark.source_bar_id.clone());
        }
    }
    for (_, _, checkpoint) in checkpoint_rows {
        if let Some(mark) = &checkpoint.initial_mark {
            ids.insert(mark.source_bar_id.clone());
        }
        if let Some(mark) = &checkpoint.latest_mark {
            ids.insert(mark.source_bar_id.clone());
        }
        if let Some(bar) = &checkpoint.prior_execution_bar {
            ids.insert(bar.id.clone());
        }
        if let Some(bar) = &checkpoint.prior_passive_liquidity_bar {
            ids.insert(bar.id.clone());
        }
    }
    ids
}

#[derive(Default)]
struct Findings {
    failed: Vec<&'static str>,
    unavailable: Vec<&'static str>,
}

impl Findings {
    fn fail(&mut self, code: &'static str) {
        if self.failed.len() < MAX_FINDINGS && !self.failed.contains(&code) {
            self.failed.push(code);
        }
    }

    fn unavailable(&mut self, code: &'static str) {
        if self.unavailable.len() < MAX_FINDINGS && !self.unavailable.contains(&code) {
            self.unavailable.push(code);
        }
    }

    fn merge(&mut self, other: Findings) {
        for code in other.failed {
            self.fail(code);
        }
        for code in other.unavailable {
            self.unavailable(code);
        }
    }

    fn into_verification(self) -> PaperVerification {
        let (status, reason) = if !self.failed.is_empty() {
            (
                PaperVerificationStatus::Fail,
                format!(
                    "PAPER_INDEPENDENT_VERIFICATION_FAIL[{}]; {VERIFICATION_SCOPE}",
                    self.failed.join(",")
                ),
            )
        } else if !self.unavailable.is_empty() {
            (
                PaperVerificationStatus::NotAvailable,
                format!(
                    "PAPER_INDEPENDENT_VERIFICATION_NOT_AVAILABLE[{}]; {VERIFICATION_SCOPE}",
                    self.unavailable.join(",")
                ),
            )
        } else {
            (
                PaperVerificationStatus::Pass,
                "PAPER_INDEPENDENT_VERIFICATION_PASS; VERIFIED=ACCOUNTING+EVENT+REFERENCES+FACT_IDEMPOTENCE; NOT_VERIFIED=STRATEGY_LOGIC,PROVIDER_FIRST_SEEN,DYNAMIC_COST_PROVENANCE,BENCHMARK_ACCOUNTING,EVENT_HASH_CHAIN,REQUEST_RECEIPT_REPLAY,EPISODE_ANALYTICS"
                    .into(),
            )
        };
        PaperVerification { status, reason }
    }
}

pub(crate) fn verify_paper_session(
    session: &PaperSessionView,
    session_row: StoredSessionRow,
    event_rows: &[StoredEventRow],
    checkpoint_rows: &[StoredCheckpointRow],
    observations: &BTreeMap<ObservationId, CandleObservation>,
    provider_coverage: &VerifiedProviderCoverage,
) -> BTreeMap<MarketId, PaperVerification> {
    if event_rows.len() > MAX_PAPER_SESSION_VERIFICATION_EVENTS {
        let findings = Findings {
            unavailable: vec!["EVENT_HISTORY_OVER_20000"],
            ..Findings::default()
        };
        return all_markets_with(session, &findings);
    }
    if observations.len() > MAX_PAPER_SESSION_VERIFICATION_OBSERVATIONS {
        let findings = Findings {
            unavailable: vec!["OBSERVATION_REFERENCES_OVER_40000"],
            ..Findings::default()
        };
        return all_markets_with(session, &findings);
    }

    let mut global = Findings::default();
    if session.mode != crate::contracts::PaperSessionMode::Simulated {
        global.fail("SESSION_MODE");
    }
    if u64::try_from(session_row.0).ok() != Some(session.sequence)
        || u64::try_from(session_row.1).ok() != Some(session.event_count)
    {
        global.fail("SESSION_SQL_JSON_SEQUENCE_OR_COUNT");
    }
    let mut snapshot_markets = BTreeSet::new();
    for snapshot in &session.markets {
        if !snapshot_markets.insert(snapshot.market.clone()) {
            global.fail("DUPLICATE_MARKET_SNAPSHOT");
        }
    }
    if session.markets.is_empty() {
        return BTreeMap::new();
    }

    for (index, row) in event_rows.iter().enumerate() {
        let event = &row.event;
        let expected = i64::try_from(index + 1).unwrap_or(i64::MAX);
        if row.sql_sequence != expected || event.sequence != u64::try_from(expected).unwrap_or(0) {
            global.fail("EVENT_SQL_JSON_SEQUENCE");
        }
        if row.sql_market.as_deref() != event.market.as_ref().map(MarketId::code).as_deref()
            || row.sql_kind != event_kind_text(event.kind)
        {
            global.fail("EVENT_SQL_JSON_METADATA");
        }
        if let Some(event_market) = &event.market
            && !snapshot_markets.contains(event_market)
        {
            global.fail("EVENT_FOR_UNKNOWN_MARKET");
        }
        if event_context_market_mismatch(event) {
            global.fail("EVENT_CONTEXT_MARKET");
        }
    }
    let last_sequence = event_rows.last().map_or(0, |row| row.event.sequence);
    if session.sequence != last_sequence
        || session.event_count != u64::try_from(event_rows.len()).unwrap_or(u64::MAX)
    {
        global.fail("SESSION_EVENT_SEQUENCE_OR_COUNT");
    }

    let mut checkpoint_by_market = BTreeMap::new();
    for (row_market, row_sequence, checkpoint) in checkpoint_rows {
        if checkpoint.session_id != session.session_id
            || row_market != &checkpoint.market.code()
            || u64::try_from(*row_sequence).ok() != Some(session.sequence)
            || checkpoint.policy_ref != session.policy_ref
        {
            global.fail("CHECKPOINT_ROW_IDENTITY_OR_SEQUENCE");
        }
        if checkpoint_by_market
            .insert(checkpoint.market.clone(), checkpoint)
            .is_some()
        {
            global.fail("DUPLICATE_CHECKPOINT_MARKET");
        }
    }
    if checkpoint_by_market.len() != session.markets.len() {
        global.fail("CHECKPOINT_MARKET_COUNT");
    }

    verify_provider_recovery_history(
        session,
        event_rows,
        &checkpoint_by_market,
        provider_coverage,
        &mut global,
    );

    let validated_observations = prevalidate_observations(observations);
    verify_market_snapshots(
        session,
        &checkpoint_by_market,
        event_rows,
        &validated_observations,
        provider_coverage,
        &global,
    )
}

#[derive(Clone, Copy)]
struct ValidatedObservation<'a> {
    observation: &'a CandleObservation,
    digest_serializable: bool,
    id_and_digest_valid: bool,
    boundary_valid: bool,
}

type ValidatedObservations<'a> = BTreeMap<ObservationId, ValidatedObservation<'a>>;

fn prevalidate_observations(
    observations: &BTreeMap<ObservationId, CandleObservation>,
) -> ValidatedObservations<'_> {
    observations
        .iter()
        .map(|(key, observation)| {
            let (digest_serializable, id_and_digest_valid) =
                match observation_digest(&observation.candle) {
                    Ok(digest) => (
                        true,
                        &observation.id == key
                            && observation.id == ObservationId::from_seed(digest.as_str())
                            && observation.content_digest == digest,
                    ),
                    Err(_) => (false, false),
                };
            let interval = observation.candle.interval;
            let expected_close = observation
                .candle
                .open_time_utc
                .0
                .checked_add_signed(interval.duration())
                .map(UtcTimestamp);
            let boundary_valid = expected_close == Some(observation.candle.close_time_utc)
                && crate::scheduling::latest_completed_boundary(
                    observation.candle.close_time_utc,
                    interval,
                )
                .ok()
                    == Some(observation.candle.close_time_utc);
            (
                key.clone(),
                ValidatedObservation {
                    observation,
                    digest_serializable,
                    id_and_digest_valid,
                    boundary_valid,
                },
            )
        })
        .collect()
}

fn verify_market_snapshots(
    session: &PaperSessionView,
    checkpoint_by_market: &BTreeMap<MarketId, &PaperMarketCheckpoint>,
    event_rows: &[StoredEventRow],
    observations: &ValidatedObservations<'_>,
    provider_coverage: &VerifiedProviderCoverage,
    global: &Findings,
) -> BTreeMap<MarketId, PaperVerification> {
    let mut results = BTreeMap::new();
    for snapshot in &session.markets {
        let mut findings = Findings::default();
        findings.merge(Findings {
            failed: global.failed.clone(),
            unavailable: global.unavailable.clone(),
        });
        let Some(checkpoint) = checkpoint_by_market.get(&snapshot.market).copied() else {
            findings.fail("CHECKPOINT_MISSING");
            results.insert(snapshot.market.clone(), findings.into_verification());
            continue;
        };
        verify_market(
            session,
            snapshot,
            checkpoint,
            event_rows,
            observations,
            provider_coverage,
            &mut findings,
        );
        results.insert(snapshot.market.clone(), findings.into_verification());
    }
    results
}

fn all_markets_with(
    session: &PaperSessionView,
    findings: &Findings,
) -> BTreeMap<MarketId, PaperVerification> {
    session
        .markets
        .iter()
        .map(|snapshot| {
            let mut per_market = Findings::default();
            per_market.merge(Findings {
                failed: findings.failed.clone(),
                unavailable: findings.unavailable.clone(),
            });
            (snapshot.market.clone(), per_market.into_verification())
        })
        .collect()
}

fn event_context_market_mismatch(event: &PaperSessionEvent) -> bool {
    let Some(event_market) = event.market.as_ref() else {
        return false;
    };
    event
        .signals
        .iter()
        .map(|fact| &fact.context.market)
        .chain(event.orders.iter().map(|fact| &fact.context.market))
        .chain(event.order_events.iter().map(|fact| &fact.context.market))
        .chain(event.fills.iter().map(|fact| &fact.context.market))
        .chain(event.account_marks.iter().map(|fact| &fact.context.market))
        .any(|market| market != event_market)
        || event
            .episodes
            .iter()
            .any(|episode| &episode.market != event_market)
}

fn event_kind_text(kind: PaperEventKind) -> &'static str {
    match kind {
        PaperEventKind::CandleProcessed => "CANDLE_PROCESSED",
        PaperEventKind::DecisionDeferred => "DECISION_DEFERRED",
        PaperEventKind::Pause => "PAUSE",
        PaperEventKind::Resume => "RESUME",
        PaperEventKind::Stop => "STOP",
        PaperEventKind::RecoveryRequired => "RECOVERY_REQUIRED",
        PaperEventKind::ProviderRecovery => "PROVIDER_RECOVERY",
    }
}

#[derive(Default)]
struct ProviderRecoveryEpoch {
    target_completed_at: Option<UtcTimestamp>,
    proof_ids: BTreeSet<String>,
    last_processed_at: Option<UtcTimestamp>,
    last_transitions: BTreeMap<MarketId, (ContentHash, Option<UtcTimestamp>)>,
}

fn verify_provider_recovery_history(
    session: &PaperSessionView,
    event_rows: &[StoredEventRow],
    checkpoints: &BTreeMap<MarketId, &PaperMarketCheckpoint>,
    coverage: &VerifiedProviderCoverage,
    findings: &mut Findings,
) {
    let verifier = ProviderRecoveryVerifier::new(session, checkpoints, coverage);
    let mut referenced_proofs = BTreeSet::new();
    let mut epoch: Option<ProviderRecoveryEpoch> = None;

    for (index, row) in event_rows.iter().enumerate() {
        let event = &row.event;
        if let Some(recovery) = event.provider_recovery.as_ref() {
            referenced_proofs.extend(
                recovery
                    .proof_ids
                    .iter()
                    .map(|proof_id| proof_id.as_str().to_owned()),
            );
        }
        verifier.verify_event(event, index + 1 == event_rows.len(), &mut epoch, findings);
    }

    verifier.finish(
        epoch.as_ref(),
        event_rows.last().map(|row| &row.event),
        &referenced_proofs,
        findings,
    );
}

struct ProviderRecoveryVerifier<'a> {
    session: &'a PaperSessionView,
    checkpoints: &'a BTreeMap<MarketId, &'a PaperMarketCheckpoint>,
    coverage: &'a VerifiedProviderCoverage,
    available_proofs: BTreeSet<String>,
}

impl<'a> ProviderRecoveryVerifier<'a> {
    fn new(
        session: &'a PaperSessionView,
        checkpoints: &'a BTreeMap<MarketId, &'a PaperMarketCheckpoint>,
        coverage: &'a VerifiedProviderCoverage,
    ) -> Self {
        let available_proofs = coverage
            .proof_ids()
            .into_iter()
            .map(|proof_id| proof_id.as_str().to_owned())
            .collect();
        Self {
            session,
            checkpoints,
            coverage,
            available_proofs,
        }
    }

    fn verify_event(
        &self,
        event: &PaperSessionEvent,
        is_last_event: bool,
        epoch: &mut Option<ProviderRecoveryEpoch>,
        findings: &mut Findings,
    ) {
        match (event.kind, event.provider_recovery.as_ref()) {
            (PaperEventKind::CandleProcessed, Some(recovery))
                if recovery.phase == PaperProviderRecoveryPhase::CatchUp =>
            {
                self.verify_catchup(event, recovery, epoch, findings);
            }
            (PaperEventKind::CandleProcessed, Some(_)) => {
                findings.fail("PROVIDER_RECOVERY_PHASE_EVENT_KIND");
            }
            (PaperEventKind::CandleProcessed, None) if epoch.is_some() => {
                findings.fail("PROVIDER_RECOVERY_UNMARKED_CATCHUP_EVENT");
            }
            (PaperEventKind::ProviderRecovery, Some(recovery)) => match recovery.phase {
                PaperProviderRecoveryPhase::Started => {
                    self.verify_started(event, recovery, epoch, findings);
                }
                PaperProviderRecoveryPhase::Progress | PaperProviderRecoveryPhase::Completed => {
                    self.verify_progress(event, recovery, is_last_event, epoch, findings);
                }
                PaperProviderRecoveryPhase::CatchUp => {
                    findings.fail("PROVIDER_RECOVERY_MARKER_PHASE");
                }
            },
            (PaperEventKind::ProviderRecovery, None) => {
                findings.fail("PROVIDER_RECOVERY_MARKER_MISSING_METADATA");
            }
            (_, Some(_)) => {
                findings.fail("PROVIDER_RECOVERY_METADATA_ON_UNRELATED_EVENT");
            }
            (_, None) => {}
        }
    }

    fn verify_catchup(
        &self,
        event: &PaperSessionEvent,
        recovery: &PaperProviderRecovery,
        epoch: &mut Option<ProviderRecoveryEpoch>,
        findings: &mut Findings,
    ) {
        let Some(active) = epoch.as_mut() else {
            findings.fail("PROVIDER_RECOVERY_CATCHUP_WITHOUT_START");
            return;
        };
        let recovery_proofs = recovery_proof_ids(recovery, &self.available_proofs);
        let target_valid = provider_recovery_target_valid(
            recovery.target_completed_at,
            active.target_completed_at,
            self.session.decision_interval,
            recovery.processed_at,
        );
        let catchup_valid = recovery_proofs.as_ref().is_some_and(|proofs| {
            active
                .proof_ids
                .iter()
                .all(|proof_id| proofs.contains(proof_id))
        }) && recovery_proofs_precede_processing(recovery, self.coverage)
            && target_valid
            && active
                .last_processed_at
                .is_none_or(|at| recovery.processed_at >= at)
            && event.source_event_at == Some(recovery.processed_at)
            && event
                .candle
                .as_ref()
                .is_some_and(|candle| candle.close_time <= recovery.target_completed_at);
        if !catchup_valid {
            findings.fail("PROVIDER_RECOVERY_CATCHUP_IDENTITY_OR_TIME");
            return;
        }
        if let Some(proofs) = recovery_proofs {
            active.proof_ids.extend(proofs);
        }
        active.last_processed_at = Some(recovery.processed_at);
        active.target_completed_at = Some(recovery.target_completed_at);
    }

    fn verify_started(
        &self,
        event: &PaperSessionEvent,
        recovery: &PaperProviderRecovery,
        epoch: &mut Option<ProviderRecoveryEpoch>,
        findings: &mut Findings,
    ) {
        if epoch.is_some()
            || !provider_recovery_marker_shape(event, recovery, true)
            || !recovery_proofs_precede_processing(recovery, self.coverage)
            || !provider_recovery_target_valid(
                recovery.target_completed_at,
                None,
                self.session.decision_interval,
                recovery.processed_at,
            )
        {
            findings.fail("PROVIDER_RECOVERY_STARTED_SHAPE");
        }
        let Some(proof_ids) = recovery_proof_ids(recovery, &self.available_proofs) else {
            findings.fail("PROVIDER_RECOVERY_PROOF_REFERENCE");
            *epoch = None;
            return;
        };
        if proof_ids.is_empty() {
            findings.fail("PROVIDER_RECOVERY_PROOF_REFERENCE");
        }
        if !verify_checkpoint_transitions(
            self.session,
            recovery,
            &BTreeMap::new(),
            self.checkpoints,
            false,
            findings,
        ) {
            findings.fail("PROVIDER_RECOVERY_CHECKPOINT_TRANSITION");
        }
        *epoch = Some(ProviderRecoveryEpoch {
            target_completed_at: Some(recovery.target_completed_at),
            proof_ids,
            last_processed_at: Some(recovery.processed_at),
            last_transitions: recovery
                .checkpoint_transitions
                .iter()
                .map(|transition| {
                    (
                        transition.market.clone(),
                        (transition.after_digest.clone(), transition.to_close),
                    )
                })
                .collect(),
        });
    }

    fn verify_progress(
        &self,
        event: &PaperSessionEvent,
        recovery: &PaperProviderRecovery,
        is_last_event: bool,
        epoch: &mut Option<ProviderRecoveryEpoch>,
        findings: &mut Findings,
    ) {
        let Some(active) = epoch.as_mut() else {
            findings.fail("PROVIDER_RECOVERY_MARKER_WITHOUT_START");
            return;
        };
        let completed = recovery.phase == PaperProviderRecoveryPhase::Completed;
        let recovery_proofs = recovery_proof_ids(recovery, &self.available_proofs);
        let proof_set_is_monotonic = recovery_proofs.as_ref().is_some_and(|proofs| {
            active
                .proof_ids
                .iter()
                .all(|proof_id| proofs.contains(proof_id))
        });
        let target_valid = provider_recovery_target_valid(
            recovery.target_completed_at,
            active.target_completed_at,
            self.session.decision_interval,
            recovery.processed_at,
        );
        let marker_valid = provider_recovery_marker_shape(event, recovery, false)
            && proof_set_is_monotonic
            && recovery_proofs_precede_processing(recovery, self.coverage)
            && target_valid
            && active
                .last_processed_at
                .is_none_or(|at| recovery.processed_at >= at);
        if !marker_valid {
            findings.fail("PROVIDER_RECOVERY_PROGRESS_SHAPE");
        }
        let transitions_valid = verify_checkpoint_transitions(
            self.session,
            recovery,
            &active.last_transitions,
            self.checkpoints,
            is_last_event,
            findings,
        );
        if !transitions_valid {
            findings.fail("PROVIDER_RECOVERY_CHECKPOINT_TRANSITION");
        }
        if !marker_valid || !transitions_valid {
            return;
        }
        active.last_processed_at = Some(recovery.processed_at);
        active.target_completed_at = Some(recovery.target_completed_at);
        if let Some(proofs) = recovery_proofs {
            active.proof_ids.extend(proofs);
        }
        active.last_transitions = recovery
            .checkpoint_transitions
            .iter()
            .map(|transition| {
                (
                    transition.market.clone(),
                    (transition.after_digest.clone(), transition.to_close),
                )
            })
            .collect();
        if completed {
            *epoch = None;
        }
    }

    fn finish(
        &self,
        epoch: Option<&ProviderRecoveryEpoch>,
        last_event: Option<&PaperSessionEvent>,
        referenced_proofs: &BTreeSet<String>,
        findings: &mut Findings,
    ) {
        if epoch.is_some() {
            let awaiting_later_chunk = self.session.state == PaperSessionState::RecoveryRequired
                && last_event.is_some_and(|event| {
                    event.kind == PaperEventKind::ProviderRecovery
                        && event.provider_recovery.as_ref().is_some_and(|recovery| {
                            recovery.phase == PaperProviderRecoveryPhase::Progress
                        })
                });
            if awaiting_later_chunk {
                findings.unavailable("PROVIDER_RECOVERY_IN_PROGRESS");
            } else {
                findings.fail("PROVIDER_RECOVERY_EPOCH_NOT_CLOSED");
            }
        }
        if referenced_proofs != &self.available_proofs {
            findings.fail("PROVIDER_RECOVERY_PROOF_SET_MISMATCH");
        }
    }
}

fn recovery_proof_ids(
    recovery: &PaperProviderRecovery,
    available_proofs: &BTreeSet<String>,
) -> Option<BTreeSet<String>> {
    let mut proof_ids = BTreeSet::new();
    for proof_id in &recovery.proof_ids {
        if !available_proofs.contains(proof_id.as_str())
            || !proof_ids.insert(proof_id.as_str().to_owned())
        {
            return None;
        }
    }
    Some(proof_ids)
}

fn recovery_proofs_precede_processing(
    recovery: &PaperProviderRecovery,
    coverage: &VerifiedProviderCoverage,
) -> bool {
    recovery.proof_ids.iter().all(|proof_id| {
        coverage
            .spans()
            .iter()
            .find(|span| span.proof_id == *proof_id)
            .is_some_and(|span| {
                span.snapshots
                    .iter()
                    .all(|snapshot| snapshot.fetched_at_utc <= recovery.processed_at)
            })
    })
}

fn provider_recovery_target_valid(
    target: UtcTimestamp,
    previous: Option<UtcTimestamp>,
    interval: CandleInterval,
    processed_at: UtcTimestamp,
) -> bool {
    target <= processed_at
        && previous.is_none_or(|previous| target >= previous)
        && crate::scheduling::latest_completed_boundary(target, interval).ok() == Some(target)
}

fn provider_recovery_marker_shape(
    event: &PaperSessionEvent,
    recovery: &PaperProviderRecovery,
    started: bool,
) -> bool {
    let marker_shell = event.market.is_none()
        && event.candle.is_none()
        && event.finalized_at.is_none()
        && event.control.is_none()
        && event.source_event_at == Some(recovery.processed_at)
        && event.signals.is_empty()
        && event.orders.is_empty()
        && event.fills.is_empty()
        && event.account_marks.is_empty()
        && event.episodes.is_empty()
        && !recovery.checkpoint_transitions.is_empty();
    let order_events_valid = if started {
        event
            .order_events
            .iter()
            .all(|order| order.status == OrderStatus::Cancelled)
    } else {
        event.order_events.is_empty()
    };
    marker_shell && order_events_valid
}

fn verify_checkpoint_transitions(
    session: &PaperSessionView,
    recovery: &PaperProviderRecovery,
    previous: &BTreeMap<MarketId, (ContentHash, Option<UtcTimestamp>)>,
    checkpoints: &BTreeMap<MarketId, &PaperMarketCheckpoint>,
    is_last_event: bool,
    findings: &mut Findings,
) -> bool {
    let mut transitions = BTreeMap::new();
    for transition in &recovery.checkpoint_transitions {
        if transitions
            .insert(transition.market.clone(), transition)
            .is_some()
            || transition
                .from_close
                .zip(transition.to_close)
                .is_some_and(|(from, to)| to < from)
        {
            return false;
        }
        if let Some((previous_digest, previous_close)) = previous.get(&transition.market)
            && (transition.before_digest != *previous_digest
                || transition.from_close != *previous_close)
        {
            return false;
        }
    }
    let session_markets = session
        .markets
        .iter()
        .map(|snapshot| snapshot.market.clone())
        .collect::<BTreeSet<_>>();
    if transitions.keys().cloned().collect::<BTreeSet<_>>() != session_markets {
        return false;
    }

    if is_last_event {
        for (market, transition) in transitions {
            let Some(checkpoint) = checkpoints.get(&market).copied() else {
                return false;
            };
            let Ok(digest) = checkpoint.checkpoint_digest() else {
                findings.fail("PROVIDER_RECOVERY_CHECKPOINT_DIGEST");
                return false;
            };
            if transition.after_digest != digest
                || transition.to_close != checkpoint.last_finalized_candle_at
            {
                return false;
            }
        }
    }
    true
}

fn verify_market(
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    event_rows: &[StoredEventRow],
    observations: &ValidatedObservations<'_>,
    provider_coverage: &VerifiedProviderCoverage,
    findings: &mut Findings,
) {
    verify_checkpoint_identity(session, snapshot, checkpoint, findings);
    let mut facts = collect_market_facts(
        session,
        snapshot,
        checkpoint,
        event_rows,
        observations,
        provider_coverage,
        findings,
    );
    let replay = replay_market_facts(snapshot, checkpoint, observations, &mut facts, findings);
    verify_market_projections(session, snapshot, checkpoint, &facts, &replay, findings);
}

fn verify_checkpoint_identity(
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    findings: &mut Findings,
) {
    if checkpoint.market != snapshot.market
        || checkpoint.model_id != snapshot.model_id
        || checkpoint.initial_cash != snapshot.initial_cash
        || checkpoint.plan.spec.decision_interval != session.decision_interval
        || checkpoint.plan.spec.execution_resolution != session.execution_resolution
        || checkpoint.engine_state.model_id != snapshot.model_id
        || checkpoint.engine_state.market != snapshot.market
    {
        findings.fail("CHECKPOINT_IMMUTABLE_IDENTITY");
    }
    if checkpoint
        .verify_integrity(
            &session.session_id,
            &snapshot.market,
            &session.policy_ref,
            snapshot.initial_cash,
        )
        .is_err()
    {
        findings.fail("CHECKPOINT_INTEGRITY");
    }
    let frozen_policy_matches = checkpoint
        .plan
        .policy_revisions
        .iter()
        .find(|revision| revision.reference == session.policy_ref)
        .is_some_and(|revision| {
            ContentHash::of_value(&revision.definition)
                .is_ok_and(|digest| digest == session.policy_ref.definition_digest)
        });
    if !checkpoint
        .plan
        .spec
        .policy_selections
        .contains(&session.policy_ref)
        || !frozen_policy_matches
    {
        findings.fail("FROZEN_POLICY_IDENTITY");
    }
    if checkpoint.costs != checkpoint.plan.spec.costs {
        findings.fail("FROZEN_COST_POLICY");
    }
    let expected_run_id = format!("run-{}", session.session_id.as_str());
    if checkpoint.engine_state.run_id.as_str() != expected_run_id
        || checkpoint.engine_state.record_count != 0
        || !checkpoint.engine_state.signals.is_empty()
        || !checkpoint.engine_state.orders.is_empty()
        || !checkpoint.engine_state.order_events.is_empty()
        || !checkpoint.engine_state.fills.is_empty()
        || !checkpoint.engine_state.marks.is_empty()
        || !checkpoint.engine_state.episodes.is_empty()
    {
        findings.fail("CHECKPOINT_UNDRAINED_OR_RUN_ID");
    }
}

#[derive(Default)]
struct CandleSequence {
    seen_close_ids: BTreeSet<(UtcTimestamp, ObservationId)>,
    last_processed_close: Option<UtcTimestamp>,
    last_processed_digest: Option<ContentHash>,
}

#[derive(Default)]
struct MarketFacts<'a> {
    facts: Vec<Fact<'a>>,
    signal_by_id: BTreeMap<SignalId, &'a SignalRecord>,
    fill_ids: BTreeSet<FillId>,
    fill_by_mark_sequence: BTreeMap<u64, &'a FillRecord>,
    order_projections: Vec<&'a OrderRecord>,
    candle_sequence: CandleSequence,
    processed_event_count: u64,
    signal_count: u64,
    last_source_event_at: Option<UtcTimestamp>,
}

fn collect_market_facts<'a>(
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &'a PaperMarketCheckpoint,
    event_rows: &'a [StoredEventRow],
    observations: &ValidatedObservations<'_>,
    provider_coverage: &VerifiedProviderCoverage,
    findings: &mut Findings,
) -> MarketFacts<'a> {
    let mut facts = MarketFacts::default();
    if let Some(initial) = &checkpoint.initial_mark {
        validate_context(&initial.context, checkpoint, None, findings);
        facts.facts.push(Fact::Mark(initial));
    }

    for row in event_rows {
        let event = &row.event;
        if event
            .market
            .as_ref()
            .is_some_and(|market| market != &snapshot.market)
        {
            continue;
        }
        if event.market.as_ref() == Some(&snapshot.market)
            && matches!(
                event.kind,
                PaperEventKind::CandleProcessed | PaperEventKind::RecoveryRequired
            )
        {
            facts.processed_event_count = facts.processed_event_count.saturating_add(1);
        }
        verify_event_shape(
            event,
            session,
            snapshot,
            observations,
            provider_coverage,
            &mut facts.candle_sequence,
            findings,
        );
        collect_event_facts(
            event,
            session,
            snapshot,
            checkpoint,
            observations,
            &mut facts,
            findings,
        );
        if event.kind == PaperEventKind::CandleProcessed {
            facts.last_source_event_at = event.source_event_at;
        }
    }
    facts
}

fn collect_event_facts<'a>(
    event: &'a PaperSessionEvent,
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    observations: &ValidatedObservations<'_>,
    facts: &mut MarketFacts<'a>,
    findings: &mut Findings,
) {
    collect_signals_and_orders(
        event,
        session,
        snapshot,
        checkpoint,
        observations,
        facts,
        findings,
    );
    collect_fills_and_marks(event, snapshot, checkpoint, facts, findings);
    verify_event_episode_control(event, snapshot, checkpoint, findings);
}

fn collect_signals_and_orders<'a>(
    event: &'a PaperSessionEvent,
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    observations: &ValidatedObservations<'_>,
    facts: &mut MarketFacts<'a>,
    findings: &mut Findings,
) {
    for signal in &event.signals {
        if signal.context.market != snapshot.market {
            continue;
        }
        validate_context(&signal.context, checkpoint, event.market.as_ref(), findings);
        if facts
            .signal_by_id
            .insert(signal.signal_id.clone(), signal)
            .is_some()
        {
            findings.fail("DUPLICATE_SIGNAL_ID");
        }
        verify_signal(signal, session, observations, findings);
        facts.facts.push(Fact::Signal(signal));
        facts.signal_count = facts.signal_count.saturating_add(1);
    }
    for order in &event.order_events {
        if order.context.market != snapshot.market {
            continue;
        }
        validate_context(&order.context, checkpoint, event.market.as_ref(), findings);
        facts.facts.push(Fact::Order(order));
        if event.kind == PaperEventKind::ProviderRecovery
            && event
                .provider_recovery
                .as_ref()
                .is_some_and(|recovery| recovery.phase == PaperProviderRecoveryPhase::Started)
            && order.status == OrderStatus::Cancelled
        {
            facts.order_projections.push(order);
        }
    }
    for order in &event.orders {
        if order.context.market != snapshot.market {
            continue;
        }
        validate_context(&order.context, checkpoint, event.market.as_ref(), findings);
        facts.order_projections.push(order);
    }
    if let Some(control) = &event.control {
        for cancelled in &control.cancelled_pending_orders {
            if cancelled.context.market != snapshot.market {
                continue;
            }
            validate_context(
                &cancelled.context,
                checkpoint,
                event.market.as_ref(),
                findings,
            );
            facts.order_projections.push(cancelled);
        }
    }
}

fn collect_fills_and_marks<'a>(
    event: &'a PaperSessionEvent,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    facts: &mut MarketFacts<'a>,
    findings: &mut Findings,
) {
    for fill in &event.fills {
        if fill.context.market != snapshot.market {
            continue;
        }
        validate_context(&fill.context, checkpoint, event.market.as_ref(), findings);
        if !facts.fill_ids.insert(fill.fill_id.clone()) {
            findings.fail("DUPLICATE_FILL_ID");
        }
        if facts
            .fill_by_mark_sequence
            .insert(fill.accounting_mark_seq, fill)
            .is_some()
        {
            findings.fail("DUPLICATE_FILL_MARK_SEQUENCE");
        }
        facts.facts.push(Fact::Fill(fill));
    }
    for mark in &event.account_marks {
        if mark.context.market != snapshot.market {
            continue;
        }
        validate_context(&mark.context, checkpoint, event.market.as_ref(), findings);
        facts.facts.push(Fact::Mark(mark));
    }
}

fn verify_event_episode_control(
    event: &PaperSessionEvent,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    findings: &mut Findings,
) {
    for episode in &event.episodes {
        if episode.model_id != snapshot.model_id
            || episode.market != snapshot.market
            || episode.run_id != checkpoint.engine_state.run_id
        {
            findings.fail("EPISODE_IDENTITY");
        }
    }
    if let Some(control) = &event.control
        && (control.cancelled_pending_orders.len() != event.order_events.len()
            || control.cancelled_pending_orders.iter().any(|cancelled| {
                !event.order_events.iter().any(|record| {
                    record.order_id == cancelled.order_id
                        && record.context.event_seq == cancelled.context.event_seq
                        && record.status == cancelled.status
                        && record.cumulative_filled_qty == cancelled.cumulative_filled_qty
                })
            }))
    {
        findings.fail("CONTROL_ORDER_EVENT_PARITY");
    }
}

fn replay_market_facts(
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    observations: &ValidatedObservations<'_>,
    facts: &mut MarketFacts<'_>,
    findings: &mut Findings,
) -> Replay {
    facts.facts.sort_by_key(Fact::sort_key);
    let mut fact_sequences = BTreeSet::new();
    for fact in &facts.facts {
        if !fact_sequences.insert(fact.context_ref().event_seq) {
            findings.fail("DUPLICATE_MARKET_FACT_SEQUENCE");
        }
    }
    let sequence_count = u64::try_from(fact_sequences.len()).unwrap_or(u64::MAX);
    if sequence_count != checkpoint.engine_state.seq
        || fact_sequences
            .iter()
            .enumerate()
            .any(|(index, sequence)| u64::try_from(index).ok() != Some(*sequence))
    {
        findings.fail("MARKET_FACT_SEQUENCE");
    }
    if checkpoint.initial_mark.is_none() && checkpoint.engine_state.seq > 0 {
        findings.fail("INITIAL_MARK_MISSING");
    }

    let mut replay = Replay::new(snapshot.initial_cash.get());
    replay.last_source_event_at = facts.last_source_event_at;
    for fact in facts.facts.iter().copied() {
        let result = match fact {
            Fact::Signal(signal) => {
                replay.register_signal(signal);
                Ok(())
            }
            Fact::Order(order) => {
                if order.status == OrderStatus::Created
                    && !created_order_parent_is_causal(order, &facts.signal_by_id)
                {
                    findings.fail("ORDER_PARENT_SIGNAL_MISSING_OR_NONCAUSAL");
                }
                replay.apply_order_event(order)
            }
            Fact::Fill(fill) => replay.apply_fill(
                fill,
                &facts.signal_by_id,
                checkpoint,
                observations,
                findings,
            ),
            Fact::Mark(mark) => replay.apply_mark(
                mark,
                checkpoint,
                observations,
                &facts.fill_by_mark_sequence,
                findings,
            ),
        };
        if let Err(code) = result {
            findings.fail(code);
            break;
        }
    }
    replay
}

fn verify_market_projections(
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    checkpoint: &PaperMarketCheckpoint,
    facts: &MarketFacts<'_>,
    replay: &Replay,
    findings: &mut Findings,
) {
    if replay.has_incomplete_order_transition() {
        findings.fail("ORDER_TRANSITION_WITHOUT_FILL");
    }
    for (mark_sequence, fill) in &facts.fill_by_mark_sequence {
        if !replay.after_fill_marks.contains_key(mark_sequence) {
            findings.fail("AFTER_FILL_MARK_MISSING");
        }
        if fill.accounting_mark_seq <= fill.context.event_seq {
            findings.fail("FILL_MARK_SEQUENCE");
        }
    }
    verify_order_projections(
        replay,
        &facts.order_projections,
        checkpoint.pending.as_ref(),
        session.state,
        findings,
    );
    if replay.fills != snapshot.fill_count || replay.fills != checkpoint.fill_count {
        findings.fail("FILL_COUNT");
    }
    if facts.signal_count != snapshot.signal_count || facts.signal_count != checkpoint.signal_count
    {
        findings.fail("SIGNAL_COUNT");
    }
    if facts.processed_event_count != snapshot.event_count
        || facts.processed_event_count != checkpoint.market_event_count
    {
        findings.fail("MARKET_EVENT_COUNT");
    }
    if facts.candle_sequence.last_processed_close != checkpoint.last_finalized_candle_at
        || facts.candle_sequence.last_processed_close != snapshot.last_finalized_candle_at
        || facts.candle_sequence.last_processed_digest != checkpoint.last_finalized_candle_digest
    {
        findings.fail("LAST_FINALIZED_CANDLE");
    }
    if facts.last_source_event_at != checkpoint.last_source_event_at
        || facts.last_source_event_at != snapshot.last_source_event_at
    {
        findings.fail("LAST_SOURCE_EVENT");
    }
    verify_checkpoint_and_snapshot(checkpoint, snapshot, replay, findings);
}

fn created_order_parent_is_causal(
    order: &OrderRecord,
    signals: &BTreeMap<SignalId, &SignalRecord>,
) -> bool {
    signals.get(&order.parent_signal_id).is_some_and(|signal| {
        signal.context.market == order.context.market
            && signal.context.event_seq < order.context.event_seq
    })
}

fn passive_fill_matches_limit(
    order_type: SimulatedOrderType,
    order_side: Side,
    requested_price: Option<crate::contracts::PriceKrw>,
    fill_side: Side,
    fill_price: crate::contracts::PriceKrw,
) -> bool {
    order_type != SimulatedOrderType::PassiveBuyLimit
        || (order_side == Side::Buy
            && fill_side == Side::Buy
            && requested_price == Some(fill_price))
}

fn verify_event_shape(
    event: &PaperSessionEvent,
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    observations: &ValidatedObservations<'_>,
    provider_coverage: &VerifiedProviderCoverage,
    sequence: &mut CandleSequence,
    findings: &mut Findings,
) {
    match event.kind {
        PaperEventKind::DecisionDeferred => {
            let matches_source = event
                .candle
                .as_ref()
                .zip(event.preparation.as_ref())
                .is_some_and(|(candle, preparation)| {
                    preparation.market == snapshot.market
                        && preparation.interval == session.decision_interval
                        && preparation.observation_id == candle.observation_id
                        && preparation.decision_time == candle.close_time
                        && preparation.required_bars > 0
                        && event
                            .source_event_at
                            .is_some_and(|at| at >= candle.close_time)
                });
            if !matches_source
                || event.control.is_some()
                || event.finalized_at.is_some()
                || event.provider_recovery.is_some()
                || !event.signals.is_empty()
                || !event.orders.is_empty()
                || !event.order_events.is_empty()
                || !event.fills.is_empty()
                || !event.account_marks.is_empty()
                || !event.episodes.is_empty()
            {
                findings.fail("DEFERRED_DECISION_EVENT_SHAPE");
            }
        }
        PaperEventKind::CandleProcessed => verify_candle_processed_event_shape(
            event,
            session,
            snapshot,
            observations,
            provider_coverage,
            sequence,
            findings,
        ),
        PaperEventKind::Pause => {
            verify_control_event_shape(event, PaperSessionCommandAction::Pause, findings);
        }
        PaperEventKind::Resume => {
            verify_control_event_shape(event, PaperSessionCommandAction::Resume, findings);
        }
        PaperEventKind::Stop => {
            verify_control_event_shape(event, PaperSessionCommandAction::Stop, findings);
        }
        PaperEventKind::RecoveryRequired => {
            verify_recovery_required_event_shape(event, session, snapshot, observations, findings);
        }
        PaperEventKind::ProviderRecovery => {}
    }
}

fn verify_candle_processed_event_shape(
    event: &PaperSessionEvent,
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    observations: &ValidatedObservations<'_>,
    provider_coverage: &VerifiedProviderCoverage,
    sequence: &mut CandleSequence,
    findings: &mut Findings,
) {
    if event.market.as_ref() != Some(&snapshot.market)
        || event.control.is_some()
        || event.candle.is_none()
        || event.source_event_at.is_none()
        || event.finalized_at.is_none()
    {
        findings.fail("CANDLE_EVENT_SHAPE");
        return;
    }
    let Some(candle) = event.candle.as_ref() else {
        return;
    };
    verify_catchup_event_shape(event, candle, findings);
    validate_candle_reference(
        candle,
        &snapshot.market,
        session.decision_interval,
        event.finalized_at,
        observations,
        findings,
    );
    if event.finalized_at != Some(candle.close_time)
        || event
            .source_event_at
            .is_some_and(|at| at < candle.close_time)
    {
        findings.fail("CANDLE_EVENT_TIME");
    }
    verify_candle_sequence(
        event,
        candle,
        &snapshot.market,
        session.decision_interval,
        provider_coverage,
        sequence,
        findings,
    );
    if !sequence
        .seen_close_ids
        .insert((candle.close_time, candle.observation_id.clone()))
    {
        findings.fail("DUPLICATE_CANDLE_EVENT");
    }
    sequence.last_processed_close = Some(candle.close_time);
    sequence.last_processed_digest = Some(candle.content_digest.clone());
}

fn verify_catchup_event_shape(
    event: &PaperSessionEvent,
    candle: &crate::contracts::PaperCandleReference,
    findings: &mut Findings,
) {
    if let Some(recovery) = event.provider_recovery.as_ref()
        && (recovery.phase != PaperProviderRecoveryPhase::CatchUp
            || !recovery.checkpoint_transitions.is_empty()
            || recovery.proof_ids.is_empty()
            || !event.signals.is_empty()
            || !event.orders.is_empty()
            || !event.order_events.is_empty()
            || !event.fills.is_empty()
            || !event.episodes.is_empty()
            || event.source_event_at != Some(recovery.processed_at)
            || recovery.processed_at < candle.close_time
            || candle.close_time > recovery.target_completed_at)
    {
        findings.fail("PROVIDER_RECOVERY_CATCHUP_SHAPE");
    }
}

fn verify_candle_sequence(
    event: &PaperSessionEvent,
    candle: &crate::contracts::PaperCandleReference,
    market: &MarketId,
    interval: CandleInterval,
    provider_coverage: &VerifiedProviderCoverage,
    sequence: &CandleSequence,
    findings: &mut Findings,
) {
    let Some(previous_close) = sequence.last_processed_close else {
        return;
    };
    let invalid_time_order = candle.close_time <= previous_close;
    let uncovered_gap = candle.open_time != previous_close
        && (candle.open_time < previous_close
            || !provider_coverage_covers_event_gap(
                event,
                market,
                interval,
                previous_close,
                candle.open_time,
                provider_coverage,
            ));
    if invalid_time_order || uncovered_gap {
        findings.fail("CANDLE_SEQUENCE_OR_GAP");
    }
}

fn verify_recovery_required_event_shape(
    event: &PaperSessionEvent,
    session: &PaperSessionView,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    observations: &ValidatedObservations<'_>,
    findings: &mut Findings,
) {
    if !event.signals.is_empty()
        || !event.orders.is_empty()
        || !event.fills.is_empty()
        || !event.account_marks.is_empty()
        || !event.episodes.is_empty()
        || event.provider_recovery.is_some()
    {
        findings.fail("RECOVERY_EVENT_HAS_ECONOMIC_FACTS");
        return;
    }
    match (&event.market, &event.candle, &event.control) {
        (Some(market), Some(candle), None) if market == &snapshot.market => {
            validate_candle_reference(
                candle,
                &snapshot.market,
                session.decision_interval,
                event.finalized_at,
                observations,
                findings,
            );
            if event.finalized_at != Some(candle.close_time)
                || event
                    .source_event_at
                    .is_none_or(|at| at < candle.close_time)
            {
                findings.fail("RECOVERY_CANDLE_TIME");
            }
        }
        (None, None, None) if event.source_event_at.is_some() => {}
        (None, None, Some(control))
            if control.action == PaperSessionCommandAction::Resume
                && event.source_event_at.is_none()
                && event.finalized_at.is_none() => {}
        _ => findings.fail("RECOVERY_EVENT_SHAPE"),
    }
}

fn provider_coverage_covers_event_gap(
    event: &PaperSessionEvent,
    market: &MarketId,
    interval: CandleInterval,
    start: UtcTimestamp,
    end: UtcTimestamp,
    coverage: &VerifiedProviderCoverage,
) -> bool {
    let Some(recovery) = event.provider_recovery.as_ref() else {
        return false;
    };
    if recovery.phase != PaperProviderRecoveryPhase::CatchUp || recovery.proof_ids.is_empty() {
        return false;
    }
    let proof_ids = recovery
        .proof_ids
        .iter()
        .map(|proof_id| proof_id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    if proof_ids.len() != recovery.proof_ids.len() {
        return false;
    }
    let spans = coverage
        .spans()
        .iter()
        .filter(|span| proof_ids.contains(span.proof_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if spans.len() != proof_ids.len() {
        return false;
    }
    VerifiedProviderCoverage::from_verified_spans(spans)
        .is_ok_and(|scoped| scoped.covers(market, interval, start, end))
}

fn verify_control_event_shape(
    event: &PaperSessionEvent,
    expected_action: PaperSessionCommandAction,
    findings: &mut Findings,
) {
    if event.market.is_some()
        || event.candle.is_some()
        || event.finalized_at.is_some()
        || event.source_event_at.is_some()
        || event.provider_recovery.is_some()
        || event.control.as_ref().map(|control| control.action) != Some(expected_action)
        || !event.signals.is_empty()
        || !event.orders.is_empty()
        || !event.fills.is_empty()
        || !event.account_marks.is_empty()
        || !event.episodes.is_empty()
    {
        findings.fail("CONTROL_EVENT_SHAPE");
    }
}

fn validate_candle_reference(
    reference: &crate::contracts::PaperCandleReference,
    market: &MarketId,
    expected_interval: CandleInterval,
    decision_boundary: Option<UtcTimestamp>,
    observations: &ValidatedObservations<'_>,
    findings: &mut Findings,
) {
    if reference.interval != expected_interval {
        findings.fail("CANDLE_REFERENCE_INTERVAL");
    }
    let Some(validated) = observations.get(&reference.observation_id) else {
        findings.unavailable("CANDLE_OBSERVATION_NOT_CAPTURED");
        return;
    };
    verify_observation(
        validated,
        &reference.observation_id,
        market,
        Some(expected_interval),
        decision_boundary,
        findings,
    );
    let observation = validated.observation;
    if observation.candle.open_time_utc != reference.open_time
        || observation.candle.close_time_utc != reference.close_time
        || observation.content_digest != reference.content_digest
    {
        findings.fail("CANDLE_REFERENCE_MISMATCH");
    }
}

fn verify_signal(
    signal: &SignalRecord,
    session: &PaperSessionView,
    observations: &ValidatedObservations<'_>,
    findings: &mut Findings,
) {
    if signal.policy_ref.as_ref() != Some(&session.policy_ref)
        || signal.source_bar_ids.is_empty()
        || signal.signal_time != signal.decision_available_at
        || signal.context.accounting_event_time != signal.decision_available_at
        || signal.valid_until < signal.decision_available_at
    {
        findings.fail("SIGNAL_IDENTITY_OR_TIME");
    }
    let mut seen = BTreeSet::new();
    for source_id in &signal.source_bar_ids {
        if !seen.insert(source_id) {
            findings.fail("DUPLICATE_SIGNAL_SOURCE");
        }
        if let Some(validated) = observations.get(source_id) {
            verify_observation(
                validated,
                source_id,
                &signal.context.market,
                None,
                Some(signal.decision_available_at),
                findings,
            );
        } else {
            findings.unavailable("SIGNAL_SOURCE_NOT_CAPTURED");
        }
    }
    if let Some(decision_id) = signal.source_bar_ids.last()
        && let Some(decision) = observations.get(decision_id)
        && (decision.observation.candle.interval != session.decision_interval
            || decision.observation.candle.close_time_utc != signal.decision_available_at)
    {
        findings.fail("SIGNAL_DECISION_REFERENCE");
    }
}

fn verify_observation(
    validated: &ValidatedObservation<'_>,
    expected_id: &ObservationId,
    market: &MarketId,
    expected_interval: Option<CandleInterval>,
    latest_close: Option<UtcTimestamp>,
    findings: &mut Findings,
) {
    if !validated.digest_serializable {
        findings.fail("OBSERVATION_DIGEST_SERIALIZATION");
        return;
    }
    let observation = validated.observation;
    if &observation.id != expected_id || !validated.id_and_digest_valid {
        findings.fail("OBSERVATION_ID_OR_DIGEST");
    }
    if observation.candle.market != market.code()
        || expected_interval.is_some_and(|interval| observation.candle.interval != interval)
        || !observation.candle.completed
    {
        findings.fail("OBSERVATION_MARKET_INTERVAL_OR_COMPLETION");
    }
    if !validated.boundary_valid {
        findings.fail("OBSERVATION_CANDLE_BOUNDARY");
    }
    if latest_close.is_some_and(|at| observation.candle.close_time_utc > at) {
        findings.fail("OBSERVATION_AFTER_DECISION");
    }
}

fn validate_context(
    context: &EventContext,
    checkpoint: &PaperMarketCheckpoint,
    event_market: Option<&MarketId>,
    findings: &mut Findings,
) {
    if context.run_id != checkpoint.engine_state.run_id
        || context.model_id != checkpoint.model_id
        || context.market != checkpoint.market
        || event_market.is_some_and(|market| market != &context.market)
    {
        findings.fail("FACT_CONTEXT_IDENTITY");
    }
}

fn verify_fill_references(
    fill: &FillRecord,
    order: &OrderRecord,
    signals: &BTreeMap<SignalId, &SignalRecord>,
    observations: &ValidatedObservations<'_>,
    checkpoint: &PaperMarketCheckpoint,
    findings: &mut Findings,
) {
    let Some(signal) = signals.get(&order.parent_signal_id).copied() else {
        findings.fail("FILL_PARENT_SIGNAL_MISSING");
        return;
    };
    let Some(source) = observation_for(
        &fill.source_bar_id,
        &fill.context.market,
        Some(checkpoint.plan.spec.execution_resolution),
        None,
        observations,
        findings,
        "FILL_SOURCE_NOT_CAPTURED",
    ) else {
        return;
    };
    let Some(liquidity) = observation_for(
        &fill.liquidity_source_bar_id,
        &fill.context.market,
        Some(checkpoint.plan.spec.execution_resolution),
        None,
        observations,
        findings,
        "FILL_LIQUIDITY_NOT_CAPTURED",
    ) else {
        return;
    };
    let decision = signal
        .source_bar_ids
        .last()
        .and_then(|id| observations.get(id).map(|validated| validated.observation));
    if decision.is_none() {
        findings.unavailable("FILL_DECISION_SOURCE_NOT_CAPTURED");
    }
    let fill_time = fill.context.accounting_event_time;
    let timing_valid = match (&fill.timing, order.order_type) {
        (FillTiming::Exact { at }, SimulatedOrderType::Market)
            if (!fill.artificial_terminal_exit && *at == source.candle.open_time_utc)
                || (fill.artificial_terminal_exit && *at == source.candle.close_time_utc) =>
        {
            Some(*at)
        }
        (
            FillTiming::Interval {
                start,
                end,
                accounting_at,
            },
            SimulatedOrderType::PassiveBuyLimit,
        ) if *start == source.candle.open_time_utc
            && *end == source.candle.close_time_utc
            && *accounting_at == *end =>
        {
            Some(*accounting_at)
        }
        _ => None,
    };
    let time_valid = timing_valid == Some(fill_time)
        && fill_time >= order.effective_at
        && if fill.artificial_terminal_exit {
            fill_time <= order.expires_at && fill_time == checkpoint.plan.spec.range.end()
        } else {
            match order.order_type {
                SimulatedOrderType::Market => {
                    fill_time < order.expires_at && fill_time < checkpoint.plan.spec.range.end()
                }
                SimulatedOrderType::PassiveBuyLimit => fill_time == order.expires_at,
            }
        };
    let source_valid = source.candle.open_time_utc < source.candle.close_time_utc
        && fill.bar_open_proxy == source.candle.open
        && (fill.artificial_terminal_exit || !signal.source_bar_ids.contains(&fill.source_bar_id));
    // A missing exact decision source is unsupported evidence, already reported as
    // NOT_AVAILABLE above. Do not promote that absence to a causal contradiction.
    let decision_valid = decision.is_none_or(|bar| {
        fill.decision_reference == bar.candle.close
            && bar.candle.close_time_utc == signal.decision_available_at
    });
    let liquidity_valid = fill.liquidity_source_close_time == liquidity.candle.close_time_utc
        && (fill.artificial_terminal_exit
            || fill.liquidity_source_close_time <= source.candle.open_time_utc);
    let terminal_valid = !fill.artificial_terminal_exit
        || (checkpoint.plan.spec.terminal_policy
            == crate::contracts::TerminalPolicy::LiquidateScenario
            && fill_time == checkpoint.plan.spec.range.end()
            && fill_time <= order.expires_at);
    if !time_valid || !source_valid || !decision_valid || !liquidity_valid || !terminal_valid {
        findings.fail("FILL_CAUSAL_REFERENCE");
    }
    if fill.accounting_mark_seq <= fill.context.event_seq {
        findings.fail("FILL_MARK_SEQUENCE");
    }
}

fn observation_for<'a>(
    id: &ObservationId,
    market: &MarketId,
    interval: Option<CandleInterval>,
    latest_close: Option<UtcTimestamp>,
    observations: &'a ValidatedObservations<'a>,
    findings: &mut Findings,
    missing_code: &'static str,
) -> Option<&'a CandleObservation> {
    let Some(validated) = observations.get(id) else {
        findings.unavailable(missing_code);
        return None;
    };
    verify_observation(validated, id, market, interval, latest_close, findings);
    Some(validated.observation)
}

fn verify_mark_reference(
    mark: &AccountMark,
    checkpoint: &PaperMarketCheckpoint,
    observations: &ValidatedObservations<'_>,
    fill_by_mark_sequence: &BTreeMap<u64, &FillRecord>,
    findings: &mut Findings,
) {
    let Some(source) = observation_for(
        &mark.source_bar_id,
        &mark.context.market,
        Some(checkpoint.plan.spec.execution_resolution),
        None,
        observations,
        findings,
        "MARK_SOURCE_NOT_CAPTURED",
    ) else {
        return;
    };
    let valid = match mark.kind {
        MarkKind::Initial => {
            mark.context.accounting_event_time == source.candle.open_time_utc
                && mark.mark_price == source.candle.open
        }
        MarkKind::AfterFill => fill_by_mark_sequence
            .get(&mark.context.event_seq)
            .is_some_and(|fill| {
                fill.accounting_mark_seq == mark.context.event_seq
                    && fill.source_bar_id == mark.source_bar_id
                    && fill.context.accounting_event_time == mark.context.accounting_event_time
                    && fill.price == mark.mark_price
            }),
        MarkKind::ExecutionClose | MarkKind::Terminal => {
            source.candle.close_time_utc == mark.context.accounting_event_time
                && mark.mark_price == source.candle.close
        }
    };
    if !valid {
        findings.fail("MARK_SOURCE_CAUSALITY");
    }
}

fn verify_order_projections(
    replay: &Replay,
    projections: &[&OrderRecord],
    pending: Option<&crate::engine::PendingExecution>,
    session_state: PaperSessionState,
    findings: &mut Findings,
) {
    let mut projected = BTreeSet::new();
    for projection in projections {
        if !projected.insert(projection.order_id.clone()) {
            findings.fail("DUPLICATE_ORDER_PROJECTION");
        }
        let Some(state) = replay.orders.get(&projection.order_id) else {
            findings.fail("ORDER_PROJECTION_WITHOUT_EVENTS");
            continue;
        };
        if !same_order_identity(projection, &state.latest)
            || projection.episode_id != state.latest.episode_id
            || projection.reason != state.latest.reason
            || projection.status != state.latest.status
            || projection.cumulative_filled_qty.get() != state.latest.cumulative_filled_qty.get()
            || projection.context.event_seq != state.latest.context.event_seq
        {
            findings.fail("ORDER_PROJECTION_MISMATCH");
        }
    }
    for (order_id, state) in &replay.orders {
        if state.open {
            let matches_pending = pending.is_some_and(|item| {
                item.order.order_id == *order_id
                    && item.order.status == OrderStatus::Created
                    && item.order.cumulative_filled_qty.get() == state.filled
            });
            if !matches_pending && session_state != PaperSessionState::RecoveryRequired {
                findings.fail("OPEN_ORDER_WITHOUT_CHECKPOINT_PENDING");
            }
        } else if !projected.contains(order_id) && state.latest.status != OrderStatus::Rejected {
            findings.fail("TERMINAL_ORDER_PROJECTION_MISSING");
        }
    }
    if let Some(pending) = pending {
        let Some(state) = replay.orders.get(&pending.order.order_id) else {
            findings.fail("CHECKPOINT_PENDING_ORDER_MISSING");
            return;
        };
        if pending.order.status != OrderStatus::Created
            || !state.open
            || pending.order.cumulative_filled_qty.get() != state.filled
            || !same_order_identity(&state.created, &pending.order)
        {
            findings.fail("CHECKPOINT_PENDING_ORDER_MISMATCH");
        }
    }
}

fn same_order_identity(left: &OrderRecord, right: &OrderRecord) -> bool {
    left.order_id == right.order_id
        && left.parent_signal_id == right.parent_signal_id
        && left.side == right.side
        && left.order_type == right.order_type
        && left.requested_price == right.requested_price
        && left.requested_qty == right.requested_qty
        && left.created_at == right.created_at
        && left.effective_at == right.effective_at
        && left.expires_at == right.expires_at
        && left.rule_snapshot_id == right.rule_snapshot_id
        && left.policy_version == right.policy_version
        && left.reserved_cash == right.reserved_cash
        && left.order_origin == right.order_origin
}

fn verify_checkpoint_and_snapshot(
    checkpoint: &PaperMarketCheckpoint,
    snapshot: &crate::contracts::PaperMarketSnapshot,
    replay: &Replay,
    findings: &mut Findings,
) {
    verify_snapshot_account_projection(snapshot, replay, findings);
    verify_checkpoint_account_projection(checkpoint, replay, findings);
    verify_snapshot_mark_projection(snapshot, replay, findings);
    verify_checkpoint_latest_mark(checkpoint, replay, findings);
    if checkpoint.signal_count != replay.signals || checkpoint.fill_count != replay.fills {
        findings.fail("CHECKPOINT_FACT_COUNTS");
    }
}

fn verify_snapshot_account_projection(
    snapshot: &crate::contracts::PaperMarketSnapshot,
    replay: &Replay,
    findings: &mut Findings,
) {
    compare(
        snapshot.cash_total.get(),
        replay.cash,
        "SNAPSHOT_CASH_TOTAL",
        findings,
    );
    compare(
        snapshot.cash_reserved.get(),
        replay.reserved,
        "SNAPSHOT_CASH_RESERVED",
        findings,
    );
    if let Some(free) = replay.cash.checked_sub(replay.reserved) {
        compare(
            snapshot.cash_free.get(),
            free,
            "SNAPSHOT_CASH_FREE",
            findings,
        );
    } else {
        findings.fail("SNAPSHOT_FREE_CASH_OVERFLOW");
    }
    compare(
        snapshot.quantity.get(),
        replay.quantity,
        "SNAPSHOT_QUANTITY",
        findings,
    );
    compare(
        snapshot.realized_pnl.get(),
        replay.realized,
        "SNAPSHOT_REALIZED",
        findings,
    );
    compare(
        snapshot.total_fees.get(),
        replay.fees,
        "SNAPSHOT_FEES",
        findings,
    );
}

fn verify_checkpoint_account_projection(
    checkpoint: &PaperMarketCheckpoint,
    replay: &Replay,
    findings: &mut Findings,
) {
    let Ok(account) = checkpoint.engine_state.account.state() else {
        findings.fail("CHECKPOINT_ACCOUNT_STATE");
        return;
    };
    compare(
        account.cash_total.get(),
        replay.cash,
        "CHECKPOINT_CASH_TOTAL",
        findings,
    );
    compare(
        account.cash_reserved.get(),
        replay.reserved,
        "CHECKPOINT_CASH_RESERVED",
        findings,
    );
    compare(
        account.qty.get(),
        replay.quantity,
        "CHECKPOINT_QUANTITY",
        findings,
    );
    compare(
        account.price_basis.get(),
        replay.basis,
        "CHECKPOINT_BASIS",
        findings,
    );
    compare(
        account.gross_realized.get(),
        replay.realized,
        "CHECKPOINT_REALIZED",
        findings,
    );
    compare(
        account.cumulative_fees.get(),
        replay.fees,
        "CHECKPOINT_FEES",
        findings,
    );
    if let Some(free) = replay.cash.checked_sub(replay.reserved) {
        compare(
            account.cash_free.get(),
            free,
            "CHECKPOINT_CASH_FREE",
            findings,
        );
    } else {
        findings.fail("CHECKPOINT_FREE_CASH_OVERFLOW");
    }
}

fn verify_snapshot_mark_projection(
    snapshot: &crate::contracts::PaperMarketSnapshot,
    replay: &Replay,
    findings: &mut Findings,
) {
    match (
        replay.last_mark.as_ref(),
        snapshot.mark_price,
        snapshot.equity,
        snapshot.unrealized_pnl,
    ) {
        (Some(mark), Some(price), Some(equity), Some(unrealized)) => {
            compare(
                price.get(),
                mark.mark_price.get(),
                "SNAPSHOT_MARK_PRICE",
                findings,
            );
            compare(equity.get(), mark.equity.get(), "SNAPSHOT_EQUITY", findings);
            compare(
                unrealized.get(),
                mark.gross_unrealized.get(),
                "SNAPSHOT_UNREALIZED",
                findings,
            );
            if snapshot.last_account_mark_at != Some(mark.context.accounting_event_time) {
                findings.fail("SNAPSHOT_LAST_MARK_TIME");
            }
            if mark.peak_equity.get().checked_sub(mark.equity.get())
                != snapshot
                    .drawdown_amount
                    .map(crate::contracts::QuoteAmount::get)
            {
                findings.fail("SNAPSHOT_DRAWDOWN_AMOUNT");
            }
            if snapshot.drawdown_fraction != Some(mark.drawdown) {
                findings.fail("SNAPSHOT_DRAWDOWN_FRACTION");
            }
        }
        (None, None, None, None) => {
            if snapshot.last_account_mark_at.is_some()
                || snapshot.drawdown_amount.is_some()
                || snapshot.drawdown_fraction.is_some()
            {
                findings.fail("SNAPSHOT_MARK_ABSENCE");
            }
        }
        _ => findings.fail("SNAPSHOT_MARK_FIELDS"),
    }
}

fn verify_checkpoint_latest_mark(
    checkpoint: &PaperMarketCheckpoint,
    replay: &Replay,
    findings: &mut Findings,
) {
    match (&checkpoint.latest_mark, replay.last_mark.as_ref()) {
        (Some(stored), Some(replayed)) => {
            if stored.context.event_seq != replayed.context.event_seq
                || stored.kind != replayed.kind
                || stored.source_bar_id != replayed.source_bar_id
            {
                findings.fail("CHECKPOINT_LATEST_MARK_IDENTITY");
            }
            compare(
                stored.equity.get(),
                replayed.equity.get(),
                "CHECKPOINT_LATEST_MARK_EQUITY",
                findings,
            );
            compare(
                stored.mark_price.get(),
                replayed.mark_price.get(),
                "CHECKPOINT_LATEST_MARK_PRICE",
                findings,
            );
        }
        (None, None) => {}
        _ => findings.fail("CHECKPOINT_LATEST_MARK"),
    }
}

fn compare(got: Decimal, expected: Decimal, code: &'static str, findings: &mut Findings) {
    if got != expected {
        findings.fail(code);
    }
}

fn verify_fill_fee_terms(
    fill: &FillRecord,
    order: &OrderRecord,
    checkpoint: &PaperMarketCheckpoint,
) -> Result<(), &'static str> {
    if !passive_fill_matches_limit(
        order.order_type,
        order.side,
        order.requested_price,
        fill.side,
        fill.price,
    ) {
        return Err("PASSIVE_LIMIT_SIDE_OR_PRICE");
    }
    let fees = checkpoint
        .plan
        .spec
        .fee_policy_at(order.created_at)
        .map_err(|_| "FEE_POLICY_UNRESOLVABLE")?;
    let expected_bps = match order.order_type {
        SimulatedOrderType::PassiveBuyLimit => fees.maker,
        SimulatedOrderType::Market => match fill.side {
            Side::Buy => fees.buy,
            Side::Sell => fees.sell,
        },
    };
    if fill.fee_bps != expected_bps {
        return Err("FILL_FEE_RATE_MISMATCH");
    }
    Ok(())
}

fn within_numeric_tolerance(left: Decimal, right: Decimal) -> bool {
    left.checked_sub(right)
        .is_some_and(|residual| residual.abs() <= crate::contracts::NUMERIC_TOLERANCE)
}

#[derive(Clone, Copy)]
enum Fact<'a> {
    Signal(&'a SignalRecord),
    Order(&'a OrderRecord),
    Fill(&'a FillRecord),
    Mark(&'a AccountMark),
}

impl Fact<'_> {
    fn context_ref(&self) -> &EventContext {
        match self {
            Self::Signal(signal) => &signal.context,
            Self::Order(order) => &order.context,
            Self::Fill(fill) => &fill.context,
            Self::Mark(mark) => &mark.context,
        }
    }

    fn sort_key(&self) -> (u64, u8) {
        let rank = match self {
            Self::Signal(_) => 0,
            Self::Order(order) if order.status == OrderStatus::Created => 1,
            Self::Fill(_) => 2,
            Self::Order(_) => 3,
            Self::Mark(_) => 4,
        };
        (self.context_ref().event_seq, rank)
    }
}

struct PendingOrderFill {
    cumulative: Decimal,
    status: OrderStatus,
    at: UtcTimestamp,
}

struct OrderReplay {
    created: OrderRecord,
    latest: OrderRecord,
    filled: Decimal,
    open: bool,
    awaiting_fill: Option<PendingOrderFill>,
}

struct Replay {
    cash: Decimal,
    reserved: Decimal,
    quantity: Decimal,
    basis: Decimal,
    realized: Decimal,
    fees: Decimal,
    peak: Decimal,
    reservations: BTreeMap<OrderId, Decimal>,
    orders: BTreeMap<OrderId, OrderReplay>,
    after_fill_marks: BTreeMap<u64, AccountMark>,
    last_mark: Option<AccountMark>,
    last_source_event_at: Option<UtcTimestamp>,
    signals: u64,
    fills: u64,
}

impl Replay {
    fn new(initial_cash: Decimal) -> Self {
        Self {
            cash: initial_cash,
            reserved: Decimal::ZERO,
            quantity: Decimal::ZERO,
            basis: Decimal::ZERO,
            realized: Decimal::ZERO,
            fees: Decimal::ZERO,
            peak: initial_cash,
            reservations: BTreeMap::new(),
            orders: BTreeMap::new(),
            after_fill_marks: BTreeMap::new(),
            last_mark: None,
            last_source_event_at: None,
            signals: 0,
            fills: 0,
        }
    }

    fn apply_order_event(&mut self, event: &OrderRecord) -> Result<(), &'static str> {
        match event.status {
            OrderStatus::Created => {
                if self.orders.contains_key(&event.order_id)
                    || event.cumulative_filled_qty.get() != Decimal::ZERO
                    || event.requested_qty.get() < Decimal::ZERO
                    || event.reserved_cash.get() < Decimal::ZERO
                {
                    return Err("ORDER_CREATED_SHAPE");
                }
                if event.requested_qty.get().is_zero() && !event.reserved_cash.get().is_zero() {
                    return Err("ZERO_QUANTITY_RESERVATION");
                }
                if event.side == Side::Sell && !event.reserved_cash.get().is_zero() {
                    return Err("SELL_ORDER_RESERVES_CASH");
                }
                self.reservations
                    .insert(event.order_id.clone(), event.reserved_cash.get());
                self.refresh_reserved()?;
                if self
                    .cash
                    .checked_sub(self.reserved)
                    .ok_or("CASH_OVERFLOW")?
                    < Decimal::ZERO
                {
                    return Err("OVER_RESERVED_CASH");
                }
                self.orders.insert(
                    event.order_id.clone(),
                    OrderReplay {
                        created: event.clone(),
                        latest: event.clone(),
                        filled: Decimal::ZERO,
                        open: true,
                        awaiting_fill: None,
                    },
                );
            }
            OrderStatus::PartiallyFilled | OrderStatus::Filled => {
                let state = self
                    .orders
                    .get_mut(&event.order_id)
                    .ok_or("ORDER_TRANSITION_WITHOUT_CREATED")?;
                if !state.open || state.awaiting_fill.is_some() {
                    return Err("ORDER_TRANSITION_AFTER_CLOSE");
                }
                if !same_order_identity(&state.created, event)
                    || event.cumulative_filled_qty.get() <= state.filled
                    || event.cumulative_filled_qty.get() > state.created.requested_qty.get()
                {
                    return Err("ORDER_FILL_TRANSITION_SHAPE");
                }
                state.awaiting_fill = Some(PendingOrderFill {
                    cumulative: event.cumulative_filled_qty.get(),
                    status: event.status,
                    at: event.context.accounting_event_time,
                });
                state.latest = event.clone();
                if event.status == OrderStatus::Filled {
                    state.open = false;
                }
            }
            OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired => {
                {
                    let state = self
                        .orders
                        .get_mut(&event.order_id)
                        .ok_or("ORDER_TERMINAL_WITHOUT_CREATED")?;
                    if !state.open
                        || state.awaiting_fill.is_some()
                        || !same_order_identity(&state.created, event)
                        || event.cumulative_filled_qty.get() != state.filled
                    {
                        return Err("ORDER_TERMINAL_SHAPE");
                    }
                    if event.status == OrderStatus::Rejected && !state.filled.is_zero() {
                        return Err("REJECTED_ORDER_HAS_PRIOR_FILL");
                    }
                    state.open = false;
                    state.latest = event.clone();
                }
                self.release_order_reservation(&event.order_id)?;
            }
        }
        Ok(())
    }

    fn apply_fill(
        &mut self,
        fill: &FillRecord,
        signals: &BTreeMap<SignalId, &SignalRecord>,
        checkpoint: &PaperMarketCheckpoint,
        observations: &ValidatedObservations<'_>,
        findings: &mut Findings,
    ) -> Result<(), &'static str> {
        let (pending, created_order, latest_episode, order_filled) = {
            let order = self
                .orders
                .get_mut(&fill.order_id)
                .ok_or("FILL_WITHOUT_ORDER")?;
            (
                order
                    .awaiting_fill
                    .take()
                    .ok_or("FILL_WITHOUT_ORDER_TRANSITION")?,
                order.created.clone(),
                order.latest.episode_id.clone(),
                order.filled,
            )
        };
        let time = fill.context.accounting_event_time;
        let expected_fill_qty = pending
            .cumulative
            .checked_sub(order_filled)
            .ok_or("FILL_QUANTITY_OVERFLOW")?;
        if pending.at != time
            || fill.qty.get() != expected_fill_qty
            || (pending.status == OrderStatus::Filled
                && pending.cumulative != created_order.requested_qty.get())
            || (pending.status == OrderStatus::PartiallyFilled
                && pending.cumulative >= created_order.requested_qty.get())
        {
            return Err("FILL_ORDER_TRANSITION_MISMATCH");
        }
        if fill.side != created_order.side
            || fill.qty.get() <= Decimal::ZERO
            || fill.price.get() <= Decimal::ZERO
            || fill.fee.get() < Decimal::ZERO
            || fill.fee_bps.get() < Decimal::ZERO
            || fill.qty.get() > created_order.requested_qty.get()
        {
            return Err("FILL_SHAPE");
        }
        if latest_episode
            .as_ref()
            .is_some_and(|episode| episode != &fill.episode_id)
        {
            return Err("FILL_EPISODE");
        }
        if fill.fee_currency != "KRW"
            || fill.execution_origin != "SIMULATED_ONLY"
            || fill.fill_observed
        {
            return Err("FILL_SIMULATED_ONLY");
        }
        verify_fill_fee_terms(fill, &created_order, checkpoint)?;
        verify_fill_references(
            fill,
            &created_order,
            signals,
            observations,
            checkpoint,
            findings,
        );
        self.apply_exact_economic_fill(&created_order, fill)?;
        {
            let order = self
                .orders
                .get_mut(&fill.order_id)
                .ok_or("FILL_WITHOUT_ORDER")?;
            order.filled = pending.cumulative;
            if pending.status == OrderStatus::PartiallyFilled {
                order.open = true;
            }
        }
        if pending.status == OrderStatus::Filled {
            self.release_order_reservation(&fill.order_id)?;
        }
        self.fills = self.fills.saturating_add(1);
        Ok(())
    }

    fn apply_exact_economic_fill(
        &mut self,
        order: &OrderRecord,
        fill: &FillRecord,
    ) -> Result<(), &'static str> {
        let qty = fill.qty.get();
        let price = fill.price.get();
        let fee = fill.fee.get();
        if qty <= Decimal::ZERO || price <= Decimal::ZERO || fee < Decimal::ZERO {
            return Err("FILL_SHAPE");
        }
        let notional = price.checked_mul(qty).ok_or("FILL_NOTIONAL_OVERFLOW")?;
        if fill.notional.get() != notional {
            return Err("FILL_NOTIONAL_MISMATCH");
        }
        let fee_rate = fill
            .fee_bps
            .get()
            .checked_div(Decimal::from(10_000_u32))
            .ok_or("FEE_RATE_OVERFLOW")?;
        let expected_fee = notional.checked_mul(fee_rate).ok_or("FILL_FEE_OVERFLOW")?;
        if fee != expected_fee {
            return Err("FILL_FEE_MISMATCH");
        }

        match fill.side {
            Side::Buy => {
                let debit = notional.checked_add(fee).ok_or("BUY_DEBIT_OVERFLOW")?;
                if debit > order.reserved_cash.get() {
                    return Err("BUY_FILL_EXCEEDS_CREATED_RESERVATION");
                }
                if self.reservations.contains_key(&order.order_id) {
                    {
                        let reserved = self
                            .reservations
                            .get_mut(&order.order_id)
                            .ok_or("ORDER_RESERVATION_MISSING")?;
                        if debit > *reserved {
                            return Err("BUY_FILL_EXCEEDS_RESERVATION");
                        }
                        *reserved = reserved.checked_sub(debit).ok_or("RESERVATION_OVERFLOW")?;
                    }
                    self.refresh_reserved()?;
                }
                self.cash = self.cash.checked_sub(debit).ok_or("BUY_CASH_OVERFLOW")?;
                self.quantity = self
                    .quantity
                    .checked_add(qty)
                    .ok_or("BUY_QUANTITY_OVERFLOW")?;
                self.basis = self
                    .basis
                    .checked_add(notional)
                    .ok_or("BUY_BASIS_OVERFLOW")?;
            }
            Side::Sell => {
                if qty > self.quantity || self.quantity.is_zero() {
                    return Err("SELL_EXCEEDS_POSITION");
                }
                let removed_basis = if qty == self.quantity {
                    self.basis
                } else {
                    let sold_fraction = qty
                        .checked_div(self.quantity)
                        .ok_or("SELL_BASIS_OVERFLOW")?;
                    self.basis
                        .checked_mul(sold_fraction)
                        .ok_or("SELL_BASIS_OVERFLOW")?
                };
                let proceeds = notional.checked_sub(fee).ok_or("SELL_PROCEEDS_OVERFLOW")?;
                self.cash = self
                    .cash
                    .checked_add(proceeds)
                    .ok_or("SELL_CASH_OVERFLOW")?;
                self.quantity = self
                    .quantity
                    .checked_sub(qty)
                    .ok_or("SELL_QUANTITY_OVERFLOW")?;
                self.basis = self
                    .basis
                    .checked_sub(removed_basis)
                    .ok_or("SELL_BASIS_OVERFLOW")?;
                self.realized = self
                    .realized
                    .checked_add(
                        notional
                            .checked_sub(removed_basis)
                            .ok_or("REALIZED_OVERFLOW")?,
                    )
                    .ok_or("REALIZED_OVERFLOW")?;
                if self.quantity.is_zero() {
                    self.basis = Decimal::ZERO;
                }
            }
        }
        self.fees = self.fees.checked_add(fee).ok_or("FEE_TOTAL_OVERFLOW")?;
        if self.cash < Decimal::ZERO || self.quantity < Decimal::ZERO || self.basis < Decimal::ZERO
        {
            return Err("NEGATIVE_ACCOUNT_STATE");
        }
        Ok(())
    }

    fn apply_mark(
        &mut self,
        mark: &AccountMark,
        checkpoint: &PaperMarketCheckpoint,
        observations: &ValidatedObservations<'_>,
        fill_by_mark_sequence: &BTreeMap<u64, &FillRecord>,
        findings: &mut Findings,
    ) -> Result<(), &'static str> {
        verify_mark_reference(
            mark,
            checkpoint,
            observations,
            fill_by_mark_sequence,
            findings,
        );
        let position = self
            .quantity
            .checked_mul(mark.mark_price.get())
            .ok_or("MARK_POSITION_OVERFLOW")?;
        let unrealized = position
            .checked_sub(self.basis)
            .ok_or("MARK_UNREALIZED_OVERFLOW")?;
        let equity = self
            .cash
            .checked_add(position)
            .ok_or("MARK_EQUITY_OVERFLOW")?;
        self.peak = self.peak.max(equity);
        let drawdown = if self.peak.is_zero() {
            Decimal::ZERO
        } else {
            self.peak
                .checked_sub(equity)
                .and_then(|v| v.checked_div(self.peak))
                .ok_or("MARK_DRAWDOWN_OVERFLOW")?
        };
        let free = self
            .cash
            .checked_sub(self.reserved)
            .ok_or("MARK_FREE_CASH_OVERFLOW")?;
        compare(
            mark.state.cash_total.get(),
            self.cash,
            "MARK_CASH_TOTAL",
            findings,
        );
        compare(mark.state.cash_free.get(), free, "MARK_CASH_FREE", findings);
        compare(
            mark.state.cash_reserved.get(),
            self.reserved,
            "MARK_CASH_RESERVED",
            findings,
        );
        compare(
            mark.state.qty.get(),
            self.quantity,
            "MARK_QUANTITY",
            findings,
        );
        compare(
            mark.state.price_basis.get(),
            self.basis,
            "MARK_BASIS",
            findings,
        );
        compare(
            mark.state.gross_realized.get(),
            self.realized,
            "MARK_REALIZED",
            findings,
        );
        compare(
            mark.state.cumulative_fees.get(),
            self.fees,
            "MARK_FEES",
            findings,
        );
        compare(
            mark.position_value.get(),
            position,
            "MARK_POSITION",
            findings,
        );
        compare(
            mark.gross_unrealized.get(),
            unrealized,
            "MARK_UNREALIZED",
            findings,
        );
        compare(mark.equity.get(), equity, "MARK_EQUITY", findings);
        compare(mark.peak_equity.get(), self.peak, "MARK_PEAK", findings);
        compare(mark.drawdown.get(), drawdown, "MARK_DRAWDOWN", findings);
        let identity = checkpoint
            .initial_cash
            .get()
            .checked_add(self.realized)
            .and_then(|v| v.checked_add(unrealized))
            .and_then(|v| v.checked_sub(self.fees))
            .ok_or("MARK_IDENTITY_OVERFLOW")?;
        if !within_numeric_tolerance(mark.equity.get(), identity) {
            findings.fail("MARK_EQUITY_IDENTITY");
        }
        if mark.kind == MarkKind::AfterFill {
            self.after_fill_marks
                .insert(mark.context.event_seq, mark.clone());
        }
        self.last_mark = Some(mark.clone());
        Ok(())
    }

    fn register_signal(&mut self, _signal: &SignalRecord) {
        self.signals = self.signals.saturating_add(1);
    }

    fn release_order_reservation(&mut self, order_id: &OrderId) -> Result<(), &'static str> {
        if self.reservations.remove(order_id).is_none() {
            return Err("TERMINAL_ORDER_WITHOUT_RESERVATION");
        }
        self.refresh_reserved()
    }

    fn refresh_reserved(&mut self) -> Result<(), &'static str> {
        self.reserved = self
            .reservations
            .values()
            .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
            .ok_or("RESERVATION_TOTAL_OVERFLOW")?;
        Ok(())
    }

    fn has_incomplete_order_transition(&self) -> bool {
        self.orders
            .values()
            .any(|order| order.awaiting_fill.is_some())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod provider_recovery_coverage_tests {
    use super::*;
    use crate::contracts::{
        PaperCandleReference, PaperProviderRecovery, PaperProviderRecoveryPhase,
        ProviderAbsenceAnchor, ProviderAbsenceSnapshotRef, ProviderAbsenceSpan, RawObjectId,
    };

    fn time(value: &str) -> UtcTimestamp {
        UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
    }

    fn span(proof_seed: &str) -> ProviderAbsenceSpan {
        let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");
        let pre_open = time("2026-10-18T17:00:00Z");
        let post_open = time("2026-10-18T22:00:00Z");
        let first_read = time("2026-10-18T22:00:01Z");
        ProviderAbsenceSpan {
            proof_id: ContentHash::of_bytes(proof_seed.as_bytes()),
            market,
            interval: CandleInterval::M5,
            start_utc: time("2026-10-18T17:05:00Z"),
            end_utc: post_open,
            pre_anchor: ProviderAbsenceAnchor {
                observation_id: ObservationId::from_seed("pre-anchor"),
                open_time_utc: pre_open,
                content_digest: ContentHash::of_bytes(b"pre-anchor"),
            },
            post_anchor: ProviderAbsenceAnchor {
                observation_id: ObservationId::from_seed("post-anchor"),
                open_time_utc: post_open,
                content_digest: ContentHash::of_bytes(b"post-anchor"),
            },
            snapshots: [
                ProviderAbsenceSnapshotRef {
                    raw_object_id: RawObjectId::from_seed("raw-read-1"),
                    fetched_at_utc: first_read,
                    body_digest: ContentHash::of_bytes(b"raw-response"),
                },
                ProviderAbsenceSnapshotRef {
                    raw_object_id: RawObjectId::from_seed("raw-read-2"),
                    fetched_at_utc: UtcTimestamp(first_read.0 + chrono::Duration::seconds(1)),
                    body_digest: ContentHash::of_bytes(b"raw-response"),
                },
            ],
        }
    }

    fn catchup_event(proof_id: ContentHash) -> PaperSessionEvent {
        let processed_at = time("2026-10-18T22:05:00Z");
        PaperSessionEvent {
            preparation: None,
            sequence: 1,
            kind: PaperEventKind::CandleProcessed,
            market: Some(MarketId::parse_upbit("KRW-BTC").expect("fixture market")),
            candle: Some(PaperCandleReference {
                observation_id: ObservationId::from_seed("post-anchor"),
                interval: CandleInterval::M5,
                open_time: time("2026-10-18T22:00:00Z"),
                close_time: time("2026-10-18T22:05:00Z"),
                content_digest: ContentHash::of_bytes(b"post-anchor"),
            }),
            source_event_at: Some(processed_at),
            finalized_at: Some(time("2026-10-18T22:05:00Z")),
            control: None,
            provider_recovery: Some(PaperProviderRecovery {
                phase: PaperProviderRecoveryPhase::CatchUp,
                processed_at,
                target_completed_at: time("2026-10-18T22:05:00Z"),
                proof_ids: vec![proof_id],
                checkpoint_transitions: Vec::new(),
            }),
            signals: Vec::new(),
            orders: Vec::new(),
            order_events: Vec::new(),
            fills: Vec::new(),
            account_marks: Vec::new(),
            episodes: Vec::new(),
        }
    }

    #[test]
    fn recovery_gap_requires_the_event_to_name_the_raw_verified_proof() {
        let verified = span("verified-btc-m5-gap");
        let coverage = VerifiedProviderCoverage::from_verified_spans(vec![verified.clone()])
            .expect("fixture metadata is bounded and aligned");
        let start = verified.start_utc;
        let end = time("2026-10-18T22:00:00Z");
        let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");

        assert!(provider_coverage_covers_event_gap(
            &catchup_event(verified.proof_id.clone()),
            &market,
            CandleInterval::M5,
            start,
            end,
            &coverage,
        ));
        assert!(!provider_coverage_covers_event_gap(
            &catchup_event(ContentHash::of_bytes(b"unreferenced-proof")),
            &market,
            CandleInterval::M5,
            start,
            end,
            &coverage,
        ));
        let mut unscoped = catchup_event(verified.proof_id);
        unscoped
            .provider_recovery
            .as_mut()
            .expect("catchup metadata")
            .proof_ids
            .clear();
        assert!(!provider_coverage_covers_event_gap(
            &unscoped,
            &market,
            CandleInterval::M5,
            start,
            end,
            &coverage,
        ));
        let mut ordinary = catchup_event(ContentHash::of_bytes(b"ordinary-event"));
        ordinary.provider_recovery = None;
        assert!(!provider_coverage_covers_event_gap(
            &ordinary,
            &market,
            CandleInterval::M5,
            start,
            end,
            &coverage,
        ));
    }

    #[test]
    fn recovery_target_advances_monotonically_with_completed_interval_boundaries() {
        let first = time("2026-10-18T22:00:00Z");
        let next = time("2026-10-18T22:05:00Z");
        let processed = time("2026-10-18T22:05:01Z");
        assert!(provider_recovery_target_valid(
            first,
            None,
            CandleInterval::M5,
            processed,
        ));
        assert!(provider_recovery_target_valid(
            next,
            Some(first),
            CandleInterval::M5,
            processed,
        ));
        assert!(!provider_recovery_target_valid(
            first,
            Some(next),
            CandleInterval::M5,
            processed,
        ));
        assert!(!provider_recovery_target_valid(
            time("2026-10-18T22:10:00Z"),
            Some(next),
            CandleInterval::M5,
            processed,
        ));
        assert!(!provider_recovery_target_valid(
            time("2026-10-18T22:06:00Z"),
            Some(first),
            CandleInterval::M5,
            processed,
        ));
    }
}

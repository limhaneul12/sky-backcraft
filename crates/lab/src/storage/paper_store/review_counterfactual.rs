//! Read-only validation of counterfactual inputs against saved paper facts.

use super::review_decision::load_observation;
use crate::contracts::{
    CounterfactualAnalysisRequest, CounterfactualEvidenceInput, LabError,
    MAX_PAPER_REVIEW_INPUT_BYTES, PaperReviewSelection, validate_frozen_policy,
};
use crate::storage::Store;
use serde::Serialize;
use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

impl Store {
    /// Verify every supplied source fact against its exact durable paper/evidence row.
    ///
    /// Unknown variant revisions are accepted only as validated caller-frozen
    /// snapshots. This method never registers them or mutates saved state.
    ///
    /// # Errors
    /// Rejects oversized or malformed requests, forged or missing source facts,
    /// noncanonical saved rows, and stored policy snapshots that differ from the
    /// supplied frozen variant.
    pub fn validate_paper_counterfactual(
        &self,
        request: &CounterfactualAnalysisRequest,
    ) -> Result<(), LabError> {
        request.validate_bounds()?;
        let request_bytes = serde_json::to_vec(request)?;
        if request_bytes.len() > MAX_PAPER_REVIEW_INPUT_BYTES {
            return Err(LabError::ResourceLimit(
                "counterfactual review request exceeds byte budget".into(),
            ));
        }

        for variant in &request.variants {
            validate_frozen_policy(&variant.policy)?;
            if let Some(stored) = self.load_policy_revision(&variant.policy.reference)?
                && !structural_eq(&stored.snapshot, &variant.policy)?
            {
                return Err(LabError::InputHashMismatch(
                    "counterfactual variant differs from its stored frozen policy snapshot".into(),
                ));
            }
        }

        for candidate in &request.candidates {
            let selection = PaperReviewSelection {
                session_id: candidate.source_session_id.clone(),
                event_sequence: candidate.source_event.sequence,
                as_of_sequence: candidate.source_event.sequence,
                signal_id: Some(candidate.source_signal_id.clone()),
            };
            let saved = self.paper_review_decision(&selection)?;
            if !structural_eq(&candidate.source_configuration, &saved.configuration)? {
                return Err(LabError::InputHashMismatch(
                    "counterfactual source configuration differs from the saved session".into(),
                ));
            }
            if !structural_eq(&candidate.source_event, &saved.event)? {
                return Err(LabError::InputHashMismatch(
                    "counterfactual source event differs from its durable event row".into(),
                ));
            }
            if saved.selected_signal_id.as_ref() != Some(&candidate.source_signal_id) {
                return Err(LabError::InputHashMismatch(
                    "counterfactual signal selection differs from the saved event".into(),
                ));
            }

            let matching_signals = saved
                .event
                .signals
                .iter()
                .filter(|signal| signal.signal_id == candidate.source_signal_id)
                .collect::<Vec<_>>();
            let signal = match matching_signals.as_slice() {
                [signal] => *signal,
                [] => {
                    return Err(LabError::DataCorrupt(
                        "selected counterfactual signal is absent from the saved event".into(),
                    ));
                }
                _ => {
                    return Err(LabError::DataCorrupt(
                        "saved counterfactual event repeats its selected signal identity".into(),
                    ));
                }
            };
            validate_source_evidence(self, signal, &candidate.evidence)?;
            validate_candidate_bars(self, candidate)?;
        }
        Ok(())
    }
}

fn validate_source_evidence(
    store: &Store,
    signal: &crate::contracts::SignalRecord,
    supplied: &CounterfactualEvidenceInput,
) -> Result<(), LabError> {
    let effect_snapshot_id = signal
        .evidence_effect
        .as_ref()
        .and_then(|effect| effect.snapshot_id.as_ref());
    let trace_snapshot_id = signal
        .policy_trace
        .as_ref()
        .and_then(|trace| trace.evidence.as_ref())
        .and_then(|trace| trace.snapshot_id.as_ref());
    if effect_snapshot_id
        .zip(trace_snapshot_id)
        .is_some_and(|(effect, trace)| effect != trace)
    {
        return Err(LabError::DataCorrupt(
            "saved signal effect and policy trace pin different evidence snapshots".into(),
        ));
    }
    let expected_snapshot_id = effect_snapshot_id.or(trace_snapshot_id);

    if let CounterfactualEvidenceInput::Snapshot { snapshot } = supplied {
        if expected_snapshot_id != Some(&snapshot.id) {
            return Err(LabError::InputHashMismatch(
                "counterfactual evidence is not pinned by the saved source signal".into(),
            ));
        }
        let saved = store.load_evidence_snapshot(&snapshot.id)?.ok_or_else(|| {
            LabError::InputHashMismatch(
                "counterfactual evidence snapshot is absent from saved evidence".into(),
            )
        })?;
        if !structural_eq(&saved, snapshot)? {
            return Err(LabError::InputHashMismatch(
                "counterfactual evidence differs from its saved snapshot".into(),
            ));
        }
    }
    Ok(())
}

fn validate_candidate_bars(
    store: &Store,
    candidate: &crate::contracts::CounterfactualCandidateInput,
) -> Result<(), LabError> {
    let market = candidate
        .source_event
        .market
        .as_ref()
        .ok_or_else(|| LabError::InputHashMismatch("source event has no market".into()))?;
    let mut observation_ids = BTreeSet::new();
    let mut slots = BTreeSet::new();
    for supplied in &candidate.bars {
        if supplied.candle.market != market.code() {
            return Err(LabError::InputHashMismatch(
                "counterfactual bar market differs from its saved source event".into(),
            ));
        }
        if !supplied.candle.completed {
            return Err(LabError::InputHashMismatch(
                "counterfactual evidence bars must be completed saved observations".into(),
            ));
        }
        if supplied.candle.close_time_utc <= supplied.candle.open_time_utc {
            return Err(LabError::InputHashMismatch(
                "counterfactual bar has an invalid point-in-time interval".into(),
            ));
        }
        if !observation_ids.insert(supplied.id.clone()) {
            return Err(LabError::InputHashMismatch(
                "counterfactual request repeats a bar observation identity".into(),
            ));
        }
        if !slots.insert((supplied.candle.interval, supplied.candle.open_time_utc)) {
            return Err(LabError::InputHashMismatch(
                "counterfactual request contains duplicate market/interval/time slots".into(),
            ));
        }
        let saved = load_observation(&store.connection, &supplied.id)?.ok_or_else(|| {
            LabError::InputHashMismatch(format!(
                "counterfactual bar {} is absent from saved observations",
                supplied.id.as_str()
            ))
        })?;
        if !structural_eq(&saved, supplied)? {
            return Err(LabError::InputHashMismatch(format!(
                "counterfactual bar {} differs from its saved observation",
                supplied.id.as_str()
            )));
        }
    }
    Ok(())
}

fn structural_eq(left: &impl Serialize, right: &impl Serialize) -> Result<bool, LabError> {
    Ok(serde_json::to_value(left)? == serde_json::to_value(right)?)
}

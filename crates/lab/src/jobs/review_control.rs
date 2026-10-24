//! Passive, bounded analysis of immutable saved paper facts.

use super::JobService;
use crate::contracts::{
    ContentHash, LabError, MAX_PAPER_REVIEW_OUTPUT_BYTES, MachineContractFact, PaperDecisionReview,
    PaperReviewQuery, PaperReviewSelection, ReviewAuditBasis, ReviewAuditDiagnostic,
    ReviewAuditDiagnosticCode, ReviewAuditKind,
};
use crate::reporting::review_audit;
use crate::storage::Store;
use serde_json::Value;
use std::sync::Arc;

impl JobService {
    /// Analyze a saved prefix without polling, reconciliation, venue or lifecycle effects.
    /// # Errors
    /// Rejects invalid identities, unavailable capacity and oversized responses.
    pub async fn paper_review_query(&self, query: PaperReviewQuery) -> Result<Value, LabError> {
        query.validate()?;
        let value = match query {
            PaperReviewQuery::Decision { selection } => {
                self.bounded_saved_review(
                    "paper_review_decision",
                    move |store| store.paper_review_decision(&selection),
                    |review| Ok(serde_json::to_value(review)?),
                )
                .await?
            }
            PaperReviewQuery::AuditRules { selection } => self.paper_rule_review(selection).await?,
            PaperReviewQuery::AuditIndicators {
                selection,
                max_source_bars,
            } => self.paper_source_review(selection, max_source_bars).await?,
            PaperReviewQuery::CompareContract {
                selection,
                mut contract,
            } => {
                // The DTO admits only caller-declared expectations, never durable consent.
                if !contract.observed.is_empty() {
                    return Err(LabError::InvalidConfig("paper comparison observed facts are resolved from storage; pass an empty observed array".into()));
                }
                self.bounded_saved_review(
                    "paper_review_contract",
                    move |store| store.paper_review_decision(&selection),
                    move |review| {
                        contract.observed = vec![
                            MachineContractFact::PolicyRevision {
                                reference: review.configuration.policy_ref.clone(),
                            },
                            MachineContractFact::DecisionInterval {
                                interval: review.configuration.decision_interval,
                            },
                            MachineContractFact::ExecutionInterval {
                                interval: review.configuration.execution_resolution,
                            },
                        ];
                        audit_response(&review, review_audit::compare_machine_contracts(&contract)?)
                    },
                )
                .await?
            }
            PaperReviewQuery::Counterfactual { request } => {
                self.bounded_saved_review(
                    "paper_review_counterfactual",
                    move |store| {
                        store.validate_paper_counterfactual(&request)?;
                        Ok(request)
                    },
                    |request| {
                        Ok(serde_json::to_value(
                            crate::research::counterfactual::analyze(&request)?,
                        )?)
                    },
                )
                .await?
            }
        };
        if serde_json::to_vec(&value)?.len() > MAX_PAPER_REVIEW_OUTPUT_BYTES {
            return Err(LabError::ResourceLimit(
                "paper review response exceeds byte budget".into(),
            ));
        }
        Ok(value)
    }

    async fn paper_rule_review(&self, selection: PaperReviewSelection) -> Result<Value, LabError> {
        self.bounded_saved_review(
            "paper_review_rule_audit",
            move |store| {
                let review = store.paper_review_decision(&selection)?;
                let input = store.paper_rule_audit_input(&review)?;
                Ok((review, input))
            },
            |(review, input)| {
                let report = match input {
                    Some(input) => review_audit::audit_snapshot_rules(&input)?,
                    None => missing_report(
                        &review,
                        ReviewAuditKind::SnapshotRuleAudit,
                        ReviewAuditBasis::ReconstructedFromSavedSnapshot,
                    )?,
                };
                audit_response(&review, report)
            },
        )
        .await
    }

    async fn paper_source_review(
        &self,
        selection: PaperReviewSelection,
        max_source_bars: u32,
    ) -> Result<Value, LabError> {
        self.bounded_saved_review(
            "paper_review_indicator_audit",
            move |store| {
                let review = store.paper_review_decision(&selection)?;
                let input = store.paper_indicator_audit_input(&review, max_source_bars)?;
                Ok((review, input))
            },
            |(review, input)| {
                let report = match input {
                    Some(input) => review_audit::audit_source_indicators(&input)?,
                    None => missing_report(
                        &review,
                        ReviewAuditKind::SourceIndicatorAudit,
                        ReviewAuditBasis::RecomputedFromRetainedSourceBars,
                    )?,
                };
                audit_response(&review, report)
            },
        )
        .await
    }

    /// Share the existing bounded review budget across saved automation and paper analyses.
    pub(super) async fn bounded_saved_review<S, T, L, F>(
        &self,
        operation: &'static str,
        load: L,
        calculate: F,
    ) -> Result<T, LabError>
    where
        S: Send + 'static,
        T: Send + 'static,
        L: FnOnce(&mut Store) -> Result<S, LabError> + Send + 'static,
        F: FnOnce(S) -> Result<T, LabError> + Send + 'static,
    {
        if self.inner.stop.is_cancelled() {
            return Err(LabError::Cancelled(
                "saved review stopped before snapshot admission".into(),
            ));
        }
        let permit = Arc::new(
            self.inner
                .automation_review_budget
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    LabError::ResourceLimit("saved review compute capacity is full".into())
                })?,
        );
        let storage_permit = Arc::clone(&permit);
        let snapshot = self
            .inner
            .database
            .call(operation, move |store| {
                let _permit = storage_permit;
                load(store)
            })
            .await?;
        if self.inner.stop.is_cancelled() {
            return Err(LabError::Cancelled(
                "saved review stopped before calculation".into(),
            ));
        }
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            calculate(snapshot)
        })
        .await
        .map_err(|_| LabError::Internal("saved review worker panicked".into()))?
    }
}

fn audit_response(
    review: &PaperDecisionReview,
    report: crate::contracts::ReviewAuditReport,
) -> Result<Value, LabError> {
    Ok(serde_json::to_value(
        crate::contracts::PaperReviewAuditResponse {
            schema_version: "1.0".into(),
            selection: review.selection.clone(),
            original_event_digest: review.original_event_digest.clone(),
            report,
        },
    )?)
}

fn missing_report(
    review: &PaperDecisionReview,
    kind: ReviewAuditKind,
    basis: ReviewAuditBasis,
) -> Result<crate::contracts::ReviewAuditReport, LabError> {
    if review.selected_signal_id.is_some()
        && matches!(
            review.policy.definition.program,
            crate::contracts::PolicyProgram::Builtin { .. }
        )
    {
        return review_audit::unsupported_audit_report(
            kind,
            basis,
            ContentHash::of_value(review)?,
            vec![review.original_event_digest.clone()],
            vec![ReviewAuditDiagnostic {
                code: ReviewAuditDiagnosticCode::BuiltinPolicyUnsupported,
                path: "policy.program".into(),
            }],
        );
    }
    review_audit::insufficient_evidence_report(
        kind,
        basis,
        ContentHash::of_value(review)?,
        vec![review.original_event_digest.clone()],
        vec![ReviewAuditDiagnostic {
            code: ReviewAuditDiagnosticCode::MissingRequiredEvidence,
            path: "saved_decision_context".into(),
        }],
    )
}

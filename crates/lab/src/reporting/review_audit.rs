//! Independent, read-only audits over bounded saved evidence.
//!
//! Snapshot rule results are reconstructed from their saved context. They are
//! not historical traces of the original evaluator's execution.

mod common;
mod machine_contracts;
mod snapshot_rules;
mod source_indicators;

#[cfg(test)]
mod tests;

pub use machine_contracts::compare_machine_contracts;
pub use snapshot_rules::{audit_snapshot_rules, reconstruct_cross_prior_snapshot};
pub use source_indicators::{
    SOURCE_INDICATOR_ABSOLUTE_TOLERANCE, SOURCE_INDICATOR_RELATIVE_TOLERANCE,
    audit_source_indicators,
};

use crate::contracts::{
    ContentHash, LabError, ReviewAuditBasis, ReviewAuditDiagnostic, ReviewAuditKind,
    ReviewAuditReport, ReviewAuditReportDigestProjection, ReviewAuditStatus,
};

pub(crate) struct ReportDraft {
    pub kind: ReviewAuditKind,
    pub basis: ReviewAuditBasis,
    pub status: ReviewAuditStatus,
    pub evidence_digest: ContentHash,
    pub evidence_digests: Vec<ContentHash>,
    pub diagnostics: Vec<ReviewAuditDiagnostic>,
    pub residuals: Vec<crate::contracts::ReviewAuditResidual>,
    pub exact_residuals: Vec<crate::contracts::ReviewAuditExactResidual>,
    pub approval_evidence_origin: Option<crate::contracts::MachineContractApprovalOrigin>,
}

/// Build an explicit insufficient-evidence result when an adapter cannot
/// resolve a saved source without manufacturing placeholder domain records.
///
/// # Errors
/// Returns a serialization error only when computing the immutable result digest.
pub fn insufficient_evidence_report(
    kind: ReviewAuditKind,
    basis: ReviewAuditBasis,
    evidence_digest: ContentHash,
    evidence_digests: Vec<ContentHash>,
    diagnostics: Vec<ReviewAuditDiagnostic>,
) -> Result<ReviewAuditReport, LabError> {
    build_report(ReportDraft {
        kind,
        basis,
        status: ReviewAuditStatus::InsufficientEvidence,
        evidence_digest,
        evidence_digests,
        diagnostics,
        residuals: Vec::new(),
        exact_residuals: Vec::new(),
        approval_evidence_origin: None,
    })
}

/// Build an explicit unsupported result for a saved shape outside the audit's
/// declared independent implementation scope.
///
/// # Errors
/// Returns a serialization error only when computing the immutable result digest.
pub fn unsupported_audit_report(
    kind: ReviewAuditKind,
    basis: ReviewAuditBasis,
    evidence_digest: ContentHash,
    evidence_digests: Vec<ContentHash>,
    diagnostics: Vec<ReviewAuditDiagnostic>,
) -> Result<ReviewAuditReport, LabError> {
    build_report(ReportDraft {
        kind,
        basis,
        status: ReviewAuditStatus::Unsupported,
        evidence_digest,
        evidence_digests,
        diagnostics,
        residuals: Vec::new(),
        exact_residuals: Vec::new(),
        approval_evidence_origin: None,
    })
}

pub(crate) fn build_report(mut draft: ReportDraft) -> Result<ReviewAuditReport, LabError> {
    draft.evidence_digests.sort();
    draft
        .diagnostics
        .sort_by(|left, right| (left.code, &left.path).cmp(&(right.code, &right.path)));
    draft.diagnostics.dedup();
    draft
        .residuals
        .sort_by(|left, right| left.path.cmp(&right.path));
    draft
        .exact_residuals
        .sort_by(|left, right| left.path.cmp(&right.path));
    let projection = ReviewAuditReportDigestProjection {
        kind: draft.kind,
        basis: draft.basis,
        status: draft.status,
        evidence_digest: &draft.evidence_digest,
        evidence_digests: &draft.evidence_digests,
        approval_evidence_origin: draft.approval_evidence_origin.as_ref(),
        diagnostics: &draft.diagnostics,
        residuals: &draft.residuals,
        exact_residuals: (!draft.exact_residuals.is_empty())
            .then_some(draft.exact_residuals.as_slice()),
    };
    let result_digest = ContentHash::of_value(&projection)?;
    Ok(ReviewAuditReport {
        kind: draft.kind,
        basis: draft.basis,
        status: draft.status,
        evidence_digest: draft.evidence_digest,
        evidence_digests: draft.evidence_digests,
        approval_evidence_origin: draft.approval_evidence_origin,
        diagnostics: draft.diagnostics,
        residuals: draft.residuals,
        exact_residuals: draft.exact_residuals,
        result_digest,
    })
}

use crate::contracts::{
    CandleInterval, CandleObservation, ContentHash, LabError, MarketId, ReviewAuditBasis,
    ReviewAuditDiagnostic, ReviewAuditDiagnosticCode, ReviewAuditExactResidual, ReviewAuditKind,
    ReviewAuditResidual, ReviewAuditStatus, UtcTimestamp,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;
use std::io::{self, Write};

use super::build_report;

pub(super) const MAX_AUDIT_BYTES: usize = crate::contracts::MAX_REVIEW_AUDIT_EVIDENCE_BYTES;
pub(super) const MAX_AUDIT_BARS: usize = crate::contracts::MAX_REVIEW_AUDIT_SOURCE_BARS;
pub(super) const MAX_VALUES: usize = crate::contracts::MAX_REVIEW_AUDIT_NAMED_VALUES;
pub(super) const MAX_CROSS_VALUES: usize = crate::contracts::MAX_REVIEW_AUDIT_CROSS_VALUES;
pub(super) const MAX_SERIES: usize = crate::contracts::MAX_REVIEW_AUDIT_SOURCE_SERIES;
pub(super) const MAX_FACTS: usize = crate::contracts::MAX_REVIEW_AUDIT_FACTS;
const MAX_LINKS_PER_BAR: usize = crate::contracts::MAX_REVIEW_AUDIT_OBSERVATION_LINKS_PER_BAR;
const MAX_TOTAL_LINKS: usize = crate::contracts::MAX_REVIEW_AUDIT_TOTAL_OBSERVATION_LINKS;

pub(super) struct AuditFindings {
    kind: ReviewAuditKind,
    basis: ReviewAuditBasis,
    evidence_digest: ContentHash,
    evidence_digests: Vec<ContentHash>,
    diagnostics: Vec<ReviewAuditDiagnostic>,
    residuals: Vec<ReviewAuditResidual>,
    exact_residuals: Vec<ReviewAuditExactResidual>,
    approval_evidence_origin: Option<crate::contracts::MachineContractApprovalOrigin>,
    contradiction: bool,
    unsupported: bool,
    insufficient: bool,
}

impl AuditFindings {
    pub(super) fn new(
        kind: ReviewAuditKind,
        basis: ReviewAuditBasis,
        evidence_digest: ContentHash,
        evidence_digests: Vec<ContentHash>,
    ) -> Self {
        Self {
            kind,
            basis,
            evidence_digest,
            evidence_digests,
            diagnostics: Vec::new(),
            residuals: Vec::new(),
            exact_residuals: Vec::new(),
            approval_evidence_origin: None,
            contradiction: false,
            unsupported: false,
            insufficient: false,
        }
    }

    pub(super) fn set_approval_origin(
        &mut self,
        origin: crate::contracts::MachineContractApprovalOrigin,
    ) {
        self.approval_evidence_origin = Some(origin);
    }

    pub(super) fn contradiction(
        &mut self,
        code: ReviewAuditDiagnosticCode,
        path: impl Into<String>,
    ) {
        self.contradiction = true;
        self.diagnostic(code, path);
    }

    pub(super) fn unsupported(&mut self, code: ReviewAuditDiagnosticCode, path: impl Into<String>) {
        self.unsupported = true;
        self.diagnostic(code, path);
    }

    pub(super) fn insufficient(
        &mut self,
        code: ReviewAuditDiagnosticCode,
        path: impl Into<String>,
    ) {
        self.insufficient = true;
        self.diagnostic(code, path);
    }

    pub(super) fn diagnostic(&mut self, code: ReviewAuditDiagnosticCode, path: impl Into<String>) {
        self.diagnostics.push(ReviewAuditDiagnostic {
            code,
            path: path.into(),
        });
    }

    pub(super) fn residual(
        &mut self,
        path: impl Into<String>,
        expected: f64,
        recomputed: f64,
        tolerance: f64,
    ) -> Result<bool, LabError> {
        if !expected.is_finite()
            || !recomputed.is_finite()
            || !tolerance.is_finite()
            || tolerance < 0.0
        {
            return Err(LabError::DataCorrupt(
                "review audit residual contains a nonfinite value".into(),
            ));
        }
        let absolute_difference = (expected - recomputed).abs();
        if !absolute_difference.is_finite() {
            return Err(LabError::DataCorrupt(
                "review audit residual overflowed".into(),
            ));
        }
        let matches = absolute_difference <= tolerance;
        self.residuals.push(ReviewAuditResidual {
            path: path.into(),
            expected,
            recomputed,
            absolute_difference,
            tolerance,
        });
        Ok(matches)
    }

    pub(super) fn exact_decimal_residual(
        &mut self,
        path: impl Into<String>,
        expected: Decimal,
        recomputed: Decimal,
    ) -> Result<(), LabError> {
        let difference = expected.checked_sub(recomputed).ok_or_else(|| {
            LabError::ResourceLimit("review audit exact residual overflow".into())
        })?;
        let absolute_difference = if difference < Decimal::ZERO {
            -difference
        } else {
            difference
        };
        self.exact_residuals.push(ReviewAuditExactResidual {
            path: path.into(),
            expected: expected.to_string(),
            recomputed: recomputed.to_string(),
            absolute_difference: absolute_difference.to_string(),
        });
        Ok(())
    }

    pub(super) fn finish(self) -> Result<crate::contracts::ReviewAuditReport, LabError> {
        let status = if self.contradiction {
            ReviewAuditStatus::Contradiction
        } else if self.unsupported {
            ReviewAuditStatus::Unsupported
        } else if self.insufficient {
            ReviewAuditStatus::InsufficientEvidence
        } else {
            ReviewAuditStatus::Verified
        };
        build_report(super::ReportDraft {
            kind: self.kind,
            basis: self.basis,
            status,
            evidence_digest: self.evidence_digest,
            evidence_digests: self.evidence_digests,
            diagnostics: self.diagnostics,
            residuals: self.residuals,
            exact_residuals: self.exact_residuals,
            approval_evidence_origin: self.approval_evidence_origin,
        })
    }
}

pub(super) fn digest_value(value: &impl Serialize) -> Result<ContentHash, LabError> {
    ContentHash::of_value(value)
}

pub(super) fn decimal_to_f64(value: Decimal, path: &str) -> Result<f64, LabError> {
    let converted = value
        .to_f64()
        .ok_or_else(|| LabError::DataCorrupt(format!("{path} cannot be represented as f64")))?;
    if converted.is_finite() {
        Ok(converted)
    } else {
        Err(LabError::DataCorrupt(format!("{path} is nonfinite")))
    }
}

pub(super) fn validate_observation(
    observation: &CandleObservation,
    market: &MarketId,
    interval: CandleInterval,
    cutoff: UtcTimestamp,
    path: &str,
) -> Result<(), LabError> {
    let candle = &observation.candle;
    let duration = interval.duration();
    let seconds = duration.num_seconds();
    if candle.market != market.code()
        || candle.interval != interval
        || !candle.completed
        || candle.close_time_utc > cutoff
        || candle.close_time_utc.0 - candle.open_time_utc.0 != duration
        || candle.open_time_utc >= candle.close_time_utc
        || candle.open_time_utc.0.timestamp_subsec_nanos() != 0
        || candle.close_time_utc.0.timestamp_subsec_nanos() != 0
        || seconds <= 0
        || candle.open_time_utc.0.timestamp().rem_euclid(seconds) != 0
        || candle.close_time_utc.0.timestamp().rem_euclid(seconds) != 0
    {
        return Err(LabError::DataCorrupt(format!(
            "review audit {path} has a market, interval, completion, or cutoff mismatch"
        )));
    }
    if candle.high < candle.open
        || candle.high < candle.close
        || candle.low > candle.open
        || candle.low > candle.close
        || candle.high < candle.low
    {
        return Err(LabError::DataCorrupt(format!(
            "review audit {path} has an invalid OHLC relation"
        )));
    }
    for (value, label) in [
        (candle.open.get(), "open"),
        (candle.high.get(), "high"),
        (candle.low.get(), "low"),
        (candle.close.get(), "close"),
        (candle.volume.get(), "volume"),
        (candle.quote_turnover.get(), "quote turnover"),
    ] {
        decimal_to_f64(value, &format!("{path} {label}"))?;
    }
    if observation.raw_object_ids.len() > MAX_LINKS_PER_BAR
        || observation.constituent_ids.len() > MAX_LINKS_PER_BAR
    {
        return Err(LabError::ResourceLimit(format!(
            "review audit {path} observation links exceed per-bar bound"
        )));
    }
    Ok(())
}

pub(super) fn validate_total_links<'a>(
    observations: impl IntoIterator<Item = &'a CandleObservation>,
) -> Result<(), LabError> {
    let mut total = 0_usize;
    for observation in observations {
        total = total
            .checked_add(observation.raw_object_ids.len())
            .and_then(|value| value.checked_add(observation.constituent_ids.len()))
            .ok_or_else(|| LabError::ResourceLimit("review audit link count overflow".into()))?;
        if total > MAX_TOTAL_LINKS {
            return Err(LabError::ResourceLimit(
                "review audit evidence exceeds total observation-link bound".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn tolerance(expected: f64, recomputed: f64) -> f64 {
    super::SOURCE_INDICATOR_ABSOLUTE_TOLERANCE
        + super::SOURCE_INDICATOR_RELATIVE_TOLERANCE * expected.abs().max(recomputed.abs())
}

/// Counts serialized bytes without retaining the JSON body.
#[derive(Default)]
struct JsonByteCounter(usize);

impl Write for JsonByteCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        // Keep accepting bytes so a later Serde error retains precedence.
        self.0 = self.0.saturating_add(buffer.len());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn check_serialized_bound(value: &impl Serialize) -> Result<(), LabError> {
    let mut counter = JsonByteCounter::default();
    serde_json::to_writer(&mut counter, value)?;
    if counter.0 > MAX_AUDIT_BYTES {
        return Err(LabError::ResourceLimit(
            "review audit supplied evidence exceeds byte bound".into(),
        ));
    }
    Ok(())
}

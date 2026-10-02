//! Read-only classification of persisted dataset quality issues.
//!
//! Classification is a presentation projection. It never changes the stored
//! issue, dataset status, severity, hashes, rows, or blocking behavior.

use crate::contracts::{LabError, QualityIssue, QualityKind, UtcTimestamp};
use schemars::JsonSchema;
use serde::Serialize;

/// Version of the reviewed, static `Upbit` maintenance evidence catalog.
pub const MAINTENANCE_EVIDENCE_VERSION: &str = "upbit-maintenance-2026-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualityCause {
    ConfirmedTradingInterruption,
    CollectionFailure,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualityCauseBasis {
    OfficialCompletedMaintenanceMatch,
    PartialMaintenanceOverlap,
    SourceCorruption,
    Unexplained,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QualityEvidence {
    pub source_url: String,
    pub announced_start: UtcTimestamp,
    pub confirmed_start: Option<UtcTimestamp>,
    pub confirmed_resume: UtcTimestamp,
    pub exact_start_unconfirmed: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QualityIssueView {
    #[serde(flatten)]
    pub issue: QualityIssue,
    pub cause: Option<QualityCause>,
    pub cause_basis: Option<QualityCauseBasis>,
    pub explanation: Option<String>,
    pub evidence: Vec<QualityEvidence>,
}

const OFFICIAL_MATCH_EXPLANATION: &str = concat!(
    "An official completed-maintenance notice covers the missing interval, but the exact ",
    "interruption bounds are unconfirmed. No halt-order, valuation, or resumption rules ",
    "were applied; dataset blocking is unchanged."
);
const CONFIRMED_INTERRUPTION_EXPLANATION: &str = concat!(
    "The actual interruption start and resume bounds are evidenced. No halt-order, ",
    "valuation, or resumption rules were applied; dataset blocking is unchanged."
);
const PARTIAL_OVERLAP_EXPLANATION: &str = concat!(
    "An official completed-maintenance notice partially overlaps the missing interval, ",
    "but the exact interruption bounds are unconfirmed. No halt-order, valuation, or ",
    "resumption rules were applied; dataset blocking is unchanged."
);
const UNEXPLAINED_GAP_EXPLANATION: &str = concat!(
    "No reviewed official maintenance evidence overlaps the missing interval; the cause ",
    "remains unexplained and dataset blocking is unchanged."
);
const SOURCE_CORRUPTION_EXPLANATION: &str =
    "Source corruption establishes a collection failure; dataset blocking is unchanged.";

struct MaintenanceEvidence {
    source_url: &'static str,
    announced_start: &'static str,
    confirmed_start: Option<&'static str>,
    confirmed_resume: &'static str,
}

const MAINTENANCE_EVIDENCE: [MaintenanceEvidence; 3] = [
    MaintenanceEvidence {
        source_url: "https://docs.upbit.com/kr/changelog/scheduled_server_maintenance_26_jan",
        announced_start: "2025-12-31T17:00:00Z",
        confirmed_start: None,
        confirmed_resume: "2025-12-31T21:50:00Z",
    },
    MaintenanceEvidence {
        source_url: "https://docs.upbit.com/kr/changelog/server_maintenance_0413",
        announced_start: "2026-04-12T17:00:00Z",
        confirmed_start: None,
        confirmed_resume: "2026-04-12T22:30:00Z",
    },
    MaintenanceEvidence {
        source_url: "https://docs.upbit.com/kr/changelog/server_maintenance_0706",
        announced_start: "2026-07-05T17:00:00Z",
        confirmed_start: None,
        confirmed_resume: "2026-07-05T21:40:00Z",
    },
];

/// Add reviewed cause evidence to a quality issue without mutating its stored meaning.
///
/// A missing interval is attributed to a confirmed trading interruption only when both
/// its actual start and resume are evidenced. A completed maintenance notice without an
/// actual start keeps the cause unknown while identifying the official maintenance
/// match. A partial overlap remains unknown and carries only candidate evidence.
///
/// # Errors
///
/// Returns [`LabError::ContractParse`] if a timestamp embedded in the reviewed catalog
/// cannot be parsed.
pub fn classify_issue(issue: &QualityIssue) -> Result<QualityIssueView, LabError> {
    if issue.kind == QualityKind::SourceCorrupt {
        return Ok(QualityIssueView {
            issue: issue.clone(),
            cause: Some(QualityCause::CollectionFailure),
            cause_basis: Some(QualityCauseBasis::SourceCorruption),
            explanation: Some(SOURCE_CORRUPTION_EXPLANATION.into()),
            evidence: Vec::new(),
        });
    }
    if issue.kind != QualityKind::UnknownSourceGap {
        return Ok(QualityIssueView {
            issue: issue.clone(),
            cause: None,
            cause_basis: None,
            explanation: None,
            evidence: Vec::new(),
        });
    }

    classify_gap(issue, &MAINTENANCE_EVIDENCE)
}

fn classify_gap(
    issue: &QualityIssue,
    sources: &[MaintenanceEvidence],
) -> Result<QualityIssueView, LabError> {
    let mut evidence = Vec::new();
    let mut maintenance_match = false;
    let mut confirmed_interruption = false;
    for source in sources {
        let announced_start = UtcTimestamp::parse_rfc3339(source.announced_start)?;
        let confirmed_start = source
            .confirmed_start
            .map(UtcTimestamp::parse_rfc3339)
            .transpose()?;
        let confirmed_resume = UtcTimestamp::parse_rfc3339(source.confirmed_resume)?;
        if issue.start < confirmed_resume && issue.end > announced_start {
            let contained = issue.start >= announced_start && issue.end <= confirmed_resume;
            maintenance_match |= contained;
            if let Some(confirmed_start) = confirmed_start {
                confirmed_interruption |=
                    issue.start >= confirmed_start && issue.end <= confirmed_resume;
            }
            evidence.push(QualityEvidence {
                source_url: source.source_url.to_owned(),
                announced_start,
                confirmed_start,
                confirmed_resume,
                exact_start_unconfirmed: source.confirmed_start.is_none(),
            });
        }
    }

    let cause = if confirmed_interruption {
        QualityCause::ConfirmedTradingInterruption
    } else {
        QualityCause::Unknown
    };
    let cause_basis = if maintenance_match {
        QualityCauseBasis::OfficialCompletedMaintenanceMatch
    } else if evidence.is_empty() {
        QualityCauseBasis::Unexplained
    } else {
        QualityCauseBasis::PartialMaintenanceOverlap
    };
    let explanation = if cause == QualityCause::ConfirmedTradingInterruption {
        CONFIRMED_INTERRUPTION_EXPLANATION
    } else {
        match cause_basis {
            QualityCauseBasis::OfficialCompletedMaintenanceMatch => OFFICIAL_MATCH_EXPLANATION,
            QualityCauseBasis::PartialMaintenanceOverlap => PARTIAL_OVERLAP_EXPLANATION,
            QualityCauseBasis::Unexplained => UNEXPLAINED_GAP_EXPLANATION,
            QualityCauseBasis::SourceCorruption => SOURCE_CORRUPTION_EXPLANATION,
        }
    };

    Ok(QualityIssueView {
        issue: issue.clone(),
        cause: Some(cause),
        cause_basis: Some(cause_basis),
        explanation: Some(explanation.into()),
        evidence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{MarketId, QualitySeverity};

    fn time(value: &str) -> UtcTimestamp {
        UtcTimestamp::parse_rfc3339(value).expect("valid fixture timestamp")
    }

    fn issue(kind: QualityKind, start: &str, end: &str) -> QualityIssue {
        QualityIssue {
            kind,
            severity: QualitySeverity::Error,
            market: MarketId::parse_upbit("KRW-BTC").expect("valid fixture market"),
            start: time(start),
            end: time(end),
            count: 1,
            raw_object_ids: Vec::new(),
            detail: "fixture".into(),
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one classification boundary fixture covers confirmed/unknown evidence and unchanged blocking"
    )]
    fn explains_gap_evidence_without_changing_original_severity() {
        for (start, end) in [
            ("2025-12-31T18:00:00Z", "2025-12-31T21:00:00Z"),
            ("2026-04-12T18:00:00Z", "2026-04-12T22:00:00Z"),
            ("2026-07-05T18:00:00Z", "2026-07-05T21:00:00Z"),
        ] {
            let view = classify_issue(&issue(QualityKind::UnknownSourceGap, start, end))
                .expect("catalog is valid");
            assert_eq!(view.cause, Some(QualityCause::Unknown));
            assert_eq!(
                view.cause_basis,
                Some(QualityCauseBasis::OfficialCompletedMaintenanceMatch)
            );
            assert_eq!(view.evidence.len(), 1);
            assert!(view.evidence[0].confirmed_start.is_none());
            assert!(view.evidence[0].exact_start_unconfirmed);
            assert_eq!(
                view.explanation.as_deref(),
                Some(OFFICIAL_MATCH_EXPLANATION)
            );
            assert_eq!(view.issue.severity, QualitySeverity::Error);
        }

        let partial = classify_issue(&issue(
            QualityKind::UnknownSourceGap,
            "2026-04-12T16:00:00Z",
            "2026-04-12T18:00:00Z",
        ))
        .expect("catalog is valid");
        assert_eq!(partial.cause, Some(QualityCause::Unknown));
        assert_eq!(
            partial.cause_basis,
            Some(QualityCauseBasis::PartialMaintenanceOverlap)
        );
        assert_eq!(partial.evidence.len(), 1);
        assert_eq!(
            partial.explanation.as_deref(),
            Some(PARTIAL_OVERLAP_EXPLANATION)
        );

        let unknown = classify_issue(&issue(
            QualityKind::UnknownSourceGap,
            "2026-08-01T00:00:00Z",
            "2026-08-01T01:00:00Z",
        ))
        .expect("catalog is valid");
        assert_eq!(unknown.cause, Some(QualityCause::Unknown));
        assert_eq!(unknown.cause_basis, Some(QualityCauseBasis::Unexplained));
        assert!(unknown.evidence.is_empty());
        assert_eq!(
            unknown.explanation.as_deref(),
            Some(UNEXPLAINED_GAP_EXPLANATION)
        );

        let confirmed_source = [MaintenanceEvidence {
            source_url: "https://example.invalid/confirmed-maintenance",
            announced_start: "2026-08-01T00:00:00Z",
            confirmed_start: Some("2026-08-01T00:05:00Z"),
            confirmed_resume: "2026-08-01T02:00:00Z",
        }];
        let confirmed = classify_gap(
            &issue(
                QualityKind::UnknownSourceGap,
                "2026-08-01T00:10:00Z",
                "2026-08-01T01:00:00Z",
            ),
            &confirmed_source,
        )
        .expect("synthetic confirmed evidence is valid");
        assert_eq!(
            confirmed.cause,
            Some(QualityCause::ConfirmedTradingInterruption)
        );
        assert_eq!(
            confirmed.evidence[0].confirmed_start,
            Some(time("2026-08-01T00:05:00Z"))
        );
        assert!(!confirmed.evidence[0].exact_start_unconfirmed);
        assert_eq!(
            confirmed.explanation.as_deref(),
            Some(CONFIRMED_INTERRUPTION_EXPLANATION)
        );

        let failed = classify_issue(&issue(
            QualityKind::SourceCorrupt,
            "2026-08-01T00:00:00Z",
            "2026-08-01T01:00:00Z",
        ))
        .expect("non-gap classification succeeds");
        assert_eq!(failed.cause, Some(QualityCause::CollectionFailure));
        assert_eq!(
            failed.cause_basis,
            Some(QualityCauseBasis::SourceCorruption)
        );
        assert!(failed.evidence.is_empty());
        assert_eq!(
            failed.explanation.as_deref(),
            Some(SOURCE_CORRUPTION_EXPLANATION)
        );

        let unrelated = classify_issue(&issue(
            QualityKind::ZeroVolume,
            "2026-08-01T00:00:00Z",
            "2026-08-01T01:00:00Z",
        ))
        .expect("non-gap classification succeeds");
        assert_eq!(unrelated.cause, None);
        assert_eq!(unrelated.cause_basis, None);
        assert_eq!(unrelated.explanation, None);
        assert_eq!(unrelated.issue.severity, QualitySeverity::Error);
    }
}

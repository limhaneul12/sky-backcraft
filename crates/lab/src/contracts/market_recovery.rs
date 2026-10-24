use super::{
    CandleInterval, ContentHash, LabError, MarketId, ObservationId, RawObjectId, UtcTimestamp,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_VERIFIED_PROVIDER_SPANS: usize = 256;
const MAX_VERIFIED_PROVIDER_GAP_SLOTS: i64 = 12_800;
const MIN_VERIFICATION_SECONDS: i64 = 1;
const MAX_VERIFICATION_SECONDS: i64 = 30;

/// One completed exchange-observed candle used as a boundary for a provider gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderAbsenceAnchor {
    pub observation_id: ObservationId,
    pub open_time_utc: UtcTimestamp,
    pub content_digest: ContentHash,
}

/// Identity and read time for one independently fetched raw REST response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderAbsenceSnapshotRef {
    pub raw_object_id: RawObjectId,
    pub fetched_at_utc: UtcTimestamp,
    pub body_digest: ContentHash,
}

/// Exact, independently corroborated range omitted from scoped provider pages.
/// This records source response absence only; it does not assert that no trades
/// occurred or synthesize candles for the missing slots. Anchor IDs/digests bind
/// the immutable raw witnesses. Later price revisions at the same endpoint slots
/// do not turn into missing inner candles or rewrite those original witnesses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderAbsenceSpan {
    pub proof_id: ContentHash,
    pub market: MarketId,
    pub interval: CandleInterval,
    /// Half-open range of missing candle open times: `[start_utc, end_utc)`.
    pub start_utc: UtcTimestamp,
    pub end_utc: UtcTimestamp,
    pub pre_anchor: ProviderAbsenceAnchor,
    pub post_anchor: ProviderAbsenceAnchor,
    /// First read and its independent confirmation, in chronological order.
    pub snapshots: [ProviderAbsenceSnapshotRef; 2],
}

/// Durable, bounded cursor state for one backwards public-feed recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderFeedRecoveryProgress {
    pub accepted_anchor: Option<ProviderAbsenceAnchor>,
    pub next_page_to_utc: Option<UtcTimestamp>,
    pub pages_scanned: u32,
    pub rows_scanned: u64,
    pub page_limit: u32,
    pub row_limit: u64,
    pub pending_absence_confirmation: bool,
}

/// Non-serialized capability to excuse only gaps already verified from stored
/// provider response witnesses. It cannot produce, interpolate, or count candles.
#[derive(Debug, Clone, Default)]
pub(crate) struct VerifiedProviderCoverage {
    spans: Vec<ProviderAbsenceSpan>,
    proof_ids: Vec<ContentHash>,
}

impl VerifiedProviderCoverage {
    /// Build bounded coverage from spans whose raw witnesses were verified by storage.
    ///
    /// # Errors
    /// Rejects malformed span geometry, conflicting proof identities, or excess spans.
    pub(crate) fn from_verified_spans(spans: Vec<ProviderAbsenceSpan>) -> Result<Self, LabError> {
        if spans.len() > MAX_VERIFIED_PROVIDER_SPANS {
            return Err(LabError::ResourceLimit(
                "verified provider coverage exceeds its span bound".into(),
            ));
        }
        let mut unique_by_proof = BTreeMap::<String, ProviderAbsenceSpan>::new();
        for span in spans {
            validate_verified_span_metadata(&span)?;
            let proof_id = span.proof_id.as_str().to_owned();
            if let Some(existing) = unique_by_proof.get(&proof_id) {
                if existing != &span {
                    return Err(LabError::DataCorrupt(
                        "one provider proof id names conflicting coverage spans".into(),
                    ));
                }
                continue;
            }
            unique_by_proof.insert(proof_id, span);
        }

        let mut spans = unique_by_proof.into_values().collect::<Vec<_>>();
        spans.sort_by(|left, right| {
            left.market
                .code()
                .cmp(&right.market.code())
                .then_with(|| left.interval.cmp(&right.interval))
                .then_with(|| left.start_utc.cmp(&right.start_utc))
                .then_with(|| left.end_utc.cmp(&right.end_utc))
                .then_with(|| left.proof_id.cmp(&right.proof_id))
        });
        let proof_ids = spans
            .iter()
            .map(|span| span.proof_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(Self { spans, proof_ids })
    }

    /// Empty capability preserves the default dense-observation contract.
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Whether ordered verified ranges cover the exact missing-open-time interval.
    /// Equal endpoints need no evidence; backward or misaligned ranges never match.
    #[must_use]
    pub(crate) fn covers(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        start: UtcTimestamp,
        end: UtcTimestamp,
    ) -> bool {
        if start > end {
            return false;
        }
        if start == end {
            return true;
        }
        if !aligned(start, interval) || !aligned(end, interval) {
            return false;
        }

        let mut covered_through = start;
        for span in self
            .spans
            .iter()
            .filter(|span| span.market == *market && span.interval == interval)
        {
            if span.end_utc <= covered_through {
                continue;
            }
            if span.start_utc > covered_through {
                return false;
            }
            covered_through = span.end_utc;
            if covered_through >= end {
                return true;
            }
        }
        false
    }

    /// Stable, unique proof identities backing this capability.
    #[must_use]
    pub(crate) fn proof_ids(&self) -> Vec<ContentHash> {
        self.proof_ids.clone()
    }

    /// Full immutable spans retained for audit and parent projections.
    #[must_use]
    pub(crate) fn spans(&self) -> &[ProviderAbsenceSpan] {
        &self.spans
    }
}

fn validate_verified_span_metadata(span: &ProviderAbsenceSpan) -> Result<(), LabError> {
    let step = span.interval.duration();
    let step_seconds = step.num_seconds();
    let expected_start = span
        .pre_anchor
        .open_time_utc
        .0
        .checked_add_signed(step)
        .ok_or_else(|| LabError::DataCorrupt("provider coverage anchor overflows".into()))?;
    let duration = span.end_utc.0 - span.start_utc.0;
    let elapsed = span.snapshots[1].fetched_at_utc.0 - span.snapshots[0].fetched_at_utc.0;
    if step_seconds <= 0
        || !aligned(span.pre_anchor.open_time_utc, span.interval)
        || !aligned(span.post_anchor.open_time_utc, span.interval)
        || !aligned(span.start_utc, span.interval)
        || !aligned(span.end_utc, span.interval)
        || span.start_utc.0 != expected_start
        || span.end_utc != span.post_anchor.open_time_utc
        || span.start_utc >= span.end_utc
        || duration.num_seconds() % step_seconds != 0
        || duration.num_seconds() / step_seconds > MAX_VERIFIED_PROVIDER_GAP_SLOTS
        || span.snapshots[0].raw_object_id == span.snapshots[1].raw_object_id
        || elapsed < chrono::Duration::seconds(MIN_VERIFICATION_SECONDS)
        || elapsed > chrono::Duration::seconds(MAX_VERIFICATION_SECONDS)
    {
        return Err(LabError::DataCorrupt(
            "provider coverage metadata is misaligned or outside verified bounds".into(),
        ));
    }
    Ok(())
}

fn aligned(time: UtcTimestamp, interval: CandleInterval) -> bool {
    let seconds = interval.duration().num_seconds();
    time.0.timestamp_subsec_nanos() == 0
        && seconds > 0
        && time.0.timestamp().rem_euclid(seconds) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(value: &str) -> UtcTimestamp {
        UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
    }

    fn span(
        interval: CandleInterval,
        pre_open: &str,
        post_open: &str,
        proof_seed: &str,
    ) -> ProviderAbsenceSpan {
        let pre_open = time(pre_open);
        let post_open = time(post_open);
        let fetched_at = time("2026-01-01T23:00:01Z");
        ProviderAbsenceSpan {
            proof_id: ContentHash::of_bytes(proof_seed.as_bytes()),
            market: MarketId::parse_upbit("KRW-BTC").expect("fixture market"),
            interval,
            start_utc: UtcTimestamp(pre_open.0 + interval.duration()),
            end_utc: post_open,
            pre_anchor: ProviderAbsenceAnchor {
                observation_id: ObservationId::from_seed(&format!("{proof_seed}-pre")),
                open_time_utc: pre_open,
                content_digest: ContentHash::of_bytes(format!("{proof_seed}-pre").as_bytes()),
            },
            post_anchor: ProviderAbsenceAnchor {
                observation_id: ObservationId::from_seed(&format!("{proof_seed}-post")),
                open_time_utc: post_open,
                content_digest: ContentHash::of_bytes(format!("{proof_seed}-post").as_bytes()),
            },
            snapshots: [
                ProviderAbsenceSnapshotRef {
                    raw_object_id: RawObjectId::from_seed(&format!("{proof_seed}-raw-1")),
                    fetched_at_utc: fetched_at,
                    body_digest: ContentHash::of_bytes(b"same response bytes are allowed"),
                },
                ProviderAbsenceSnapshotRef {
                    raw_object_id: RawObjectId::from_seed(&format!("{proof_seed}-raw-2")),
                    fetched_at_utc: UtcTimestamp(fetched_at.0 + chrono::Duration::seconds(1)),
                    body_digest: ContentHash::of_bytes(b"same response bytes are allowed"),
                },
            ],
        }
    }

    #[test]
    fn verified_coverage_uses_bounded_interval_unions_and_allows_partial_ranges() {
        let m5 = span(
            CandleInterval::M5,
            "2026-01-01T17:00:00Z",
            "2026-01-01T22:00:00Z",
            "m5-outage",
        );
        let h1 = span(
            CandleInterval::H1,
            "2026-01-01T17:00:00Z",
            "2026-01-01T22:00:00Z",
            "h1-outage",
        );
        let coverage =
            VerifiedProviderCoverage::from_verified_spans(vec![h1.clone(), m5.clone(), h1.clone()])
                .expect("admit structurally valid spans already verified by storage");
        let market = MarketId::parse_upbit("KRW-BTC").expect("fixture market");

        // The real outage geometry is 59 missing M5 opens and four missing H1 opens.
        assert!(coverage.covers(
            &market,
            CandleInterval::M5,
            time("2026-01-01T17:05:00Z"),
            time("2026-01-01T22:00:00Z"),
        ));
        assert!(coverage.covers(
            &market,
            CandleInterval::H1,
            time("2026-01-01T18:00:00Z"),
            time("2026-01-01T22:00:00Z"),
        ));
        // A source suffix can be covered by the tail of a larger same-timeframe proof.
        assert!(coverage.covers(
            &market,
            CandleInterval::M5,
            time("2026-01-01T18:00:00Z"),
            time("2026-01-01T22:00:00Z"),
        ));
        assert!(coverage.covers(
            &market,
            CandleInterval::M5,
            time("2026-01-01T18:00:00Z"),
            time("2026-01-01T18:00:00Z"),
        ));
        assert!(!coverage.covers(
            &market,
            CandleInterval::M5,
            time("2026-01-01T18:05:00Z"),
            time("2026-01-01T18:00:00Z"),
        ));
        assert!(!coverage.covers(
            &MarketId::parse_upbit("KRW-ETH").expect("other market"),
            CandleInterval::M5,
            time("2026-01-01T17:05:00Z"),
            time("2026-01-01T22:00:00Z"),
        ));
        assert_eq!(coverage.spans(), &[m5, h1]);
        let proof_ids = coverage.proof_ids();
        assert_eq!(proof_ids.len(), 2);
        assert!(proof_ids[0] < proof_ids[1]);
    }

    #[test]
    fn verified_coverage_rejects_misaligned_or_unbounded_span_metadata() {
        let mut misaligned = span(
            CandleInterval::M5,
            "2026-01-01T17:00:00Z",
            "2026-01-01T22:00:00Z",
            "misaligned",
        );
        misaligned.start_utc = time("2026-01-01T17:06:00Z");
        assert!(matches!(
            VerifiedProviderCoverage::from_verified_spans(vec![misaligned]),
            Err(LabError::DataCorrupt(_))
        ));

        let too_many = (0..=MAX_VERIFIED_PROVIDER_SPANS)
            .map(|index| {
                span(
                    CandleInterval::M5,
                    "2026-01-01T17:00:00Z",
                    "2026-01-01T22:00:00Z",
                    &format!("proof-{index}"),
                )
            })
            .collect();
        assert!(matches!(
            VerifiedProviderCoverage::from_verified_spans(too_many),
            Err(LabError::ResourceLimit(_))
        ));
    }
}

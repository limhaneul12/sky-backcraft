//! Shared finite statistic values and explicit absence reasons.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Why a statistic is absent rather than silently coerced to zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NullReason {
    NoAccountMarks,
    InsufficientSamples,
    InsufficientCalendarDuration,
    ZeroDenominator,
    NoClosedEpisodes,
    NoLosingEpisodes,
    NoPositiveProfit,
    DataNotCaptured,
    NotApplicable,
    NonFiniteResult,
}

/// A finite statistic or a typed explanation for its absence.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetricValue {
    pub value: Option<f64>,
    pub null_reason: Option<NullReason>,
}

impl MetricValue {
    pub(crate) fn value(value: f64) -> Self {
        if value.is_finite() {
            Self {
                value: Some(value),
                null_reason: None,
            }
        } else {
            Self::null(NullReason::NonFiniteResult)
        }
    }

    pub(crate) const fn null(reason: NullReason) -> Self {
        Self {
            value: None,
            null_reason: Some(reason),
        }
    }
}

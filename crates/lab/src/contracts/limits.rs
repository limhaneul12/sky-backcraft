//! Typed request-size ceilings and actionable limit rejection details.

use super::{MAX_DATASET_ROWS, MAX_MODEL_EVENTS, MAX_RUN_EVENTS};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

pub const MAX_COLLECTION_EVALUATION_DAYS: u64 = 366;
const SECONDS_PER_DAY: u64 = 86_400;
pub const MAX_COLLECTION_EVALUATION_SECONDS: u64 = MAX_COLLECTION_EVALUATION_DAYS * SECONDS_PER_DAY;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LimitItem {
    CollectionEvaluationSeconds,
    CollectionRows,
    ModelEvents,
    RunEvents,
}

impl LimitItem {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CollectionEvaluationSeconds => "COLLECTION_EVALUATION_SECONDS",
            Self::CollectionRows => "COLLECTION_ROWS",
            Self::ModelEvents => "MODEL_EVENTS",
            Self::RunEvents => "RUN_EVENTS",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActiveLimits {
    pub collection_evaluation_days: u64,
    pub collection_evaluation_seconds: u64,
    pub collection_rows: u64,
    pub model_events: u64,
    pub run_events: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestSize {
    pub evaluation_days: Option<u64>,
    pub evaluation_seconds: Option<u64>,
    pub collection_rows: Option<u64>,
    pub model_count: Option<u64>,
    pub max_model_events: Option<u64>,
    pub run_events: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExceededLimit {
    pub item: LimitItem,
    pub requested: u64,
    pub allowed: u64,
    pub excess: u64,
    pub reduction_conditions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LimitReport {
    pub request_size: RequestSize,
    pub active_limits: ActiveLimits,
    pub exceeded: ExceededLimit,
}

impl LimitReport {
    #[must_use]
    pub fn exceeded(
        request_size: RequestSize,
        item: LimitItem,
        requested: u64,
        allowed: u64,
        reduction_conditions: &[&str],
    ) -> Self {
        Self {
            request_size,
            active_limits: active_limits(),
            exceeded: ExceededLimit {
                item,
                requested,
                allowed,
                excess: requested.saturating_sub(allowed),
                reduction_conditions: reduction_conditions
                    .iter()
                    .map(|condition| (*condition).to_owned())
                    .collect(),
            },
        }
    }
}

impl fmt::Display for LimitReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} requested {}, allowed {}, excess {}; reduce by {}",
            self.exceeded.item.as_str(),
            self.exceeded.requested,
            self.exceeded.allowed,
            self.exceeded.excess,
            self.exceeded.reduction_conditions.join("; ")
        )
    }
}

#[must_use]
pub fn active_limits() -> ActiveLimits {
    ActiveLimits {
        collection_evaluation_days: MAX_COLLECTION_EVALUATION_DAYS,
        collection_evaluation_seconds: MAX_COLLECTION_EVALUATION_SECONDS,
        collection_rows: usize_limit(MAX_DATASET_ROWS),
        model_events: usize_limit(MAX_MODEL_EVENTS),
        run_events: usize_limit(MAX_RUN_EVENTS),
    }
}

/// Convert a positive request duration to whole billed/request-size days.
/// Any fractional day counts as one additional requested day.
#[must_use]
pub fn evaluation_days_ceil(seconds: u64) -> u64 {
    seconds.div_ceil(SECONDS_PER_DAY)
}

fn usize_limit(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_report_preserves_size_threshold_excess_and_reduction_conditions() {
        assert_eq!(evaluation_days_ceil(366 * SECONDS_PER_DAY), 366);
        assert_eq!(evaluation_days_ceil(366 * SECONDS_PER_DAY + 60), 367);
        let size = RequestSize {
            evaluation_days: Some(367),
            evaluation_seconds: Some(MAX_COLLECTION_EVALUATION_SECONDS + 60),
            collection_rows: Some(30_001),
            ..RequestSize::default()
        };
        let report = LimitReport::exceeded(
            size,
            LimitItem::CollectionEvaluationSeconds,
            MAX_COLLECTION_EVALUATION_SECONDS + 60,
            active_limits().collection_evaluation_seconds,
            &["shorten the evaluation range"],
        );
        assert_eq!(report.exceeded.excess, 60);
        assert_eq!(report.request_size, size);
        assert!(report.to_string().contains("COLLECTION_EVALUATION_SECONDS"));
        assert!(
            serde_json::to_value(report)
                .expect("limit report serializes")
                .get("active_limits")
                .is_some()
        );
    }
}

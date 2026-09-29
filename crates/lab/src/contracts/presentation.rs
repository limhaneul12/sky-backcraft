//! Typed units and linked read projections derived from immutable ledger facts.

use super::{ReasonCode, SignalId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PriceCostAttributionUnit {
    /// Correct quantity-weighted signed quote-currency amount.
    Krw,
    /// Immutable `FillRecord.price_cost_attribution` signed price delta.
    KrwPerBaseUnit,
    /// Historical review values summed signed KRW-per-base-unit deltas without quantity weights.
    #[default]
    LegacySumKrwPerBaseUnit,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EpisodeExitDetails {
    pub execution_result: Option<ReasonCode>,
    pub closing_signal_id: Option<SignalId>,
    pub strategy_exit_reasons: Vec<ReasonCode>,
}

//! Causal regime classification contracts: OHLCV-only features, deterministic
//! rule labels and frozen gating policies. Labels use only data at or before
//! each decision time — never the future path.

use super::{ContentHash, LabError, MarketId, UtcTimestamp, Weight};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const CLASSIFIER_REVISION: &str = "rule-v1-ohlcv-sma-slope-vol";
pub const MAX_CLASSIFIER_LOOKBACK: usize = 5_000;

/// Deterministic, frozen rule classifier parameters.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegimeClassifierSpec {
    /// Frozen identity of this rule set; changes invalidate comparability.
    pub revision: String,
    pub sma_long: usize,
    pub sma_mid: usize,
    pub sma_short: usize,
    /// Bars between SMA samples for the short-average slope.
    pub slope_lookback: usize,
    /// Realized-volatility window (sample std of per-bar returns).
    pub vol_lookback: usize,
    /// ATR window for the ATR/price feature.
    pub atr_lookback: usize,
    /// Optional annualized realized-vol thresholds for the volatility overlay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub high_vol_annualized: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub low_vol_annualized: Option<f64>,
}

impl RegimeClassifierSpec {
    /// # Errors
    /// Rejects unbounded lookbacks, unordered averages and crossed vol bounds.
    pub fn validate(&self) -> Result<(), LabError> {
        if self.revision.is_empty() || self.revision.len() > 64 {
            return Err(LabError::InvalidConfig(
                "classifier revision must be 1..=64 characters".into(),
            ));
        }
        for length in [
            self.sma_long,
            self.sma_mid,
            self.sma_short,
            self.slope_lookback,
            self.vol_lookback,
            self.atr_lookback,
        ] {
            if !(2..=MAX_CLASSIFIER_LOOKBACK).contains(&length) {
                return Err(LabError::InvalidConfig(format!(
                    "classifier lookback {length} must be in 2..={MAX_CLASSIFIER_LOOKBACK}"
                )));
            }
        }
        if self.sma_long < self.sma_short || self.sma_short < self.sma_mid {
            return Err(LabError::InvalidConfig(
                "classifier requires sma_long >= sma_short >= sma_mid".into(),
            ));
        }
        if let (Some(high), Some(low)) = (self.high_vol_annualized, self.low_vol_annualized) {
            if !high.is_finite() || !low.is_finite() || high <= low || high <= 0.0 {
                return Err(LabError::InvalidConfig(
                    "vol thresholds require 0 <= low < high and finite values".into(),
                ));
            }
        } else if self.high_vol_annualized.is_some() || self.low_vol_annualized.is_some() {
            return Err(LabError::InvalidConfig(
                "both vol thresholds must be set together".into(),
            ));
        }
        Ok(())
    }

    /// Completed bars needed before the first non-unknown label.
    #[must_use]
    pub fn warmup_bars(&self) -> usize {
        self.sma_long.max(self.vol_lookback).max(self.atr_lookback) + self.slope_lookback
    }
}

/// Trend regime label; `Unknown` marks insufficient warmup, never a guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RegimeLabel {
    TrendUp,
    TrendDown,
    Chop,
    Unknown,
}

/// Orthogonal volatility state; only set when both thresholds are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VolState {
    HighVol,
    NormalVol,
    LowVol,
}

/// OHLCV-only features at one decision time; all values are PIT.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegimeFeatures {
    /// `close / SMA(long) - 1`.
    pub close_vs_sma_long: f64,
    /// `SMA(mid) / SMA(short) - 1`.
    pub sma_mid_vs_short: f64,
    /// Per-bar slope of the short SMA over `slope_lookback` bars.
    pub sma_short_slope: f64,
    /// `ATR / close` over the ATR window.
    pub atr_ratio: f64,
    /// Annualized realized volatility over the volatility window.
    pub realized_vol_annualized: f64,
    /// Volume relative to its rolling average (>= 0).
    pub volume_expansion: f64,
    /// Fraction of up closes over the slope lookback window.
    pub directional_persistence: f64,
}

/// One causal regime observation with its exact input identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegimeObservation {
    pub market: MarketId,
    pub decision_time: UtcTimestamp,
    /// Classification features are absent only during frozen classifier
    /// warmup. UNKNOWN never carries fabricated zero features.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<RegimeFeatures>,
    pub regime: RegimeLabel,
    pub vol_state: VolState,
    pub classifier_revision: String,
    /// Digest binding the classifier revision, dataset identity, decision time
    /// and window geometry; the observation never re-hashes whole bars.
    pub causal_input_digest: ContentHash,
}

/// Gating action derived from one regime label.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum RegimeGateAction {
    Enabled,
    Disabled,
    ReducedExposure { max_weight: Weight },
}

impl RegimeGateAction {
    /// Whether strategy entries are permitted under this action.
    #[must_use]
    pub fn allows_entries(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// Exposure ceiling implied by this action; `None` means uncapped.
    #[must_use]
    pub fn exposure_cap(&self) -> Option<Weight> {
        match self {
            Self::Enabled => None,
            Self::Disabled => Some(Weight::new(rust_decimal::Decimal::ZERO).ok()?),
            Self::ReducedExposure { max_weight } => Some(*max_weight),
        }
    }
}

/// Frozen per-label gating rules for one strategy family.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegimeGateRule {
    pub trend_up: RegimeGateAction,
    pub trend_down: RegimeGateAction,
    pub chop: RegimeGateAction,
    pub unknown: RegimeGateAction,
}

impl RegimeGateRule {
    /// # Errors
    /// Rejects negative or oversized exposure caps.
    pub fn validate(&self) -> Result<(), LabError> {
        for (name, action) in [
            ("trend_up", &self.trend_up),
            ("trend_down", &self.trend_down),
            ("chop", &self.chop),
            ("unknown", &self.unknown),
        ] {
            if let RegimeGateAction::ReducedExposure { max_weight } = action
                && (max_weight.get() <= rust_decimal::Decimal::ZERO
                    || max_weight.get() > rust_decimal::Decimal::ONE)
            {
                return Err(LabError::InvalidConfig(format!(
                    "{name} reduced exposure must be in (0,1]"
                )));
            }
        }
        Ok(())
    }

    /// The action for one label.
    #[must_use]
    pub fn action(&self, regime: RegimeLabel) -> &RegimeGateAction {
        match regime {
            RegimeLabel::TrendUp => &self.trend_up,
            RegimeLabel::TrendDown => &self.trend_down,
            RegimeLabel::Chop => &self.chop,
            RegimeLabel::Unknown => &self.unknown,
        }
    }
}

/// Frozen classifier plus gating rules for a portfolio run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegimeGateSpec {
    pub classifier: RegimeClassifierSpec,
    pub rules: RegimeGateRule,
}

impl RegimeGateSpec {
    /// # Errors
    /// Propagates classifier and rule validation failures.
    pub fn validate(&self) -> Result<(), LabError> {
        self.classifier.validate()?;
        self.rules.validate()
    }
}

/// Aggregate regime share summary for one gated run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegimeSummary {
    /// Decision-bar count per label.
    pub bars: std::collections::BTreeMap<String, u64>,
    /// Label-transition count along the decision timeline.
    pub transitions: u64,
}

impl RegimeSummary {
    /// Aggregate one run's observations in decision-time order.
    ///
    /// # Errors
    /// Retains the result shape used by callers while aggregating only typed
    /// labels; no string parsing or fallback participates.
    pub fn from_observations(observations: &[RegimeObservation]) -> Result<Self, LabError> {
        let mut ordered: Vec<&RegimeObservation> = observations.iter().collect();
        ordered.sort_by(|left, right| {
            left.decision_time
                .cmp(&right.decision_time)
                .then_with(|| left.market.code().cmp(&right.market.code()))
        });
        let mut bars = std::collections::BTreeMap::new();
        let mut transitions = 0_u64;
        let mut previous = std::collections::BTreeMap::<String, RegimeLabel>::new();
        for observation in ordered {
            let key = match observation.regime {
                RegimeLabel::TrendUp => "TREND_UP",
                RegimeLabel::TrendDown => "TREND_DOWN",
                RegimeLabel::Chop => "CHOP",
                RegimeLabel::Unknown => "UNKNOWN",
            }
            .to_string();
            bars.entry(key.clone())
                .and_modify(|count: &mut u64| *count = count.saturating_add(1))
                .or_insert(1_u64);
            let market = observation.market.code();
            if previous
                .insert(market, observation.regime)
                .is_some_and(|label| label != observation.regime)
            {
                transitions = transitions.saturating_add(1);
            }
        }
        Ok(Self { bars, transitions })
    }
}

#[cfg(test)]
mod summary_tests {
    use super::*;

    #[test]
    fn regime_summary_counts_bars_and_transitions_per_market()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::parse_upbit("KRW-BTC")?;
        let observation = |time: &str,
                           label: RegimeLabel|
         -> Result<RegimeObservation, Box<dyn std::error::Error>> {
            Ok(RegimeObservation {
                market: market.clone(),
                decision_time: UtcTimestamp::parse_rfc3339(time)?,
                features: Some(RegimeFeatures {
                    close_vs_sma_long: 0.0,
                    sma_mid_vs_short: 0.0,
                    sma_short_slope: 0.0,
                    atr_ratio: 0.0,
                    realized_vol_annualized: 0.0,
                    volume_expansion: 0.0,
                    directional_persistence: 0.0,
                }),
                regime: label,
                vol_state: VolState::NormalVol,
                classifier_revision: "test".into(),
                causal_input_digest: ContentHash::of_bytes(time.as_bytes()),
            })
        };
        let observations = vec![
            observation("2025-01-01T02:00:00Z", RegimeLabel::Unknown)?,
            observation("2025-01-01T03:00:00Z", RegimeLabel::TrendUp)?,
            observation("2025-01-01T04:00:00Z", RegimeLabel::TrendUp)?,
            observation("2025-01-01T05:00:00Z", RegimeLabel::Chop)?,
        ];
        let summary = RegimeSummary::from_observations(&observations)?;
        assert_eq!(summary.transitions, 2);
        assert_eq!(summary.bars.get("TREND_UP"), Some(&2));
        assert_eq!(summary.bars.get("UNKNOWN"), Some(&1));
        let shuffled = vec![
            observations[3].clone(),
            observations[0].clone(),
            observations[2].clone(),
            observations[1].clone(),
        ];
        assert_eq!(RegimeSummary::from_observations(&shuffled)?, summary);

        let eth = MarketId::parse_upbit("KRW-ETH")?;
        let mut interleaved = observations.clone();
        for (index, label) in [RegimeLabel::Unknown, RegimeLabel::TrendDown]
            .into_iter()
            .enumerate()
        {
            let mut entry = observations[index].clone();
            entry.market = eth.clone();
            entry.regime = label;
            interleaved.push(entry);
        }
        let interleaved = RegimeSummary::from_observations(&interleaved)?;
        assert_eq!(
            interleaved.transitions, 3,
            "two BTC and one ETH transitions"
        );
        Ok(())
    }
}

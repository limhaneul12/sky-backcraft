//! Explicit research assumptions and frozen experiment admission.

use super::{
    AssetQuantity, BasisPoints, CandleInterval, ContentHash, DatasetId, EvidenceSnapshotId,
    LabError, MarketId, ModelId, PlanId, PriceKrw, QuoteAmount, RequestId, RuleSnapshotId,
    UtcRange, UtcTimestamp, Weight,
};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const SCHEMA_VERSION: &str = "1.0";
pub const ENGINE_VERSION: &str = "spot-lab-engine-v2-policy-state";
pub const INDICATOR_VERSION: &str = "ema-sma-seed-rsi-wilder-sample-vol-v1";
pub const ROUNDING_VERSION: &str = "decimal-checked-tick-qty-floor-reconciliation-1e-8-v2";
/// Maximum aggregate equity reconciliation residual; ledger amounts are never adjusted.
pub const NUMERIC_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 8);
pub const MAX_MODELS: usize = 21;
pub const MAX_MODEL_EVENTS: usize = 20_000;
pub const MAX_RUN_EVENTS: usize = 100_000;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyKind {
    S1,
    S2,
    S3,
    S4,
    S5,
    BuyAndHold,
    S1CoverageControl,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StateParameters {
    pub ema_length: usize,
    pub vol_length: usize,
    #[serde(deserialize_with = "finite_statistic")]
    pub k: f64,
}

impl StateParameters {
    /// # Errors
    /// Rejects invalid lookbacks and nonfinite/negative band coefficients.
    pub fn validate(&self) -> Result<(), LabError> {
        lookback(self.ema_length)?;
        lookback(self.vol_length)?;
        if self.vol_length < 2 || !self.k.is_finite() || self.k < 0.0 {
            return Err(LabError::InvalidConfig(
                "vol_length>=2 and finite k>=0 required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum StrategySpec {
    S1 {
        state: StateParameters,
    },
    S2 {
        entry_length: usize,
        exit_length: usize,
    },
    S3 {
        trend_length: usize,
        rsi_length: usize,
        #[serde(deserialize_with = "finite_statistic")]
        entry_threshold: f64,
        #[serde(deserialize_with = "finite_statistic")]
        exit_threshold: f64,
        max_holding_bars: usize,
        signal_expiry_bars: usize,
    },
    S4 {
        state: StateParameters,
        #[serde(deserialize_with = "finite_statistic")]
        target_annual_vol: f64,
        #[serde(deserialize_with = "finite_statistic")]
        vol_floor: f64,
        rebalance_band: Weight,
    },
    S5 {
        state: StateParameters,
    },
    BuyAndHold,
    S1CoverageControl {
        state: StateParameters,
    },
}

impl StrategySpec {
    #[must_use]
    pub fn kind(&self) -> StrategyKind {
        match self {
            Self::S1 { .. } => StrategyKind::S1,
            Self::S2 { .. } => StrategyKind::S2,
            Self::S3 { .. } => StrategyKind::S3,
            Self::S4 { .. } => StrategyKind::S4,
            Self::S5 { .. } => StrategyKind::S5,
            Self::BuyAndHold => StrategyKind::BuyAndHold,
            Self::S1CoverageControl { .. } => StrategyKind::S1CoverageControl,
        }
    }
    /// Required completed decision bars before evaluation.
    /// # Errors
    /// Rejects invalid strategy parameters before calculating lengths.
    pub fn warmup_bars(&self) -> Result<usize, LabError> {
        self.validate()?;
        Ok(match self {
            Self::S1 { state }
            | Self::S4 { state, .. }
            | Self::S5 { state }
            | Self::S1CoverageControl { state } => state.ema_length.max(state.vol_length + 1),
            Self::S2 {
                entry_length,
                exit_length,
            } => (*entry_length).max(*exit_length) + 1,
            Self::S3 {
                trend_length,
                rsi_length,
                ..
            } => (*trend_length).max(*rsi_length + 1),
            Self::BuyAndHold => 1,
        })
    }
    /// # Errors
    /// Rejects unbounded lookbacks, invalid thresholds and nonfinite values.
    pub fn validate(&self) -> Result<(), LabError> {
        match self {
            Self::S1 { state } | Self::S5 { state } | Self::S1CoverageControl { state } => {
                state.validate()
            }
            Self::S2 {
                entry_length,
                exit_length,
            } => {
                lookback(*entry_length)?;
                lookback(*exit_length)
            }
            Self::S3 {
                trend_length,
                rsi_length,
                entry_threshold,
                exit_threshold,
                max_holding_bars,
                signal_expiry_bars,
            } => {
                for length in [
                    *trend_length,
                    *rsi_length,
                    *max_holding_bars,
                    *signal_expiry_bars,
                ] {
                    lookback(length)?;
                }
                if !entry_threshold.is_finite()
                    || !exit_threshold.is_finite()
                    || !(0.0..=100.0).contains(entry_threshold)
                    || !(0.0..=100.0).contains(exit_threshold)
                    || entry_threshold >= exit_threshold
                {
                    return Err(LabError::InvalidConfig(
                        "RSI thresholds must be finite 0<=entry<exit<=100".into(),
                    ));
                }
                Ok(())
            }
            Self::S4 {
                state,
                target_annual_vol,
                vol_floor,
                ..
            } => {
                state.validate()?;
                if !target_annual_vol.is_finite()
                    || !vol_floor.is_finite()
                    || *target_annual_vol <= 0.0
                    || *vol_floor <= 0.0
                {
                    return Err(LabError::InvalidConfig(
                        "positive finite volatility target/floor required".into(),
                    ));
                }
                Ok(())
            }
            Self::BuyAndHold => Ok(()),
        }
    }
}

fn lookback(value: usize) -> Result<(), LabError> {
    if (1..=5000).contains(&value) {
        Ok(())
    } else {
        Err(LabError::InvalidConfig(
            "lookback/holding/expiry must be in 1..=5000 decision bars".into(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TerminalPolicy {
    MarkToMarket,
    LiquidateScenario,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PitPolicy {
    StrictPit,
    LatestVersionProxy,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvidenceUnavailablePolicy {
    CashWithMatchedControl,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuleProvenance {
    VerifiedHistorical,
    ExplicitScenario,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TickBand {
    pub lower_bound: QuoteAmount,
    pub tick: PriceKrw,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MarketRuleSnapshot {
    pub id: RuleSnapshotId,
    pub provenance: RuleProvenance,
    pub valid_range: UtcRange,
    pub observed_at: UtcTimestamp,
    pub source_refs: Vec<String>,
    pub assumption_label: String,
    pub min_notional: QuoteAmount,
    pub quantity_step: AssetQuantity,
    /// Ascending lower bounds, first bound zero; last band has no upper bound.
    pub ticks: Vec<TickBand>,
}

impl MarketRuleSnapshot {
    /// # Errors
    /// Rejects noncovering rules, zero steps, unordered bands and missing provenance.
    pub fn validate(&self, range: UtcRange) -> Result<(), LabError> {
        if range.start() < self.valid_range.start() || range.end() > self.valid_range.end() {
            return Err(LabError::UnverifiedMarketRules(
                "rules do not cover evaluation range".into(),
            ));
        }
        if self.quantity_step.get() <= Decimal::ZERO
            || self.ticks.is_empty()
            || self.ticks.len() > 32
            || self.ticks[0].lower_bound.get() != Decimal::ZERO
        {
            return Err(LabError::InvalidConfig(
                "positive quantity step and 1..32 ticks anchored at zero required".into(),
            ));
        }
        if self
            .ticks
            .windows(2)
            .any(|p| p[0].lower_bound >= p[1].lower_bound || p[0].tick > p[1].tick)
        {
            return Err(LabError::InvalidConfig(
                "tick bands must have increasing boundaries and nondecreasing ticks".into(),
            ));
        }
        if self.source_refs.is_empty()
            || self.source_refs.len() > 16
            || self.assumption_label.is_empty()
            || self.assumption_label.len() > 512
            || self.source_refs.iter().any(|s| s.len() > 2048)
        {
            return Err(LabError::InvalidConfig(
                "bounded rule provenance is required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostPolicy {
    pub buy_fee_bps: BasisPoints,
    pub sell_fee_bps: BasisPoints,
    pub maker_fee_bps: BasisPoints,
    pub half_spread_bps: BasisPoints,
    pub slippage_bps: BasisPoints,
    pub impact_bps: BasisPoints,
    pub assumption_label: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PassiveFraction {
    Quarter,
    Half,
    Full,
}
impl PassiveFraction {
    #[must_use]
    pub fn decimal(self) -> Decimal {
        match self {
            Self::Quarter => Decimal::new(25, 2),
            Self::Half => Decimal::new(5, 1),
            Self::Full => Decimal::ONE,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ExecutionPolicy {
    NextBarOpen {
        participation_cap: Weight,
    },
    PassiveBuy {
        offset_bps: BasisPoints,
        penetration_ticks: u32,
        fill_fraction: PassiveFraction,
        ttl_execution_bars: u32,
        participation_cap: Weight,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportClock {
    /// Initial supported aggregation clock: UTC calendar day.
    pub timezone: String,
    pub min_annualization_days: u32,
    pub risk_free_annual: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentSpec {
    pub schema_version: String,
    pub dataset_ids: Vec<DatasetId>,
    pub markets: Vec<MarketId>,
    pub range: UtcRange,
    pub strategies: Vec<StrategySpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_selections: Vec<super::PolicyRevisionRef>,
    pub decision_interval: CandleInterval,
    pub execution_resolution: CandleInterval,
    pub latency_ms: u64,
    pub initial_cash: QuoteAmount,
    pub costs: CostPolicy,
    pub execution: ExecutionPolicy,
    pub market_rules: MarketRuleSnapshot,
    pub terminal_policy: TerminalPolicy,
    pub evidence_snapshot_id: Option<EvidenceSnapshotId>,
    pub pit_policy: PitPolicy,
    pub evidence_unavailable: EvidenceUnavailablePolicy,
    pub report_clock: ReportClock,
    pub seed: u64,
}

impl ExperimentSpec {
    fn validate_selections(&self) -> Result<(), LabError> {
        let selections =
            match self.schema_version.as_str() {
                "1.0" if self.policy_selections.is_empty() => self.strategies.len(),
                "2.0" if self.strategies.is_empty() => self.policy_selections.len(),
                _ => return Err(LabError::InvalidConfig(
                    "experiment v1 requires legacy strategies; v2 requires exact policy selections"
                        .into(),
                )),
            };
        if self.dataset_ids.is_empty()
            || self.dataset_ids.len() > 6
            || self.markets.is_empty()
            || self.markets.len() > 3
            || selections == 0
            || selections > 7
        {
            return Err(LabError::InvalidConfig(
                "invalid experiment schema, input or model cardinality".into(),
            ));
        }
        if self
            .markets
            .len()
            .checked_mul(selections)
            .is_none_or(|n| n > MAX_MODELS)
        {
            return Err(LabError::ResourceLimit("model count exceeds 21".into()));
        }
        let markets: BTreeSet<_> = self.markets.iter().map(MarketId::code).collect();
        let kinds: BTreeSet<_> = self.strategies.iter().map(StrategySpec::kind).collect();
        let revisions: BTreeSet<_> = self
            .policy_selections
            .iter()
            .map(|reference| &reference.revision_id)
            .collect();
        let datasets: BTreeSet<_> = self.dataset_ids.iter().collect();
        if markets.len() != self.markets.len()
            || kinds.len() != self.strategies.len()
            || revisions.len() != self.policy_selections.len()
            || datasets.len() != self.dataset_ids.len()
        {
            return Err(LabError::InvalidConfig(
                "duplicate market, strategy kind or input dataset".into(),
            ));
        }
        for strategy in &self.strategies {
            strategy.validate()?;
        }
        Ok(())
    }

    /// Validate explicit economic inputs; dataset availability is a separate admission step.
    /// # Errors
    /// Returns typed configuration/resource/rule errors without changing inputs.
    pub fn validate(&self) -> Result<(), LabError> {
        self.validate_selections()?;
        self.range.aligned(self.decision_interval)?;
        self.range.aligned(self.execution_resolution)?;
        if self.decision_interval.duration().num_seconds()
            % self.execution_resolution.duration().num_seconds()
            != 0
        {
            return Err(LabError::InvalidConfig(
                "execution must divide decision interval".into(),
            ));
        }
        if self.initial_cash.get() <= Decimal::ZERO || self.latency_ms > 86_400_000 {
            return Err(LabError::InvalidConfig(
                "positive cash and latency <= one day required".into(),
            ));
        }
        for fee in [
            self.costs.buy_fee_bps,
            self.costs.sell_fee_bps,
            self.costs.maker_fee_bps,
        ] {
            if fee.get() >= Decimal::from(10_000) {
                return Err(LabError::InvalidConfig(
                    "fees must be below 10000 bps".into(),
                ));
            }
        }
        let price_cost = self
            .costs
            .half_spread_bps
            .get()
            .checked_add(self.costs.slippage_bps.get())
            .and_then(|v| v.checked_add(self.costs.impact_bps.get()))
            .ok_or_else(|| LabError::InvalidConfig("price cost overflow".into()))?;
        if price_cost >= Decimal::from(10_000)
            || self.costs.assumption_label.is_empty()
            || self.costs.assumption_label.len() > 512
        {
            return Err(LabError::InvalidConfig(
                "price costs must be below 10000 bps with explicit bounded assumptions".into(),
            ));
        }
        if let ExecutionPolicy::PassiveBuy {
            offset_bps,
            penetration_ticks,
            ttl_execution_bars,
            ..
        } = &self.execution
            && (offset_bps.get() >= Decimal::from(10_000)
                || *penetration_ticks > 100
                || *ttl_execution_bars != 1)
        {
            return Err(LabError::InvalidConfig(
                "passive policy requires offset<10000bps, penetration<=100 and one-bar TTL".into(),
            ));
        }
        self.market_rules.validate(self.range)?;
        if !matches!(self.report_clock.timezone.as_str(), "UTC" | "Asia/Seoul")
            || !self.report_clock.risk_free_annual.is_finite()
            || self.report_clock.min_annualization_days == 0
        {
            return Err(LabError::InvalidConfig("report clock requires UTC/Asia-Seoul, finite risk-free rate and positive annualization duration".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ModelStatus {
    Completed,
    BlockedEvidence,
    BlockedData,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdmissionStatus {
    Eligible,
    BlockedEvidence,
    BlockedData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelAdmission {
    pub model_id: ModelId,
    pub market: MarketId,
    pub strategy: StrategyKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<super::PolicyRevisionRef>,
    pub status: AdmissionStatus,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResolvedPlan {
    pub id: PlanId,
    pub spec: ExperimentSpec,
    pub config_digest: ContentHash,
    pub input_digest: ContentHash,
    pub dataset_digests: Vec<(DatasetId, ContentHash)>,
    pub evidence_digest: Option<ContentHash>,
    pub admissions: Vec<ModelAdmission>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_revisions: Vec<super::FrozenPolicyRevision>,
    pub warnings: Vec<String>,
    pub estimated_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    pub request_id: RequestId,
    pub plan_id: PlanId,
    pub input_digest: ContentHash,
}

// Internally tagged Serde variants buffer arbitrary-precision JSON numbers.
// Deserialize statistics through Number so valid decimal tokens survive that
// buffer; economic amounts continue to use exact decimal string newtypes.
pub(super) fn finite_statistic<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<f64, D::Error> {
    let number = serde_json::Number::deserialize(deserializer)?;
    number
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| serde::de::Error::custom("statistic must be a finite representable number"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_strategy_numeric_inputs_roundtrip_without_loss_of_decimal_token()
    -> Result<(), Box<dyn std::error::Error>> {
        for text in [
            r#"{"kind":"S1","state":{"ema_length":100,"vol_length":20,"k":0.5}}"#,
            r#"{"kind":"S3","trend_length":200,"rsi_length":14,"entry_threshold":35.0,"exit_threshold":55.0,"max_holding_bars":20,"signal_expiry_bars":1}"#,
            r#"{"kind":"S4","state":{"ema_length":100,"vol_length":20,"k":0.5},"target_annual_vol":0.4,"vol_floor":0.01,"rebalance_band":"0.01"}"#,
        ] {
            let strategy: StrategySpec = serde_json::from_str(text)?;
            strategy.validate()?;
            let encoded = serde_json::to_string(&strategy)?;
            let decoded: StrategySpec = serde_json::from_str(&encoded)?;
            assert_eq!(
                serde_json::to_value(&strategy)?,
                serde_json::to_value(&decoded)?
            );
        }
        assert!(
            serde_json::from_str::<StrategySpec>(
                r#"{"kind":"S1","state":{"ema_length":100,"vol_length":20,"k":1e400}}"#
            )
            .is_err()
        );
        // Actual persisted indicator values: the statistical path also requires
        // exact f64 roundtrips for immutable fact/replay hashes.
        for value in [
            108_265_545.499_470_31_f64,
            0.001_997_070_865_960_398_5,
            0.186_979_592_615_857_61,
        ] {
            let decoded: f64 = serde_json::from_str(&serde_json::to_string(&value)?)?;
            assert_eq!(value.to_bits(), decoded.to_bits());
        }
        Ok(())
    }
}

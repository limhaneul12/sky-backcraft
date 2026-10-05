//! Explicit research assumptions and frozen experiment admission.

use super::{
    AssetQuantity, BasisPoints, CandleInterval, ContentHash, DatasetId, EvidenceSnapshotId,
    LabError, MarketId, ModelId, PlanId, PriceKrw, QuoteAmount, RequestId, RuleSnapshotId,
    UtcRange, UtcTimestamp, Weight,
};
use rust_decimal::{Decimal, MathematicalOps};
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TickBand {
    pub lower_bound: QuoteAmount,
    pub tick: PriceKrw,
}

/// Fees verified to be in effect for one historical rule segment.
///
/// Presence overrides [`CostPolicy`] fee fields for that segment only when the
/// snapshot provenance is [`RuleProvenance::VerifiedHistorical`]; explicit
/// scenarios keep [`CostPolicy`] as their single fee authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoricalFeeSchedule {
    pub taker_fee_bps: BasisPoints,
    pub maker_fee_bps: BasisPoints,
    pub assumption_label: String,
}

/// Exchange-side market availability declared for one historical rule segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketTradingState {
    Active,
    Suspended,
}

pub const MAX_RULE_HISTORY_SEGMENTS: usize = 32;
pub const MAX_MAINTENANCE_WINDOWS: usize = 64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    /// Verified fee schedule for this segment; rejected on explicit scenarios.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_schedule: Option<HistoricalFeeSchedule>,
    /// Declared market availability; absent means active (no claim about history).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trading_state: Option<MarketTradingState>,
    /// Bounded maintenance/interruption windows inside `valid_range`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub maintenance_windows: Vec<UtcRange>,
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
        if let Some(schedule) = &self.fee_schedule {
            if self.provenance != RuleProvenance::VerifiedHistorical {
                return Err(LabError::InvalidConfig(
                    "verified historical provenance is required for a rule fee schedule".into(),
                ));
            }
            if schedule.assumption_label.is_empty() || schedule.assumption_label.len() > 512 {
                return Err(LabError::InvalidConfig(
                    "fee schedule requires bounded assumptions".into(),
                ));
            }
            for fee in [schedule.taker_fee_bps, schedule.maker_fee_bps] {
                if fee.get() >= Decimal::from(10_000) {
                    return Err(LabError::InvalidConfig(
                        "fee schedule fees must be below 10000 bps".into(),
                    ));
                }
            }
        }
        if self.maintenance_windows.len() > MAX_MAINTENANCE_WINDOWS {
            return Err(LabError::InvalidConfig(
                "maintenance windows exceed the bounded count".into(),
            ));
        }
        if self.maintenance_windows.iter().any(|window| {
            window.start() < self.valid_range.start() || window.end() > self.valid_range.end()
        }) {
            return Err(LabError::InvalidConfig(
                "maintenance windows must stay inside the snapshot valid range".into(),
            ));
        }
        Ok(())
    }

    /// Whether orders may be created or filled at this instant under declared
    /// availability and maintenance windows. Absent trading state is active.
    #[must_use]
    pub fn tradable_at(&self, at: UtcTimestamp) -> bool {
        if self.trading_state == Some(MarketTradingState::Suspended) {
            return false;
        }
        !self
            .maintenance_windows
            .iter()
            .any(|window| window.contains(at))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostPolicy {
    pub buy_fee_bps: BasisPoints,
    pub sell_fee_bps: BasisPoints,
    pub maker_fee_bps: BasisPoints,
    pub half_spread_bps: BasisPoints,
    pub slippage_bps: BasisPoints,
    pub impact_bps: BasisPoints,
    pub assumption_label: String,
    /// Optional OHLCV-proxy dynamic adjustment of the slippage component.
    /// Absent keeps the fixed-bps model byte-compatible with prior plans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic: Option<DynamicCostModel>,
}

/// Closed tag of the optional dynamic cost model, mirrored on fill provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DynamicCostKind {
    VolatilityAware,
    TurnoverAware,
    ParticipationAware,
}

/// OHLCV-proxy dynamic slippage adjustments; never an orderbook claim.
///
/// Every variant computes `effective_slippage_bps` from the completed
/// liquidity-source bar that precedes the execution instant, so no future
/// candle high/low can enter a next-bar-open cost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum DynamicCostModel {
    VolatilityAware {
        base_slippage_bps: BasisPoints,
        /// Slippage bps added per unit of prior-bar range fraction `(high-low)/close`.
        #[schemars(with = "String")]
        range_weight: Decimal,
        max_slippage_bps: BasisPoints,
    },
    TurnoverAware {
        base_slippage_bps: BasisPoints,
        /// Quote turnover below which the excess component rises linearly to `max_excess_bps`.
        turnover_floor: QuoteAmount,
        max_excess_bps: BasisPoints,
    },
    ParticipationAware {
        base_slippage_bps: BasisPoints,
        /// Slippage bps added per unit of `sqrt(requested_notional / quote_turnover)`.
        #[schemars(with = "String")]
        participation_weight: Decimal,
        max_slippage_bps: BasisPoints,
    },
}

/// Observed liquidity-proxy features of one completed bar, shared by every
/// dynamic cost variant. All values are labeled proxies in fill provenance.
#[derive(Debug, Clone, Copy)]
pub struct CostFeatureInputs {
    pub bar_range_fraction: Decimal,
    pub quote_turnover: Decimal,
    pub requested_notional: Decimal,
}

impl DynamicCostModel {
    #[must_use]
    pub fn kind(&self) -> DynamicCostKind {
        match self {
            Self::VolatilityAware { .. } => DynamicCostKind::VolatilityAware,
            Self::TurnoverAware { .. } => DynamicCostKind::TurnoverAware,
            Self::ParticipationAware { .. } => DynamicCostKind::ParticipationAware,
        }
    }

    /// # Errors
    /// Rejects checked-arithmetic overflow while clamping to the configured bound.
    pub fn effective_slippage_bps(
        &self,
        inputs: CostFeatureInputs,
    ) -> Result<BasisPoints, LabError> {
        let (base, max) = match self {
            Self::VolatilityAware {
                base_slippage_bps,
                max_slippage_bps,
                ..
            }
            | Self::ParticipationAware {
                base_slippage_bps,
                max_slippage_bps,
                ..
            } => (*base_slippage_bps, *max_slippage_bps),
            Self::TurnoverAware {
                base_slippage_bps,
                max_excess_bps,
                ..
            } => (
                *base_slippage_bps,
                BasisPoints::new(
                    base_slippage_bps
                        .get()
                        .checked_add(max_excess_bps.get())
                        .ok_or_else(|| LabError::InvalidConfig("cost bound overflow".into()))?,
                )?,
            ),
        };
        let component = match self {
            Self::VolatilityAware { range_weight, .. } => checked::mul(
                *range_weight,
                inputs.bar_range_fraction,
                "volatility cost component",
            )?,
            Self::TurnoverAware {
                turnover_floor,
                max_excess_bps,
                ..
            } => {
                if inputs.quote_turnover >= turnover_floor.get() {
                    Decimal::ZERO
                } else if turnover_floor.get().is_zero() {
                    return Err(LabError::InvalidConfig(
                        "turnover floor must be positive".into(),
                    ));
                } else {
                    let deficit = checked::div(
                        checked::sub(
                            turnover_floor.get(),
                            inputs.quote_turnover,
                            "turnover deficit",
                        )?,
                        turnover_floor.get(),
                        "turnover deficit ratio",
                    )?;
                    checked::mul(max_excess_bps.get(), deficit, "turnover cost component")?
                }
            }
            Self::ParticipationAware {
                participation_weight,
                ..
            } => {
                if inputs.quote_turnover.is_zero() {
                    // Zero liquidity: bounded worst case, never an invented spread.
                    checked::sub(max.get(), base.get(), "participation worst case")?
                } else {
                    let participation = checked::div(
                        inputs.requested_notional,
                        inputs.quote_turnover,
                        "participation rate",
                    )?;
                    if participation <= Decimal::ZERO {
                        Decimal::ZERO
                    } else {
                        let sqrt = participation.sqrt().ok_or_else(|| {
                            LabError::InvalidConfig("participation square root failed".into())
                        })?;
                        checked::mul(*participation_weight, sqrt, "participation cost component")?
                    }
                }
            }
        };
        let effective = checked::add(base.get(), component, "dynamic slippage")?
            .min(max.get())
            .max(base.get());
        BasisPoints::new(effective)
    }

    /// Validate bounded, finite dynamic cost parameters.
    /// # Errors
    /// Rejects negative weights, nonpositive bounds and crossed base/max bounds.
    pub fn validate(&self) -> Result<(), LabError> {
        let (base, max, weights): (BasisPoints, BasisPoints, Vec<Decimal>) = match self {
            Self::VolatilityAware {
                base_slippage_bps,
                range_weight,
                max_slippage_bps,
            } => (*base_slippage_bps, *max_slippage_bps, vec![*range_weight]),
            Self::TurnoverAware {
                base_slippage_bps,
                turnover_floor,
                max_excess_bps,
            } => {
                if turnover_floor.get() <= Decimal::ZERO {
                    return Err(LabError::InvalidConfig(
                        "turnover floor must be positive".into(),
                    ));
                }
                let ceiling = BasisPoints::new(checked::add(
                    base_slippage_bps.get(),
                    max_excess_bps.get(),
                    "turnover cost bound",
                )?)?;
                (*base_slippage_bps, ceiling, vec![max_excess_bps.get()])
            }
            Self::ParticipationAware {
                base_slippage_bps,
                participation_weight,
                max_slippage_bps,
            } => (
                *base_slippage_bps,
                *max_slippage_bps,
                vec![*participation_weight],
            ),
        };
        if base.get() < Decimal::ZERO
            || max.get() < base.get()
            || max.get() >= Decimal::from(10_000)
            || weights.iter().any(|weight| *weight < Decimal::ZERO)
        {
            return Err(LabError::InvalidConfig(
                "dynamic cost requires 0<=base<=max<10000bps and nonnegative weights".into(),
            ));
        }
        Ok(())
    }
}

/// Proxy liquidity inputs recorded on taker fills produced by a dynamic model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostProxyInputs {
    pub liquidity_bar_id: super::ObservationId,
    /// `(high-low)/close * 10000` of the completed liquidity-source bar (proxy).
    #[schemars(with = "String")]
    pub bar_range_bps: Decimal,
    /// `volume * close` of that bar (quote turnover proxy).
    pub quote_turnover: QuoteAmount,
    /// Requested notional used for the participation component.
    pub requested_notional: QuoteAmount,
    /// `requested_notional / quote_turnover`, zero when turnover is zero.
    #[schemars(with = "String")]
    pub participation_rate: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostProvenance {
    pub model_kind: DynamicCostKind,
    pub effective_slippage_bps: BasisPoints,
    pub proxy_inputs: CostProxyInputs,
}

/// Checked decimal arithmetic shared by cost provenance helpers.
pub(super) mod checked {
    use rust_decimal::Decimal;

    use super::LabError;

    /// # Errors
    /// Propagates overflow as an accounting-invariant error with the label.
    pub fn add(a: Decimal, b: Decimal, label: &str) -> Result<Decimal, LabError> {
        a.checked_add(b)
            .ok_or_else(|| LabError::AccountingInvariant(format!("{label} overflow")))
    }

    /// # Errors
    /// Propagates underflow as an accounting-invariant error with the label.
    pub fn sub(a: Decimal, b: Decimal, label: &str) -> Result<Decimal, LabError> {
        a.checked_sub(b)
            .ok_or_else(|| LabError::AccountingInvariant(format!("{label} overflow")))
    }

    /// # Errors
    /// Propagates overflow as an accounting-invariant error with the label.
    pub fn mul(a: Decimal, b: Decimal, label: &str) -> Result<Decimal, LabError> {
        a.checked_mul(b)
            .ok_or_else(|| LabError::AccountingInvariant(format!("{label} overflow")))
    }

    /// # Errors
    /// Propagates division failure as an accounting-invariant error with the label.
    pub fn div(a: Decimal, b: Decimal, label: &str) -> Result<Decimal, LabError> {
        a.checked_div(b)
            .ok_or_else(|| LabError::AccountingInvariant(format!("{label} division failed")))
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_execution: Option<CausalExecutionPolicy>,
    pub decision_interval: CandleInterval,
    pub execution_resolution: CandleInterval,
    pub latency_ms: u64,
    pub initial_cash: QuoteAmount,
    pub costs: CostPolicy,
    pub execution: ExecutionPolicy,
    pub market_rules: MarketRuleSnapshot,
    /// Optional point-in-time rule history. When present it must exactly tile
    /// the evaluation range with adjacent, non-overlapping segments, and
    /// `market_rules` must equal the first segment so legacy readers keep a
    /// meaningful projection. Empty preserves the single-snapshot contract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub market_rules_history: Vec<MarketRuleSnapshot>,
    pub terminal_policy: TerminalPolicy,
    pub evidence_snapshot_id: Option<EvidenceSnapshotId>,
    pub pit_policy: PitPolicy,
    pub evidence_unavailable: EvidenceUnavailablePolicy,
    pub report_clock: ReportClock,
    pub seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CausalExecutionPolicy {
    /// Admit only the declared policy warmup and evaluation range into decision state.
    DeclaredPolicyWarmup,
}

impl ExperimentSpec {
    fn validate_selections(&self) -> Result<(), LabError> {
        let selections = match (
            self.schema_version.as_str(),
            self.causal_execution,
            self.strategies.is_empty(),
            self.policy_selections.is_empty(),
        ) {
            ("1.0", None, false, true) => self.strategies.len(),
            ("2.0", None, true, false)
            | ("3.0", Some(CausalExecutionPolicy::DeclaredPolicyWarmup), true, false) => {
                self.policy_selections.len()
            }
            _ => {
                return Err(LabError::InvalidConfig(
                    "experiment v1 requires legacy strategies without causal execution; v2 requires exact policy selections without causal execution; v3 requires exact policy selections with causal execution"
                        .into(),
                ));
            }
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
        if self.schema_version == "3.0" && self.pit_policy != PitPolicy::StrictPit {
            return Err(LabError::InvalidConfig(
                "experiment v3 causal execution requires STRICT_PIT".into(),
            ));
        }
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
        if let Some(dynamic) = &self.costs.dynamic {
            dynamic.validate()?;
        }
        if self.market_rules_history.is_empty() {
            self.market_rules.validate(self.range)?;
        } else {
            self.market_rules.validate(self.market_rules.valid_range)?;
            validate_rule_history(&self.market_rules, &self.market_rules_history, self.range)?;
        }
        if !matches!(self.report_clock.timezone.as_str(), "UTC" | "Asia/Seoul")
            || !self.report_clock.risk_free_annual.is_finite()
            || self.report_clock.min_annualization_days == 0
        {
            return Err(LabError::InvalidConfig("report clock requires UTC/Asia-Seoul, finite risk-free rate and positive annualization duration".into()));
        }
        Ok(())
    }

    /// Resolve the exact rule snapshot governing `at`.
    ///
    /// Single-snapshot specs always return `market_rules`; historical specs
    /// search the validated tiling. Admission already proved exact coverage,
    /// so an unresolved instant is an internal invariant breach.
    /// # Errors
    /// Returns [`LabError::Conflict`] when no unique segment covers `at`.
    pub fn rules_at(&self, at: UtcTimestamp) -> Result<&MarketRuleSnapshot, LabError> {
        if self.market_rules_history.is_empty() {
            return Ok(&self.market_rules);
        }
        let covering: Vec<&MarketRuleSnapshot> = self
            .market_rules_history
            .iter()
            .filter(|segment| segment.valid_range.contains(at))
            .collect();
        match covering.len() {
            1 => Ok(covering[0]),
            0 => {
                let last = self.market_rules_history.last().ok_or_else(|| {
                    LabError::Internal("validated rule history lost its last segment".into())
                })?;
                if last.valid_range.end() == at {
                    Ok(last)
                } else {
                    Err(LabError::Conflict(format!(
                        "no historical rule segment covers {at}"
                    )))
                }
            }
            _ => Err(LabError::Conflict(
                "overlapping historical rule segments cannot be resolved".into(),
            )),
        }
    }

    /// Effective taker/maker fees at `at`: verified historical fee schedules
    /// override the scenario-level [`CostPolicy`] fees for their segment.
    /// # Errors
    /// Propagates rule-resolution errors.
    pub fn fee_policy_at(&self, at: UtcTimestamp) -> Result<EffectiveFees, LabError> {
        let rules = self.rules_at(at)?;
        if let Some(schedule) = &rules.fee_schedule {
            return Ok(EffectiveFees {
                buy: schedule.taker_fee_bps,
                sell: schedule.taker_fee_bps,
                maker: schedule.maker_fee_bps,
            });
        }
        Ok(EffectiveFees {
            buy: self.costs.buy_fee_bps,
            sell: self.costs.sell_fee_bps,
            maker: self.costs.maker_fee_bps,
        })
    }
}

/// Fee rates in effect for one execution instant after PIT resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveFees {
    pub buy: BasisPoints,
    pub sell: BasisPoints,
    pub maker: BasisPoints,
}

/// Enforce exact adjacent tiling with no gaps, overlaps or reorderings.
/// # Errors
/// Rejects empty/oversized history, unsorted or conflicting segments, gaps and
/// a legacy projection that differs from the first segment.
fn validate_rule_history(
    legacy: &MarketRuleSnapshot,
    history: &[MarketRuleSnapshot],
    range: UtcRange,
) -> Result<(), LabError> {
    if history.is_empty() || history.len() > MAX_RULE_HISTORY_SEGMENTS {
        return Err(LabError::InvalidConfig(format!(
            "rule history requires 1..={MAX_RULE_HISTORY_SEGMENTS} segments"
        )));
    }
    for (index, pair) in history.windows(2).enumerate() {
        let (previous, next) = (pair[0].valid_range, pair[1].valid_range);
        if next.start() < previous.end() {
            return Err(LabError::Conflict(format!(
                "historical rule segments {index} and {} overlap or are unsorted",
                index + 1
            )));
        }
        if next.start() > previous.end() {
            return Err(LabError::UnverifiedMarketRules(
                "historical rule segments leave an uncovered gap".into(),
            ));
        }
    }
    if history[0].valid_range.start() > range.start()
        || history[history.len() - 1].valid_range.end() < range.end()
    {
        return Err(LabError::UnverifiedMarketRules(
            "historical rule segments do not cover the evaluation range".into(),
        ));
    }
    for segment in history {
        segment.validate(segment.valid_range)?;
    }
    if *legacy != history[0] {
        return Err(LabError::InvalidConfig(
            "market_rules must equal the first historical segment".into(),
        ));
    }
    Ok(())
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

    fn bps(value: &str) -> BasisPoints {
        BasisPoints::new(decimal(value)).expect("valid bps")
    }
    fn decimal(value: &str) -> Decimal {
        value.parse::<Decimal>().expect("valid decimal")
    }
    fn quote(value: &str) -> QuoteAmount {
        QuoteAmount::new(decimal(value)).expect("valid quote")
    }
    fn at(value: &str) -> UtcTimestamp {
        UtcTimestamp::parse_rfc3339(value).expect("valid rfc3339")
    }
    fn segment(id: &str, start: &str, end: &str) -> MarketRuleSnapshot {
        MarketRuleSnapshot {
            id: RuleSnapshotId::new(id).expect("valid rule id"),
            provenance: RuleProvenance::ExplicitScenario,
            valid_range: UtcRange::new(at(start), at(end)).expect("valid range"),
            observed_at: at(start),
            source_refs: vec!["synthetic://rule-history-test".into()],
            assumption_label: "rule history fixture".into(),
            min_notional: quote("1"),
            quantity_step: super::AssetQuantity::new(Decimal::new(1, 2)).expect("valid step"),
            ticks: vec![TickBand {
                lower_bound: quote("0"),
                tick: super::PriceKrw::new(Decimal::ONE).expect("valid tick"),
            }],
            fee_schedule: None,
            trading_state: None,
            maintenance_windows: Vec::new(),
        }
    }

    #[test]
    fn rule_history_requires_sorted_adjacent_tiles_and_matching_legacy_projection() {
        let legacy = segment("a", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z");
        let second = segment("b", "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z");
        let range = UtcRange::new(at("2025-01-01T00:00:00Z"), at("2025-01-03T00:00:00Z"))
            .expect("valid range");
        validate_rule_history(&legacy, &[legacy.clone(), second.clone()], range)
            .expect("exact tiling is valid");
        // Overlap and reversal are both conflicts.
        let overlapping = segment("c", "2025-01-01T12:00:00Z", "2025-01-03T00:00:00Z");
        assert!(matches!(
            validate_rule_history(&legacy, &[legacy.clone(), overlapping], range),
            Err(LabError::Conflict(_))
        ));
        // A coverage gap is never silently filled by the latest rule.
        let late = segment("d", "2025-01-02T06:00:00Z", "2025-01-03T00:00:00Z");
        assert!(matches!(
            validate_rule_history(&legacy, &[legacy.clone(), late], range),
            Err(LabError::UnverifiedMarketRules(_))
        ));
        // The legacy snapshot must equal the first segment.
        let other = segment("e", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z");
        assert!(matches!(
            validate_rule_history(&other, &[legacy, second], range),
            Err(LabError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rules_at_resolves_unique_segments_and_reports_conflicts() {
        let first = segment("a", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z");
        let second = segment("b", "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z");
        let mut spec = reference_spec_for_rules();
        spec.market_rules_history = vec![first, second];
        assert_eq!(
            spec.rules_at(at("2025-01-01T12:00:00Z"))
                .expect("covered instant")
                .id
                .as_str(),
            "a"
        );
        assert!(matches!(
            spec.rules_at(at("2025-01-03T12:00:00Z")),
            Err(LabError::Conflict(_))
        ));
    }

    /// Minimal spec shell so `rules_at` can run without a full experiment fixture.
    fn reference_spec_for_rules() -> ExperimentSpec {
        let range = UtcRange::new(at("2025-01-01T00:00:00Z"), at("2025-01-03T00:00:00Z"))
            .expect("valid range");
        let first = segment("a", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z");
        ExperimentSpec {
            schema_version: "1.0".into(),
            dataset_ids: Vec::new(),
            markets: Vec::new(),
            range,
            strategies: Vec::new(),
            policy_selections: Vec::new(),
            causal_execution: None,
            decision_interval: super::CandleInterval::H1,
            execution_resolution: super::CandleInterval::H1,
            latency_ms: 0,
            initial_cash: quote("1000"),
            costs: CostPolicy {
                buy_fee_bps: bps("10"),
                sell_fee_bps: bps("10"),
                maker_fee_bps: bps("5"),
                half_spread_bps: bps("0"),
                slippage_bps: bps("0"),
                impact_bps: bps("0"),
                assumption_label: "rules-at fixture".into(),
                dynamic: None,
            },
            execution: ExecutionPolicy::NextBarOpen {
                participation_cap: super::Weight::new(Decimal::ONE).expect("valid weight"),
            },
            market_rules: first,
            market_rules_history: Vec::new(),
            terminal_policy: TerminalPolicy::MarkToMarket,
            evidence_snapshot_id: None,
            pit_policy: PitPolicy::StrictPit,
            evidence_unavailable: EvidenceUnavailablePolicy::CashWithMatchedControl,
            report_clock: ReportClock {
                timezone: "UTC".into(),
                min_annualization_days: 1,
                risk_free_annual: 0.0,
            },
            seed: 1,
        }
    }

    #[test]
    fn verified_fee_schedule_overrides_scenario_fees_only_for_verified_provenance() {
        let mut spec = reference_spec_for_rules();
        let mut verified = segment("a", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z");
        verified.provenance = RuleProvenance::VerifiedHistorical;
        verified.fee_schedule = Some(HistoricalFeeSchedule {
            taker_fee_bps: bps("7"),
            maker_fee_bps: bps("3"),
            assumption_label: "verified schedule fixture".into(),
        });
        spec.market_rules = verified.clone();
        spec.market_rules_history = vec![verified];
        let fees = spec
            .fee_policy_at(at("2025-01-01T12:00:00Z"))
            .expect("verified schedule applies");
        assert_eq!(fees.buy, bps("7"));
        assert_eq!(fees.maker, bps("3"));
    }

    #[test]
    fn dynamic_cost_models_are_bounded_deterministic_and_proxy_only() {
        let inputs = |range: &str, turnover: &str, notional: &str| CostFeatureInputs {
            bar_range_fraction: decimal(range),
            quote_turnover: decimal(turnover),
            requested_notional: decimal(notional),
        };
        let volatility = DynamicCostModel::VolatilityAware {
            base_slippage_bps: bps("10"),
            range_weight: Decimal::from(5_000),
            max_slippage_bps: bps("200"),
        };
        volatility.validate().expect("valid volatility model");
        assert_eq!(
            volatility
                .effective_slippage_bps(inputs("0", "1000", "10"))
                .expect("zero range"),
            bps("10")
        );
        assert_eq!(
            volatility
                .effective_slippage_bps(inputs("1", "1000", "10"))
                .expect("huge range clamps at max"),
            bps("200")
        );
        // Determinism: identical inputs produce identical outputs.
        assert_eq!(
            volatility
                .effective_slippage_bps(inputs("0.5", "1000", "10"))
                .expect("mid range"),
            volatility
                .effective_slippage_bps(inputs("0.5", "1000", "10"))
                .expect("repeat")
        );
        let turnover = DynamicCostModel::TurnoverAware {
            base_slippage_bps: bps("10"),
            turnover_floor: quote("1000"),
            max_excess_bps: bps("90"),
        };
        turnover.validate().expect("valid turnover model");
        assert_eq!(
            turnover
                .effective_slippage_bps(inputs("0", "0", "10"))
                .expect("zero turnover is the bounded worst case"),
            bps("100")
        );
        assert_eq!(
            turnover
                .effective_slippage_bps(inputs("0", "5000", "10"))
                .expect("above floor adds nothing"),
            bps("10")
        );
        let participation = DynamicCostModel::ParticipationAware {
            base_slippage_bps: bps("10"),
            participation_weight: Decimal::from(1_000),
            max_slippage_bps: bps("150"),
        };
        participation.validate().expect("valid participation model");
        assert_eq!(
            participation
                .effective_slippage_bps(inputs("0", "0", "10"))
                .expect("zero liquidity is the bounded worst case"),
            bps("150")
        );
        // participation = 0.25 -> sqrt = 0.5 -> component 500bps -> clamped to 150.
        assert_eq!(
            participation
                .effective_slippage_bps(inputs("0", "40", "10"))
                .expect("high participation clamps"),
            bps("150")
        );
        assert!(matches!(
            DynamicCostModel::TurnoverAware {
                base_slippage_bps: bps("10"),
                turnover_floor: quote("0"),
                max_excess_bps: bps("90"),
            }
            .validate(),
            Err(LabError::InvalidConfig(_))
        ));
    }

    #[test]
    fn maintenance_windows_and_suspension_block_tradability() {
        let mut rules = segment("a", "2025-01-01T00:00:00Z", "2025-01-03T00:00:00Z");
        assert!(rules.tradable_at(at("2025-01-01T12:00:00Z")));
        rules.trading_state = Some(MarketTradingState::Suspended);
        assert!(!rules.tradable_at(at("2025-01-01T12:00:00Z")));
        rules.trading_state = None;
        rules.maintenance_windows = vec![
            UtcRange::new(at("2025-01-01T11:00:00Z"), at("2025-01-01T13:00:00Z"))
                .expect("valid window"),
        ];
        assert!(!rules.tradable_at(at("2025-01-01T12:00:00Z")));
        assert!(rules.tradable_at(at("2025-01-01T14:00:00Z")));
    }
}

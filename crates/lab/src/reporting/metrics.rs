//! Typed, fact-only review projections and F08 metric definitions.

#![allow(
    clippy::cast_precision_loss,
    reason = "report statistics intentionally convert bounded counts and elapsed times to f64"
)]

use crate::contracts::{
    ContentHash, EpisodeExitDetails, EpisodeRecord, EpisodeStatus, FillId, LabError, ModelId,
    ModelLedger, ModelStatus, PriceCostAttributionUnit, QuoteAmount, ReasonCode, RunBundle, RunId,
    SignalId, SignalOutcome, SignedAmount, StrategyKind, UtcTimestamp,
};
use chrono::Datelike;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_SELECTED_SIGNALS: usize = 100;
const SECONDS_PER_YEAR: f64 = 365.2425 * 86_400.0;
const REVIEW_SCHEMA_VERSION: &str = "spot-lab-review-v2";
const PRICE_COST_SCHEMA_VERSION: &str = "quantity-weighted-quote-v2";

fn legacy_price_cost_schema() -> String {
    "legacy-unweighted-per-unit-sum-v1".into()
}

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
    fn value(value: f64) -> Self {
        if value.is_finite() {
            Self {
                value: Some(value),
                null_reason: None,
            }
        } else {
            Self::null(NullReason::NonFiniteResult)
        }
    }

    const fn null(reason: NullReason) -> Self {
        Self {
            value: None,
            null_reason: Some(reason),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetricDefinition {
    pub metric: String,
    pub definition: String,
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuantileMetrics {
    pub p05: MetricValue,
    pub p25: MetricValue,
    pub p50: MetricValue,
    pub p75: MetricValue,
    pub p95: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DrawdownPeriod {
    pub peak_at: UtcTimestamp,
    pub trough_at: UtcTimestamp,
    pub recovered_at: Option<UtcTimestamp>,
    pub censored: bool,
    pub drawdown_fraction: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CalendarReturn {
    pub period: String,
    pub opening_equity: QuoteAmount,
    pub closing_equity: QuoteAmount,
    pub return_fraction: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DailySlice {
    pub utc_date: String,
    pub opening_cash: QuoteAmount,
    pub closing_cash: QuoteAmount,
    pub opening_qty: crate::contracts::AssetQuantity,
    pub closing_qty: crate::contracts::AssetQuantity,
    pub opening_equity: QuoteAmount,
    pub closing_equity: QuoteAmount,
    pub equity_delta: SignedAmount,
    pub cumulative_fees_open: QuoteAmount,
    pub cumulative_fees_close: QuoteAmount,
    pub fees_delta: QuoteAmount,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegimeEpisodeMetrics {
    pub regime_label: String,
    pub closed_episodes: u64,
    pub net_realized: SignedAmount,
    pub win_rate: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActiveRegimeMetrics {
    pub regime_label: String,
    pub sampled_intervals: u64,
    pub sampled_seconds: u64,
    pub compounded_account_return: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostMetrics {
    pub cumulative_fees: QuoteAmount,
    pub gross_price_pnl: SignedAmount,
    pub net_pnl: SignedAmount,
    #[serde(default = "legacy_price_cost_schema")]
    pub price_cost_schema_version: String,
    #[serde(default)]
    pub embedded_price_cost_attribution_unit: PriceCostAttributionUnit,
    pub embedded_price_cost_attribution: SignedAmount,
    pub fees_over_positive_gross: MetricValue,
    pub shadow_zero_cost_return: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EpisodeMetrics {
    pub closed_count: u64,
    pub open_count: u64,
    pub winning_count: u64,
    pub losing_count: u64,
    pub break_even_count: u64,
    pub profit_factor: MetricValue,
    pub win_rate: MetricValue,
    pub net_realized_mean: MetricValue,
    pub net_realized_quantiles: QuantileMetrics,
    pub holding_seconds_quantiles: QuantileMetrics,
    pub mae_fraction_quantiles: QuantileMetrics,
    pub mfe_fraction_quantiles: QuantileMetrics,
    pub longest_winning_streak: u64,
    pub longest_losing_streak: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EquityMetrics {
    pub initial_equity: Option<QuoteAmount>,
    pub final_equity: Option<QuoteAmount>,
    pub total_return: MetricValue,
    pub cagr: MetricValue,
    pub sharpe: MetricValue,
    pub sortino: MetricValue,
    pub max_drawdown: MetricValue,
    pub exposure: MetricValue,
    pub turnover: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConcentrationMetric {
    pub top_k: u32,
    pub positive_profit_fraction: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EpisodeSummary {
    pub episode_id: crate::contracts::EpisodeId,
    pub status: EpisodeStatus,
    pub opened_at: UtcTimestamp,
    pub closed_at: Option<UtcTimestamp>,
    pub holding_seconds: u64,
    pub fill_ids: Vec<FillId>,
    pub realized_price_pnl: SignedAmount,
    pub fees: QuoteAmount,
    pub net_realized: SignedAmount,
    pub residual_qty: crate::contracts::AssetQuantity,
    pub marked_unrealized: SignedAmount,
    pub mae_amount: SignedAmount,
    pub mfe_amount: SignedAmount,
    /// Historical field; production facts contain the execution result here.
    pub exit_reason: Option<ReasonCode>,
    #[serde(default)]
    pub exit_details: EpisodeExitDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectedSignal {
    pub signal_id: SignalId,
    pub event_seq: u64,
    pub at: UtcTimestamp,
    pub outcome: SignalOutcome,
    pub reasons: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignalSelection {
    pub rule: String,
    pub total_count: u64,
    pub selected_count: u64,
    pub omitted_count: u64,
    pub signals: Vec<SelectedSignal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelReview {
    pub model_id: ModelId,
    pub market: crate::contracts::MarketId,
    pub strategy: StrategyKind,
    pub status: ModelStatus,
    pub status_reason: Option<String>,
    pub equity: EquityMetrics,
    pub episodes: EpisodeMetrics,
    pub episode_summaries: Vec<EpisodeSummary>,
    pub signal_selection: SignalSelection,
    pub drawdown_periods: Vec<DrawdownPeriod>,
    pub monthly_returns: Vec<CalendarReturn>,
    pub quarterly_returns: Vec<CalendarReturn>,
    pub yearly_returns: Vec<CalendarReturn>,
    pub entry_regime_closed_episodes: Vec<RegimeEpisodeMetrics>,
    pub active_regime_returns: Vec<ActiveRegimeMetrics>,
    pub active_regime_null_reason: Option<NullReason>,
    pub costs: CostMetrics,
    pub positive_pnl_concentration: Vec<ConcentrationMetric>,
    pub daily_slices: Vec<DailySlice>,
}

/// F08 review artifact. It contains facts and definitions, never generated narration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewPayload {
    pub schema_version: String,
    pub run_id: RunId,
    pub input_digest: ContentHash,
    pub semantic_digest: ContentHash,
    pub definitions: Vec<MetricDefinition>,
    pub models: Vec<ModelReview>,
    pub warnings: Vec<String>,
}

/// Build the portable review projection without reading clocks or external state.
///
/// # Errors
/// Returns an accounting error if exact daily or aggregate deltas violate ledger invariants.
pub fn build_review(bundle: &RunBundle) -> Result<ReviewPayload, LabError> {
    let models = bundle
        .models
        .iter()
        .map(|model| model_review(bundle, model))
        .collect::<Result<_, _>>()?;
    Ok(ReviewPayload {
        schema_version: REVIEW_SCHEMA_VERSION.into(),
        run_id: bundle.manifest.run_id.clone(),
        input_digest: bundle.plan.input_digest.clone(),
        semantic_digest: bundle.semantic_digest.clone(),
        definitions: definitions(),
        models,
        warnings: bundle.plan.warnings.clone(),
    })
}

fn model_review(bundle: &RunBundle, model: &ModelLedger) -> Result<ModelReview, LabError> {
    let closed: Vec<_> = model
        .episodes
        .iter()
        .filter(|episode| episode.status == EpisodeStatus::Closed)
        .collect();
    let open_count = model.episodes.len().saturating_sub(closed.len());
    let episode_values: Vec<_> = closed
        .iter()
        .filter_map(|episode| decimal_f64(episode.net_realized.get()))
        .collect();
    let holding: Vec<_> = closed
        .iter()
        .map(|episode| episode.holding_seconds as f64)
        .collect();
    let mae: Vec<_> = closed
        .iter()
        .filter_map(|episode| decimal_f64(episode.mae_pct_of_start_equity.get()))
        .collect();
    let mfe: Vec<_> = closed
        .iter()
        .filter_map(|episode| decimal_f64(episode.mfe_pct_of_start_equity.get()))
        .collect();
    let wins = episode_values.iter().filter(|value| **value > 0.0).count();
    let losses = episode_values.iter().filter(|value| **value < 0.0).count();
    let break_even = episode_values.len().saturating_sub(wins + losses);
    let gross_profit: f64 = episode_values
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .sum();
    let gross_loss: f64 = episode_values
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .map(f64::abs)
        .sum();
    let (win_streak, loss_streak) = streaks(&episode_values);
    let episode_metrics = EpisodeMetrics {
        closed_count: usize_u64(closed.len()),
        open_count: usize_u64(open_count),
        winning_count: usize_u64(wins),
        losing_count: usize_u64(losses),
        break_even_count: usize_u64(break_even),
        profit_factor: if closed.is_empty() {
            MetricValue::null(NullReason::NoClosedEpisodes)
        } else if gross_loss == 0.0 {
            MetricValue::null(NullReason::NoLosingEpisodes)
        } else {
            MetricValue::value(gross_profit / gross_loss)
        },
        win_rate: ratio(wins, closed.len(), NullReason::NoClosedEpisodes),
        net_realized_mean: mean(&episode_values, NullReason::NoClosedEpisodes),
        net_realized_quantiles: quantiles(&episode_values, NullReason::NoClosedEpisodes),
        holding_seconds_quantiles: quantiles(&holding, NullReason::NoClosedEpisodes),
        mae_fraction_quantiles: quantiles(&mae, NullReason::NoClosedEpisodes),
        mfe_fraction_quantiles: quantiles(&mfe, NullReason::NoClosedEpisodes),
        longest_winning_streak: win_streak,
        longest_losing_streak: loss_streak,
    };

    let active_regimes = active_regime_metrics(bundle, model);
    Ok(ModelReview {
        model_id: model.model_id.clone(),
        market: model.market.clone(),
        strategy: model.strategy.kind(),
        status: model.status,
        status_reason: model.status_reason.clone(),
        equity: equity_metrics(bundle, model),
        episodes: episode_metrics,
        episode_summaries: episode_summaries(model)?,
        signal_selection: select_signals(model),
        drawdown_periods: drawdown_periods(model),
        monthly_returns: calendar_returns(model, CalendarKind::Month),
        quarterly_returns: calendar_returns(model, CalendarKind::Quarter),
        yearly_returns: calendar_returns(model, CalendarKind::Year),
        entry_regime_closed_episodes: regime_metrics(bundle, model, &closed)?,
        active_regime_null_reason: active_regimes
            .is_empty()
            .then_some(NullReason::DataNotCaptured),
        active_regime_returns: active_regimes,
        costs: cost_metrics(model)?,
        positive_pnl_concentration: concentration(&episode_values),
        daily_slices: daily_slices(model)?,
    })
}

fn episode_summaries(model: &ModelLedger) -> Result<Vec<EpisodeSummary>, LabError> {
    let fills: BTreeMap<_, _> = model
        .fills
        .iter()
        .map(|fill| (&fill.fill_id, fill))
        .collect();
    let orders: BTreeMap<_, _> = model
        .orders
        .iter()
        .map(|order| (&order.order_id, order))
        .collect();
    let signals: BTreeMap<_, _> = model
        .signals
        .iter()
        .map(|signal| (&signal.signal_id, signal))
        .collect();
    model
        .episodes
        .iter()
        .map(|episode| episode_summary(episode, &fills, &orders, &signals))
        .collect()
}

fn definitions() -> Vec<MetricDefinition> {
    [
        ("total_return", "final marked equity / initial equity - 1", "fraction"),
        ("cagr", "equity ratio annualized over elapsed UTC calendar seconds; null below configured minimum days", "fraction_per_year"),
        ("sharpe", "mean mark-to-mark return minus periodized configured risk-free rate, divided by sample standard deviation and annualized", "ratio"),
        ("sortino", "mean mark-to-mark excess return divided by root-mean-square negative return and annualized", "ratio"),
        ("max_drawdown", "largest peak-to-later-trough loss on the sampled account-equity path", "fraction"),
        ("profit_factor", "sum positive CLOSED episode net realized / absolute sum negative CLOSED episode net realized", "ratio"),
        ("win_rate", "positive-net CLOSED episodes / all CLOSED episodes; break-even remains in denominator", "fraction"),
        ("exposure", "time-weighted sampled position value / equity", "fraction"),
        ("turnover", "sum both-side fill notional / time-weighted average sampled equity", "ratio"),
        ("embedded_price_cost_attribution", "sum of each immutable signed KRW-per-base-unit fill price delta multiplied by that fill's base quantity; diagnostic already embedded in fill price and never debited again", "KRW"),
        ("mae_fraction", "episode sampled minimum net-PnL path / equity immediately before episode", "fraction"),
        ("mfe_fraction", "episode sampled maximum net-PnL path / equity immediately before episode", "fraction"),
        ("positive_pnl_concentration", "top-k positive CLOSED episode net profits / total positive CLOSED episode net profits", "fraction"),
        ("entry_regime_closed_episode", "CLOSED episode facts grouped by the regime label linked through its entry fill's parent signal evidence effect; descriptive, not causal attribution", "group"),
        ("active_regime_return", "compounded account mark-to-mark return for intervals assigned to regime labels through the latest prior recorded signal evidence effect", "fraction"),
    ].into_iter().map(|(metric, definition, unit)| MetricDefinition {
        metric: metric.into(), definition: definition.into(), unit: unit.into(),
    }).collect()
}

fn equity_metrics(bundle: &RunBundle, model: &ModelLedger) -> EquityMetrics {
    let Some(first) = model.account_marks.first() else {
        return EquityMetrics {
            initial_equity: None,
            final_equity: None,
            total_return: MetricValue::null(NullReason::NoAccountMarks),
            cagr: MetricValue::null(NullReason::NoAccountMarks),
            sharpe: MetricValue::null(NullReason::NoAccountMarks),
            sortino: MetricValue::null(NullReason::NoAccountMarks),
            max_drawdown: MetricValue::null(NullReason::NoAccountMarks),
            exposure: MetricValue::null(NullReason::NoAccountMarks),
            turnover: MetricValue::null(NullReason::NoAccountMarks),
        };
    };
    let last = model.account_marks.last().unwrap_or(first);
    let initial = decimal_f64(first.equity.get());
    let final_value = decimal_f64(last.equity.get());
    let total_return = match (initial, final_value) {
        (Some(start), Some(end)) if start > 0.0 => MetricValue::value(end / start - 1.0),
        _ => MetricValue::null(NullReason::ZeroDenominator),
    };
    let elapsed = (last.context.accounting_event_time.0 - first.context.accounting_event_time.0)
        .num_milliseconds() as f64
        / 1000.0;
    let min_elapsed = f64::from(bundle.plan.spec.report_clock.min_annualization_days) * 86_400.0;
    let cagr = match (initial, final_value) {
        (Some(start), Some(end)) if elapsed >= min_elapsed && start > 0.0 && end >= 0.0 => {
            MetricValue::value((end / start).powf(SECONDS_PER_YEAR / elapsed) - 1.0)
        }
        _ if elapsed < min_elapsed => MetricValue::null(NullReason::InsufficientCalendarDuration),
        _ => MetricValue::null(NullReason::ZeroDenominator),
    };
    let returns = mark_returns(model);
    let periods_per_year = annual_periods(model);
    let rf_period =
        bundle.plan.spec.report_clock.risk_free_annual / periods_per_year.unwrap_or(1.0);
    let sharpe = risk_ratio(&returns, rf_period, periods_per_year, false);
    let sortino = risk_ratio(&returns, rf_period, periods_per_year, true);
    EquityMetrics {
        initial_equity: Some(first.equity),
        final_equity: Some(last.equity),
        total_return,
        cagr,
        sharpe,
        sortino,
        max_drawdown: maximum_drawdown(model),
        exposure: exposure(model),
        turnover: turnover(model),
    }
}

fn risk_ratio(
    returns: &[f64],
    rf_period: f64,
    periods_per_year: Option<f64>,
    downside: bool,
) -> MetricValue {
    if returns.len() < 2 {
        return MetricValue::null(NullReason::InsufficientSamples);
    }
    let Some(annual) = periods_per_year else {
        return MetricValue::null(NullReason::InsufficientSamples);
    };
    let excess: Vec<_> = returns.iter().map(|value| value - rf_period).collect();
    let Some(avg) = average(&excess) else {
        return MetricValue::null(NullReason::InsufficientSamples);
    };
    let denominator = if downside {
        let sum: f64 = excess
            .iter()
            .filter(|value| **value < 0.0)
            .map(|value| value * value)
            .sum();
        (sum / excess.len() as f64).sqrt()
    } else {
        sample_std_dev(&excess, avg)
    };
    if denominator == 0.0 {
        MetricValue::null(NullReason::ZeroDenominator)
    } else {
        MetricValue::value(avg / denominator * annual.sqrt())
    }
}

fn mark_returns(model: &ModelLedger) -> Vec<f64> {
    model
        .account_marks
        .windows(2)
        .filter_map(|pair| {
            let before = decimal_f64(pair[0].equity.get())?;
            let after = decimal_f64(pair[1].equity.get())?;
            (before > 0.0).then_some(after / before - 1.0)
        })
        .collect()
}

fn annual_periods(model: &ModelLedger) -> Option<f64> {
    let durations: Vec<_> = model
        .account_marks
        .windows(2)
        .map(|pair| {
            (pair[1].context.accounting_event_time.0 - pair[0].context.accounting_event_time.0)
                .num_milliseconds() as f64
                / 1000.0
        })
        .filter(|duration| *duration > 0.0)
        .collect();
    average(&durations).map(|average_seconds| SECONDS_PER_YEAR / average_seconds)
}

fn maximum_drawdown(model: &ModelLedger) -> MetricValue {
    let mut peak = None::<f64>;
    let mut maximum = 0.0_f64;
    for mark in &model.account_marks {
        let Some(equity) = decimal_f64(mark.equity.get()) else {
            return MetricValue::null(NullReason::NonFiniteResult);
        };
        peak = Some(peak.map_or(equity, |value| value.max(equity)));
        if let Some(high) = peak.filter(|value| *value > 0.0) {
            maximum = maximum.max((high - equity) / high);
        }
    }
    if peak.is_none() {
        MetricValue::null(NullReason::NoAccountMarks)
    } else {
        MetricValue::value(maximum)
    }
}

fn exposure(model: &ModelLedger) -> MetricValue {
    weighted_mark_average(model, |mark| {
        let equity = decimal_f64(mark.equity.get())?;
        let position = decimal_f64(mark.position_value.get())?;
        (equity > 0.0).then_some(position / equity)
    })
}

fn turnover(model: &ModelLedger) -> MetricValue {
    let Some(notional) = checked_decimal_sum(model.fills.iter().map(|fill| fill.notional.get()))
    else {
        return MetricValue::null(NullReason::NonFiniteResult);
    };
    let average_equity = weighted_mark_average(model, |mark| decimal_f64(mark.equity.get()));
    match (decimal_f64(notional), average_equity.value) {
        (Some(numerator), Some(denominator)) if denominator > 0.0 => {
            MetricValue::value(numerator / denominator)
        }
        _ => MetricValue::null(NullReason::ZeroDenominator),
    }
}

fn weighted_mark_average(
    model: &ModelLedger,
    value: impl Fn(&crate::contracts::AccountMark) -> Option<f64>,
) -> MetricValue {
    if model.account_marks.is_empty() {
        return MetricValue::null(NullReason::NoAccountMarks);
    }
    if model.account_marks.len() == 1 {
        return value(&model.account_marks[0]).map_or_else(
            || MetricValue::null(NullReason::ZeroDenominator),
            MetricValue::value,
        );
    }
    let mut weighted = 0.0;
    let mut total_seconds = 0.0;
    for pair in model.account_marks.windows(2) {
        let seconds = (pair[1].context.accounting_event_time.0
            - pair[0].context.accounting_event_time.0)
            .num_milliseconds() as f64
            / 1000.0;
        if seconds > 0.0 {
            let Some(sample) = value(&pair[0]) else {
                return MetricValue::null(NullReason::ZeroDenominator);
            };
            weighted += sample * seconds;
            total_seconds += seconds;
        }
    }
    if total_seconds == 0.0 {
        MetricValue::null(NullReason::InsufficientSamples)
    } else {
        MetricValue::value(weighted / total_seconds)
    }
}

fn drawdown_periods(model: &ModelLedger) -> Vec<DrawdownPeriod> {
    let mut periods = Vec::new();
    let mut peak_index = None;
    let mut trough_index = None;
    let mut peak_equity = Decimal::ZERO;
    for (index, mark) in model.account_marks.iter().enumerate() {
        let equity = mark.equity.get();
        if equity >= peak_equity {
            if let (Some(peak), Some(trough)) = (peak_index, trough_index) {
                periods.push(drawdown(model, peak, trough, Some(index)));
            }
            peak_equity = equity;
            peak_index = Some(index);
            trough_index = None;
        } else if trough_index
            .is_none_or(|trough| equity < model.account_marks[trough].equity.get())
        {
            trough_index = Some(index);
        }
    }
    if let (Some(peak), Some(trough)) = (peak_index, trough_index) {
        periods.push(drawdown(model, peak, trough, None));
    }
    periods
}

fn drawdown(
    model: &ModelLedger,
    peak: usize,
    trough: usize,
    recovery: Option<usize>,
) -> DrawdownPeriod {
    let peak_mark = &model.account_marks[peak];
    let trough_mark = &model.account_marks[trough];
    let fraction = peak_mark
        .equity
        .get()
        .checked_sub(trough_mark.equity.get())
        .map_or_else(
            || MetricValue::null(NullReason::NonFiniteResult),
            |loss| ratio_decimal(loss, peak_mark.equity.get()),
        );
    DrawdownPeriod {
        peak_at: peak_mark.context.accounting_event_time,
        trough_at: trough_mark.context.accounting_event_time,
        recovered_at: recovery
            .map(|index| model.account_marks[index].context.accounting_event_time),
        censored: recovery.is_none(),
        drawdown_fraction: fraction,
    }
}

#[derive(Clone, Copy)]
enum CalendarKind {
    Month,
    Quarter,
    Year,
}

fn calendar_returns(model: &ModelLedger, kind: CalendarKind) -> Vec<CalendarReturn> {
    let mut groups: BTreeMap<String, Vec<&crate::contracts::AccountMark>> = BTreeMap::new();
    for mark in &model.account_marks {
        let date = mark.context.accounting_event_time.0;
        let key = match kind {
            CalendarKind::Month => date.format("%Y-%m").to_string(),
            CalendarKind::Quarter => {
                format!("{}-Q{}", date.format("%Y"), date.month0() / 3 + 1)
            }
            CalendarKind::Year => date.format("%Y").to_string(),
        };
        groups.entry(key).or_default().push(mark);
    }
    groups
        .into_iter()
        .filter_map(|(period, marks)| {
            let first = marks.first()?;
            let last = marks.last()?;
            Some(CalendarReturn {
                period,
                opening_equity: first.equity,
                closing_equity: last.equity,
                return_fraction: ratio_decimal(
                    last.equity.get() - first.equity.get(),
                    first.equity.get(),
                ),
            })
        })
        .collect()
}

fn daily_slices(model: &ModelLedger) -> Result<Vec<DailySlice>, LabError> {
    let mut groups: BTreeMap<String, Vec<&crate::contracts::AccountMark>> = BTreeMap::new();
    for mark in &model.account_marks {
        groups
            .entry(
                mark.context
                    .accounting_event_time
                    .0
                    .format("%Y-%m-%d")
                    .to_string(),
            )
            .or_default()
            .push(mark);
    }
    groups
        .into_iter()
        .map(|(utc_date, marks)| {
            let first = marks
                .first()
                .ok_or_else(|| LabError::Internal("empty daily mark group".into()))?;
            let last = marks
                .last()
                .ok_or_else(|| LabError::Internal("empty daily mark group".into()))?;
            let fees_delta = last
                .state
                .cumulative_fees
                .get()
                .checked_sub(first.state.cumulative_fees.get())
                .ok_or_else(|| {
                    LabError::AccountingInvariant("cumulative fees decreased within a day".into())
                })?;
            Ok(DailySlice {
                utc_date,
                opening_cash: first.state.cash_total,
                closing_cash: last.state.cash_total,
                opening_qty: first.state.qty,
                closing_qty: last.state.qty,
                opening_equity: first.equity,
                closing_equity: last.equity,
                equity_delta: SignedAmount::new(last.equity.get() - first.equity.get())?,
                cumulative_fees_open: first.state.cumulative_fees,
                cumulative_fees_close: last.state.cumulative_fees,
                fees_delta: QuoteAmount::new(fees_delta)?,
            })
        })
        .collect()
}

fn regime_metrics(
    bundle: &RunBundle,
    model: &ModelLedger,
    closed: &[&EpisodeRecord],
) -> Result<Vec<RegimeEpisodeMetrics>, LabError> {
    let Some(evidence) = &bundle.evidence else {
        return Ok(Vec::new());
    };
    let labels: BTreeMap<_, _> = evidence
        .versions
        .iter()
        .filter_map(|version| {
            version
                .regime_label
                .as_ref()
                .map(|label| (version.revision_id.as_str(), label.as_str()))
        })
        .collect();
    let fills: BTreeMap<_, _> = model
        .fills
        .iter()
        .map(|fill| (&fill.fill_id, fill))
        .collect();
    let orders: BTreeMap<_, _> = model
        .orders
        .iter()
        .map(|order| (&order.order_id, order))
        .collect();
    let signals: BTreeMap<_, _> = model
        .signals
        .iter()
        .map(|signal| (&signal.signal_id, signal))
        .collect();
    let mut grouped: BTreeMap<String, Vec<Decimal>> = BTreeMap::new();
    for episode in closed {
        let label = episode
            .fill_ids
            .first()
            .and_then(|fill_id| fills.get(fill_id))
            .and_then(|fill| orders.get(&fill.order_id))
            .and_then(|order| signals.get(&order.parent_signal_id))
            .and_then(|signal| signal.evidence_effect.as_ref())
            .and_then(|effect| effect.eligible.first())
            .and_then(|eligible| labels.get(eligible.revision_id.as_str()))
            .copied()
            .unwrap_or("UNLABELED");
        grouped
            .entry(label.to_owned())
            .or_default()
            .push(episode.net_realized.get());
    }
    grouped
        .into_iter()
        .map(|(regime_label, values)| {
            let wins = values
                .iter()
                .filter(|value| **value > Decimal::ZERO)
                .count();
            let total = values.len();
            Ok(RegimeEpisodeMetrics {
                regime_label,
                closed_episodes: usize_u64(total),
                net_realized: SignedAmount::new(checked_decimal_sum(values).ok_or_else(|| {
                    LabError::AccountingInvariant("regime PnL sum overflow".into())
                })?)?,
                win_rate: ratio(wins, total, NullReason::NoClosedEpisodes),
            })
        })
        .collect()
}

fn active_regime_metrics(bundle: &RunBundle, model: &ModelLedger) -> Vec<ActiveRegimeMetrics> {
    let Some(evidence) = &bundle.evidence else {
        return Vec::new();
    };
    let labels: BTreeMap<_, _> = evidence
        .versions
        .iter()
        .filter_map(|version| {
            version
                .regime_label
                .as_ref()
                .map(|label| (version.revision_id.as_str(), label.as_str()))
        })
        .collect();
    let mut grouped: BTreeMap<String, (u64, u64, f64)> = BTreeMap::new();
    let mut signals: Vec<_> = model.signals.iter().collect();
    signals.sort_by_key(|signal| signal.context.event_seq);
    let mut signal_index = 0;
    let mut active_signal = None;
    for pair in model.account_marks.windows(2) {
        while signal_index < signals.len()
            && signals[signal_index].context.event_seq <= pair[0].context.event_seq
        {
            active_signal = Some(signals[signal_index]);
            signal_index += 1;
        }
        let Some(signal) = active_signal else {
            continue;
        };
        let Some(effect) = signal.evidence_effect.as_ref() else {
            continue;
        };
        let active_labels: BTreeSet<_> = effect
            .eligible
            .iter()
            .filter_map(|eligible| labels.get(eligible.revision_id.as_str()).copied())
            .collect();
        if active_labels.is_empty() {
            continue;
        }
        let Some(opening) = decimal_f64(pair[0].equity.get()).filter(|value| *value > 0.0) else {
            continue;
        };
        let Some(closing) = decimal_f64(pair[1].equity.get()) else {
            continue;
        };
        let growth = closing / opening;
        if !growth.is_finite() || growth < 0.0 {
            continue;
        }
        let Ok(seconds) = u64::try_from(
            (pair[1].context.accounting_event_time.0 - pair[0].context.accounting_event_time.0)
                .num_seconds(),
        ) else {
            continue;
        };
        for label in active_labels {
            let entry = grouped.entry(label.to_owned()).or_insert((0, 0, 1.0));
            entry.0 = entry.0.saturating_add(1);
            entry.1 = entry.1.saturating_add(seconds);
            entry.2 *= growth;
        }
    }
    grouped
        .into_iter()
        .map(
            |(regime_label, (sampled_intervals, sampled_seconds, growth))| ActiveRegimeMetrics {
                regime_label,
                sampled_intervals,
                sampled_seconds,
                compounded_account_return: MetricValue::value(growth - 1.0),
            },
        )
        .collect()
}

fn cost_metrics(model: &ModelLedger) -> Result<CostMetrics, LabError> {
    let fees = checked_decimal_sum(model.fills.iter().map(|fill| fill.fee.get()))
        .ok_or_else(|| LabError::AccountingInvariant("fee sum overflow".into()))?;
    let gross = model
        .episodes
        .iter()
        .try_fold(Decimal::ZERO, |sum, episode| {
            episode
                .realized_price_pnl
                .get()
                .checked_add(episode.marked_unrealized.get())
                .and_then(|component| sum.checked_add(component))
        })
        .ok_or_else(|| LabError::AccountingInvariant("gross PnL sum overflow".into()))?;
    let net = gross
        .checked_sub(fees)
        .ok_or_else(|| LabError::AccountingInvariant("net PnL overflow".into()))?;
    let attribution = model.fills.iter().try_fold(Decimal::ZERO, |sum, fill| {
        crate::reporting::fill_price_cost_quote(fill)?
            .get()
            .checked_add(sum)
            .ok_or_else(|| LabError::ResourceLimit("price attribution quote sum overflow".into()))
    })?;
    Ok(CostMetrics {
        cumulative_fees: QuoteAmount::new(fees)?,
        gross_price_pnl: SignedAmount::new(gross)?,
        net_pnl: SignedAmount::new(net)?,
        price_cost_schema_version: PRICE_COST_SCHEMA_VERSION.into(),
        embedded_price_cost_attribution_unit: PriceCostAttributionUnit::Krw,
        embedded_price_cost_attribution: SignedAmount::new(attribution)?,
        fees_over_positive_gross: if gross > Decimal::ZERO {
            ratio_decimal(fees, gross)
        } else {
            MetricValue::null(NullReason::ZeroDenominator)
        },
        shadow_zero_cost_return: MetricValue::null(NullReason::NotApplicable),
    })
}

fn concentration(values: &[f64]) -> Vec<ConcentrationMetric> {
    let mut positive: Vec<_> = values
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .collect();
    positive.sort_by(|left, right| right.total_cmp(left));
    let total: f64 = positive.iter().sum();
    [1_u32, 3, 5]
        .into_iter()
        .map(|top_k| {
            let numerator: f64 = positive.iter().take(top_k as usize).sum();
            ConcentrationMetric {
                top_k,
                positive_profit_fraction: if total > 0.0 {
                    MetricValue::value(numerator / total)
                } else {
                    MetricValue::null(NullReason::NoPositiveProfit)
                },
            }
        })
        .collect()
}

fn episode_summary(
    episode: &EpisodeRecord,
    fills: &BTreeMap<&FillId, &crate::contracts::FillRecord>,
    orders: &BTreeMap<&crate::contracts::OrderId, &crate::contracts::OrderRecord>,
    signals: &BTreeMap<&SignalId, &crate::contracts::SignalRecord>,
) -> Result<EpisodeSummary, LabError> {
    let closing = if episode.status == EpisodeStatus::Closed {
        let fill = episode
            .fill_ids
            .last()
            .and_then(|id| fills.get(id))
            .copied();
        let order = fill.and_then(|fill| orders.get(&fill.order_id)).copied();
        let signal = order
            .and_then(|order| signals.get(&order.parent_signal_id))
            .copied();
        match (fill, order, signal) {
            (Some(fill), Some(order), Some(signal)) => Some((fill, order, signal)),
            _ => None,
        }
    } else {
        None
    };
    let exit_details = crate::reporting::episode_exit_details(episode, closing)?;
    Ok(EpisodeSummary {
        episode_id: episode.episode_id.clone(),
        status: episode.status,
        opened_at: episode.opened_at,
        closed_at: episode.closed_at,
        holding_seconds: episode.holding_seconds,
        fill_ids: episode.fill_ids.clone(),
        realized_price_pnl: episode.realized_price_pnl,
        fees: episode.fees,
        net_realized: episode.net_realized,
        residual_qty: episode.residual_qty,
        marked_unrealized: episode.marked_unrealized,
        mae_amount: episode.mae_amount,
        mfe_amount: episode.mfe_amount,
        exit_reason: episode.exit_reason,
        exit_details,
    })
}

fn select_signals(model: &ModelLedger) -> SignalSelection {
    let mut selected = Vec::new();
    let mut ids = BTreeSet::new();
    for signal in model
        .signals
        .iter()
        .filter(|signal| signal.outcome != SignalOutcome::Executed)
        .chain(
            model
                .signals
                .iter()
                .filter(|signal| signal.outcome == SignalOutcome::Executed),
        )
        .take(MAX_SELECTED_SIGNALS)
    {
        if ids.insert(signal.signal_id.as_str()) {
            selected.push(SelectedSignal {
                signal_id: signal.signal_id.clone(),
                event_seq: signal.context.event_seq,
                at: signal.signal_time,
                outcome: signal.outcome,
                reasons: signal.reasons.clone(),
            });
        }
    }
    selected.sort_by_key(|signal| signal.event_seq);
    SignalSelection {
        rule: "all non-executed outcomes first, then executed outcomes; stable event order; maximum 100".into(),
        total_count: usize_u64(model.signals.len()),
        selected_count: usize_u64(selected.len()),
        omitted_count: usize_u64(model.signals.len().saturating_sub(selected.len())),
        signals: selected,
    }
}

fn streaks(values: &[f64]) -> (u64, u64) {
    let (mut current_wins, mut current_losses, mut max_wins, mut max_losses) =
        (0_u64, 0_u64, 0_u64, 0_u64);
    for value in values {
        if *value > 0.0 {
            current_wins += 1;
            current_losses = 0;
            max_wins = max_wins.max(current_wins);
        } else if *value < 0.0 {
            current_losses += 1;
            current_wins = 0;
            max_losses = max_losses.max(current_losses);
        } else {
            current_wins = 0;
            current_losses = 0;
        }
    }
    (max_wins, max_losses)
}

fn quantiles(values: &[f64], empty: NullReason) -> QuantileMetrics {
    QuantileMetrics {
        p05: quantile(values, 5, empty),
        p25: quantile(values, 25, empty),
        p50: quantile(values, 50, empty),
        p75: quantile(values, 75, empty),
        p95: quantile(values, 95, empty),
    }
}

fn quantile(values: &[f64], percentile: usize, empty: NullReason) -> MetricValue {
    if values.is_empty() {
        return MetricValue::null(empty);
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let numerator = percentile.saturating_mul(ordered.len().saturating_sub(1));
    let lower = numerator / 100;
    let upper = numerator.div_ceil(100);
    let fraction = (numerator % 100) as f64 / 100.0;
    let result = ordered[lower] + (ordered[upper] - ordered[lower]) * fraction;
    MetricValue::value(result)
}

fn mean(values: &[f64], empty: NullReason) -> MetricValue {
    average(values).map_or_else(|| MetricValue::null(empty), MetricValue::value)
}

fn average(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn sample_std_dev(values: &[f64], average: f64) -> f64 {
    (values
        .iter()
        .map(|value| (value - average).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64)
        .sqrt()
}

fn ratio(numerator: usize, denominator: usize, empty: NullReason) -> MetricValue {
    if denominator == 0 {
        MetricValue::null(empty)
    } else {
        MetricValue::value(numerator as f64 / denominator as f64)
    }
}

fn ratio_decimal(numerator: Decimal, denominator: Decimal) -> MetricValue {
    if denominator == Decimal::ZERO {
        return MetricValue::null(NullReason::ZeroDenominator);
    }
    let Some(ratio) = numerator.checked_div(denominator) else {
        return MetricValue::null(NullReason::NonFiniteResult);
    };
    decimal_f64(ratio).map_or_else(
        || MetricValue::null(NullReason::NonFiniteResult),
        MetricValue::value,
    )
}

fn decimal_f64(value: Decimal) -> Option<f64> {
    value.to_f64().filter(|number| number.is_finite())
}

fn usize_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn checked_decimal_sum(values: impl IntoIterator<Item = Decimal>) -> Option<Decimal> {
    values
        .into_iter()
        .try_fold(Decimal::ZERO, Decimal::checked_add)
}

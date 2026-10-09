//! Causal regime timeline and exact market `PnL` projection.

use crate::contracts::checked::{
    add as checked_add, div as checked_div, mul as checked_mul, sub as checked_sub,
};
use crate::contracts::{
    LabError, MarketId, PortfolioLedger, PortfolioRegimeMetrics, PortfolioRegimeResult,
    PortfolioRegimeTimelinePoint, QuoteAmount, RegimeLabel, RegimeObservation,
    RegimeObservationReason, SignedAmount, Weight,
};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

struct Bucket {
    market: MarketId,
    regime: RegimeLabel,
    time_seconds: u64,
    pnl: Decimal,
    max_drawdown: Decimal,
    trade_count: u64,
    turnover_notional: Decimal,
    fees: Decimal,
    embedded_cost: Decimal,
    transition_count: u64,
}

type Buckets = BTreeMap<String, Bucket>;

#[derive(Default)]
struct SegmentFlows {
    buys: Decimal,
    sells: Decimal,
    fees: Decimal,
    first_decision_time: Option<crate::contracts::UtcTimestamp>,
    fill_count: usize,
}

impl Bucket {
    fn new(market: MarketId, regime: RegimeLabel) -> Self {
        Self {
            market,
            regime,
            time_seconds: 0,
            pnl: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            trade_count: 0,
            turnover_notional: Decimal::ZERO,
            fees: Decimal::ZERO,
            embedded_cost: Decimal::ZERO,
            transition_count: 0,
        }
    }
}

/// Build the durable causal timeline. Previous labels are maintained per
/// market so interleaved markets cannot hide transitions.
pub(crate) fn timeline(ledger: &PortfolioLedger) -> Vec<PortfolioRegimeTimelinePoint> {
    let mut observations: Vec<&RegimeObservation> = ledger.regime_observations.iter().collect();
    observations.sort_by(|left, right| {
        left.decision_time
            .cmp(&right.decision_time)
            .then_with(|| left.market.code().cmp(&right.market.code()))
    });
    let mut previous = BTreeMap::<String, RegimeLabel>::new();
    observations
        .into_iter()
        .map(|observation| {
            let code = observation.market.code();
            let previous_regime = previous.insert(code, observation.regime);
            PortfolioRegimeTimelinePoint {
                market: observation.market.clone(),
                timestamp: observation.decision_time,
                previous_regime,
                current_regime: observation.regime,
                transitioned: previous_regime.is_some_and(|label| label != observation.regime),
                vol_state: observation.vol_state,
                features: observation.features.clone(),
                reason: if observation.features.is_some() {
                    RegimeObservationReason::Classified
                } else {
                    RegimeObservationReason::WarmupIncomplete
                },
                classifier_revision: observation.classifier_revision.clone(),
                feature_reference: observation.causal_input_digest.clone(),
            }
        })
        .collect()
}

/// Project time, exact market `PnL`, observed drawdown, trades, turnover and
/// costs into causal market/regime buckets.
pub(crate) fn summary(ledger: &PortfolioLedger) -> Result<PortfolioRegimeResult, LabError> {
    let timeline = timeline(ledger);
    let mut buckets = Buckets::new();
    let interval_seconds = u64::try_from(ledger.decision_interval.duration().num_seconds())
        .map_err(|_| LabError::DataCorrupt("negative decision interval".into()))?;
    accumulate_timeline_metrics(ledger, &timeline, interval_seconds, &mut buckets);
    accumulate_fill_metrics(ledger, &mut buckets)?;
    accumulate_mark_pnl(ledger, &mut buckets)?;
    let transitions = transition_counts(&timeline);
    let rows = finish_buckets(buckets, ledger.spec.initial_cash.get())?;
    Ok(PortfolioRegimeResult {
        run_id: ledger.run_id.clone(),
        buckets: rows,
        transitions,
    })
}

fn accumulate_timeline_metrics(
    ledger: &PortfolioLedger,
    timeline: &[PortfolioRegimeTimelinePoint],
    interval_seconds: u64,
    buckets: &mut Buckets,
) {
    for point in timeline {
        let bucket = bucket_mut(buckets, &point.market, point.current_regime);
        bucket.time_seconds = bucket.time_seconds.saturating_add(interval_seconds);
        if point.transitioned {
            bucket.transition_count = bucket.transition_count.saturating_add(1);
        }
        if let Some(mark) = ledger
            .marks
            .iter()
            .find(|mark| mark.time == point.timestamp)
        {
            bucket.max_drawdown = bucket.max_drawdown.max(mark.drawdown.get());
        }
    }
}

fn accumulate_fill_metrics(
    ledger: &PortfolioLedger,
    buckets: &mut Buckets,
) -> Result<(), LabError> {
    for fill in &ledger.fills {
        let label = label_at(
            &ledger.regime_observations,
            &fill.market,
            fill.decision_time,
        );
        let bucket = bucket_mut(buckets, &fill.market, label);
        if fill.side == crate::contracts::Side::Sell {
            bucket.trade_count = bucket.trade_count.saturating_add(1);
        }
        bucket.turnover_notional = checked_add(
            bucket.turnover_notional,
            fill.notional.get(),
            "regime turnover",
        )?;
        bucket.fees = checked_add(bucket.fees, fill.fee.get(), "regime fees")?;
        bucket.embedded_cost = checked_add(
            bucket.embedded_cost,
            fill.price_cost.get(),
            "regime embedded cost",
        )?;
    }
    Ok(())
}

/// Attribute each durable mark segment using event order. Ordinary fills occur
/// after their same-time mark; terminal liquidation occurs before its mark.
fn accumulate_mark_pnl(ledger: &PortfolioLedger, buckets: &mut Buckets) -> Result<(), LabError> {
    let mut previous = None;
    let mut projected_fill_count = 0_usize;
    for current in &ledger.marks {
        for asset in &ledger.spec.assets {
            let code = asset.market.code();
            let previous_value =
                previous.map_or(Ok(Decimal::ZERO), |mark| market_value(mark, &code))?;
            let current_value = market_value(current, &code)?;
            let flows = segment_flows(ledger, &asset.market, previous, current.event_seq)?;
            projected_fill_count = projected_fill_count.saturating_add(flows.fill_count);
            let value_change = checked_sub(current_value, previous_value, "market value change")?;
            let before_fees = checked_add(
                checked_sub(value_change, flows.buys, "market contribution buys")?,
                flows.sells,
                "market contribution sells",
            )?;
            let net = checked_sub(before_fees, flows.fees, "market contribution fees")?;
            let label = previous.map_or_else(
                || {
                    flows
                        .first_decision_time
                        .map_or(RegimeLabel::Unknown, |time| {
                            label_at(&ledger.regime_observations, &asset.market, time)
                        })
                },
                |mark| label_at(&ledger.regime_observations, &asset.market, mark.time),
            );
            let bucket = bucket_mut(buckets, &asset.market, label);
            bucket.pnl = checked_add(bucket.pnl, net, "regime pnl")?;
            bucket.max_drawdown = bucket.max_drawdown.max(current.drawdown.get());
        }
        previous = Some(current);
    }
    if projected_fill_count != ledger.fills.len() {
        return Err(LabError::DataCorrupt(
            "portfolio fill is not enclosed by durable marks".into(),
        ));
    }
    reconcile_projected_pnl(ledger, buckets)
}

fn market_value(
    mark: &crate::contracts::PortfolioMarkRecord,
    market_code: &str,
) -> Result<Decimal, LabError> {
    let weight = mark.weights.get(market_code).copied().ok_or_else(|| {
        LabError::DataCorrupt(format!("portfolio mark lacks weight for {market_code}"))
    })?;
    checked_mul(weight, mark.equity.get(), "marked market value")
}

fn segment_flows(
    ledger: &PortfolioLedger,
    market: &MarketId,
    previous: Option<&crate::contracts::PortfolioMarkRecord>,
    current_event_seq: u64,
) -> Result<SegmentFlows, LabError> {
    let mut flows = SegmentFlows::default();
    for fill in ledger.fills.iter().filter(|fill| {
        fill.market == *market
            && previous.is_none_or(|mark| fill.event_seq > mark.event_seq)
            && fill.event_seq < current_event_seq
    }) {
        flows.fill_count = flows.fill_count.saturating_add(1);
        if flows.first_decision_time.is_none() {
            flows.first_decision_time = Some(fill.decision_time);
        }
        match fill.side {
            crate::contracts::Side::Buy => {
                flows.buys = checked_add(flows.buys, fill.notional.get(), "regime buy flow")?;
            }
            crate::contracts::Side::Sell => {
                flows.sells = checked_add(flows.sells, fill.notional.get(), "regime sell flow")?;
            }
        }
        flows.fees = checked_add(flows.fees, fill.fee.get(), "regime interval fees")?;
    }
    Ok(flows)
}

fn reconcile_projected_pnl(ledger: &PortfolioLedger, buckets: &Buckets) -> Result<(), LabError> {
    let projected_pnl = buckets.values().map(|bucket| bucket.pnl).sum::<Decimal>();
    let portfolio_pnl = checked_sub(
        ledger.totals.terminal_equity.get(),
        ledger.spec.initial_cash.get(),
        "regime portfolio pnl",
    )?;
    let residual = checked_sub(portfolio_pnl, projected_pnl, "regime pnl residual")?;
    if residual.abs() > crate::reporting::portfolio_projection::ATTRIBUTION_TOLERANCE {
        return Err(LabError::AccountingInvariant(format!(
            "regime PnL does not reconcile: portfolio={portfolio_pnl}, projected={projected_pnl}, residual={residual}"
        )));
    }
    Ok(())
}

fn transition_counts(timeline: &[PortfolioRegimeTimelinePoint]) -> BTreeMap<String, u64> {
    let mut transitions = BTreeMap::new();
    for point in timeline {
        if let Some(previous) = point.previous_regime
            && point.transitioned
        {
            let key = format!(
                "{}:{}->{}",
                point.market.code(),
                label_name(previous),
                label_name(point.current_regime)
            );
            transitions
                .entry(key)
                .and_modify(|count: &mut u64| *count = count.saturating_add(1))
                .or_insert(1);
        }
    }
    transitions
}

fn finish_buckets(
    buckets: Buckets,
    initial: Decimal,
) -> Result<Vec<PortfolioRegimeMetrics>, LabError> {
    buckets
        .into_values()
        .map(|bucket| {
            Ok(PortfolioRegimeMetrics {
                market: bucket.market,
                regime: bucket.regime,
                time_seconds: bucket.time_seconds,
                pnl: SignedAmount::new(bucket.pnl)?,
                max_drawdown: Weight::new(bucket.max_drawdown)?,
                trade_count: bucket.trade_count,
                turnover: if initial.is_zero() {
                    Decimal::ZERO
                } else {
                    checked_div(bucket.turnover_notional, initial, "regime turnover rate")?
                },
                fees: QuoteAmount::new(bucket.fees)?,
                embedded_cost: SignedAmount::new(bucket.embedded_cost)?,
                transition_count: bucket.transition_count,
            })
        })
        .collect()
}

fn label_at(
    observations: &[RegimeObservation],
    market: &MarketId,
    time: crate::contracts::UtcTimestamp,
) -> RegimeLabel {
    observations
        .iter()
        .filter(|observation| observation.market == *market && observation.decision_time <= time)
        .max_by_key(|observation| observation.decision_time)
        .map_or(RegimeLabel::Unknown, |observation| observation.regime)
}

fn bucket_mut<'a>(
    buckets: &'a mut BTreeMap<String, Bucket>,
    market: &MarketId,
    regime: RegimeLabel,
) -> &'a mut Bucket {
    let key = format!("{}:{}", market.code(), label_name(regime));
    buckets
        .entry(key)
        .or_insert_with(|| Bucket::new(market.clone(), regime))
}

fn label_name(label: RegimeLabel) -> &'static str {
    match label {
        RegimeLabel::TrendUp => "TREND_UP",
        RegimeLabel::TrendDown => "TREND_DOWN",
        RegimeLabel::Chop => "CHOP",
        RegimeLabel::Unknown => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        ArbitrationPolicy, AssetQuantity, BasisPoints, CandleInterval, ContentHash, ModelId,
        PortfolioAssetSpec, PortfolioAttribution, PortfolioFillRecord, PortfolioMarkRecord,
        PortfolioRiskPolicy, PortfolioSpec, PortfolioStatus, PortfolioTotals, PriceKrw,
        RegimeFeatures, RegimeObservation, RunId, Side, StrategyKind, UtcTimestamp, VolState,
    };
    use std::str::FromStr;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("fixture decimal")
    }

    fn time(value: &str) -> UtcTimestamp {
        UtcTimestamp::parse_rfc3339(value).expect("fixture time")
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one hand-computed ledger proves event-sequence attribution end to end"
    )]
    fn first_durable_mark_after_buy_and_later_exit_reconcile_by_event_sequence() {
        let market = MarketId::parse_upbit("KRW-BTC").expect("market");
        let t0 = time("2025-01-01T00:00:00Z");
        let t1 = time("2025-01-01T01:00:00Z");
        let t2 = time("2025-01-01T02:00:00Z");
        let fill =
            |event_seq, side, notional: &str, fee: &str, at, cash: &str| PortfolioFillRecord {
                event_seq,
                market: market.clone(),
                side,
                price: PriceKrw::new(if side == Side::Buy {
                    decimal("100")
                } else {
                    decimal("110")
                })
                .expect("price"),
                qty: AssetQuantity::new(Decimal::ONE).expect("qty"),
                notional: QuoteAmount::new(decimal(notional)).expect("notional"),
                fee: QuoteAmount::new(decimal(fee)).expect("fee"),
                fee_bps: BasisPoints::new(decimal("100")).expect("fee bps"),
                price_cost: SignedAmount::new(Decimal::ZERO).expect("price cost"),
                price_difference_per_unit: None,
                reserved_cash: QuoteAmount::new(Decimal::ZERO).expect("reservation"),
                decision_time: at,
                execution_time: at,
                source_bar_id: format!("source-{event_seq}"),
                liquidity_source_bar_id: format!("liquidity-{event_seq}"),
                cash_after: QuoteAmount::new(decimal(cash)).expect("cash"),
            };
        let mark = |event_seq, at, cash: &str, position: &str, equity: &str, weight: &str| {
            PortfolioMarkRecord {
                event_seq,
                time: at,
                cash: QuoteAmount::new(decimal(cash)).expect("cash"),
                position_value: QuoteAmount::new(decimal(position)).expect("position"),
                gross_exposure_weight: Weight::new(decimal(weight)).expect("gross weight"),
                equity: QuoteAmount::new(decimal(equity)).expect("equity"),
                peak_equity: QuoteAmount::new(decimal("1009")).expect("peak"),
                drawdown: Weight::new(if event_seq == 5 {
                    decimal("0.0009910802775024777006937562")
                } else {
                    Decimal::ZERO
                })
                .expect("drawdown"),
                stopped: false,
                weights: BTreeMap::from([(market.code(), decimal(weight))]),
            }
        };
        let observation = |at| RegimeObservation {
            market: market.clone(),
            decision_time: at,
            features: Some(RegimeFeatures {
                close_vs_sma_long: 0.1,
                sma_mid_vs_short: 0.1,
                sma_short_slope: 0.1,
                atr_ratio: 0.01,
                realized_vol_annualized: 0.2,
                volume_expansion: 1.0,
                directional_persistence: 1.0,
            }),
            regime: RegimeLabel::TrendUp,
            vol_state: VolState::NormalVol,
            classifier_revision: "fixture-v1".into(),
            causal_input_digest: ContentHash::of_bytes(at.to_string().as_bytes()),
        };
        let ledger = PortfolioLedger {
            run_id: RunId::new("regime-event-sequence").expect("run"),
            model_ids: vec![ModelId::from_seed("regime-event-sequence-model")],
            spec: PortfolioSpec {
                initial_cash: QuoteAmount::new(decimal("1000")).expect("initial"),
                assets: vec![PortfolioAssetSpec {
                    market: market.clone(),
                    max_weight: Weight::new(Decimal::ONE).expect("weight"),
                }],
                risk: PortfolioRiskPolicy {
                    max_gross_exposure: Weight::new(Decimal::ONE).expect("gross"),
                    min_cash_weight: Weight::new(Decimal::ZERO).expect("cash"),
                    drawdown_stop: Weight::new(Decimal::ZERO).expect("stop"),
                },
                arbitration: ArbitrationPolicy::Priority,
            },
            decision_interval: CandleInterval::H1,
            execution_resolution: CandleInterval::H1,
            status: PortfolioStatus::Completed,
            status_reason: None,
            intents: Vec::new(),
            fills: vec![
                fill(2, Side::Buy, "100", "1", t0, "899"),
                fill(4, Side::Sell, "110", "1", t1, "1008"),
            ],
            rejections: Vec::new(),
            // The first durable mark already contains the buy. Event sequence
            // proves that its principal belongs to the initial segment.
            marks: vec![
                mark(
                    3,
                    t1,
                    "899",
                    "110",
                    "1009",
                    "0.1090188305252725470763131814",
                ),
                mark(5, t2, "1008", "0", "1008", "0"),
            ],
            attribution: vec![PortfolioAttribution {
                market: market.clone(),
                strategy: StrategyKind::BuyAndHold,
                buy_notional: QuoteAmount::new(decimal("100")).expect("buy"),
                sell_notional: QuoteAmount::new(decimal("110")).expect("sell"),
                fees: QuoteAmount::new(decimal("2")).expect("fees"),
                realized_pnl: SignedAmount::new(decimal("10")).expect("realized"),
                unrealized_pnl: SignedAmount::new(Decimal::ZERO).expect("unrealized"),
                closed_trades: 1,
                max_weight_seen: Weight::new(decimal("0.1090188305252725470763131814"))
                    .expect("max weight"),
                exposure_seconds: 3_600,
            }],
            totals: PortfolioTotals {
                terminal_equity: QuoteAmount::new(decimal("1008")).expect("terminal"),
                total_return: decimal("0.008"),
                max_drawdown: Weight::new(decimal("0.0009910802775024777006937562")).expect("mdd"),
                turnover: decimal("0.21"),
                total_fees: QuoteAmount::new(decimal("2")).expect("fees"),
                price_cost_drag: SignedAmount::new(Decimal::ZERO).expect("cost"),
                exposure_seconds: 3_600,
                rejected_signals: 0,
                rejection_reasons: BTreeMap::new(),
            },
            regime_observations: vec![observation(t0), observation(t1)],
            last_event_seq: 5,
        };

        let result = summary(&ledger).expect("event-sequence projection reconciles");
        let trend = result
            .buckets
            .iter()
            .find(|bucket| bucket.market == market && bucket.regime == RegimeLabel::TrendUp)
            .expect("trend bucket");
        assert_eq!(trend.pnl.get(), decimal("8"));
        assert_eq!(trend.fees.get(), decimal("2"));
        assert_eq!(trend.trade_count, 1);
        assert_eq!(trend.turnover, decimal("0.21"));
        assert_eq!(
            result
                .buckets
                .iter()
                .map(|bucket| bucket.pnl.get())
                .sum::<Decimal>(),
            decimal("8")
        );
    }
}

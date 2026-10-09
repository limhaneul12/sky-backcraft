//! Pure typed projections over one immutable shared-portfolio ledger.

use crate::contracts::checked::{add as checked_add, div as checked_div, sub as checked_sub};
use crate::contracts::{
    LabError, PortfolioAllocationPoint, PortfolioAssetAllocation, PortfolioContributions,
    PortfolioEquityPoint, PortfolioLedger, PortfolioMarketContribution,
    PortfolioPolicyContribution, PortfolioPolicySource, PortfolioRebalancePoint,
    PortfolioResultSummary, ResolvedPlan, SignedAmount,
};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

pub(crate) const ATTRIBUTION_TOLERANCE: Decimal = crate::contracts::NUMERIC_TOLERANCE;

/// Complete deterministic projection payload persisted in one transaction.
pub(crate) struct PortfolioProjectionSet {
    pub summary: PortfolioResultSummary,
    pub equity: Vec<PortfolioEquityPoint>,
    pub allocations: Vec<PortfolioAllocationPoint>,
    pub rebalances: Vec<PortfolioRebalancePoint>,
    pub contributions: PortfolioContributions,
}

/// Build public projections from exact ledger facts and the frozen plan.
pub(crate) fn build(
    ledger: &PortfolioLedger,
    plan: &ResolvedPlan,
) -> Result<PortfolioProjectionSet, LabError> {
    let sources = policy_sources(ledger, plan)?;
    let (summary, portfolio_pnl) = result_summary(ledger, &sources)?;
    let equity = equity_points(ledger);
    let source_by_market: BTreeMap<_, _> = sources
        .into_iter()
        .map(|source| (source.market.code(), source))
        .collect();
    let allocations = allocation_points(ledger, &source_by_market)?;
    let rebalances = rebalance_points(ledger);
    let contributions = contributions(ledger, &source_by_market, portfolio_pnl)?;
    Ok(PortfolioProjectionSet {
        summary,
        equity,
        allocations,
        rebalances,
        contributions,
    })
}

fn result_summary(
    ledger: &PortfolioLedger,
    sources: &[PortfolioPolicySource],
) -> Result<(PortfolioResultSummary, Decimal), LabError> {
    let terminal = ledger
        .marks
        .last()
        .ok_or_else(|| LabError::DataCorrupt("portfolio ledger has no terminal mark".into()))?;
    if terminal.equity.get() != ledger.totals.terminal_equity.get() {
        return Err(LabError::AccountingInvariant(
            "portfolio terminal mark differs from totals".into(),
        ));
    }
    let cash_weight = if terminal.equity.get().is_zero() {
        Decimal::ZERO
    } else {
        checked_div(
            terminal.cash.get(),
            terminal.equity.get(),
            "terminal cash weight",
        )?
    };
    let portfolio_pnl = checked_sub(
        ledger.totals.terminal_equity.get(),
        ledger.spec.initial_cash.get(),
        "portfolio pnl",
    )?;
    Ok((
        PortfolioResultSummary {
            run_id: ledger.run_id.clone(),
            initial_cash: ledger.spec.initial_cash,
            terminal_equity: ledger.totals.terminal_equity,
            total_return: ledger.totals.total_return,
            max_drawdown: ledger.totals.max_drawdown,
            cash_weight,
            turnover: ledger.totals.turnover,
            fees: ledger.totals.total_fees,
            embedded_cost: ledger.totals.price_cost_drag,
            portfolio_pnl: SignedAmount::new(portfolio_pnl)?,
            policy_sources: sources.to_vec(),
        },
        portfolio_pnl,
    ))
}

fn equity_points(ledger: &PortfolioLedger) -> Vec<PortfolioEquityPoint> {
    ledger
        .marks
        .iter()
        .map(|mark| PortfolioEquityPoint {
            timestamp: mark.time,
            equity: mark.equity,
            cash: mark.cash,
            position_value: mark.position_value,
            drawdown: mark.drawdown,
        })
        .collect()
}

fn allocation_points(
    ledger: &PortfolioLedger,
    source_by_market: &BTreeMap<String, PortfolioPolicySource>,
) -> Result<Vec<PortfolioAllocationPoint>, LabError> {
    let mut allocations = Vec::with_capacity(ledger.marks.len());
    for mark in &ledger.marks {
        let mut assets = Vec::with_capacity(ledger.spec.assets.len());
        for asset in &ledger.spec.assets {
            let code = asset.market.code();
            let source = source_by_market.get(&code).cloned().ok_or_else(|| {
                LabError::DataCorrupt(format!("portfolio source missing for {code}"))
            })?;
            assets.push(PortfolioAssetAllocation {
                market: asset.market.clone(),
                weight: mark.weights.get(&code).copied().ok_or_else(|| {
                    LabError::DataCorrupt(format!("allocation mark lacks weight for {code}"))
                })?,
                source,
            });
        }
        let cash_weight = if mark.equity.get().is_zero() {
            Decimal::ZERO
        } else {
            checked_div(mark.cash.get(), mark.equity.get(), "allocation cash weight")?
        };
        let asset_weight = assets.iter().map(|asset| asset.weight).sum::<Decimal>();
        let weight_total = checked_add(asset_weight, cash_weight, "allocation weight total")?;
        if cash_weight < Decimal::ZERO
            || assets.iter().any(|asset| asset.weight < Decimal::ZERO)
            || asset_weight > Decimal::ONE + ATTRIBUTION_TOLERANCE
            || (weight_total - Decimal::ONE).abs() > ATTRIBUTION_TOLERANCE
        {
            return Err(LabError::AccountingInvariant(format!(
                "portfolio allocation violates long-only shared-pool weights at {}: assets={asset_weight}, cash={cash_weight}",
                mark.time
            )));
        }
        allocations.push(PortfolioAllocationPoint {
            timestamp: mark.time,
            cash_weight,
            assets,
        });
    }
    Ok(allocations)
}

fn rebalance_points(ledger: &PortfolioLedger) -> Vec<PortfolioRebalancePoint> {
    ledger
        .fills
        .iter()
        .map(|fill| {
            let target_weight = ledger
                .intents
                .iter()
                .find(|intent| {
                    intent.market == fill.market
                        && intent.side == fill.side
                        && intent.decision_time == fill.decision_time
                })
                .map(|intent| intent.target_weight);
            PortfolioRebalancePoint {
                decision_time: fill.decision_time,
                execution_time: fill.execution_time,
                market: fill.market.clone(),
                side: fill.side,
                target_weight,
                notional: fill.notional,
                fee: fill.fee,
                embedded_cost: fill.price_cost,
                price_difference_per_unit: fill.price_difference_per_unit,
                cash_after: fill.cash_after,
            }
        })
        .collect()
}

fn policy_sources(
    ledger: &PortfolioLedger,
    plan: &ResolvedPlan,
) -> Result<Vec<PortfolioPolicySource>, LabError> {
    if ledger.spec.assets.len() != ledger.model_ids.len() {
        return Err(LabError::DataCorrupt(
            "portfolio asset/model cardinality mismatch".into(),
        ));
    }
    ledger
        .spec
        .assets
        .iter()
        .zip(&ledger.model_ids)
        .map(|(asset, model_id)| {
            let admissions: Vec<_> = plan
                .admissions
                .iter()
                .filter(|admission| admission.market == asset.market)
                .collect();
            let [admission] = admissions.as_slice() else {
                return Err(LabError::DataCorrupt(format!(
                    "portfolio market {} must have exactly one frozen admission",
                    asset.market
                )));
            };
            Ok(PortfolioPolicySource {
                market: asset.market.clone(),
                model_id: model_id.clone(),
                strategy: admission.strategy,
                policy_ref: admission.policy_ref.clone(),
            })
        })
        .collect()
}

fn contributions(
    ledger: &PortfolioLedger,
    source_by_market: &BTreeMap<String, PortfolioPolicySource>,
    portfolio_pnl: Decimal,
) -> Result<PortfolioContributions, LabError> {
    let attribution_markets = ledger
        .attribution
        .iter()
        .map(|item| item.market.code())
        .collect::<std::collections::BTreeSet<_>>();
    let source_markets = source_by_market
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if attribution_markets != source_markets || ledger.attribution.len() != source_by_market.len() {
        return Err(LabError::DataCorrupt(
            "portfolio attribution markets differ from frozen assets".into(),
        ));
    }
    let mut market = Vec::with_capacity(ledger.attribution.len());
    let mut policy = Vec::with_capacity(ledger.attribution.len());
    let mut explained = Decimal::ZERO;
    for attribution in &ledger.attribution {
        let gross = checked_add(
            attribution.realized_pnl.get(),
            attribution.unrealized_pnl.get(),
            "market gross pnl",
        )?;
        let net = checked_sub(gross, attribution.fees.get(), "market net pnl")?;
        explained = checked_add(explained, net, "attribution explained pnl")?;
        market.push(PortfolioMarketContribution {
            market: attribution.market.clone(),
            strategy: attribution.strategy,
            gross_pnl: SignedAmount::new(gross)?,
            fees: attribution.fees,
            net_pnl: SignedAmount::new(net)?,
        });
        let source = source_by_market
            .get(&attribution.market.code())
            .cloned()
            .ok_or_else(|| LabError::DataCorrupt("attribution policy source missing".into()))?;
        if source.strategy != attribution.strategy {
            return Err(LabError::DataCorrupt(format!(
                "attribution strategy differs from frozen source for {}",
                attribution.market
            )));
        }
        policy.push(PortfolioPolicyContribution {
            source,
            gross_pnl: SignedAmount::new(gross)?,
            fees: attribution.fees,
            net_pnl: SignedAmount::new(net)?,
        });
    }
    let residual = checked_sub(portfolio_pnl, explained, "portfolio attribution residual")?;
    if residual.abs() > ATTRIBUTION_TOLERANCE {
        return Err(LabError::AccountingInvariant(format!(
            "portfolio projection does not reconcile: pnl={portfolio_pnl}, explained={explained}, residual={residual}, tolerance={ATTRIBUTION_TOLERANCE}"
        )));
    }
    Ok(PortfolioContributions {
        run_id: ledger.run_id.clone(),
        market,
        policy,
        fees: SignedAmount::new(-ledger.totals.total_fees.get())?,
        embedded_cost: ledger.totals.price_cost_drag,
        cash_drag: SignedAmount::new(Decimal::ZERO)?,
        portfolio_pnl: SignedAmount::new(portfolio_pnl)?,
        residual: SignedAmount::new(residual)?,
        tolerance: ATTRIBUTION_TOLERANCE,
    })
}

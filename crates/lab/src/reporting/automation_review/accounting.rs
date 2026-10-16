use super::{corrupt, overflow, saved_evaluation};
use crate::contracts::{
    AutomationOrderState, AutomationReviewSnapshot, AutomationSide, LabError, UtcTimestamp,
};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

pub(super) struct Accounting {
    pub buys: Decimal,
    pub sells: Decimal,
    pub fees: Decimal,
    pub cashflow: Decimal,
    pub realized: Option<Decimal>,
    pub unrealized: Option<Decimal>,
    pub total: Option<Decimal>,
    pub volume: Option<Decimal>,
    pub cost: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub mark_time: Option<UtcTimestamp>,
    pub reason: String,
    pub mark_reason: String,
    pub warnings: Vec<String>,
}

fn add(a: Decimal, b: Decimal) -> Result<Decimal, LabError> {
    a.checked_add(b).ok_or_else(overflow)
}
fn sub(a: Decimal, b: Decimal) -> Result<Decimal, LabError> {
    a.checked_sub(b).ok_or_else(overflow)
}
fn mul(a: Decimal, b: Decimal) -> Result<Decimal, LabError> {
    a.checked_mul(b).ok_or_else(overflow)
}
fn div(a: Decimal, b: Decimal) -> Result<Decimal, LabError> {
    a.checked_div(b).ok_or_else(overflow)
}

pub(super) fn calculate(s: &AutomationReviewSnapshot) -> Result<Accounting, LabError> {
    let mut warnings = Vec::new();
    let (allocated, fees, unresolved) = allocate_fees(s, &mut warnings)?;
    let mut buys = Decimal::ZERO;
    let mut sells = Decimal::ZERO;
    let mut volume = Decimal::ZERO;
    let mut cost = Decimal::ZERO;
    let mut realized = Decimal::ZERO;
    let mut inherited = s
        .opening_inventory
        .as_ref()
        .is_some_and(|reference| reference.base_quantity.get() > Decimal::ZERO);
    for row in &s.fills {
        let f = &row.fill;
        let fee = allocated
            .get(f.trade_uuid.as_str())
            .copied()
            .unwrap_or(Decimal::ZERO);
        match f.side {
            AutomationSide::Bid => {
                buys = add(buys, f.funds_krw.get())?;
                volume = add(volume, f.volume.get())?;
                cost = add(cost, add(f.funds_krw.get(), fee)?)?;
            }
            AutomationSide::Ask => {
                sells = add(sells, f.funds_krw.get())?;
                if f.volume.get() > volume {
                    inherited = true;
                }
                if !inherited {
                    let consumed = if f.volume.get() == volume {
                        cost
                    } else {
                        mul(cost, div(f.volume.get(), volume)?)?
                    };
                    realized = add(realized, sub(sub(f.funds_krw.get(), fee)?, consumed)?)?;
                    cost = sub(cost, consumed)?;
                    volume = sub(volume, f.volume.get())?;
                }
            }
        }
    }
    let missing_opening = (!s.fills.is_empty() || !fees.is_zero()) && s.opening_inventory.is_none();
    let reason = if inherited {
        "OPENING_INVENTORY_COST_UNKNOWN"
    } else if missing_opening {
        "OPENING_INVENTORY_UNVERIFIED"
    } else if unresolved {
        "UNRECONCILED_ORDER_OR_FILL_COVERAGE"
    } else {
        ""
    };
    let reliable = !inherited && !missing_opening && !unresolved;
    let valuation = mark_position(s, volume, cost, reliable, reason)?;
    if !reason.is_empty() {
        warnings.push(reason.into());
    }
    if reliable && valuation.value.is_none() {
        warnings.push(valuation.reason.into());
    }
    if s.decisions
        .iter()
        .any(|d| saved_evaluation(d).get("indicators").is_none())
    {
        warnings.push("SOME_SAVED_DECISIONS_HAVE_NO_INDICATOR_TRACE; NO_VALUES_SYNTHESIZED".into());
    }
    let total = valuation
        .value
        .filter(|_| reliable)
        .map(|u| add(realized, u))
        .transpose()?;
    Ok(Accounting {
        buys,
        sells,
        fees,
        cashflow: sub(sub(sells, buys)?, fees)?,
        realized: reliable.then_some(realized),
        unrealized: valuation.value,
        total,
        volume: reliable.then_some(volume),
        cost: reliable.then_some(cost),
        mark_price: valuation.price,
        mark_time: valuation.time,
        reason: reason.into(),
        mark_reason: valuation.reason.into(),
        warnings,
    })
}

struct Valuation {
    value: Option<Decimal>,
    price: Option<Decimal>,
    time: Option<UtcTimestamp>,
    reason: &'static str,
}

fn mark_position(
    s: &AutomationReviewSnapshot,
    volume: Decimal,
    cost: Decimal,
    reliable: bool,
    reason: &'static str,
) -> Result<Valuation, LabError> {
    if !reliable {
        return Ok(Valuation {
            value: None,
            price: None,
            time: None,
            reason,
        });
    }
    if volume.is_zero() {
        return Ok(Valuation {
            value: Some(Decimal::ZERO),
            price: None,
            time: None,
            reason: "",
        });
    }
    let last_fill = s.fills.iter().map(|f| f.stored_trade_time_utc).max();
    let mark = s
        .candles
        .iter()
        .filter(|c| last_fill.is_none_or(|time| c.candle.close_time_utc > time))
        .max_by_key(|c| c.candle.close_time_utc);
    if let Some(mark) = mark {
        return Ok(Valuation {
            value: Some(sub(mul(volume, mark.candle.close.get())?, cost)?),
            price: Some(mark.candle.close.get()),
            time: Some(mark.candle.close_time_utc),
            reason: "",
        });
    }
    Ok(Valuation {
        value: None,
        price: None,
        time: None,
        reason: "NO_SAVED_COMPLETED_MARK_STRICTLY_AFTER_LAST_FILL",
    })
}

fn allocate_fees<'a>(
    s: &'a AutomationReviewSnapshot,
    warnings: &mut Vec<String>,
) -> Result<(BTreeMap<&'a str, Decimal>, Decimal, bool), LabError> {
    let mut allocation = BTreeMap::new();
    let mut total = Decimal::ZERO;
    let mut unresolved = false;
    let mut grouped = BTreeMap::<_, Vec<_>>::new();
    for row in &s.fills {
        grouped
            .entry(&row.fill.client_identifier)
            .or_default()
            .push(row);
    }
    let mut grouped_fees = BTreeMap::<_, Vec<_>>::new();
    for row in &s.fee_accruals {
        grouped_fees
            .entry(&row.client_identifier)
            .or_default()
            .push(row);
    }
    for row in &s.orders {
        let order = &row.order;
        let paid = order.paid_fee_krw.get();
        total = add(total, paid)?;
        if !matches!(
            order.state,
            AutomationOrderState::Filled
                | AutomationOrderState::Cancelled
                | AutomationOrderState::Rejected
        ) {
            unresolved = true;
        }
        let fills = grouped
            .get(&order.client_identifier)
            .map_or(&[][..], Vec::as_slice);
        let volume = fills
            .iter()
            .try_fold(Decimal::ZERO, |v, f| add(v, f.fill.volume.get()))?;
        let funds = fills
            .iter()
            .try_fold(Decimal::ZERO, |v, f| add(v, f.fill.funds_krw.get()))?;
        if volume != order.executed_volume.get()
            || u64::try_from(fills.len()).map_err(|_| overflow())? != order.trade_count
        {
            unresolved = true;
        }
        let accruals = grouped_fees
            .get(&order.client_identifier)
            .map_or(&[][..], Vec::as_slice);
        let mut cumulative = Decimal::ZERO;
        for accrual in accruals {
            cumulative = add(cumulative, accrual.delta_paid_fee_krw.get())?;
            if cumulative != accrual.cumulative_paid_fee_krw.get() {
                return Err(corrupt("fee accrual cumulative amount mismatch"));
            }
        }
        if !accruals.is_empty() && cumulative != paid {
            unresolved = true;
        }
        if accruals.is_empty() && !paid.is_zero() {
            warnings.push(format!(
                "FEE_ACCRUAL_HISTORY_MISSING:{}",
                order.client_identifier
            ));
        }
        if funds.is_zero() {
            if !paid.is_zero() {
                unresolved = true;
            }
            continue;
        }
        let mut remaining = paid;
        for (index, fill) in fills.iter().enumerate() {
            let fee = if index + 1 == fills.len() {
                remaining
            } else {
                mul(paid, div(fill.fill.funds_krw.get(), funds)?)?.min(remaining)
            };
            remaining = sub(remaining, fee)?;
            allocation.insert(fill.fill.trade_uuid.as_str(), fee);
        }
    }
    Ok((allocation, total, unresolved))
}

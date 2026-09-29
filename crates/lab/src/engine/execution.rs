use super::accounting::{Account, checked_add, checked_div, checked_mul, checked_sub};
use crate::contracts::{
    AssetQuantity, BasisPoints, CostPolicy, LabError, MarketRuleSnapshot, PassiveFraction,
    PriceKrw, QuoteAmount, ReasonCode, Side, Weight,
};
use rust_decimal::{Decimal, RoundingStrategy};

const BPS_DENOMINATOR: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

#[derive(Debug, Clone, Copy)]
pub(super) struct PlannedFill {
    pub(super) side: Side,
    pub(super) price: PriceKrw,
    pub(super) qty: AssetQuantity,
    pub(super) notional: QuoteAmount,
    pub(super) fee: QuoteAmount,
    pub(super) fee_bps: BasisPoints,
    pub(super) reserved_cash: QuoteAmount,
    pub(super) price_cost_attribution: Decimal,
    pub(super) reason: ReasonCode,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExecutionDecision {
    pub(super) fill: Option<PlannedFill>,
    pub(super) reason: ReasonCode,
}

#[allow(
    clippy::too_many_lines,
    reason = "the checked post-fee sizing equation is kept in one auditable phase"
)]
pub(super) fn plan_market_request(
    account: &Account,
    target: Weight,
    decision_close: PriceKrw,
    costs: &CostPolicy,
    rules: &MarketRuleSnapshot,
) -> Result<ExecutionDecision, LabError> {
    let position_at_reference =
        checked_mul(account.qty(), decision_close.get(), "reference position")?;
    let equity_at_reference = checked_add(
        account.cash_free()?,
        position_at_reference,
        "reference equity",
    )?;
    let target_value = checked_mul(equity_at_reference, target.get(), "target value")?;
    if target_value == position_at_reference {
        return Ok(no_fill(ReasonCode::NoAction));
    }
    let side = if target_value > position_at_reference {
        Side::Buy
    } else {
        Side::Sell
    };
    let (price, price_cost_attribution) = market_price(decision_close, side, costs, rules)?;
    let fee_bps = match side {
        Side::Buy => costs.buy_fee_bps,
        Side::Sell => costs.sell_fee_bps,
    };
    let fee_rate = bps_rate(fee_bps)?;
    let position_value = checked_mul(account.qty(), price.get(), "fill-price position")?;
    let equity = checked_add(account.cash_free()?, position_value, "fill-price equity")?;
    let desired_notional = match side {
        Side::Buy => {
            let numerator = checked_sub(
                checked_mul(target.get(), equity, "target post-trade equity")?,
                position_value,
                "buy target gap",
            )?;
            let denominator = checked_add(
                Decimal::ONE,
                checked_mul(target.get(), fee_rate, "buy target fee term")?,
                "buy sizing denominator",
            )?;
            checked_div(numerator, denominator, "buy target notional")?
        }
        Side::Sell => {
            let numerator = checked_sub(
                position_value,
                checked_mul(target.get(), equity, "target post-trade equity")?,
                "sell target gap",
            )?;
            let denominator = checked_sub(
                Decimal::ONE,
                checked_mul(target.get(), fee_rate, "sell target fee term")?,
                "sell sizing denominator",
            )?;
            checked_div(numerator, denominator, "sell target notional")?
        }
    };
    if desired_notional <= Decimal::ZERO {
        return Ok(no_fill(ReasonCode::NoAction));
    }

    let desired_qty = checked_div(desired_notional, price.get(), "target quantity")?;
    let cash_limited_qty = match side {
        Side::Buy => {
            let per_unit_debit = checked_mul(
                price.get(),
                checked_add(Decimal::ONE, fee_rate, "fee multiplier")?,
                "per-unit buy debit",
            )?;
            checked_div(
                account.cash_free()?,
                per_unit_debit,
                "cash-limited quantity",
            )?
        }
        Side::Sell => account.qty(),
    };
    let capped_qty = desired_qty.min(cash_limited_qty);
    let qty = floor_to_step(capped_qty, rules.quantity_step.get())?;
    if qty.is_zero() {
        return Ok(no_fill(ReasonCode::QuantityRoundedToZero));
    }
    let notional = checked_mul(qty, price.get(), "rounded notional")?;
    if notional < rules.min_notional.get() {
        return Ok(no_fill(ReasonCode::RuleBlocked));
    }
    let fee = checked_mul(notional, fee_rate, "fill fee")?;
    let reserved_cash = if side == Side::Buy {
        checked_add(notional, fee, "reserved buy cash")?
    } else {
        Decimal::ZERO
    };
    let reason = ReasonCode::MarketFilled;
    Ok(ExecutionDecision {
        fill: Some(PlannedFill {
            side,
            price,
            qty: AssetQuantity::new(qty)?,
            notional: QuoteAmount::new(notional)?,
            fee: QuoteAmount::new(fee)?,
            fee_bps,
            reserved_cash: QuoteAmount::new(reserved_cash)?,
            price_cost_attribution,
            reason,
        }),
        reason,
    })
}

pub(super) fn plan_market_arrival(
    account: &Account,
    request: PlannedFill,
    execution_open: PriceKrw,
    costs: &CostPolicy,
    rules: &MarketRuleSnapshot,
    prior_volume: AssetQuantity,
    participation_cap: Weight,
) -> Result<ExecutionDecision, LabError> {
    let (price, price_cost_attribution) = market_price(execution_open, request.side, costs, rules)?;
    let fee_rate = bps_rate(request.fee_bps)?;
    let volume_cap = checked_mul(prior_volume.get(), participation_cap.get(), "volume cap")?;
    let balance_cap = match request.side {
        Side::Buy => {
            let per_unit_debit = checked_mul(
                price.get(),
                checked_add(Decimal::ONE, fee_rate, "fee multiplier")?,
                "arrival per-unit buy debit",
            )?;
            checked_div(
                request.reserved_cash.get(),
                per_unit_debit,
                "reservation-limited quantity",
            )?
        }
        Side::Sell => account.qty(),
    };
    let clipped = request.qty.get().min(volume_cap).min(balance_cap);
    let qty = floor_to_step(clipped, rules.quantity_step.get())?;
    if qty.is_zero() {
        let reason = if volume_cap < rules.quantity_step.get() {
            ReasonCode::VolumeCap
        } else if balance_cap < rules.quantity_step.get() {
            ReasonCode::CapitalBlocked
        } else {
            ReasonCode::QuantityRoundedToZero
        };
        return Ok(no_fill(reason));
    }
    let notional = checked_mul(qty, price.get(), "arrival notional")?;
    if notional < rules.min_notional.get() {
        return Ok(no_fill(ReasonCode::RuleBlocked));
    }
    let fee = checked_mul(notional, fee_rate, "arrival fee")?;
    let reserved_cash = if request.side == Side::Buy {
        checked_add(notional, fee, "arrival reserved cash")?
    } else {
        Decimal::ZERO
    };
    let reason = if qty < request.qty.get() {
        if volume_cap <= balance_cap {
            ReasonCode::VolumeCap
        } else {
            ReasonCode::CapitalBlocked
        }
    } else {
        ReasonCode::MarketFilled
    };
    Ok(ExecutionDecision {
        fill: Some(PlannedFill {
            side: request.side,
            price,
            qty: AssetQuantity::new(qty)?,
            notional: QuoteAmount::new(notional)?,
            fee: QuoteAmount::new(fee)?,
            fee_bps: request.fee_bps,
            reserved_cash: QuoteAmount::new(reserved_cash)?,
            price_cost_attribution,
            reason,
        }),
        reason,
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "passive request sizing keeps the checked post-fee equation auditable"
)]
pub(super) fn plan_passive_buy_request(
    account: &Account,
    target: Weight,
    decision_close: PriceKrw,
    offset_bps: BasisPoints,
    maker_fee_bps: BasisPoints,
    rules: &MarketRuleSnapshot,
) -> Result<ExecutionDecision, LabError> {
    let offset = bps_rate(offset_bps)?;
    let unrounded = checked_mul(
        decision_close.get(),
        checked_sub(Decimal::ONE, offset, "passive offset multiplier")?,
        "passive limit",
    )?;
    let tick = tick_for(unrounded, rules)?;
    let limit = round_to_tick(unrounded, tick, Side::Sell)?;
    let fee_rate = bps_rate(maker_fee_bps)?;
    let position_value = checked_mul(account.qty(), limit, "passive-limit position")?;
    let equity = checked_add(account.cash_free()?, position_value, "passive-limit equity")?;
    let numerator = checked_sub(
        checked_mul(target.get(), equity, "passive target post-trade equity")?,
        position_value,
        "passive buy target gap",
    )?;
    if numerator <= Decimal::ZERO {
        return Ok(no_fill(ReasonCode::NoAction));
    }
    let denominator = checked_add(
        Decimal::ONE,
        checked_mul(target.get(), fee_rate, "passive target fee term")?,
        "passive sizing denominator",
    )?;
    let desired_notional = checked_div(numerator, denominator, "passive target notional")?;
    let desired_qty = checked_div(desired_notional, limit, "passive target quantity")?;
    let per_unit_debit = checked_mul(
        limit,
        checked_add(Decimal::ONE, fee_rate, "passive fee multiplier")?,
        "passive per-unit debit",
    )?;
    let cash_cap = checked_div(account.cash_free()?, per_unit_debit, "passive cash cap")?;
    let qty = floor_to_step(desired_qty.min(cash_cap), rules.quantity_step.get())?;
    if qty.is_zero() {
        return Ok(no_fill(ReasonCode::QuantityRoundedToZero));
    }
    let notional = checked_mul(qty, limit, "passive requested notional")?;
    if notional < rules.min_notional.get() {
        return Ok(no_fill(ReasonCode::RuleBlocked));
    }
    let fee = checked_mul(notional, fee_rate, "passive requested fee")?;
    let reserved = checked_add(notional, fee, "passive reservation")?;
    Ok(ExecutionDecision {
        fill: Some(PlannedFill {
            side: Side::Buy,
            price: PriceKrw::new(limit)?,
            qty: AssetQuantity::new(qty)?,
            notional: QuoteAmount::new(notional)?,
            fee: QuoteAmount::new(fee)?,
            fee_bps: maker_fee_bps,
            reserved_cash: QuoteAmount::new(reserved)?,
            price_cost_attribution: checked_sub(
                limit,
                decision_close.get(),
                "passive price attribution",
            )?,
            reason: ReasonCode::MarketFilled,
        }),
        reason: ReasonCode::MarketFilled,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_passive_arrival(
    request: PlannedFill,
    bar_open: PriceKrw,
    bar_low: PriceKrw,
    prior_volume: AssetQuantity,
    participation_cap: Weight,
    fill_fraction: PassiveFraction,
    penetration_ticks: u32,
    rules: &MarketRuleSnapshot,
) -> Result<ExecutionDecision, LabError> {
    if bar_open.get() <= request.price.get() {
        return Ok(no_fill(ReasonCode::WouldTakeOrUnknown));
    }
    let tick = tick_for(request.price.get(), rules)?;
    let penetration = checked_mul(tick, Decimal::from(penetration_ticks), "penetration")?;
    let threshold = checked_sub(request.price.get(), penetration, "penetration threshold")?;
    if bar_low.get() >= threshold {
        return Ok(no_fill(ReasonCode::PassiveNotPenetrated));
    }
    let fraction_cap = checked_mul(request.qty.get(), fill_fraction.decimal(), "fill fraction")?;
    let volume_cap = checked_mul(prior_volume.get(), participation_cap.get(), "volume cap")?;
    let fee_rate = bps_rate(request.fee_bps)?;
    let per_unit_debit = checked_mul(
        request.price.get(),
        checked_add(Decimal::ONE, fee_rate, "passive fee multiplier")?,
        "passive fill debit",
    )?;
    let cash_cap = checked_div(
        request.reserved_cash.get(),
        per_unit_debit,
        "passive cash cap",
    )?;
    let qty = floor_to_step(
        fraction_cap.min(volume_cap).min(cash_cap),
        rules.quantity_step.get(),
    )?;
    if qty.is_zero() {
        return Ok(no_fill(ReasonCode::QuantityRoundedToZero));
    }
    let notional = checked_mul(qty, request.price.get(), "passive fill notional")?;
    if notional < rules.min_notional.get() {
        return Ok(no_fill(ReasonCode::RuleBlocked));
    }
    let fee = checked_mul(notional, fee_rate, "passive fill fee")?;
    Ok(ExecutionDecision {
        fill: Some(PlannedFill {
            side: Side::Buy,
            price: request.price,
            qty: AssetQuantity::new(qty)?,
            notional: QuoteAmount::new(notional)?,
            fee: QuoteAmount::new(fee)?,
            fee_bps: request.fee_bps,
            reserved_cash: QuoteAmount::new(checked_add(
                notional,
                fee,
                "passive fill reservation",
            )?)?,
            price_cost_attribution: request.price_cost_attribution,
            reason: if qty < request.qty.get() {
                ReasonCode::PassivePartial
            } else {
                ReasonCode::MarketFilled
            },
        }),
        reason: if qty < request.qty.get() {
            ReasonCode::PassivePartial
        } else {
            ReasonCode::MarketFilled
        },
    })
}

fn market_price(
    reference: PriceKrw,
    side: Side,
    costs: &CostPolicy,
    rules: &MarketRuleSnapshot,
) -> Result<(PriceKrw, Decimal), LabError> {
    let price_cost_bps = checked_add(
        checked_add(
            costs.half_spread_bps.get(),
            costs.slippage_bps.get(),
            "price costs",
        )?,
        costs.impact_bps.get(),
        "price costs",
    )?;
    let rate = checked_div(price_cost_bps, BPS_DENOMINATOR, "price cost rate")?;
    let multiplier = match side {
        Side::Buy => checked_add(Decimal::ONE, rate, "buy price multiplier")?,
        Side::Sell => checked_sub(Decimal::ONE, rate, "sell price multiplier")?,
    };
    let unrounded = checked_mul(reference.get(), multiplier, "cost-adjusted price")?;
    let tick = tick_for(unrounded, rules)?;
    let rounded = round_to_tick(unrounded, tick, side)?;
    let signed_attribution = match side {
        Side::Buy => checked_sub(rounded, reference.get(), "buy price attribution")?,
        Side::Sell => checked_sub(reference.get(), rounded, "sell price attribution")?,
    };
    Ok((PriceKrw::new(rounded)?, signed_attribution))
}

fn tick_for(value: Decimal, rules: &MarketRuleSnapshot) -> Result<Decimal, LabError> {
    let tick = rules
        .ticks
        .iter()
        .rev()
        .find(|band| value >= band.lower_bound.get())
        .ok_or_else(|| LabError::InvalidConfig("tick ladder does not cover price".into()))?
        .tick
        .get();
    Ok(tick)
}

fn round_to_tick(value: Decimal, tick: Decimal, side: Side) -> Result<Decimal, LabError> {
    if tick <= Decimal::ZERO {
        return Err(LabError::InvalidConfig("tick must be positive".into()));
    }
    let units = checked_div(value, tick, "price tick units")?;
    let rounded_units = units.round_dp_with_strategy(
        0,
        match side {
            Side::Buy => RoundingStrategy::ToPositiveInfinity,
            Side::Sell => RoundingStrategy::ToNegativeInfinity,
        },
    );
    checked_mul(rounded_units, tick, "tick-rounded price")
}

fn floor_to_step(value: Decimal, step: Decimal) -> Result<Decimal, LabError> {
    if step <= Decimal::ZERO {
        return Err(LabError::InvalidConfig(
            "quantity step must be positive".into(),
        ));
    }
    let units = checked_div(value, step, "quantity step units")?
        .round_dp_with_strategy(0, RoundingStrategy::ToNegativeInfinity);
    checked_mul(units, step, "step-rounded quantity")
}

fn bps_rate(bps: BasisPoints) -> Result<Decimal, LabError> {
    checked_div(bps.get(), BPS_DENOMINATOR, "basis-point rate")
}

fn no_fill(reason: ReasonCode) -> ExecutionDecision {
    ExecutionDecision { fill: None, reason }
}

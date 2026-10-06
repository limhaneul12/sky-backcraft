use crate::contracts::{
    AccountState, AssetQuantity, LabError, NUMERIC_TOLERANCE, PriceKrw, QuoteAmount, Side,
    SignedAmount,
};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub(super) struct Account {
    initial_cash: Decimal,
    cash_total: Decimal,
    cash_reserved: Decimal,
    qty: Decimal,
    price_basis: Decimal,
    gross_realized: Decimal,
    cumulative_fees: Decimal,
    peak_equity: Decimal,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct AppliedFill {
    pub(super) removed_basis: Decimal,
    pub(super) realized_price_pnl: Decimal,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MarkedAccount {
    pub(super) position_value: Decimal,
    pub(super) gross_unrealized: Decimal,
    pub(super) equity: Decimal,
    pub(super) actual_weight: Decimal,
    pub(super) peak_equity: Decimal,
    pub(super) drawdown: Decimal,
}

impl Account {
    pub(super) fn new(initial_cash: QuoteAmount) -> Self {
        let initial_cash = initial_cash.get();
        Self {
            initial_cash,
            cash_total: initial_cash,
            cash_reserved: Decimal::ZERO,
            qty: Decimal::ZERO,
            price_basis: Decimal::ZERO,
            gross_realized: Decimal::ZERO,
            cumulative_fees: Decimal::ZERO,
            peak_equity: initial_cash,
        }
    }

    pub(super) fn cash_free(&self) -> Result<Decimal, LabError> {
        checked_sub(self.cash_total, self.cash_reserved, "free cash")
    }

    pub(super) fn qty(&self) -> Decimal {
        self.qty
    }

    pub(super) fn reserve(&mut self, amount: QuoteAmount) -> Result<(), LabError> {
        if amount.get() > self.cash_free()? {
            return Err(invariant("reservation exceeds free cash"));
        }
        self.cash_reserved = checked_add(self.cash_reserved, amount.get(), "cash reservation")?;
        Ok(())
    }

    pub(super) fn release(&mut self, amount: QuoteAmount) -> Result<(), LabError> {
        if amount.get() > self.cash_reserved {
            return Err(invariant("release exceeds reserved cash"));
        }
        self.cash_reserved = checked_sub(self.cash_reserved, amount.get(), "cash release")?;
        Ok(())
    }

    pub(super) fn apply_fill(
        &mut self,
        side: Side,
        price: PriceKrw,
        qty: AssetQuantity,
        fee: QuoteAmount,
        reserved_cash: QuoteAmount,
    ) -> Result<AppliedFill, LabError> {
        let notional = checked_mul(price.get(), qty.get(), "fill notional")?;
        match side {
            Side::Buy => self.apply_buy(notional, qty.get(), fee.get(), reserved_cash.get()),
            Side::Sell => self.apply_sell(notional, qty.get(), fee.get(), reserved_cash.get()),
        }
    }

    fn apply_buy(
        &mut self,
        notional: Decimal,
        qty: Decimal,
        fee: Decimal,
        reserved_cash: Decimal,
    ) -> Result<AppliedFill, LabError> {
        let debit = checked_add(notional, fee, "buy debit")?;
        if reserved_cash != debit || reserved_cash > self.cash_reserved || debit > self.cash_total {
            return Err(invariant(
                "buy fill is not exactly cash-reserved or exceeds cash",
            ));
        }
        self.cash_total = checked_sub(self.cash_total, debit, "buy cash")?;
        self.cash_reserved = checked_sub(self.cash_reserved, reserved_cash, "filled reservation")?;
        self.qty = checked_add(self.qty, qty, "buy quantity")?;
        self.price_basis = checked_add(self.price_basis, notional, "buy price basis")?;
        self.cumulative_fees = checked_add(self.cumulative_fees, fee, "buy fee")?;
        self.validate()?;
        Ok(AppliedFill {
            removed_basis: Decimal::ZERO,
            realized_price_pnl: Decimal::ZERO,
        })
    }

    fn apply_sell(
        &mut self,
        notional: Decimal,
        qty: Decimal,
        fee: Decimal,
        reserved_cash: Decimal,
    ) -> Result<AppliedFill, LabError> {
        if !reserved_cash.is_zero() || qty > self.qty {
            return Err(invariant(
                "sell fill exceeds inventory or reserves quote cash",
            ));
        }
        let prior_qty = self.qty;
        let removed_basis = if qty == prior_qty {
            self.price_basis
        } else {
            checked_mul(
                self.price_basis,
                checked_div(qty, prior_qty, "sold quantity fraction")?,
                "removed price basis",
            )?
        };
        let proceeds = checked_sub(notional, fee, "net sell proceeds")?;
        self.cash_total = checked_add(self.cash_total, proceeds, "sell cash")?;
        self.qty = checked_sub(self.qty, qty, "sell quantity")?;
        self.price_basis = checked_sub(self.price_basis, removed_basis, "remaining price basis")?;
        if self.qty.is_zero() {
            self.price_basis = Decimal::ZERO;
        }
        let realized = checked_sub(notional, removed_basis, "gross realized price pnl")?;
        self.gross_realized =
            checked_add(self.gross_realized, realized, "cumulative realized pnl")?;
        self.cumulative_fees = checked_add(self.cumulative_fees, fee, "sell fee")?;
        self.validate()?;
        Ok(AppliedFill {
            removed_basis,
            realized_price_pnl: realized,
        })
    }

    pub(super) fn mark(&mut self, price: PriceKrw) -> Result<MarkedAccount, LabError> {
        let position_value = checked_mul(self.qty, price.get(), "position value")?;
        let gross_unrealized = checked_sub(position_value, self.price_basis, "gross unrealized")?;
        let equity = checked_add(self.cash_total, position_value, "equity")?;
        let expected_change = checked_sub(
            checked_add(self.gross_realized, gross_unrealized, "gross pnl")?,
            self.cumulative_fees,
            "net pnl",
        )?;
        let actual_change = checked_sub(equity, self.initial_cash, "equity change")?;
        if !aggregate_identity_within_tolerance(actual_change, expected_change)? {
            let residual = checked_sub(actual_change, expected_change, "equity identity residual")?;
            return Err(invariant(format!(
                "equity identity does not reconcile: actual_change={actual_change}, expected_change={expected_change}, residual={residual}, initial_cash={}, cash_total={}, qty={}, mark_price={}, price_basis={}, gross_realized={}, gross_unrealized={gross_unrealized}, cumulative_fees={}",
                self.initial_cash,
                self.cash_total,
                self.qty,
                price.get(),
                self.price_basis,
                self.gross_realized,
                self.cumulative_fees,
            )));
        }
        self.peak_equity = self.peak_equity.max(equity);
        let actual_weight = if equity.is_zero() {
            Decimal::ZERO
        } else {
            checked_div(position_value, equity, "actual weight")?
        };
        let drawdown = if self.peak_equity.is_zero() {
            Decimal::ZERO
        } else {
            checked_div(
                checked_sub(self.peak_equity, equity, "drawdown amount")?,
                self.peak_equity,
                "drawdown",
            )?
        };
        Ok(MarkedAccount {
            position_value,
            gross_unrealized,
            equity,
            actual_weight,
            peak_equity: self.peak_equity,
            drawdown,
        })
    }

    pub(super) fn state(&self) -> Result<AccountState, LabError> {
        Ok(AccountState {
            cash_total: QuoteAmount::new(self.cash_total)?,
            cash_free: QuoteAmount::new(self.cash_free()?)?,
            cash_reserved: QuoteAmount::new(self.cash_reserved)?,
            qty: AssetQuantity::new(self.qty)?,
            price_basis: QuoteAmount::new(self.price_basis)?,
            gross_realized: SignedAmount::new(self.gross_realized)?,
            cumulative_fees: QuoteAmount::new(self.cumulative_fees)?,
        })
    }

    fn validate(&self) -> Result<(), LabError> {
        if self.cash_total < Decimal::ZERO
            || self.cash_reserved < Decimal::ZERO
            || self.cash_reserved > self.cash_total
            || self.qty < Decimal::ZERO
            || self.price_basis < Decimal::ZERO
            || (self.qty.is_zero() != self.price_basis.is_zero())
        {
            return Err(invariant(
                "account contains negative or inconsistent balances",
            ));
        }
        Ok(())
    }
}

pub(super) fn aggregate_identity_within_tolerance(
    actual: Decimal,
    expected: Decimal,
) -> Result<bool, LabError> {
    Ok(checked_sub(actual, expected, "aggregate identity residual")?.abs() <= NUMERIC_TOLERANCE)
}

// The checked arithmetic authority lives in `contracts::checked`; these
// aliases keep the engine call sites and error labels unchanged.
pub(crate) use crate::contracts::checked::{
    add as checked_add, div as checked_div, mul as checked_mul, sub as checked_sub,
};

fn invariant(message: impl Into<String>) -> LabError {
    LabError::AccountingInvariant(message.into())
}

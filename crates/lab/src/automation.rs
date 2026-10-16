//! Durable, causal strategy automation over one frozen policy revision.
//!
//! This runtime is intentionally separate from historical replay and
//! simulated-paper sessions. Persistent application state remains owned by
//! `DatabaseOwner`; venue effects cross `VenueAdapter` only after a durable
//! intent and risk reservation have committed.

use crate::contracts::{
    AssetQuantity, AutomationFillView, AutomationOrderIdentifier, AutomationOrderKind,
    AutomationOrderRequest, AutomationOrderState, AutomationSessionId, AutomationSessionMode,
    AutomationSessionView, AutomationSide, LabError, MarketId, PriceKrw, QuoteAmount, UtcTimestamp,
};
use crate::contracts::{CandleInterval, CandleObservation, FrozenPolicyRevision};
use crate::policy_engine::PolicyEvaluator;
use crate::strategy::{PositionView, StrategyEvaluation};
use crate::upbit::client::{ClientError, UpbitAccountBalance, UpbitOrderChance, UpbitOrderDetail};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;

mod mock_venue;
pub(crate) use mock_venue::PersistentMockVenue;

pub(crate) type VenueFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ClientError>> + Send + 'a>>;

/// Object-safe asynchronous exchange boundary shared by production and the
/// durable deterministic mock venue.
pub(crate) trait VenueAdapter: Send + Sync {
    fn is_live(&self) -> bool;
    fn account_identity(&self) -> String;

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>>;

    fn order_chance<'a>(&'a self, market: &'a MarketId) -> VenueFuture<'a, UpbitOrderChance>;

    /// `decision_mark` is for deterministic mock settlement only. Production
    /// adapters must omit it from the exchange request body.
    fn submit_order<'a>(
        &'a self,
        request: &'a AutomationOrderRequest,
        decision_mark: PriceKrw,
        send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail>;

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>>;

    fn cancel_order<'a>(&'a self, uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerifiedMarketRules {
    pub market: String,
    pub source_mode: AutomationSessionMode,
    pub observed_at_utc: UtcTimestamp,
    pub state: String,
    pub bid_types: Vec<String>,
    pub ask_types: Vec<String>,
    pub bid_fee: Decimal,
    pub ask_fee: Decimal,
    pub bid_min_total: Decimal,
    pub ask_min_total: Decimal,
    pub max_total: Decimal,
    pub bid_price_unit: Option<Decimal>,
    pub ask_price_unit: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub(crate) struct AutomationOrderAdmission {
    pub session_id: crate::contracts::AutomationSessionId,
    pub intent_id: crate::contracts::AutomationIntentId,
    pub request: AutomationOrderRequest,
    pub decision_mark: PriceKrw,
    pub decision_time: UtcTimestamp,
    pub requested_notional: QuoteAmount,
    pub quote_reserve: QuoteAmount,
    pub asset_currency: String,
    pub asset_reserve: AssetQuantity,
    pub fee_reserve: QuoteAmount,
    pub rules: VerifiedMarketRules,
    pub evaluation_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub(crate) struct AutomationOrderDispatch {
    pub request: AutomationOrderRequest,
    pub decision_mark: PriceKrw,
}

#[derive(Debug, Clone)]
pub(crate) struct AutomationCancelPreparation {
    pub response: crate::contracts::AutomationSessionCommandResponse,
    pub venue_uuid: Option<String>,
    /// True only for the transaction that first persisted the cancel claim.
    /// Replayed pending requests must read by identifier instead of sending again.
    pub should_send: bool,
}

/// Durable strategy decision that has not yet been reserved as an order.
/// Recovery reuses this exact evaluation and source candle after fresh current
/// account/rule checks; it never reevaluates history or creates a new intent.
#[derive(Debug, Clone)]
pub(crate) struct AutomationPendingIntent {
    pub session_id: crate::contracts::AutomationSessionId,
    pub observation_id: crate::contracts::ObservationId,
    pub intent_id: crate::contracts::AutomationIntentId,
    pub action: String,
    pub decision_time: UtcTimestamp,
    pub evaluation_json: serde_json::Value,
    pub observation: CandleObservation,
    pub decision_mark: PriceKrw,
}

/// Local lifecycle command admitted before a crash but not durably finalized.
/// Replaying this exact payload is safe because these actions do not call the venue.
#[derive(Debug, Clone)]
pub(crate) struct AutomationPendingLifecycleRequest {
    pub write: crate::contracts::AutomationSessionWrite,
    pub account_identity: Option<String>,
}

/// A cancellation's durable command plus whether its no-resend claim exists.
#[derive(Debug, Clone)]
pub(crate) struct AutomationPendingCancelRequest {
    pub write: crate::contracts::AutomationSessionWrite,
    pub claimed: bool,
}

/// Runtime input recovered from the durable session row.
#[derive(Debug, Clone)]
pub(crate) struct AutomationRuntimeRecord {
    pub view: AutomationSessionView,
    pub frozen_policy: FrozenPolicyRevision,
    pub account_identity: String,
    pub checkpoint_json: String,
}

/// Stable keyset cursor for the bounded coordinator's due-session scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AutomationDueSessionCursor {
    pub updated_at_utc: UtcTimestamp,
    pub session_id: AutomationSessionId,
}

/// One fair, bounded coordinator page. Empty pages signal the caller to wrap to
/// the beginning; `total_count` describes the current due set for observability.
#[derive(Debug, Clone)]
pub(crate) struct AutomationDueSessionPage {
    pub sessions: Vec<AutomationRuntimeRecord>,
    pub total_count: u64,
    pub next_cursor: Option<AutomationDueSessionCursor>,
}

/// Nonterminal local order row presented to the bounded coordinator.
#[derive(Debug, Clone)]
pub(crate) struct AutomationPendingOrder {
    pub session_id: AutomationSessionId,
    pub account_identity: String,
    pub state: AutomationOrderState,
    pub request: AutomationOrderRequest,
    pub decision_mark: PriceKrw,
}

/// Stream checkpoint persisted after every accepted completed candle.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct AutomationCheckpoint {
    pub evaluator: PolicyEvaluator,
    pub last_observation: Option<CandleObservation>,
    pub episode_opened_at: Option<UtcTimestamp>,
    pub held_decision_bars: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct AutomationAccountPosition {
    pub actual_qty: AssetQuantity,
    pub actual_weight: crate::contracts::Weight,
    pub equity_krw: QuoteAmount,
    pub episode_opened_at: Option<UtcTimestamp>,
    pub account_observed_at: UtcTimestamp,
}

impl AutomationCheckpoint {
    /// Compile the selected immutable policy revision for one market stream.
    /// # Errors
    /// Rejects invalid frozen policy identity or unsupported evaluator configuration.
    pub(crate) fn compile(
        revision: &FrozenPolicyRevision,
        market: crate::contracts::MarketId,
        interval: CandleInterval,
    ) -> Result<Self, LabError> {
        Ok(Self {
            evaluator: PolicyEvaluator::compile_frozen_revision(revision, market, interval)?,
            last_observation: None,
            episode_opened_at: None,
            held_decision_bars: 0,
        })
    }

    pub(crate) fn observe(
        &mut self,
        candle: &CandleObservation,
        position: PositionView,
    ) -> Result<Option<StrategyEvaluation>, LabError> {
        self.evaluator.observe(candle, position, None)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketChanceRules {
    id: String,
    name: String,
    #[serde(default)]
    tick_size_source: Option<String>,
    #[serde(default)]
    order_types: Option<Vec<String>>,
    order_sides: Vec<String>,
    bid_types: Vec<String>,
    ask_types: Vec<String>,
    bid: MarketSideRules,
    ask: MarketSideRules,
    max_total: Decimal,
    state: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketSideRules {
    currency: String,
    #[serde(default)]
    price_unit: Option<Decimal>,
    min_total: Decimal,
}

/// Parse and validate the exchange's current per-market execution constraints.
/// Deprecated `market.order_types` is accepted for wire compatibility but never
/// used as the source of supported order types. Live rules also require the
/// adapter's current `orderbook/instruments` provenance marker; mock rules may
/// omit it because their tick rules are explicitly simulated.
///
/// # Errors
/// Rejects inactive markets, missing current side/type rules, unsupported market
/// order types, malformed fee/min/max amounts, or a market identity mismatch.
pub(crate) fn verify_market_rules(
    market: &MarketId,
    chance: &UpbitOrderChance,
    source_mode: AutomationSessionMode,
    observed_at_utc: UtcTimestamp,
) -> Result<VerifiedMarketRules, LabError> {
    let rules: MarketChanceRules =
        serde_json::from_value(chance.market.clone()).map_err(|error| {
            LabError::DataCorrupt(format!("malformed Upbit market rule response: {error}"))
        })?;
    let expected_market = market.code();
    let tick_size_source_is_verified = match source_mode {
        AutomationSessionMode::Live => {
            rules.tick_size_source.as_deref() == Some("UPBIT_ORDERBOOK_INSTRUMENTS")
        }
        AutomationSessionMode::Mock => rules
            .tick_size_source
            .as_deref()
            .is_none_or(|source| source == "UPBIT_ORDERBOOK_INSTRUMENTS"),
    };
    if rules.id != expected_market
        || !tick_size_source_is_verified
        || rules.name.is_empty()
        || rules.name.len() > 64
        || rules.state != "active"
        || !rules.order_sides.iter().any(|side| side == "bid")
        || !rules.order_sides.iter().any(|side| side == "ask")
        || !rules.bid_types.iter().any(|kind| kind == "price")
        || !rules.ask_types.iter().any(|kind| kind == "market")
        || rules.bid.currency != "KRW"
        || rules.ask.currency != market.base.code()
        || rules.bid_types.len() > 32
        || rules.ask_types.len() > 32
        || rules
            .order_types
            .as_ref()
            .is_some_and(|types| types.len() > 32)
        || chance.bid_fee < Decimal::ZERO
        || chance.ask_fee < Decimal::ZERO
        || chance.bid_fee > Decimal::new(1, 1)
        || chance.ask_fee > Decimal::new(1, 1)
        || rules.bid.min_total <= Decimal::ZERO
        || rules.ask.min_total <= Decimal::ZERO
        || rules.max_total <= Decimal::ZERO
        || rules.max_total < rules.bid.min_total
        || rules.max_total < rules.ask.min_total
        || rules
            .bid
            .price_unit
            .is_some_and(|unit| unit <= Decimal::ZERO)
        || rules
            .ask
            .price_unit
            .is_some_and(|unit| unit <= Decimal::ZERO)
    {
        return Err(LabError::UnverifiedMarketRules(format!(
            "current Upbit market constraints do not support automation for {expected_market}"
        )));
    }
    Ok(VerifiedMarketRules {
        market: expected_market,
        source_mode,
        observed_at_utc,
        state: rules.state,
        bid_types: rules.bid_types,
        ask_types: rules.ask_types,
        bid_fee: chance.bid_fee,
        ask_fee: chance.ask_fee,
        bid_min_total: rules.bid.min_total,
        ask_min_total: rules.ask.min_total,
        max_total: rules.max_total,
        bid_price_unit: rules.bid.price_unit,
        ask_price_unit: rules.ask.price_unit,
    })
}

#[derive(Debug)]
pub(crate) struct ValidatedOrderUpdate {
    pub state: AutomationOrderState,
    pub venue_uuid: String,
    pub executed_volume: AssetQuantity,
    pub remaining_volume: Option<AssetQuantity>,
    pub reserved_fee: QuoteAmount,
    pub remaining_fee: QuoteAmount,
    pub paid_fee: QuoteAmount,
    pub locked_amount: Decimal,
    pub locked_currency: String,
    pub trade_count: u64,
    pub fills: Vec<AutomationFillView>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VenueTrade {
    market: String,
    uuid: String,
    price: Decimal,
    volume: Decimal,
    funds: Decimal,
    created_at: String,
    side: String,
    #[serde(default)]
    trend: Option<String>,
}

/// Validate the order identity returned by `POST /v1/orders` before issuing `GET /v1/order`.
///
/// `POST /v1/orders` only proves acceptance of the order identity (`uuid`, `identifier`,
/// `market`, `side`, `ord_type`). It never proves execution or fill settlement.
pub(crate) fn validate_submit_ack_identity(
    request: &AutomationOrderRequest,
    acknowledgement: &UpbitOrderDetail,
) -> Result<(), LabError> {
    let expected_side = match request.side {
        AutomationSide::Bid => "bid",
        AutomationSide::Ask => "ask",
    };
    let expected_type = match &request.order {
        AutomationOrderKind::Limit { .. } => "limit",
        AutomationOrderKind::MarketBuy { .. } => "price",
        AutomationOrderKind::MarketSell { .. } => "market",
    };
    if acknowledgement.identifier.as_deref() != Some(request.client_identifier.as_str())
        || acknowledgement.uuid.is_empty()
        || acknowledgement.uuid.len() > 128
        || acknowledgement.market != request.market.code()
        || acknowledgement.side != expected_side
        || acknowledgement.ord_type != expected_type
    {
        return Err(LabError::DataCorrupt(
            "venue submit acknowledgement identity differs from the claimed order".into(),
        ));
    }
    Ok(())
}

/// Validate one complete venue readback before any order or fill fact is applied.
///
/// This deliberately treats an absent identifier, omitted accounting field,
/// trade-count mismatch, malformed fill, or future timestamp as unresolved data.
///
/// # Errors
/// Returns `DataCorrupt` for inconsistent or economically incompatible venue facts.
#[allow(clippy::too_many_lines)] // Every venue total and constituent trade is validated before any fill enters the durable ledger.
pub(crate) fn validate_order_update(
    request: &AutomationOrderRequest,
    detail: &UpbitOrderDetail,
    now: UtcTimestamp,
) -> Result<ValidatedOrderUpdate, LabError> {
    request.validate()?;
    let expected_side = match request.side {
        AutomationSide::Bid => "bid",
        AutomationSide::Ask => "ask",
    };
    let expected_type = match request.order {
        AutomationOrderKind::Limit { .. } => "limit",
        AutomationOrderKind::MarketBuy { .. } => "price",
        AutomationOrderKind::MarketSell { .. } => "market",
    };
    if detail.identifier.as_deref() != Some(request.client_identifier.as_str())
        || detail.uuid.is_empty()
        || detail.uuid.len() > 128
        || detail.market != request.market.code()
        || detail.side != expected_side
        || detail.ord_type != expected_type
    {
        return Err(LabError::DataCorrupt(
            "venue order identity, market, side, or type differs from durable intent".into(),
        ));
    }
    let order_created = parse_venue_timestamp(&detail.created_at)?;
    if order_created > now {
        return Err(LabError::DataCorrupt(
            "venue order creation timestamp is in the future".into(),
        ));
    }

    match &request.order {
        AutomationOrderKind::Limit { volume, price } => {
            if detail.volume != Some(volume.get()) || detail.price != Some(price.get()) {
                return Err(LabError::DataCorrupt(
                    "venue limit order amounts differ from durable intent".into(),
                ));
            }
        }
        AutomationOrderKind::MarketBuy { budget } => {
            if detail.price != Some(budget.get()) {
                return Err(LabError::DataCorrupt(
                    "venue market-buy budget differs from durable intent".into(),
                ));
            }
        }
        AutomationOrderKind::MarketSell { volume } => {
            if detail.volume != Some(volume.get()) || detail.price.is_some() {
                return Err(LabError::DataCorrupt(
                    "venue market-sell volume differs from durable intent".into(),
                ));
            }
        }
    }

    let parse_nonnegative = |value: Option<Decimal>, name: &str| {
        let value = value
            .ok_or_else(|| LabError::DataCorrupt(format!("venue order omitted required {name}")))?;
        if value < Decimal::ZERO {
            return Err(LabError::DataCorrupt(format!(
                "venue order {name} is negative"
            )));
        }
        Ok(value)
    };
    let executed = parse_nonnegative(detail.executed_volume, "executed_volume")?;
    let remaining = detail
        .remaining_volume
        .map(|value| parse_nonnegative(Some(value), "remaining_volume"))
        .transpose()?;
    if remaining.is_none() && !matches!(&request.order, AutomationOrderKind::MarketBuy { .. }) {
        return Err(LabError::DataCorrupt(
            "venue omitted remaining_volume for an order with a known requested volume".into(),
        ));
    }
    let reserved_fee = parse_nonnegative(detail.reserved_fee, "reserved_fee")?;
    let remaining_fee = parse_nonnegative(detail.remaining_fee, "remaining_fee")?;
    let paid_fee = parse_nonnegative(detail.paid_fee, "paid_fee")?;
    let locked = parse_nonnegative(detail.locked, "locked")?;
    let total_fee = paid_fee.checked_add(remaining_fee).ok_or_else(|| {
        LabError::DataCorrupt("venue cumulative fee exceeds decimal capacity".into())
    })?;
    match request.side {
        AutomationSide::Bid
            if paid_fee > reserved_fee
                || remaining_fee > reserved_fee
                || total_fee > reserved_fee =>
        {
            return Err(LabError::DataCorrupt(
                "venue buy fee totals exceed the originally reserved fee".into(),
            ));
        }
        AutomationSide::Ask if reserved_fee != Decimal::ZERO || remaining_fee != Decimal::ZERO => {
            return Err(LabError::DataCorrupt(
                "venue sell fee reserve must be zero; sell fees are paid from proceeds".into(),
            ));
        }
        _ => {}
    }

    let mut trade_ids = std::collections::BTreeSet::new();
    let mut fills = Vec::with_capacity(detail.trades.len());
    let mut trade_volume = Decimal::ZERO;
    let mut trade_funds = Decimal::ZERO;
    for raw in &detail.trades {
        let trade: VenueTrade = serde_json::from_value(raw.clone()).map_err(|error| {
            LabError::DataCorrupt(format!("malformed venue trade detail: {error}"))
        })?;
        if trade.uuid.is_empty()
            || trade.uuid.len() > 128
            || !trade
                .uuid
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_:".contains(&byte))
            || !trade_ids.insert(trade.uuid.clone())
            || trade.market != request.market.code()
            || trade.side != expected_side
            || trade.volume <= Decimal::ZERO
            || trade.price <= Decimal::ZERO
            || trade.funds <= Decimal::ZERO
            || trade.price.checked_mul(trade.volume) != Some(trade.funds)
        {
            return Err(LabError::DataCorrupt(
                "venue trade identity, currency, side, or exact amount is invalid".into(),
            ));
        }
        if trade
            .trend
            .as_ref()
            .is_some_and(|trend| !matches!(trend.as_str(), "up" | "down" | "even"))
        {
            return Err(LabError::DataCorrupt(
                "venue trade trend has an unsupported value".into(),
            ));
        }
        if let AutomationOrderKind::Limit { price, .. } = &request.order {
            let violates_limit = match request.side {
                AutomationSide::Bid => trade.price > price.get(),
                AutomationSide::Ask => trade.price < price.get(),
            };
            if violates_limit {
                return Err(LabError::DataCorrupt(
                    "venue fill price violates the durable limit-order price".into(),
                ));
            }
        }
        let created_at = parse_venue_timestamp(&trade.created_at)?;
        if created_at > now || created_at < order_created {
            return Err(LabError::DataCorrupt(
                "venue trade timestamp is future-dated or precedes its order".into(),
            ));
        }
        trade_volume = trade_volume.checked_add(trade.volume).ok_or_else(|| {
            LabError::DataCorrupt("venue executed volume exceeds decimal capacity".into())
        })?;
        trade_funds = trade_funds.checked_add(trade.funds).ok_or_else(|| {
            LabError::DataCorrupt("venue cumulative funds exceed decimal capacity".into())
        })?;
        fills.push(AutomationFillView {
            trade_uuid: trade.uuid,
            client_identifier: request.client_identifier.clone(),
            market: request.market.clone(),
            side: request.side,
            price_krw: PriceKrw::new(trade.price)?,
            volume: AssetQuantity::new(trade.volume)?,
            funds_krw: QuoteAmount::new(trade.funds)?,
            observed_at_utc: created_at,
        });
    }
    let trade_count = detail
        .trades_count
        .ok_or_else(|| LabError::DataCorrupt("venue order omitted required trades_count".into()))?;
    if usize::try_from(trade_count).ok() != Some(detail.trades.len()) || trade_volume != executed {
        return Err(LabError::DataCorrupt(
            "venue order fill count or cumulative volume differs from trade details".into(),
        ));
    }
    if request.side == AutomationSide::Ask && executed == Decimal::ZERO && paid_fee != Decimal::ZERO
    {
        return Err(LabError::DataCorrupt(
            "venue sell fee was paid without an executed trade".into(),
        ));
    }
    validate_cumulative_trade_funds(request, executed, trade_funds)?;

    let max_volume = match request.order {
        AutomationOrderKind::Limit { volume, .. } | AutomationOrderKind::MarketSell { volume } => {
            Some(volume.get())
        }
        AutomationOrderKind::MarketBuy { .. } => None,
    };
    if max_volume.is_some_and(|requested| {
        remaining.is_none_or(|remaining| executed.checked_add(remaining) != Some(requested))
    }) {
        return Err(LabError::DataCorrupt(
            "venue executed plus remaining volume differs from requested volume".into(),
        ));
    }

    let state = match detail.state.as_str() {
        "wait" | "watch" => {
            if executed > Decimal::ZERO {
                AutomationOrderState::PartiallyFilled
            } else {
                AutomationOrderState::Open
            }
        }
        "done" => {
            if executed <= Decimal::ZERO || remaining.is_some_and(|value| value != Decimal::ZERO) {
                return Err(LabError::DataCorrupt(
                    "venue marked an order done without complete positive execution".into(),
                ));
            }
            AutomationOrderState::Filled
        }
        "cancel" => AutomationOrderState::Cancelled,
        other => {
            return Err(LabError::DataCorrupt(format!(
                "venue returned unknown order state {other}"
            )));
        }
    };

    Ok(ValidatedOrderUpdate {
        state,
        venue_uuid: detail.uuid.clone(),
        executed_volume: AssetQuantity::new(executed)?,
        remaining_volume: remaining.map(AssetQuantity::new).transpose()?,
        reserved_fee: QuoteAmount::new(reserved_fee)?,
        remaining_fee: QuoteAmount::new(remaining_fee)?,
        paid_fee: QuoteAmount::new(paid_fee)?,
        locked_amount: locked,
        locked_currency: match request.side {
            AutomationSide::Bid => "KRW".into(),
            AutomationSide::Ask => request.market.base.code().to_owned(),
        },
        trade_count,
        fills,
    })
}

fn validate_cumulative_trade_funds(
    request: &AutomationOrderRequest,
    executed_volume: Decimal,
    trade_funds: Decimal,
) -> Result<(), LabError> {
    match &request.order {
        AutomationOrderKind::MarketBuy { budget } if trade_funds > budget.get() => {
            Err(LabError::DataCorrupt(
                "venue market-buy trades exceed the persisted quote budget".into(),
            ))
        }
        AutomationOrderKind::Limit { price, .. } => {
            let executed_limit_value =
                executed_volume.checked_mul(price.get()).ok_or_else(|| {
                    LabError::DataCorrupt("limit fill value exceeds decimal capacity".into())
                })?;
            match request.side {
                AutomationSide::Bid => {
                    if trade_funds > executed_limit_value {
                        return Err(LabError::DataCorrupt(
                            "venue bid-limit fills exceed the exact limit-price value".into(),
                        ));
                    }
                }
                AutomationSide::Ask => {
                    if trade_funds < executed_limit_value {
                        return Err(LabError::DataCorrupt(
                            "venue ask-limit fills are below the exact minimum limit-price value"
                                .into(),
                        ));
                    }
                }
            }
            Ok(())
        }
        AutomationOrderKind::MarketBuy { .. } | AutomationOrderKind::MarketSell { .. } => Ok(()),
    }
}

fn parse_venue_timestamp(value: &str) -> Result<UtcTimestamp, LabError> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| LabError::DataCorrupt(format!("invalid venue timestamp: {error}")))?;
    Ok(UtcTimestamp(parsed))
}

#[cfg(test)]
mod order_update_tests {
    use super::*;
    use rust_decimal::Decimal;
    use serde_json::json;

    #[test]
    fn live_market_rules_require_orderbook_instrument_provenance() -> Result<(), LabError> {
        let market = MarketId::parse_upbit("KRW-BTC")?;
        let observed_at = UtcTimestamp::parse_rfc3339("2026-10-14T02:00:00Z")?;
        let make_chance = |tick_size_source: Option<&str>| {
            let mut market_json = json!({
                "id":"KRW-BTC",
                "name":"BTC/KRW",
                "order_sides":["bid","ask"],
                "bid_types":["limit","price"],
                "ask_types":["limit","market"],
                "bid":{"currency":"KRW","min_total":"5000","price_unit":"1"},
                "ask":{"currency":"BTC","min_total":"5000","price_unit":"1"},
                "max_total":"1000000000",
                "state":"active"
            });
            if let Some(source) = tick_size_source {
                market_json["tick_size_source"] = json!(source);
            }
            UpbitOrderChance {
                bid_fee: Decimal::new(5, 4),
                ask_fee: Decimal::new(5, 4),
                maker_bid_fee: None,
                maker_ask_fee: None,
                market: market_json,
                bid_account: None,
                ask_account: None,
            }
        };

        assert!(
            verify_market_rules(
                &market,
                &make_chance(None),
                AutomationSessionMode::Live,
                observed_at,
            )
            .is_err()
        );
        assert!(
            verify_market_rules(
                &market,
                &make_chance(None),
                AutomationSessionMode::Mock,
                observed_at,
            )
            .is_ok()
        );
        assert!(
            verify_market_rules(
                &market,
                &make_chance(Some("UPBIT_ORDERBOOK_INSTRUMENTS")),
                AutomationSessionMode::Live,
                observed_at,
            )
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn market_buy_rejects_unique_trade_funds_over_the_persisted_budget() -> Result<(), LabError> {
        let intent_id = crate::contracts::AutomationIntentId::from_seed("buy-budget-bound");
        let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
        let request = AutomationOrderRequest {
            client_identifier: identifier.clone(),
            market: MarketId::parse_upbit("KRW-BTC")?,
            side: AutomationSide::Bid,
            order: AutomationOrderKind::MarketBuy {
                budget: QuoteAmount::new(Decimal::from(9_000))?,
            },
        };
        let mut detail = UpbitOrderDetail {
            uuid: "market-buy-uuid".into(),
            identifier: Some(identifier.as_str().into()),
            side: "bid".into(),
            ord_type: "price".into(),
            price: Some(Decimal::from(9_000)),
            state: "done".into(),
            market: "KRW-BTC".into(),
            created_at: "2026-10-14T02:00:00Z".into(),
            volume: Some(Decimal::from(9)),
            remaining_volume: Some(Decimal::ZERO),
            reserved_fee: Some(Decimal::new(45, 1)),
            remaining_fee: Some(Decimal::ZERO),
            paid_fee: Some(Decimal::new(45, 1)),
            locked: Some(Decimal::ZERO),
            executed_volume: Some(Decimal::from(9)),
            trades_count: Some(1),
            trades: vec![json!({
                "market":"KRW-BTC","uuid":"market-buy-trade","price":"1000",
                "volume":"9","funds":"9000","trend":"even",
                "created_at":"2026-10-14T02:00:01Z","side":"bid"
            })],
        };
        assert_eq!(
            validate_order_update(
                &request,
                &detail,
                UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
            )?
            .state,
            AutomationOrderState::Filled
        );

        detail.volume = Some(Decimal::from(10));
        detail.executed_volume = Some(Decimal::from(10));
        detail.trades[0]["volume"] = json!("10");
        detail.trades[0]["funds"] = json!("10000");
        assert!(matches!(
            validate_order_update(
                &request,
                &detail,
                UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
            ),
            Err(LabError::DataCorrupt(message)) if message.contains("quote budget")
        ));
        Ok(())
    }

    #[test]
    fn limit_trade_price_cannot_cross_the_side_specific_bound() -> Result<(), LabError> {
        for (side, wire_side, trade_price) in [
            (AutomationSide::Bid, "bid", Decimal::from(10_001)),
            (AutomationSide::Ask, "ask", Decimal::from(9_999)),
        ] {
            let intent_id = crate::contracts::AutomationIntentId::from_seed(wire_side);
            let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
            let request = AutomationOrderRequest {
                client_identifier: identifier.clone(),
                market: MarketId::parse_upbit("KRW-BTC")?,
                side,
                order: AutomationOrderKind::Limit {
                    volume: AssetQuantity::new(Decimal::from(2))?,
                    price: PriceKrw::new(Decimal::from(10_000))?,
                },
            };
            let funds = trade_price;
            let detail = UpbitOrderDetail {
                uuid: format!("limit-{wire_side}-uuid"),
                identifier: Some(identifier.as_str().into()),
                side: wire_side.into(),
                ord_type: "limit".into(),
                price: Some(Decimal::from(10_000)),
                state: "wait".into(),
                market: "KRW-BTC".into(),
                created_at: "2026-10-14T02:00:00Z".into(),
                volume: Some(Decimal::from(2)),
                remaining_volume: Some(Decimal::ONE),
                reserved_fee: Some(if side == AutomationSide::Bid {
                    Decimal::from(10)
                } else {
                    Decimal::ZERO
                }),
                remaining_fee: Some(if side == AutomationSide::Bid {
                    Decimal::from(10)
                } else {
                    Decimal::ZERO
                }),
                paid_fee: Some(Decimal::ZERO),
                locked: Some(if side == AutomationSide::Bid {
                    Decimal::from(20_010)
                } else {
                    Decimal::from(10_000)
                }),
                executed_volume: Some(Decimal::ONE),
                trades_count: Some(1),
                trades: vec![json!({
                    "market":"KRW-BTC","uuid":format!("limit-{wire_side}-trade"),
                    "price":trade_price.to_string(),"volume":"1","funds":funds.to_string(),
                    "trend":"even","created_at":"2026-10-14T02:00:01Z","side":wire_side
                })],
            };
            assert!(matches!(
                validate_order_update(
                    &request,
                    &detail,
                    UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
                ),
                Err(LabError::DataCorrupt(message)) if message.contains("limit-order price")
            ));
        }
        Ok(())
    }

    #[test]
    fn official_sell_readback_allows_proceeds_paid_fee_without_fee_reserve() -> Result<(), LabError>
    {
        let intent_id = crate::contracts::AutomationIntentId::from_seed("ask-zero-fee-reserve");
        let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
        let request = AutomationOrderRequest {
            client_identifier: identifier.clone(),
            market: MarketId::parse_upbit("KRW-BTC")?,
            side: AutomationSide::Ask,
            order: AutomationOrderKind::MarketSell {
                volume: AssetQuantity::new(Decimal::ONE)?,
            },
        };
        let detail: UpbitOrderDetail = serde_json::from_value(json!({
            "uuid":"upbit-market-sell",
            "identifier":identifier.as_str(),
            "side":"ask",
            "ord_type":"market",
            "price":null,
            "state":"done",
            "market":"KRW-BTC",
            "created_at":"2026-10-14T02:00:00Z",
            "volume":"1",
            "remaining_volume":"0",
            "reserved_fee":"0",
            "remaining_fee":"0",
            "paid_fee":"3.15",
            "locked":"0",
            "executed_volume":"1",
            "trades_count":1,
            "trades":[{
                "market":"KRW-BTC","uuid":"upbit-market-sell-trade",
                "price":"6300","volume":"1","funds":"6300",
                "trend":"even","created_at":"2026-10-14T02:00:01Z","side":"ask"
            }]
        }))?;
        let update = validate_order_update(
            &request,
            &detail,
            UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?,
        )?;
        assert_eq!(update.state, AutomationOrderState::Filled);
        assert_eq!(update.reserved_fee.get(), Decimal::ZERO);
        assert_eq!(update.remaining_fee.get(), Decimal::ZERO);
        assert_eq!(update.paid_fee.get(), Decimal::new(315, 2));
        Ok(())
    }

    #[test]
    fn market_buy_accepts_official_null_volume_fields_and_preserves_dust_cancel()
    -> Result<(), LabError> {
        let intent_id = crate::contracts::AutomationIntentId::from_seed("market-buy-official-wire");
        let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
        let request = AutomationOrderRequest {
            client_identifier: identifier.clone(),
            market: MarketId::parse_upbit("KRW-BTC")?,
            side: AutomationSide::Bid,
            order: AutomationOrderKind::MarketBuy {
                budget: QuoteAmount::new(Decimal::from(9_000))?,
            },
        };
        let mut detail: UpbitOrderDetail = serde_json::from_value(json!({
            "uuid":"upbit-market-buy-dust",
            "identifier":identifier.as_str(),
            "side":"bid",
            "ord_type":"price",
            "price":"9000",
            "state":"cancel",
            "market":"KRW-BTC",
            "created_at":"2026-10-14T02:00:00Z",
            "volume":null,
            "remaining_volume":null,
            "reserved_fee":"0.045",
            "remaining_fee":"0",
            "paid_fee":"0.045",
            "locked":"0",
            "executed_volume":"8.999",
            "trades_count":1,
            "trades":[{
                "market":"KRW-BTC","uuid":"upbit-market-buy-trade",
                "price":"1000","volume":"8.999","funds":"8999",
                "trend":"even","created_at":"2026-10-14T02:00:01Z","side":"bid"
            }]
        }))?;
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T02:00:02Z")?;
        let update = validate_order_update(&request, &detail, now)?;
        assert_eq!(update.state, AutomationOrderState::Cancelled);
        assert_eq!(update.executed_volume.get(), Decimal::new(8_999, 3));
        assert_eq!(update.remaining_volume, None);

        detail.state = "wait".into();
        detail.executed_volume = Some(Decimal::ZERO);
        detail.paid_fee = Some(Decimal::ZERO);
        detail.trades_count = Some(0);
        detail.trades.clear();
        let open = validate_order_update(&request, &detail, now)?;
        assert_eq!(open.state, AutomationOrderState::Open);
        assert_eq!(open.remaining_volume, None);
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)] // Deterministic settlement contract walk covers ACK vs GET, partial fill + cancel, duplicate trade UUIDs, and zero-epsilon funds.
    fn upbit_settlement_contract_validates_partial_fill_cancel_duplicate_trades_and_ack_vs_get_separation()
    -> Result<(), LabError> {
        let intent_id = crate::contracts::AutomationIntentId::from_seed("settlement-contract-walk");
        let identifier = AutomationOrderIdentifier::from_intent(&intent_id);
        let request = AutomationOrderRequest {
            client_identifier: identifier.clone(),
            market: MarketId::parse_upbit("KRW-BTC")?,
            side: AutomationSide::Bid,
            order: AutomationOrderKind::Limit {
                volume: AssetQuantity::new(Decimal::ONE)?,
                price: PriceKrw::new(Decimal::from(100_000))?,
            },
        };
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T03:00:05Z")?;

        // 1. POST /v1/orders ACK only proves order acceptance identity, never fill settlement.
        let ack = UpbitOrderDetail {
            uuid: "upbit-limit-partial-cancel".into(),
            identifier: Some(identifier.as_str().into()),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::from(100_000)),
            state: "wait".into(),
            market: "KRW-BTC".into(),
            created_at: "2026-10-14T03:00:00Z".into(),
            volume: Some(Decimal::ONE),
            remaining_volume: Some(Decimal::ONE),
            reserved_fee: Some(Decimal::from(50)),
            remaining_fee: Some(Decimal::from(50)),
            paid_fee: Some(Decimal::ZERO),
            locked: Some(Decimal::from(100_050)),
            executed_volume: Some(Decimal::ZERO),
            trades_count: Some(0),
            trades: Vec::new(),
        };
        validate_submit_ack_identity(&request, &ack)?;

        // 2. Partial fill via GET /v1/order: 0.4 executed at 100,000 KRW (funds = 40,000, paid_fee = 20, remaining_fee = 30, locked = 60,030).
        let partial_detail: UpbitOrderDetail = serde_json::from_value(json!({
            "uuid":"upbit-limit-partial-cancel",
            "identifier":identifier.as_str(),
            "side":"bid",
            "ord_type":"limit",
            "price":"100000",
            "state":"wait",
            "market":"KRW-BTC",
            "created_at":"2026-10-14T03:00:00Z",
            "volume":"1",
            "remaining_volume":"0.6",
            "reserved_fee":"50",
            "remaining_fee":"30",
            "paid_fee":"20",
            "locked":"60030",
            "executed_volume":"0.4",
            "trades_count":1,
            "trades":[{
                "market":"KRW-BTC","uuid":"trade-partial-1",
                "price":"100000","volume":"0.4","funds":"40000",
                "trend":"up","created_at":"2026-10-14T03:00:01Z","side":"bid"
            }]
        }))?;
        let partial_update = validate_order_update(&request, &partial_detail, now)?;
        assert_eq!(partial_update.state, AutomationOrderState::PartiallyFilled);
        assert_eq!(partial_update.executed_volume.get(), Decimal::new(4, 1));
        assert_eq!(
            partial_update.remaining_volume.map(AssetQuantity::get),
            Some(Decimal::new(6, 1))
        );
        assert_eq!(partial_update.paid_fee.get(), Decimal::from(20));
        assert_eq!(partial_update.remaining_fee.get(), Decimal::from(30));
        assert_eq!(partial_update.locked_amount, Decimal::from(60_030));

        // 3. Cancel after partial fill: state = "cancel", executed_volume = 0.4, remaining_volume = 0.6, paid_fee = 20, remaining_fee = 0, locked = 0.
        let mut cancel_detail = partial_detail.clone();
        cancel_detail.state = "cancel".into();
        cancel_detail.remaining_fee = Some(Decimal::ZERO);
        cancel_detail.locked = Some(Decimal::ZERO);
        let cancel_update = validate_order_update(&request, &cancel_detail, now)?;
        assert_eq!(cancel_update.state, AutomationOrderState::Cancelled);
        assert_eq!(cancel_update.executed_volume.get(), Decimal::new(4, 1));
        assert_eq!(cancel_update.paid_fee.get(), Decimal::from(20));
        assert_eq!(cancel_update.remaining_fee.get(), Decimal::ZERO);
        assert_eq!(cancel_update.locked_amount, Decimal::ZERO);

        // 4. Duplicate trade UUID in GET /v1/order trades array must fail closed.
        let mut dup_detail = partial_detail.clone();
        dup_detail.executed_volume = Some(Decimal::new(8, 1));
        dup_detail.remaining_volume = Some(Decimal::new(2, 1));
        dup_detail.paid_fee = Some(Decimal::from(40));
        dup_detail.remaining_fee = Some(Decimal::from(10));
        dup_detail.trades_count = Some(2);
        dup_detail.trades.push(dup_detail.trades[0].clone());
        assert!(matches!(
            validate_order_update(&request, &dup_detail, now),
            Err(LabError::DataCorrupt(msg)) if msg.contains("venue trade identity, currency, side, or exact amount is invalid")
        ));

        // 5. Any non-exact funds discrepancy (no epsilon tolerance) must fail closed.
        let mut bad_funds_detail = partial_detail;
        bad_funds_detail.trades[0]["funds"] = json!("39999.99999999");
        assert!(matches!(
            validate_order_update(&request, &bad_funds_detail, now),
            Err(LabError::DataCorrupt(msg)) if msg.contains("venue trade identity, currency, side, or exact amount is invalid")
        ));
        Ok(())
    }
}

use super::*;

use crate::automation::{AutomationCheckpoint, VenueAdapter, VenueFuture};
use crate::contracts::{
    AssetQuantity, AutomationAccountDayBaselineInput, AutomationActivationReferenceSource,
    AutomationActivationValuationState, AutomationBaselineAssetInput, AutomationBaselineSource,
    AutomationCommandOutcome, AutomationEffectiveLossProjection, AutomationLossReferenceBasis,
    AutomationSessionId, AutomationSessionMode, AutomationSessionQuery, AutomationSessionState,
    AutomationSessionView, AutomationSessionWrite, BoolExpr, CandleInterval, CandleObservation,
    CandleRecord, CompareOp, ContentHash, FrozenPolicyRevision, MarketId, NumericExpr,
    ObservationId, PolicyDefinition, PolicyIndicator, PolicyIndicatorKind, PolicyProgram,
    PolicyRule, PolicySignalExpiry, PolicyState, PolicyTarget, PriceKrw, QuoteAmount, RequestId,
    RulesProgram, UtcTimestamp, Weight,
};
use crate::database::{DatabaseHandle, DatabaseOwner};
use crate::jobs::JobService;
use crate::upbit::client::{
    ClientError, UpbitAccountBalance, UpbitOrderChance, UpbitOrderDetail, UpbitPrivateClient,
};
use crate::upbit::control::{
    ExchangeReadMode, TradingControlAction, TradingControlRequest, TradingControlService,
};
use crate::upbit::execution::LiveExecutionPolicy;
use crate::upbit::live_order_control::{
    LiveOrderControlAction, LiveOrderControlRequest, LiveOrderControlService, LiveOrderSendGate,
};
use crate::upbit::realtime::{
    PrivateAccountEvent, PrivateOrderEventIdentity, PrivateOrderObservation, PrivateOrderSide,
    PrivateOrderState, SessionTiming, private_account_realtime_channels,
    run_private_account_stream_for_test,
};
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

const LIVE_ACCOUNT_ID: &str =
    "live:upbit:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MARKET: &str = "KRW-BTC";
const INITIAL_KRW: i64 = 10_000_000;
const TARGET_WEIGHT: f64 = 0.001;
const LIVE_LOSS_LIMIT_KRW: i64 = 50_000;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Result<Self, std::io::Error> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(std::io::Error::other)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "spot-lab-live-service-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct SyntheticOrder {
    identifier: String,
    market: String,
    budget: Decimal,
    mark: Decimal,
    volume: Decimal,
    funds: Decimal,
    fee: Decimal,
    created_at: UtcTimestamp,
    partial_applied: bool,
    full_applied: bool,
}

struct SyntheticVenueState {
    quote_free: Decimal,
    base_free: Decimal,
    base_average_price: Decimal,
    submit_count: usize,
    lookup_count: usize,
    private_not_ready: bool,
    private_not_ready_count: usize,
    hide_order_readbacks: bool,
    order: Option<SyntheticOrder>,
}

impl SyntheticVenueState {
    fn new() -> Self {
        Self {
            quote_free: Decimal::from(INITIAL_KRW),
            base_free: Decimal::ZERO,
            base_average_price: Decimal::ZERO,
            submit_count: 0,
            lookup_count: 0,
            private_not_ready: false,
            private_not_ready_count: 0,
            hide_order_readbacks: false,
            order: None,
        }
    }
}

#[derive(Clone)]
struct SyntheticLiveVenue {
    state: Arc<Mutex<SyntheticVenueState>>,
    account_identity: String,
}

impl SyntheticLiveVenue {
    fn new(state: Arc<Mutex<SyntheticVenueState>>) -> Self {
        Self::for_account(state, LIVE_ACCOUNT_ID)
    }

    fn for_account(state: Arc<Mutex<SyntheticVenueState>>, account_identity: &str) -> Self {
        Self {
            state,
            account_identity: account_identity.to_owned(),
        }
    }
}

impl VenueAdapter for SyntheticLiveVenue {
    fn is_live(&self) -> bool {
        true
    }

    fn account_identity(&self) -> String {
        self.account_identity.clone()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok(vec![
                UpbitAccountBalance {
                    currency: "KRW".into(),
                    balance: state.quote_free,
                    locked: Decimal::ZERO,
                    avg_buy_price: Decimal::ZERO,
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
                UpbitAccountBalance {
                    currency: "BTC".into(),
                    balance: state.base_free,
                    locked: Decimal::ZERO,
                    avg_buy_price: state.base_average_price,
                    avg_buy_price_modified: false,
                    unit_currency: "KRW".into(),
                },
            ])
        })
    }

    fn order_chance<'a>(&'a self, market: &'a MarketId) -> VenueFuture<'a, UpbitOrderChance> {
        Box::pin(async move {
            Ok(UpbitOrderChance {
                bid_fee: Decimal::new(5, 4),
                ask_fee: Decimal::new(5, 4),
                maker_bid_fee: None,
                maker_ask_fee: None,
                market: serde_json::json!({
                    "id": market.code(),
                    "name": "BTC/KRW",
                    "tick_size_source": "UPBIT_ORDERBOOK_INSTRUMENTS",
                    "order_sides": ["bid", "ask"],
                    "bid_types": ["price"],
                    "ask_types": ["market"],
                    "bid": {"currency": "KRW", "min_total": "5000", "price_unit": null},
                    "ask": {"currency": "BTC", "min_total": "5000", "price_unit": null},
                    "max_total": "1000000000",
                    "state": "active"
                }),
                bid_account: None,
                ask_account: None,
            })
        })
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a crate::contracts::AutomationOrderRequest,
        decision_mark: PriceKrw,
        _send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        let state = Arc::clone(&self.state);
        let request = request.clone();
        Box::pin(async move {
            {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.private_not_ready {
                    state.private_not_ready_count += 1;
                    return Err(ClientError::PrivateAccountNotReady);
                }
            }
            let crate::contracts::AutomationOrderKind::MarketBuy { budget } = &request.order else {
                return Err(ClientError::InvalidParams(
                    "synthetic LIVE journey expects its bounded market-buy intent".into(),
                ));
            };
            let budget = budget.get();
            let mark = decision_mark.get();
            let volume =
                (budget / mark).round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
            let funds = mark.checked_mul(volume).ok_or_else(|| {
                ClientError::InvalidParams("synthetic LIVE fill funds overflow".into())
            })?;
            let fee = funds
                .checked_mul(Decimal::new(5, 4))
                .ok_or_else(|| ClientError::InvalidParams("synthetic fee overflow".into()))?;
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.submit_count += 1;
            if state.order.is_some() {
                return Err(ClientError::InvalidParams(
                    "synthetic LIVE fixture received a duplicate submit".into(),
                ));
            }
            state.order = Some(SyntheticOrder {
                identifier: request.client_identifier.as_str().into(),
                market: request.market.code().clone(),
                budget,
                mark,
                volume,
                funds,
                fee,
                created_at: UtcTimestamp::now(),
                partial_applied: false,
                full_applied: false,
            });
            Err(ClientError::Network(
                "synthetic venue accepted the order and lost the submit reply".into(),
            ))
        })
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a crate::contracts::AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        let state = Arc::clone(&self.state);
        let identifier = identifier.clone();
        Box::pin(async move {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.hide_order_readbacks {
                state.lookup_count += 1;
                return Ok(None);
            }
            let Some(mut order) = state.order.take() else {
                return Ok(None);
            };
            if order.identifier != identifier.as_str() {
                state.order = Some(order);
                return Ok(None);
            }
            state.lookup_count += 1;
            let result = if order.partial_applied {
                if !order.full_applied {
                    apply_synthetic_fill(&mut state, &order);
                    order.full_applied = true;
                }
                synthetic_order_detail(&order, false)
            } else {
                apply_synthetic_fill(&mut state, &order);
                order.partial_applied = true;
                synthetic_order_detail(&order, true)
            };
            state.order = Some(order);
            Ok(Some(result))
        })
    }

    fn cancel_order<'a>(&'a self, _uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        Box::pin(async { Err(ClientError::LiveTradingDisabled) })
    }
}

fn apply_synthetic_fill(state: &mut SyntheticVenueState, order: &SyntheticOrder) {
    let divisor = Decimal::from(2);
    let volume = order.volume / divisor;
    let funds = order.funds / divisor;
    let fee = order.fee / divisor;
    state.quote_free -= funds + fee;
    state.base_free += volume;
    state.base_average_price = order.mark;
}

fn synthetic_trade(order: &SyntheticOrder, suffix: &str) -> serde_json::Value {
    let volume = order.volume / Decimal::from(2);
    let funds = order.funds / Decimal::from(2);
    serde_json::json!({
        "market": order.market,
        "uuid": format!("synthetic-live-trade-{suffix}"),
        "price": order.mark.to_string(),
        "volume": volume.to_string(),
        "funds": funds.to_string(),
        "trend": "even",
        "created_at": order.created_at.to_rfc3339(),
        "side": "bid"
    })
}

fn synthetic_order_detail(order: &SyntheticOrder, partial: bool) -> UpbitOrderDetail {
    let half_volume = order.volume / Decimal::from(2);
    let half_funds = order.funds / Decimal::from(2);
    let half_fee = order.fee / Decimal::from(2);
    if partial {
        UpbitOrderDetail {
            uuid: "synthetic-live-order-1".into(),
            identifier: Some(order.identifier.clone()),
            side: "bid".into(),
            ord_type: "price".into(),
            price: Some(order.budget),
            state: "wait".into(),
            market: order.market.clone(),
            created_at: order.created_at.to_rfc3339(),
            volume: None,
            remaining_volume: None,
            reserved_fee: Some(order.fee),
            remaining_fee: Some(order.fee - half_fee),
            paid_fee: Some(half_fee),
            locked: Some(order.budget - half_funds + (order.fee - half_fee)),
            executed_volume: Some(half_volume),
            trades_count: Some(1),
            trades: vec![synthetic_trade(order, "1")],
        }
    } else {
        UpbitOrderDetail {
            uuid: "synthetic-live-order-1".into(),
            identifier: Some(order.identifier.clone()),
            side: "bid".into(),
            ord_type: "price".into(),
            price: Some(order.budget),
            state: "done".into(),
            market: order.market.clone(),
            created_at: order.created_at.to_rfc3339(),
            volume: None,
            remaining_volume: None,
            reserved_fee: Some(order.fee),
            remaining_fee: Some(Decimal::ZERO),
            paid_fee: Some(order.fee),
            locked: Some(Decimal::ZERO),
            executed_volume: Some(order.volume),
            trades_count: Some(2),
            trades: vec![synthetic_trade(order, "1"), synthetic_trade(order, "2")],
        }
    }
}

fn live_service(
    database: DatabaseHandle,
    root: PathBuf,
    venue_state: Arc<Mutex<SyntheticVenueState>>,
    risk_guard: crate::upbit::risk::RiskGuard,
) -> TestResult<JobService> {
    let mock_venue = crate::automation::PersistentMockVenue::new(database.clone());
    let adapter: Arc<dyn VenueAdapter> = Arc::new(SyntheticLiveVenue::new(venue_state));
    let mut service = make_service_with_live_override(
        database,
        crate::market_data::UpbitClient::synthetic_local("http://127.0.0.1:1")?,
        mock_venue,
        root,
        adapter,
    );
    let inner =
        Arc::get_mut(&mut service.inner).expect("test service is unique before its first clone");
    inner.automation_live_venue = UpbitPrivateClient::loopback_fixture(
        "http://127.0.0.1:1",
        LiveExecutionPolicy::OperatorEnabled,
    )?;
    inner.risk_guard = risk_guard;
    Ok(service)
}

fn live_service_with_private_client(
    database: DatabaseHandle,
    root: PathBuf,
    venue_state: Arc<Mutex<SyntheticVenueState>>,
    risk_guard: crate::upbit::risk::RiskGuard,
    private_client: UpbitPrivateClient,
) -> TestResult<JobService> {
    let account_identity = private_client.account_identity();
    let mock_venue = crate::automation::PersistentMockVenue::new(database.clone());
    let adapter: Arc<dyn VenueAdapter> = Arc::new(SyntheticLiveVenue::for_account(
        venue_state,
        &account_identity,
    ));
    let mut service = make_service_with_live_override(
        database,
        crate::market_data::UpbitClient::synthetic_local("http://127.0.0.1:1")?,
        mock_venue,
        root,
        adapter,
    );
    let inner = Arc::get_mut(&mut service.inner)
        .expect("test service remains unique while its private client is installed");
    inner.automation_live_venue = private_client;
    inner.risk_guard = risk_guard;
    Ok(service)
}

fn live_service_with_adapter_and_gate(
    database: DatabaseHandle,
    root: PathBuf,
    adapter: Arc<dyn VenueAdapter>,
    risk_guard: crate::upbit::risk::RiskGuard,
    gate: LiveOrderSendGate,
) -> TestResult<JobService> {
    let upbit = crate::market_data::UpbitClient::synthetic_local("http://127.0.0.1:1")?;
    Ok(live_service_with_client_adapter_and_gate(
        database, root, upbit, adapter, risk_guard, gate,
    ))
}

fn live_service_with_client_adapter_and_gate(
    database: DatabaseHandle,
    root: PathBuf,
    upbit: crate::market_data::UpbitClient,
    adapter: Arc<dyn VenueAdapter>,
    risk_guard: crate::upbit::risk::RiskGuard,
    gate: LiveOrderSendGate,
) -> JobService {
    let mock_venue = crate::automation::PersistentMockVenue::new(database.clone());
    let mut service = make_service_with_live_override(database, upbit, mock_venue, root, adapter);
    let inner =
        Arc::get_mut(&mut service.inner).expect("test service is unique before its first clone");
    inner.automation_live_venue = UpbitPrivateClient::new_mock_with_policy_and_send_gate(
        LiveExecutionPolicy::OperatorEnabled,
        gate,
    );
    inner.risk_guard = risk_guard;
    service
}

#[derive(Clone)]
struct BlockingOrderChanceVenue {
    inner: SyntheticLiveVenue,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl VenueAdapter for BlockingOrderChanceVenue {
    fn is_live(&self) -> bool {
        true
    }

    fn account_identity(&self) -> String {
        self.inner.account_identity()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        self.inner.account_balances()
    }

    fn order_chance<'a>(&'a self, market: &'a MarketId) -> VenueFuture<'a, UpbitOrderChance> {
        let inner = self.inner.clone();
        let market = market.clone();
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            inner.order_chance(&market).await
        })
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a crate::contracts::AutomationOrderRequest,
        decision_mark: PriceKrw,
        send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner
            .submit_order(request, decision_mark, send_generation)
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a crate::contracts::AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        self.inner.order_by_identifier(identifier)
    }

    fn cancel_order<'a>(&'a self, uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner.cancel_order(uuid)
    }
}

#[derive(Clone)]
struct BlockingAccountBalanceVenue {
    inner: SyntheticLiveVenue,
    block_next: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl BlockingAccountBalanceVenue {
    fn arm(&self) {
        self.block_next.store(true, Ordering::SeqCst);
    }
}

impl VenueAdapter for BlockingAccountBalanceVenue {
    fn is_live(&self) -> bool {
        true
    }

    fn account_identity(&self) -> String {
        self.inner.account_identity()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        let inner = self.inner.clone();
        let block_next = Arc::clone(&self.block_next);
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            if block_next.swap(false, Ordering::SeqCst) {
                entered.notify_one();
                release.notified().await;
            }
            inner.account_balances().await
        })
    }

    fn order_chance<'a>(&'a self, market: &'a MarketId) -> VenueFuture<'a, UpbitOrderChance> {
        self.inner.order_chance(market)
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a crate::contracts::AutomationOrderRequest,
        decision_mark: PriceKrw,
        send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner
            .submit_order(request, decision_mark, send_generation)
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a crate::contracts::AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        self.inner.order_by_identifier(identifier)
    }

    fn cancel_order<'a>(&'a self, uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        self.inner.cancel_order(uuid)
    }
}

fn live_policy_definition() -> PolicyDefinition {
    PolicyDefinition {
        schema_version: "1.0".into(),
        name: "synthetic-live-target-weight".into(),
        description: "one deterministic target-weight LIVE fixture".into(),
        program: PolicyProgram::Rules {
            program: RulesProgram {
                condition_roles: Vec::new(),
                indicators: vec![PolicyIndicator {
                    id: "close".into(),
                    indicator: PolicyIndicatorKind::Close,
                    source_interval: None,
                }],
                states: vec![PolicyState {
                    id: "memory".into(),
                    initial: 0.0,
                    next: NumericExpr::State {
                        id: "memory".into(),
                    },
                }],
                rules: vec![PolicyRule {
                    id: "enter".into(),
                    condition: BoolExpr::Compare {
                        comparison: CompareOp::Gt,
                        left: NumericExpr::Indicator { id: "close".into() },
                        right: NumericExpr::Constant { value: 0.0 },
                    },
                    target: PolicyTarget::Weight {
                        value: NumericExpr::Constant {
                            value: TARGET_WEIGHT,
                        },
                    },
                }],
                fallback: PolicyTarget::Hold,
                signal_expiry: PolicySignalExpiry::EndOfRange,
            },
        },
    }
}

fn live_risk() -> TestResult<crate::contracts::AutomationRiskPolicy> {
    Ok(crate::contracts::AutomationRiskPolicy {
        max_order_notional_krw: QuoteAmount::new(Decimal::from(100_000))?,
        max_daily_notional_krw: QuoteAmount::new(Decimal::from(1_000_000))?,
        max_asset_exposure_krw: QuoteAmount::new(Decimal::from(100_000))?,
        max_daily_loss_krw: QuoteAmount::new(Decimal::from(100_000))?,
        activation_loss_limit_krw: Some(QuoteAmount::new(Decimal::from(LIVE_LOSS_LIMIT_KRW))?),
        max_open_orders: 1,
        max_orders_per_interval: 1,
        max_balance_age_seconds: 3_600,
        max_feed_age_seconds: 3_600,
    })
}

fn first_post_start_close(now: UtcTimestamp) -> (UtcTimestamp, UtcTimestamp) {
    let close_epoch = now.0.timestamp().div_euclid(60) * 60;
    let close = chrono::DateTime::from_timestamp(close_epoch, 0)
        .map(UtcTimestamp)
        .expect("current minute timestamp is in range");
    (UtcTimestamp(close.0 - chrono::Duration::seconds(30)), close)
}

async fn create_policy(
    database: &DatabaseHandle,
    at: UtcTimestamp,
) -> TestResult<FrozenPolicyRevision> {
    let write = crate::contracts::PolicyWrite::Create {
        request_id: RequestId::new("synthetic-live-policy")?,
        definition: live_policy_definition(),
    };
    let response = database
        .call("test_create_synthetic_live_policy", move |store| {
            store.write_policy(&write, at)
        })
        .await?;
    Ok(response.snapshot)
}

async fn start_live_session_fixture(
    database: &DatabaseHandle,
    policy: &FrozenPolicyRevision,
    started_at: UtcTimestamp,
) -> TestResult<(AutomationSessionId, AutomationSessionWrite)> {
    start_live_session_fixture_for_account(database, policy, started_at, LIVE_ACCOUNT_ID).await
}

async fn start_live_session_fixture_for_account(
    database: &DatabaseHandle,
    policy: &FrozenPolicyRevision,
    started_at: UtcTimestamp,
    account_identity: &str,
) -> TestResult<(AutomationSessionId, AutomationSessionWrite)> {
    let start = AutomationSessionWrite::Start {
        request_id: RequestId::new("synthetic-live-session-start")?,
        policy_ref: policy.reference.clone(),
        market: MarketId::parse_upbit(MARKET)?,
        decision_interval: CandleInterval::M1,
        mode: AutomationSessionMode::Live,
        risk: live_risk()?,
    };
    let start_for_begin = start.clone();
    let start_for_write = start.clone();
    let frozen_policy = policy.clone();
    let account_for_begin = account_identity.to_owned();
    let account_for_write = account_identity.to_owned();
    let response = database
        .call("test_start_synthetic_live_session", move |store| {
            store.automation_begin_request(
                &start_for_begin,
                Some(&account_for_begin),
                started_at,
            )?;
            store.automation_write_with_policy(
                &start_for_write,
                Some(frozen_policy),
                Some(account_for_write),
                started_at,
                LiveExecutionPolicy::OperatorEnabled,
            )
        })
        .await?;
    assert_eq!(response.session.mode, AutomationSessionMode::Live);
    assert_eq!(response.session.state, AutomationSessionState::Running);
    Ok((response.session.session_id, start))
}

async fn seed_causal_live_buy(
    database: &DatabaseHandle,
    policy: &FrozenPolicyRevision,
    session_id: &AutomationSessionId,
    close_time: UtcTimestamp,
) -> TestResult<()> {
    let market = MarketId::parse_upbit(MARKET)?;
    let close_price = PriceKrw::new(Decimal::from(10_000))?;
    let candle = CandleRecord {
        market: market.code().clone(),
        interval: CandleInterval::M1,
        open_time_utc: UtcTimestamp(close_time.0 - chrono::Duration::minutes(1)),
        close_time_utc: close_time,
        open: close_price,
        high: close_price,
        low: close_price,
        close: close_price,
        volume: AssetQuantity::new(Decimal::ONE)?,
        quote_turnover: QuoteAmount::new(Decimal::from(10_000))?,
        completed: true,
    };
    let content_digest = crate::contracts::observation_digest(&candle)?;
    let observation = CandleObservation {
        id: ObservationId::from_seed(content_digest.as_str()),
        candle,
        content_digest,
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    };
    let mut checkpoint = AutomationCheckpoint::compile(policy, market, CandleInterval::M1)?;
    let position = crate::strategy::PositionView {
        actual_qty: AssetQuantity::new(Decimal::ZERO)?,
        actual_weight: Weight::new(Decimal::ZERO)?,
        episode_opened_at: None,
        held_decision_bars: None,
    };
    let evaluation = checkpoint
        .observe(&observation, position)?
        .ok_or("one completed Close bar should evaluate the synthetic Rules policy")?;
    assert_eq!(
        evaluation.constrained_target_weight.get(),
        Decimal::new(1, 3)
    );
    checkpoint.last_observation = Some(observation.clone());
    checkpoint.held_decision_bars = 1;
    let checkpoint_json = serde_json::to_string(&checkpoint)?;
    let evaluation_json =
        super::super::evaluation_to_value(&evaluation, Some(position.actual_weight));
    let action = super::super::action_for_target(
        evaluation.constrained_target_weight,
        Weight::new(Decimal::ZERO)?,
    );
    assert_eq!(action, "BUY");
    let intent_id = crate::contracts::AutomationIntentId::from_seed("synthetic-live-buy-intent");
    let session_id = session_id.clone();
    let observation_for_store = observation.clone();
    let received_at = UtcTimestamp::now();
    let inserted = database
        .call("test_record_synthetic_live_decision", move |store| {
            store.automation_record_decision(
                &session_id,
                &observation_for_store,
                &checkpoint_json,
                &intent_id,
                action,
                &evaluation_json,
                received_at,
            )
        })
        .await?;
    assert!(inserted);
    Ok(())
}

async fn read_session(
    service: &JobService,
    session_id: &AutomationSessionId,
) -> TestResult<AutomationSessionView> {
    let value = service
        .automation_session_query(AutomationSessionQuery::Get {
            session_id: session_id.clone(),
        })
        .await?;
    Ok(serde_json::from_value(value)?)
}

async fn read_ledger(
    service: &JobService,
    session_id: &AutomationSessionId,
) -> TestResult<serde_json::Value> {
    Ok(service
        .automation_session_query(AutomationSessionQuery::Ledger {
            session_id: session_id.clone(),
            after_sequence: None,
            limit: 20,
        })
        .await?)
}

fn assert_activation_projection(
    session: &AutomationSessionView,
    expected_reference: Option<&crate::contracts::AutomationLiveActivationReferenceView>,
) -> TestResult<serde_json::Value> {
    let account_risk = session
        .account_risk
        .as_ref()
        .ok_or("LIVE session omitted its account risk projection")?;
    let effective_loss: &AutomationEffectiveLossProjection = &account_risk.effective_loss;
    assert_eq!(
        effective_loss.basis,
        AutomationLossReferenceBasis::LiveActivationObserved
    );
    assert_eq!(
        effective_loss.limit_krw,
        Some(QuoteAmount::new(Decimal::from(LIVE_LOSS_LIMIT_KRW))?)
    );
    assert!(effective_loss.used_krw.is_some());
    assert!(!effective_loss.admission_blocked);

    let reference = account_risk
        .activation_reference
        .as_ref()
        .ok_or("LIVE projection omitted its immutable activation reference")?;
    assert_eq!(reference.account_identity, LIVE_ACCOUNT_ID);
    assert_eq!(reference.mode, AutomationSessionMode::Live);
    assert_eq!(
        reference.source,
        AutomationActivationReferenceSource::CurrentLocalObserved
    );
    assert_eq!(
        reference.valuation_state,
        AutomationActivationValuationState::Verified
    );
    assert!(reference.snapshot_sequence > 0);
    assert!(reference.control_sequence > 0);
    assert_eq!(
        reference.reference_equity_krw,
        Some(QuoteAmount::new(Decimal::from(INITIAL_KRW))?)
    );
    assert_eq!(
        reference.balances_digest,
        ContentHash::of_value(&vec![
            UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(INITIAL_KRW),
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            },
            UpbitAccountBalance {
                currency: "BTC".into(),
                balance: Decimal::ZERO,
                locked: Decimal::ZERO,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            },
        ])?
    );
    assert_ne!(reference.valuation_digest, ContentHash::of_bytes(b""));
    if let Some(expected) = expected_reference {
        assert_eq!(reference, expected);
    }

    let day_baseline = account_risk
        .day_baseline
        .as_ref()
        .ok_or("LIVE projection omitted the separately recorded UTC-day baseline")?;
    assert_eq!(
        day_baseline.verification_state,
        crate::contracts::AutomationBaselineVerificationState::BlockedInsufficientEvidence
    );
    assert!(!day_baseline.sufficient_for_risk_admission);
    assert!(
        day_baseline
            .block_reasons
            .iter()
            .any(|reason| reason == "LIVE_PROVIDER_AS_OF_UNAVAILABLE")
    );

    Ok(serde_json::to_value(effective_loss)?)
}

async fn start_live_control(
    database: DatabaseHandle,
    risk_guard: crate::upbit::risk::RiskGuard,
) -> TestResult<TradingControlService> {
    let control = TradingControlService::production_with_policy(
        database,
        risk_guard,
        ExchangeReadMode::LiveRead,
        LiveExecutionPolicy::OperatorEnabled,
    );
    let current = control
        .execute(TradingControlRequest {
            action: TradingControlAction::Status,
            request_id: None,
            expected_sequence: None,
            approval_token: None,
        })
        .await?;
    let started = control
        .execute_with_operator_approval(TradingControlRequest {
            action: TradingControlAction::Start,
            request_id: Some("synthetic-live-control-start".into()),
            expected_sequence: Some(current.sequence),
            approval_token: None,
        })
        .await?;
    assert!(started.live_trading_enabled);
    assert!(!started.order_admission_blocked);
    Ok(control)
}

async fn set_live_order_sending(
    control: &LiveOrderControlService,
    action: LiveOrderControlAction,
    request_id: &str,
) -> TestResult<crate::upbit::live_order_control::LiveOrderControlResponse> {
    let current = control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Status,
            request_id: None,
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?;
    let confirmation =
        (action == LiveOrderControlAction::Enable).then(|| "ENABLE_LIVE_ORDER_SENDING".to_owned());
    Ok(control
        .execute(LiveOrderControlRequest {
            action,
            request_id: Some(request_id.to_owned()),
            expected_sequence: Some(current.sequence),
            expected_epoch: Some(current.boot_epoch),
            confirmation,
        })
        .await?)
}

async fn seed_pending_live_intent(
    database: &DatabaseHandle,
    service: &JobService,
) -> TestResult<(AutomationSessionId, crate::contracts::AutomationIntentId)> {
    let close_time = first_post_start_close(UtcTimestamp::now()).1;
    let (session_id, policy, close_time) =
        seed_live_session_with_baseline(database, service, close_time).await?;
    seed_causal_live_buy(database, &policy, &session_id, close_time).await?;

    let pending = database
        .call("test_read_seeded_live_intent", {
            let session_id = session_id.clone();
            move |store| store.automation_pending_intents(&session_id)
        })
        .await?;
    let intent = pending
        .first()
        .ok_or("live intent fixture did not persist a pending decision")?;
    Ok((session_id, intent.intent_id.clone()))
}

async fn seed_live_session_with_baseline(
    database: &DatabaseHandle,
    service: &JobService,
    close_time: UtcTimestamp,
) -> TestResult<(AutomationSessionId, FrozenPolicyRevision, UtcTimestamp)> {
    seed_live_session_with_baseline_for_account(database, service, close_time, LIVE_ACCOUNT_ID)
        .await
}

async fn seed_live_session_with_baseline_for_account(
    database: &DatabaseHandle,
    service: &JobService,
    close_time: UtcTimestamp,
    account_identity: &str,
) -> TestResult<(AutomationSessionId, FrozenPolicyRevision, UtcTimestamp)> {
    let started_at = UtcTimestamp(close_time.0 - chrono::Duration::seconds(30));
    let policy = create_policy(database, started_at).await?;
    let (session_id, _) =
        start_live_session_fixture_for_account(database, &policy, started_at, account_identity)
            .await?;

    service
        .refresh_account(AutomationSessionMode::Live, account_identity)
        .await?;
    let view_after_refresh = read_session(service, &session_id).await?;
    let day = UtcTimestamp::now().0.format("%Y-%m-%d").to_string();
    let midnight = UtcTimestamp::parse_rfc3339(&format!("{day}T00:00:00Z"))?;
    let baseline = AutomationSessionWrite::RecordBaseline {
        request_id: RequestId::new("live-order-switch-pending-baseline")?,
        session_id: session_id.clone(),
        expected_sequence: view_after_refresh.sequence,
        baseline: AutomationAccountDayBaselineInput {
            account_identity: account_identity.into(),
            day_utc: day,
            reference_time_utc: midnight,
            source: AutomationBaselineSource::ExchangeObservedDayStart,
            assets: vec![AutomationBaselineAssetInput {
                currency: "KRW".into(),
                free: AssetQuantity::new(Decimal::from(INITIAL_KRW))?,
                locked: AssetQuantity::new(Decimal::ZERO)?,
                valuation_basis:
                    crate::contracts::AutomationAssetValuationBasis::QuoteCurrencyParity,
                mark_price_krw: None,
            }],
            operator_note: None,
        },
    };
    service.automation_session_write(baseline).await?;
    Ok((session_id, policy, close_time))
}

async fn assert_intent_rejected_event(
    service: &JobService,
    session_id: &AutomationSessionId,
    intent_id: &crate::contracts::AutomationIntentId,
    expected_reason: &str,
) -> TestResult<()> {
    let decision = service
        .automation_session_query(AutomationSessionQuery::Decision {
            session_id: session_id.clone(),
            intent_id: intent_id.clone(),
        })
        .await?;
    assert_eq!(decision["decision"]["state"], "REJECTED");

    let events = service
        .automation_session_query(AutomationSessionQuery::Events {
            session_id: session_id.clone(),
            after_sequence: None,
            limit: 100,
        })
        .await?;
    let items = events["items"]
        .as_array()
        .ok_or("event page omitted items")?;
    let rejected = items.iter().find(|event| {
        event["kind"] == "INTENT_REJECTED"
            && event["detail"]["intent_id"] == intent_id.as_str()
            && event["detail"]["reason"] == expected_reason
    });
    assert!(
        rejected.is_some(),
        "LIVE intent lacks a durable OFF rejection event"
    );
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one actual service-to-Store-to-projection journey proves deferred gate capture and unchanged financial state"
)]
async fn pending_intent_records_private_readiness_deferral_without_re_evaluation_or_orders()
-> TestResult {
    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    let risk_guard = crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let control = start_live_control(database.clone(), risk_guard.clone()).await?;
    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    let service = live_service(
        database.clone(),
        root.0.clone(),
        Arc::clone(&venue_state),
        risk_guard,
    )?;
    let (session_id, intent_id) = seed_pending_live_intent(&database, &service).await?;
    let id = session_id.clone();
    let (before, pending) = database
        .call("read_before_deferred_admission", move |store| {
            Ok((
                store.automation_runtime_record_with_policy(
                    &id,
                    UtcTimestamp::now(),
                    LiveExecutionPolicy::OperatorEnabled,
                )?,
                store.automation_pending_intents(&id)?,
            ))
        })
        .await?;
    let intent = pending.first().ok_or("seeded pending intent missing")?;
    assert_eq!(intent.intent_id, intent_id);
    let evaluation = intent.evaluation_json.clone();
    let fence = service
        .inner
        .automation_live_venue
        .private_readiness_fence();
    fence.invalidate_for_recovery(fence.generation())?;
    service.admit_pending_intent(&before, intent).await?;
    service.admit_pending_intent(&before, intent).await?;
    let id = session_id.clone();
    let (after, still_pending, events) = database
        .call("read_after_deferred_admission", move |store| {
            Ok((
                store.automation_runtime_record_with_policy(
                    &id,
                    UtcTimestamp::now(),
                    LiveExecutionPolicy::OperatorEnabled,
                )?,
                store.automation_pending_intents(&id)?,
                store.automation_query(
                    AutomationSessionQuery::Events {
                        session_id: id,
                        after_sequence: None,
                        limit: 100,
                    },
                    UtcTimestamp::now(),
                )?,
            ))
        })
        .await?;
    assert_eq!(before.checkpoint_json, after.checkpoint_json);
    assert_eq!(before.view.sequence, after.view.sequence);
    assert_eq!(still_pending.len(), 1);
    assert_eq!(still_pending[0].evaluation_json, evaluation);
    let deferred: Vec<_> = events["items"]
        .as_array()
        .ok_or("event page missing")?
        .iter()
        .filter(|event| event["kind"] == "DECISION_DEFERRED")
        .collect();
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0]["detail"]["intent_id"], intent_id.as_str());
    assert_eq!(
        deferred[0]["detail"]["admission_deferral"]["phase"],
        "BEFORE_ADMISSION"
    );
    assert_eq!(
        deferred[0]["detail"]["admission_deferral"]["reason"],
        "PRIVATE_ACCOUNT_RECOVERY_REQUIRED"
    );
    let selected_session = session_id.clone();
    let selected_intent = intent_id.clone();
    let projected = database
        .call("read_stored_admission_deferral_projection", move |store| {
            store.automation_review_detail(&selected_session, &selected_intent)
        })
        .await?
        .recorded_decision
        .ok_or("stored decision projection missing")?;
    assert!(projected.reasons.iter().any(|reason| reason.stage
        == crate::contracts::RecordedDecisionStage::Admission
        && reason.code == "PRIVATE_ACCOUNT_RECOVERY_REQUIRED"));
    assert_eq!(
        projected.execution_state,
        crate::contracts::RecordedExecutionState::NotCaptured
    );
    assert_eq!(
        projected.strategy_action,
        crate::contracts::RecordedDecisionAction::Buy
    );
    assert_eq!(
        projected.actual_weight_at_decision,
        Some(Weight::new(Decimal::ZERO)?)
    );
    assert_eq!(
        projected.condition_trace_capture,
        crate::contracts::SavedFactCaptureStatus::Captured
    );
    let ledger = read_ledger(&service, &session_id).await?;
    assert_eq!(ledger["orders"].as_array().map(Vec::len), Some(0));
    assert_eq!(ledger["fills"].as_array().map(Vec::len), Some(0));
    assert_eq!(venue_state_counts(&venue_state), (0, 0));
    drop(service);
    drop(control);
    drop(database);
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn private_account_not_ready_releases_claimed_live_order_without_venue_effect() -> TestResult
{
    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    let risk_guard = crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let control = start_live_control(database.clone(), risk_guard.clone()).await?;
    let send_gate = LiveOrderSendGate::new_disabled();
    let send_control = LiveOrderControlService::new(
        database.clone(),
        LiveExecutionPolicy::OperatorEnabled,
        Some(send_gate),
        ExchangeReadMode::Mock,
    );
    assert!(
        set_live_order_sending(
            &send_control,
            LiveOrderControlAction::Enable,
            "enable-private-not-ready-test"
        )
        .await?
        .live_trading_enabled
    );
    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    venue_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .private_not_ready = true;
    let service = live_service(
        database.clone(),
        root.0.clone(),
        Arc::clone(&venue_state),
        risk_guard,
    )?;

    let close_time = first_post_start_close(UtcTimestamp::now()).1;
    let (session_id, policy, _) =
        seed_live_session_with_baseline(&database, &service, close_time).await?;
    seed_causal_live_buy(&database, &policy, &session_id, close_time).await?;

    service
        .process_due_automation_sessions(UtcTimestamp::now())
        .await?;

    let ledger = read_ledger(&service, &session_id).await?;
    assert_eq!(ledger["orders"].as_array().map(Vec::len), Some(1));
    assert_eq!(ledger["orders"][0]["state"], "REJECTED");
    assert_eq!(ledger["fills"].as_array().map(Vec::len), Some(0));
    let intent_id: crate::contracts::AutomationIntentId =
        serde_json::from_value(ledger["orders"][0]["intent_id"].clone())?;
    assert_intent_rejected_event(
        &service,
        &session_id,
        &intent_id,
        "PRIVATE_ACCOUNT_RECOVERY_REQUIRED",
    )
    .await?;
    let projection = service
        .automation_session_query(AutomationSessionQuery::Get {
            session_id: session_id.clone(),
        })
        .await?;
    assert_eq!(projection["account_risk"]["risk"]["open_order_count"], 0);
    let quote_reserved: QuoteAmount = serde_json::from_value(
        projection["account_risk"]["balance"]["quote_local_reserved"].clone(),
    )?;
    assert_eq!(quote_reserved.get(), Decimal::ZERO);
    let state = venue_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(state.private_not_ready_count, 1);
    assert_eq!(state.submit_count, 0);
    assert_eq!(state.lookup_count, 0);
    assert!(state.order.is_none());
    drop(state);

    drop(service);
    drop(send_control);
    drop(control);
    drop(database);
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn duplicate_private_trade_is_durably_deduplicated_without_creating_manual_intent()
-> TestResult {
    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    let service = live_service(
        database.clone(),
        root.0.clone(),
        Arc::clone(&venue_state),
        crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default()),
    )?;
    let generation = service
        .inner
        .automation_live_venue
        .private_readiness_fence()
        .generation();
    let provider_at = UtcTimestamp::parse_rfc3339("2026-10-17T01:02:03Z")?;
    let received_at = UtcTimestamp::now();
    let order = PrivateOrderObservation {
        generation,
        stream_sequence: 1,
        identity: PrivateOrderEventIdentity::Trade {
            trade_uuid: "manual-provider-trade-duplicate-test".into(),
        },
        order_uuid: "manual-provider-order-duplicate-test".into(),
        client_identifier: Some("provider/manual order id".into()),
        market: MarketId::parse_upbit(MARKET)?,
        side: PrivateOrderSide::Bid,
        order_type: "limit".into(),
        state: PrivateOrderState::Trade,
        price: Some(Decimal::from(10_000)),
        average_price: Some(Decimal::from(10_000)),
        volume: Some(Decimal::ONE),
        remaining_volume: Some(Decimal::ZERO),
        executed_volume: Some(Decimal::ONE),
        trades_count: Some(1),
        reserved_fee: Some(Decimal::ZERO),
        remaining_fee: Some(Decimal::ZERO),
        paid_fee: Some(Decimal::ZERO),
        locked: Some(Decimal::ZERO),
        executed_funds: Some(Decimal::from(10_000)),
        trade_fee: Some(Decimal::ZERO),
        is_maker: Some(false),
        smp_type: None,
        prevented_volume: None,
        prevented_locked: None,
        provider_order_at_utc: provider_at,
        provider_trade_at_utc: Some(provider_at),
        provider_at_utc: provider_at,
        received_at_utc: received_at,
    };
    service
        .consume_private_stream_event(&PrivateAccountEvent::OrderObserved(Box::new(order.clone())))
        .await?;
    let duplicate = PrivateAccountEvent::OrderObserved(Box::new(PrivateOrderObservation {
        stream_sequence: 2,
        received_at_utc: UtcTimestamp(received_at.0 + chrono::Duration::seconds(1)),
        ..order
    }));
    service.consume_private_stream_event(&duplicate).await?;

    let account_identity = service.inner.automation_live_venue.account_identity();
    let observations = database
        .call("test_duplicate_private_trade_journal", {
            let account_identity = account_identity.clone();
            move |store| store.private_stream_observations(&account_identity, 0, 100)
        })
        .await?;
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].sequence, 1);
    assert_eq!(
        observations[0]
            .observation
            .transport
            .as_ref()
            .map(|transport| transport.message_sequence),
        Some(1),
        "duplicate provider trade keeps its first transport receipt"
    );
    let automation_orders = database
        .call("test_manual_private_trade_no_owned_intent", move |store| {
            store.automation_account_order_readback_targets(&account_identity)
        })
        .await?;
    assert!(
        automation_orders.is_empty(),
        "manual orders do not create intents"
    );
    let venue_state = venue_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(venue_state.submit_count, 0);
    assert_eq!(venue_state.lookup_count, 0);
    assert!(venue_state.order.is_none());
    drop(venue_state);

    drop(service);
    drop(database);
    owner.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[expect(
    clippy::too_many_lines,
    reason = "one loopback private owner journey proves baseline, buffered trade, exact pending readback, bounded history marker, readiness, and idempotent fill"
)]
async fn private_bootstrap_exact_reads_pending_orders_and_marks_only_bounded_coverage() -> TestResult
{
    use axum::http::{StatusCode, Uri};
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::AtomicUsize;
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, oneshot};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_util::sync::CancellationToken;

    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    let risk_guard = crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let control = start_live_control(database.clone(), risk_guard.clone()).await?;
    let send_gate = LiveOrderSendGate::new_disabled();
    let send_control = LiveOrderControlService::new(
        database.clone(),
        LiveExecutionPolicy::OperatorEnabled,
        Some(send_gate),
        ExchangeReadMode::Mock,
    );
    assert!(
        set_live_order_sending(
            &send_control,
            LiveOrderControlAction::Enable,
            "enable-private-bootstrap-test"
        )
        .await?
        .live_trading_enabled
    );

    let order_body = Arc::new(Mutex::new(Vec::<u8>::new()));
    let open_orders_body = Arc::new(Mutex::new(Vec::<u8>::new()));
    let baseline_account_body = Arc::new(Mutex::new(Vec::<u8>::new()));
    let account_body = Arc::new(Mutex::new(Vec::<u8>::new()));
    let frame_body = Arc::new(Mutex::new(Vec::<u8>::new()));
    let open_reads = Arc::new(AtomicUsize::new(0));
    let exact_order_reads = Arc::new(AtomicUsize::new(0));
    let closed_history_reads = Arc::new(AtomicUsize::new(0));
    let account_reads = Arc::new(AtomicUsize::new(0));
    let frame_sent = Arc::new(AtomicBool::new(false));
    let frame_sent_notify = Arc::new(Notify::new());
    let rest_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rest_address = rest_listener.local_addr()?;
    let rest_app = axum::Router::new().fallback({
        let order_body = Arc::clone(&order_body);
        let open_orders_body = Arc::clone(&open_orders_body);
        let baseline_account_body = Arc::clone(&baseline_account_body);
        let account_body = Arc::clone(&account_body);
        let open_reads = Arc::clone(&open_reads);
        let exact_order_reads = Arc::clone(&exact_order_reads);
        let closed_history_reads = Arc::clone(&closed_history_reads);
        let account_reads = Arc::clone(&account_reads);
        let frame_sent = Arc::clone(&frame_sent);
        let frame_sent_notify = Arc::clone(&frame_sent_notify);
        move |uri: Uri| {
            let order_body = Arc::clone(&order_body);
            let open_orders_body = Arc::clone(&open_orders_body);
            let baseline_account_body = Arc::clone(&baseline_account_body);
            let account_body = Arc::clone(&account_body);
            let open_reads = Arc::clone(&open_reads);
            let exact_order_reads = Arc::clone(&exact_order_reads);
            let closed_history_reads = Arc::clone(&closed_history_reads);
            let account_reads = Arc::clone(&account_reads);
            let frame_sent = Arc::clone(&frame_sent);
            let frame_sent_notify = Arc::clone(&frame_sent_notify);
            let path = uri.path().to_owned();
            async move {
                let body = match path.as_str() {
                    "/v1/accounts" => {
                        let read_index = account_reads.fetch_add(1, Ordering::SeqCst);
                        if !frame_sent.load(Ordering::Acquire) {
                            frame_sent_notify.notified().await;
                        }
                        if read_index == 0 {
                            baseline_account_body
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone()
                        } else {
                            account_body
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone()
                        }
                    }
                    "/v1/orders/open" => {
                        open_reads.fetch_add(1, Ordering::SeqCst);
                        open_orders_body
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone()
                    }
                    "/v1/order" => {
                        exact_order_reads.fetch_add(1, Ordering::SeqCst);
                        order_body
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone()
                    }
                    "/v1/orders/closed" => {
                        closed_history_reads.fetch_add(1, Ordering::SeqCst);
                        b"[]".to_vec()
                    }
                    _ => br#"{"error":{"name":"order_not_found"}}"#.to_vec(),
                };
                (
                    StatusCode::OK,
                    [("Remaining-Req", "group=default; min=1800; sec=59")],
                    body,
                )
            }
        }
    });
    let (rest_stop, rest_stopped) = oneshot::channel();
    let rest_server = tokio::spawn(async move {
        axum::serve(rest_listener, rest_app)
            .with_graceful_shutdown(async {
                let _ = rest_stopped.await;
            })
            .await
    });

    let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
    let ws_address = ws_listener.local_addr()?;
    let ws_endpoint = format!("ws://{ws_address}/websocket/v1/private");
    let (subscription_tx, subscription_rx) = oneshot::channel();
    let (ws_closed_tx, ws_closed_rx) = oneshot::channel();
    let ws_frame_buffer = Arc::clone(&frame_body);
    let ws_frame_sent = Arc::clone(&frame_sent);
    let cancellation = CancellationToken::new();
    let ws_cancellation = cancellation.clone();
    let ws_server = tokio::spawn(async move {
        let mut subscription_tx = Some(subscription_tx);
        let mut ws_closed_tx = Some(ws_closed_tx);
        for _ in 0..8 {
            let (stream, _) =
                tokio::time::timeout(std::time::Duration::from_secs(10), ws_listener.accept())
                    .await??;
            let mut socket = tokio_tungstenite::accept_async(stream)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let subscription =
                match tokio::time::timeout(std::time::Duration::from_secs(5), socket.next()).await?
                {
                    Some(Ok(Message::Text(text))) => text.to_string(),
                    _ => {
                        return Err(std::io::Error::other(
                            "private subscription request missing",
                        ));
                    }
                };
            if let Some(sender) = subscription_tx.take() {
                let _sent = sender.send(subscription);
            }
            let frame = ws_frame_buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            socket
                .send(Message::Text(
                    String::from_utf8(frame)
                        .map_err(|error| std::io::Error::other(error.to_string()))?
                        .into(),
                ))
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            ws_frame_sent.store(true, Ordering::Release);
            frame_sent_notify.notify_one();
            loop {
                match socket.next().await {
                    Some(Ok(Message::Ping(payload))) => {
                        socket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|error| std::io::Error::other(error.to_string()))?;
                    }
                    Some(Ok(Message::Close(_frame))) => {
                        socket
                            .flush()
                            .await
                            .map_err(|error| std::io::Error::other(error.to_string()))?;
                        if ws_cancellation.is_cancelled() {
                            if let Some(sender) = ws_closed_tx.take() {
                                let _sent = sender.send(());
                            }
                            return Ok::<(), std::io::Error>(());
                        }
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(std::io::Error::other(error.to_string())),
                    None => break,
                }
            }
        }
        Err(std::io::Error::other(
            "private WebSocket fixture reconnect bound exhausted",
        ))
    });

    let private_client = UpbitPrivateClient::loopback_fixture(
        &format!("http://{rest_address}"),
        LiveExecutionPolicy::OperatorEnabled,
    )?;
    let account_identity = private_client.account_identity();
    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    venue_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .hide_order_readbacks = true;
    let service = live_service_with_private_client(
        database.clone(),
        root.0.clone(),
        Arc::clone(&venue_state),
        risk_guard,
        private_client.clone(),
    )?;
    let close_time = first_post_start_close(UtcTimestamp::now()).1;
    let (session_id, policy, close_time) = seed_live_session_with_baseline_for_account(
        &database,
        &service,
        close_time,
        &account_identity,
    )
    .await?;
    seed_causal_live_buy(&database, &policy, &session_id, close_time).await?;
    service
        .process_due_automation_sessions(UtcTimestamp::now())
        .await?;
    let pending_ledger = read_ledger(&service, &session_id).await?;
    assert_eq!(pending_ledger["orders"][0]["state"], "SUBMITTED_UNKNOWN");
    let (
        order_detail_body,
        open_orders_body_bytes,
        baseline_account_body_bytes,
        account_body_bytes,
        ws_frame_body,
    ) = {
        let state = venue_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let order = state
            .order
            .as_ref()
            .ok_or("synthetic order was not prepared")?;
        let detail = synthetic_order_detail(order, true);
        let detail_json = serde_json::to_vec(&detail)?;
        let open_orders_json = serde_json::to_vec(&vec![detail])?;
        let partial_volume = order.volume / Decimal::from(2);
        let partial_funds = order.funds / Decimal::from(2);
        let partial_fee = order.fee / Decimal::from(2);
        let balances = vec![
            UpbitAccountBalance {
                currency: "KRW".into(),
                balance: Decimal::from(INITIAL_KRW) - order.budget - order.fee,
                locked: order.budget - partial_funds + order.fee - partial_fee,
                avg_buy_price: Decimal::ZERO,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            },
            UpbitAccountBalance {
                currency: "BTC".into(),
                balance: partial_volume,
                locked: Decimal::ZERO,
                avg_buy_price: order.mark,
                avg_buy_price_modified: false,
                unit_currency: "KRW".into(),
            },
        ];
        let post_order_balances = serde_json::to_vec(&balances)?;
        let frame_timestamp = UtcTimestamp::now();
        let frame = serde_json::json!({
            "type": "myOrder",
            "code": order.market,
            "uuid": "synthetic-live-order-1",
            "ask_bid": "BID",
            "order_type": "price",
            "state": "trade",
            "trade_uuid": "synthetic-live-trade-1",
            "identifier": order.identifier,
            "price": order.mark.to_string(),
            "avg_price": order.mark.to_string(),
            "volume": order.volume.to_string(),
            "remaining_volume": "0.5",
            "executed_volume": partial_volume.to_string(),
            "trades_count": 1,
            "reserved_fee": order.fee.to_string(),
            "remaining_fee": (order.fee - partial_fee).to_string(),
            "paid_fee": partial_fee.to_string(),
            "locked": (order.budget - partial_funds + order.fee - partial_fee).to_string(),
            "executed_funds": partial_funds.to_string(),
            "trade_fee": partial_fee.to_string(),
            "is_maker": true,
            "smp_type": null,
            "prevented_volume": "0",
            "prevented_locked": "0",
            "order_timestamp": order.created_at.0.timestamp_millis(),
            "trade_timestamp": frame_timestamp.0.timestamp_millis(),
            "timestamp": frame_timestamp.0.timestamp_millis(),
            "stream_type": "REALTIME",
        });
        (
            detail_json,
            open_orders_json,
            post_order_balances.clone(),
            post_order_balances,
            serde_json::to_vec(&frame)?,
        )
    };
    *order_body
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = order_detail_body;
    *open_orders_body
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = open_orders_body_bytes;
    *baseline_account_body
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = baseline_account_body_bytes;
    *account_body
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = account_body_bytes;
    *frame_body
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = ws_frame_body;

    let (producer, consumer) = private_account_realtime_channels(&private_client);
    let private_task = tokio::spawn(run_private_account_stream_for_test(
        private_client.clone(),
        cancellation.clone(),
        producer,
        ws_endpoint,
        SessionTiming::test(),
    ));
    let coordinator_service = service.clone();
    let coordinator =
        tokio::spawn(async move { coordinator_service.coordinate(Some(consumer)).await });
    let _subscription = tokio::time::timeout(std::time::Duration::from_secs(10), subscription_rx)
        .await
        .map_err(|error| {
            std::io::Error::other(format!("private WS subscription timed out: {error}"))
        })??;
    let readiness = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if private_client
                .private_readiness_fence()
                .snapshot()
                .ready_for_decisions
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await;
    if let Err(error) = readiness {
        let health = private_client.private_readiness_fence().snapshot();
        service.inner.stop.cancel();
        cancellation.cancel();
        let coordinator_result = coordinator.await;
        let private_result = private_task.await;
        let websocket_result = ws_server.await;
        let _sent = rest_stop.send(());
        let _rest_result = rest_server.await;
        return Err(std::io::Error::other(format!(
            "private bootstrap readiness timed out: {error}; health={health:?}; frame_sent={}; account_reads={}, open_reads={}, exact_reads={}, closed_reads={}; task={private_result:?}; websocket={websocket_result:?}; coordinator={coordinator_result:?}",
            frame_sent.load(Ordering::Acquire),
            account_reads.load(Ordering::SeqCst),
            open_reads.load(Ordering::SeqCst),
            exact_order_reads.load(Ordering::SeqCst),
            closed_history_reads.load(Ordering::SeqCst),
        ))
        .into());
    }

    assert!(service.realtime_status().private.ready_for_decisions);
    assert_eq!(exact_order_reads.load(Ordering::SeqCst), 3);
    assert_eq!(open_reads.load(Ordering::SeqCst), 1);
    assert_eq!(closed_history_reads.load(Ordering::SeqCst), 1);
    assert!(account_reads.load(Ordering::SeqCst) >= 2);
    let observations = database
        .call("test_private_bootstrap_journal", {
            let account_identity = account_identity.clone();
            move |store| store.private_stream_observations(&account_identity, 0, 100)
        })
        .await?;
    let baseline_position = observations
        .iter()
        .position(|record| {
            record.observation.kind == crate::contracts::PrivateStreamObservationKind::Baseline
        })
        .ok_or("private baseline journal record missing")?;
    let stream_order_position = observations
        .iter()
        .position(|record| {
            record.observation.kind == crate::contracts::PrivateStreamObservationKind::OrderObserved
        })
        .ok_or("buffered private order journal record missing")?;
    let stream_order = &observations[stream_order_position].observation;
    assert_eq!(stream_order.event_key, "trade:synthetic-live-trade-1");
    assert_eq!(
        stream_order
            .transport
            .as_ref()
            .map(|transport| transport.message_sequence),
        Some(1),
        "the private REST readback is preceded by the buffered WebSocket trade"
    );
    let baseline_balances = observations[baseline_position].observation.payload["balances"]
        .as_array()
        .ok_or("private baseline journal omitted balances")?;
    let btc_balance = baseline_balances
        .iter()
        .find(|balance| balance["currency"] == "BTC")
        .and_then(|balance| balance["balance"].as_str())
        .ok_or("post-fill baseline omitted the BTC balance")?;
    assert_eq!(Decimal::from_str_exact(btc_balance)?, Decimal::new(5, 1));
    let reconciliation = observations
        .iter()
        .find(|record| {
            record.observation.kind
                == crate::contracts::PrivateStreamObservationKind::RestReconciled
        })
        .ok_or("bounded closed-order coverage marker missing")?;
    let complete_position = observations
        .iter()
        .position(|record| {
            record.observation.kind
                == crate::contracts::PrivateStreamObservationKind::BootstrapComplete
        })
        .ok_or("private bootstrap completion marker missing")?;
    assert!(baseline_position < stream_order_position);
    assert!(stream_order_position < complete_position);
    assert!(
        observations
            .iter()
            .position(|record| record.observation.kind
                == crate::contracts::PrivateStreamObservationKind::RestReconciled)
            .is_some_and(|position| position < complete_position)
    );
    assert_eq!(reconciliation.observation.payload["window_complete"], true);
    assert_eq!(
        reconciliation.observation.payload["full_history_complete"],
        false
    );
    assert_eq!(
        reconciliation.observation.payload["coverage_kind"],
        "CLOSED_ORDER_CREATED_TIME_RANGE"
    );
    assert_eq!(
        reconciliation.observation.payload["time_basis"],
        "PROVIDER_ORDER_CREATED_AT"
    );
    let ledger = read_ledger(&service, &session_id).await?;
    assert_eq!(ledger["fills"].as_array().map(Vec::len), Some(1));

    cancellation.cancel();
    private_task.await??;
    service.inner.stop.cancel();
    coordinator.await??;
    tokio::time::timeout(std::time::Duration::from_secs(2), ws_closed_rx).await??;
    ws_server.await??;
    let _sent = rest_stop.send(());
    rest_server.await??;
    drop(service);
    drop(send_control);
    drop(control);
    drop(database);
    owner.shutdown()?;
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "one financial journey binds approval, lost reply, partial fills, immutable risk reference and lifecycle receipts"
)]
async fn run_initial_live_journey(
    owner: &DatabaseOwner,
    root: PathBuf,
    venue_state: Arc<Mutex<SyntheticVenueState>>,
) -> TestResult<(
    AutomationSessionId,
    crate::contracts::AutomationLiveActivationReferenceView,
    serde_json::Value,
)> {
    let database = owner.handle();
    let risk_guard = crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let control = start_live_control(database.clone(), risk_guard.clone()).await?;
    let service = live_service(database.clone(), root, venue_state.clone(), risk_guard)?;

    let close_time = first_post_start_close(UtcTimestamp::now()).1;
    let started_at = UtcTimestamp(close_time.0 - chrono::Duration::seconds(30));
    let policy = create_policy(&database, started_at).await?;
    let (session_id, start_write) =
        start_live_session_fixture(&database, &policy, started_at).await?;
    let start_replay = service
        .automation_session_write(start_write.clone())
        .await?;
    assert_eq!(
        start_replay.receipt.outcome,
        AutomationCommandOutcome::Replayed
    );
    assert_eq!(start_replay.session.session_id, session_id);

    service
        .refresh_account(AutomationSessionMode::Live, LIVE_ACCOUNT_ID)
        .await?;
    let view_after_anchor = read_session(&service, &session_id).await?;
    let anchor = view_after_anchor
        .account_risk
        .as_ref()
        .and_then(|account| account.activation_reference.clone())
        .ok_or("LIVE activation refresh did not persist its account reference")?;
    assert_eq!(
        anchor.valuation_state,
        AutomationActivationValuationState::Verified
    );

    let day = UtcTimestamp::now().0.format("%Y-%m-%d").to_string();
    let midnight = UtcTimestamp::parse_rfc3339(&format!("{day}T00:00:00Z"))?;
    let baseline_write = AutomationSessionWrite::RecordBaseline {
        request_id: RequestId::new("synthetic-live-provider-asof-baseline")?,
        session_id: session_id.clone(),
        expected_sequence: view_after_anchor.sequence,
        baseline: AutomationAccountDayBaselineInput {
            account_identity: LIVE_ACCOUNT_ID.into(),
            day_utc: day.clone(),
            reference_time_utc: midnight,
            source: AutomationBaselineSource::ExchangeObservedDayStart,
            assets: vec![AutomationBaselineAssetInput {
                currency: "KRW".into(),
                free: AssetQuantity::new(Decimal::from(INITIAL_KRW))?,
                locked: AssetQuantity::new(Decimal::ZERO)?,
                valuation_basis:
                    crate::contracts::AutomationAssetValuationBasis::QuoteCurrencyParity,
                mark_price_krw: None,
            }],
            operator_note: None,
        },
    };
    let baseline = service.automation_session_write(baseline_write).await?;
    assert_eq!(baseline.receipt.outcome, AutomationCommandOutcome::Applied);
    assert_eq!(
        baseline
            .baseline
            .as_ref()
            .ok_or("baseline command omitted its verification projection")?
            .verification_state,
        crate::contracts::AutomationBaselineVerificationState::BlockedInsufficientEvidence
    );

    seed_causal_live_buy(&database, &policy, &session_id, close_time).await?;
    let stale_pause = service
        .automation_session_write(AutomationSessionWrite::Pause {
            request_id: RequestId::new("synthetic-live-stale-pause")?,
            session_id: session_id.clone(),
            expected_sequence: start_replay.session.sequence,
        })
        .await;
    assert!(matches!(
        stale_pause,
        Err(crate::contracts::LabError::Conflict(_))
    ));

    service
        .process_due_automation_sessions(UtcTimestamp::now())
        .await?;
    assert_eq!(venue_state_counts(&venue_state).0, 1);
    assert_eq!(venue_state_counts(&venue_state).1, 0);
    let unknown = read_ledger(&service, &session_id).await?;
    assert_eq!(unknown["orders"].as_array().map(Vec::len), Some(1));
    assert_eq!(unknown["orders"][0]["state"], "SUBMITTED_UNKNOWN");
    assert_eq!(unknown["fills"].as_array().map(Vec::len), Some(0));

    let current = read_session(&service, &session_id).await?;
    let pause_write = AutomationSessionWrite::Pause {
        request_id: RequestId::new("synthetic-live-pause")?,
        session_id: session_id.clone(),
        expected_sequence: current.sequence,
    };
    let paused = service
        .automation_session_write(pause_write.clone())
        .await?;
    assert_eq!(paused.session.state, AutomationSessionState::Paused);
    let pause_replay = service.automation_session_write(pause_write).await?;
    assert_eq!(
        pause_replay.receipt.outcome,
        AutomationCommandOutcome::Replayed
    );

    service
        .process_due_automation_sessions(UtcTimestamp::now())
        .await?;
    assert_eq!(venue_state_counts(&venue_state), (1, 1));
    let partial = read_ledger(&service, &session_id).await?;
    assert_eq!(partial["orders"][0]["state"], "PARTIALLY_FILLED");
    assert_eq!(partial["fills"].as_array().map(Vec::len), Some(1));

    service
        .process_due_automation_sessions(UtcTimestamp::now())
        .await?;
    assert_eq!(venue_state_counts(&venue_state), (1, 2));
    let ledger = read_ledger(&service, &session_id).await?;
    assert_eq!(ledger["orders"].as_array().map(Vec::len), Some(1));
    assert_eq!(ledger["orders"][0]["state"], "FILLED");
    assert_eq!(ledger["fills"].as_array().map(Vec::len), Some(2));
    let paid_fee: QuoteAmount =
        serde_json::from_value(ledger["orders"][0]["paid_fee_krw"].clone())?;
    assert_eq!(paid_fee.get(), Decimal::from(5));

    let running_view = read_session(&service, &session_id).await?;
    let effective_loss = assert_activation_projection(&running_view, Some(&anchor))?;
    assert_eq!(effective_loss["basis"], "LIVE_ACTIVATION_OBSERVED");
    assert_eq!(effective_loss["admission_blocked"], false);
    let used_loss: QuoteAmount = serde_json::from_value(effective_loss["used_krw"].clone())?;
    assert_eq!(used_loss.get(), Decimal::from(5));

    let resumed = service
        .automation_session_write(AutomationSessionWrite::Resume {
            request_id: RequestId::new("synthetic-live-resume")?,
            session_id: session_id.clone(),
            expected_sequence: paused.session.sequence,
        })
        .await?;
    assert_eq!(resumed.session.state, AutomationSessionState::Running);
    let stopped = service
        .automation_session_write(AutomationSessionWrite::Stop {
            request_id: RequestId::new("synthetic-live-stop")?,
            session_id: session_id.clone(),
            expected_sequence: resumed.session.sequence,
        })
        .await?;
    assert_eq!(stopped.session.state, AutomationSessionState::Stopped);
    assert_eq!(venue_state_counts(&venue_state), (1, 2));

    let events = service
        .automation_session_query(AutomationSessionQuery::Events {
            session_id: session_id.clone(),
            after_sequence: None,
            limit: 100,
        })
        .await?;
    let items = events["items"]
        .as_array()
        .ok_or("event page omitted items")?;
    assert_eq!(
        items
            .iter()
            .filter(|event| event["kind"] == "ORDER_SEND_CLAIMED")
            .count(),
        1
    );

    drop(service);
    drop(control);
    drop(database);
    Ok((session_id, anchor, ledger))
}

fn venue_state_counts(state: &Arc<Mutex<SyntheticVenueState>>) -> (usize, usize) {
    let state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    (state.submit_count, state.lookup_count)
}

#[test]
fn synthetic_live_service_lifecycle_books_partial_fills_and_uses_activation_loss_anchor()
-> TestResult {
    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open(root.0.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    let outcome = runtime.block_on(run_initial_live_journey(
        &owner,
        root.0.clone(),
        Arc::clone(&venue_state),
    ));
    drop(runtime);
    owner.shutdown()?;
    let (session_id, anchor, ledger) = outcome?;
    assert_eq!(venue_state_counts(&venue_state), (1, 2));

    let restored = DatabaseOwner::open(root.0.clone())?;
    let restored_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let recovered = restored_runtime.block_on(async {
        let risk_guard =
            crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
        let control = TradingControlService::production_with_policy(
            restored.handle(),
            risk_guard.clone(),
            ExchangeReadMode::LiveRead,
            LiveExecutionPolicy::OperatorEnabled,
        );
        control.synchronize_from_storage().await?;
        let status = control
            .execute(TradingControlRequest {
                action: TradingControlAction::Status,
                request_id: None,
                expected_sequence: None,
                approval_token: None,
            })
            .await?;
        assert_eq!(
            status.state,
            crate::upbit::control::TradingControlState::Paused
        );
        assert!(status.order_admission_blocked);
        let service = live_service(
            restored.handle(),
            root.0.clone(),
            venue_state.clone(),
            risk_guard,
        )?;
        let view = read_session(&service, &session_id).await?;
        assert_eq!(view.state, AutomationSessionState::Stopped);
        assert_activation_projection(&view, Some(&anchor))?;
        let recovered_ledger = read_ledger(&service, &session_id).await?;
        assert_eq!(recovered_ledger["orders"], ledger["orders"]);
        assert_eq!(recovered_ledger["fills"], ledger["fills"]);
        service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await?;
        assert_eq!(venue_state_counts(&venue_state), (1, 2));
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(restored_runtime);
    restored.shutdown()?;
    recovered?;
    Ok(())
}

#[tokio::test]
async fn live_order_disable_rejects_pending_live_intent_and_preserves_mock_reservation()
-> TestResult {
    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    crate::automation::PersistentMockVenue::new(database.clone())
        .initialize(UtcTimestamp::now())
        .await?;

    let risk_guard = crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let trading_control = start_live_control(database.clone(), risk_guard.clone()).await?;
    let gate = LiveOrderSendGate::new_disabled();
    let live_control = LiveOrderControlService::new(
        database.clone(),
        LiveExecutionPolicy::OperatorEnabled,
        Some(gate.clone()),
        ExchangeReadMode::Mock,
    );
    assert!(
        set_live_order_sending(
            &live_control,
            LiveOrderControlAction::Enable,
            "enable-pending-live"
        )
        .await?
        .live_trading_enabled
    );

    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    let adapter: Arc<dyn VenueAdapter> = Arc::new(SyntheticLiveVenue::new(venue_state));
    let service = live_service_with_adapter_and_gate(
        database.clone(),
        root.0.clone(),
        adapter,
        risk_guard,
        gate,
    )?;
    let (live_session_id, live_intent_id) = seed_pending_live_intent(&database, &service).await?;

    let mock_risk_guard =
        crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let (mock_order, _) = super::seed_reserved_mock_order(&database, &mock_risk_guard).await?;
    let disabled = set_live_order_sending(
        &live_control,
        LiveOrderControlAction::Disable,
        "disable-pending-live",
    )
    .await?;
    assert!(!disabled.live_trading_enabled);

    let session_for_read = live_session_id.clone();
    let pending = database
        .call("test_read_live_pending_after_disable", move |store| {
            store.automation_pending_intents(&session_for_read)
        })
        .await?;
    assert!(
        pending.is_empty(),
        "OFF left a LIVE intent pending for replay"
    );
    assert_intent_rejected_event(
        &service,
        &live_session_id,
        &live_intent_id,
        "LIVE_ORDER_SENDING_DISABLED",
    )
    .await?;

    let mock_identifier = mock_order.request.client_identifier.clone();
    let mock_readback = database
        .call(
            "test_read_mock_reservation_after_live_disable",
            move |store| store.automation_order_readback(&mock_identifier),
        )
        .await?
        .ok_or("LIVE switch cleanup removed the MOCK order")?;
    assert_eq!(
        mock_readback.state,
        crate::contracts::AutomationOrderState::Reserved
    );

    drop(service);
    drop(live_control);
    drop(trading_control);
    drop(database);
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn old_live_intent_cannot_be_reserved_after_disable_then_enable() -> TestResult {
    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    let risk_guard = crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let trading_control = start_live_control(database.clone(), risk_guard.clone()).await?;
    let gate = LiveOrderSendGate::new_disabled();
    let live_control = LiveOrderControlService::new(
        database.clone(),
        LiveExecutionPolicy::OperatorEnabled,
        Some(gate.clone()),
        ExchangeReadMode::Mock,
    );
    assert!(
        set_live_order_sending(
            &live_control,
            LiveOrderControlAction::Enable,
            "enable-stale-live"
        )
        .await?
        .live_trading_enabled
    );

    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter: Arc<dyn VenueAdapter> = Arc::new(BlockingOrderChanceVenue {
        inner: SyntheticLiveVenue::new(venue_state.clone()),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let service = Arc::new(live_service_with_adapter_and_gate(
        database.clone(),
        root.0.clone(),
        adapter,
        risk_guard,
        gate,
    )?);
    let (session_id, intent_id) = seed_pending_live_intent(&database, &service).await?;

    let processing_service = Arc::clone(&service);
    let processing = tokio::spawn(async move {
        processing_service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified()).await?;

    assert!(
        !set_live_order_sending(
            &live_control,
            LiveOrderControlAction::Disable,
            "disable-stale-live"
        )
        .await?
        .live_trading_enabled
    );
    assert!(
        set_live_order_sending(
            &live_control,
            LiveOrderControlAction::Enable,
            "reenable-stale-live"
        )
        .await?
        .live_trading_enabled
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(10), processing).await???;

    assert_eq!(
        venue_state_counts(&venue_state).0,
        0,
        "old LIVE intent reached submit"
    );
    let ledger = read_ledger(&service, &session_id).await?;
    assert_eq!(ledger["orders"].as_array().map(Vec::len), Some(0));
    assert_eq!(ledger["fills"].as_array().map(Vec::len), Some(0));
    let session_for_read = session_id.clone();
    let pending = database
        .call(
            "test_read_stale_live_pending_after_reenable",
            move |store| store.automation_pending_intents(&session_for_read),
        )
        .await?;
    assert!(pending.is_empty(), "old intent was not durably rejected");
    assert_intent_rejected_event(
        &service,
        &session_id,
        &intent_id,
        "LIVE_ORDER_SENDING_DISABLED",
    )
    .await?;

    drop(service);
    drop(live_control);
    drop(trading_control);
    drop(database);
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the race fixture holds a real decision batch across a durable LIVE switch transition"
)]
async fn disable_then_enable_during_live_decision_account_read_rejects_old_decision() -> TestResult
{
    use axum::http::StatusCode;

    let root = TestRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    let risk_guard = crate::upbit::risk::RiskGuard::new(crate::upbit::risk::RiskConfig::default());
    let trading_control = start_live_control(database.clone(), risk_guard.clone()).await?;
    let gate = LiveOrderSendGate::new_disabled();
    let live_control = LiveOrderControlService::new(
        database.clone(),
        LiveExecutionPolicy::OperatorEnabled,
        Some(gate.clone()),
        ExchangeReadMode::Mock,
    );
    assert!(
        set_live_order_sending(
            &live_control,
            LiveOrderControlAction::Enable,
            "enable-decision-race"
        )
        .await?
        .live_trading_enabled
    );

    let close_time = first_post_start_close(UtcTimestamp::now()).1;
    let open_time = close_time.0 - chrono::Duration::minutes(1);
    let candle_body = serde_json::json!([{
        "market": MARKET,
        "candle_date_time_utc": open_time.format("%Y-%m-%dT%H:%M:%S").to_string(),
        "opening_price": 10000,
        "high_price": 10000,
        "low_price": 10000,
        "trade_price": 10000,
        "candle_acc_trade_volume": 1,
        "candle_acc_trade_price": 10000
    }])
    .to_string();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = axum::Router::new().fallback(move || {
        let body = candle_body.clone();
        async move {
            (
                StatusCode::OK,
                [("Remaining-Req", "group=candle; min=1800; sec=59")],
                body,
            )
        }
    });
    let (stop_http, stopped_http) = tokio::sync::oneshot::channel();
    let http_server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped_http.await;
            })
            .await
    });
    let upbit = crate::market_data::UpbitClient::synthetic_local(&format!("http://{address}"))?;

    let venue_state = Arc::new(Mutex::new(SyntheticVenueState::new()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocking_venue = BlockingAccountBalanceVenue {
        inner: SyntheticLiveVenue::new(venue_state.clone()),
        block_next: Arc::new(AtomicBool::new(false)),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    let adapter: Arc<dyn VenueAdapter> = Arc::new(blocking_venue.clone());
    let service = Arc::new(live_service_with_client_adapter_and_gate(
        database.clone(),
        root.0.clone(),
        upbit,
        adapter,
        risk_guard,
        gate,
    ));
    let (session_id, _policy, _) =
        seed_live_session_with_baseline(&database, &service, close_time).await?;

    blocking_venue.arm();
    let processing_service = Arc::clone(&service);
    let processing = tokio::spawn(async move {
        processing_service
            .process_due_automation_sessions(UtcTimestamp::now())
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified()).await?;

    assert!(
        !set_live_order_sending(
            &live_control,
            LiveOrderControlAction::Disable,
            "disable-decision-race"
        )
        .await?
        .live_trading_enabled
    );
    assert!(
        set_live_order_sending(
            &live_control,
            LiveOrderControlAction::Enable,
            "reenable-decision-race"
        )
        .await?
        .live_trading_enabled
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(15), processing).await???;

    let session = read_session(&service, &session_id).await?;
    assert_eq!(session.last_finalized_candle_at, Some(close_time));
    let decisions = service
        .automation_session_query(AutomationSessionQuery::Decisions {
            session_id: session_id.clone(),
            offset: 0,
            limit: 100,
        })
        .await?;
    let decision_items = decisions["items"]
        .as_array()
        .ok_or("decision page omitted items")?;
    assert_eq!(decision_items.len(), 1);
    assert_eq!(decision_items[0]["state"], "REJECTED");
    let intent_id = crate::contracts::AutomationIntentId::new(
        decision_items[0]["intent_id"]
            .as_str()
            .ok_or("decision page omitted intent identity")?,
    )?;
    assert_intent_rejected_event(
        &service,
        &session_id,
        &intent_id,
        "LIVE_ORDER_SEND_GENERATION_CHANGED",
    )
    .await?;

    let session_for_read = session_id.clone();
    let pending = database
        .call("test_read_decision_race_pending_intents", move |store| {
            store.automation_pending_intents(&session_for_read)
        })
        .await?;
    assert!(pending.is_empty());
    let ledger = read_ledger(&service, &session_id).await?;
    assert_eq!(ledger["orders"].as_array().map(Vec::len), Some(0));
    assert_eq!(ledger["fills"].as_array().map(Vec::len), Some(0));
    assert_eq!(venue_state_counts(&venue_state).0, 0);

    let _ = stop_http.send(());
    http_server.await??;
    drop(service);
    drop(live_control);
    drop(trading_control);
    drop(database);
    owner.shutdown()?;
    Ok(())
}

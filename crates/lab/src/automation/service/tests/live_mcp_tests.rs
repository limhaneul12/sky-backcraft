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
            order: None,
        }
    }
}

#[derive(Clone)]
struct SyntheticLiveVenue {
    state: Arc<Mutex<SyntheticVenueState>>,
}

impl SyntheticLiveVenue {
    fn new(state: Arc<Mutex<SyntheticVenueState>>) -> Self {
        Self { state }
    }
}

impl VenueAdapter for SyntheticLiveVenue {
    fn is_live(&self) -> bool {
        true
    }

    fn account_identity(&self) -> String {
        LIVE_ACCOUNT_ID.into()
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
    let account_for_begin = LIVE_ACCOUNT_ID.to_owned();
    let account_for_write = LIVE_ACCOUNT_ID.to_owned();
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
    let evaluation_json = super::super::evaluation_to_value(&evaluation);
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
    let started_at = UtcTimestamp(close_time.0 - chrono::Duration::seconds(30));
    let policy = create_policy(database, started_at).await?;
    let (session_id, _) = start_live_session_fixture(database, &policy, started_at).await?;

    service
        .refresh_account(AutomationSessionMode::Live, LIVE_ACCOUNT_ID)
        .await?;
    let view_after_refresh = read_session(service, &session_id).await?;
    let day = UtcTimestamp::now().0.format("%Y-%m-%d").to_string();
    let midnight = UtcTimestamp::parse_rfc3339(&format!("{day}T00:00:00Z"))?;
    let baseline = AutomationSessionWrite::RecordBaseline {
        request_id: RequestId::new("live-order-switch-pending-baseline")?,
        session_id: session_id.clone(),
        expected_sequence: view_after_refresh.sequence,
        baseline: AutomationAccountDayBaselineInput {
            account_identity: LIVE_ACCOUNT_ID.into(),
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

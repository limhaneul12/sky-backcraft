//! Opt-in end-to-end proof for the existing persistent paper session.
//!
//! This test contacts only public Upbit quotation endpoints. It mutates an
//! explicitly supplied isolated database copy; it never opens the configured
//! original root and never constructs a private or trading client.

use rust_decimal::Decimal;
use spot_lab::contracts::{
    CandleInterval, CandleObservation, LabError, MarketDataOrigin, MarketId, ObservationId,
    PaperEventKind, PaperFreshness, PaperSessionEvent, PaperSessionId, PaperSessionMode,
    PaperSessionState, PaperVerificationStatus, ProbeCount, RawObjectRef, UtcTimestamp,
};
use spot_lab::database::{DatabaseHandle, DatabaseOwner};
use spot_lab::market_data::UpbitClient;
use spot_lab::realtime::{PublicMarketFeed, PublicMarketFeedConfig, PublicMarketSubscription};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const SESSION_ID: &str =
    "paper-session-83af0a9b150da971ee2a4616b8e821c5e981f6b88d7d756469f7ccc5f57bc555";
const ORIGIN_REQUEST_ID: &str = "paper-r1-v1-btc-xrp-eth-20261017-100k-each-start";
const POLICY_ID: &str = "policy-1fdab53dd8008615b35dbb728ccab6b4fb88f25377564dff262ff91cedd00bb5";
const POLICY_REVISION_ID: &str =
    "policy-revision-8fdeae8a65ae465d09803155eb3312f4846f31048cf319a5916ed034f7438195";
const POLICY_DIGEST: &str = "c9421936fd732d58c7e67e7ba64f17816a15f3f303046c31fef91bdf0f426093";
const MARKETS: [&str; 3] = ["KRW-BTC", "KRW-XRP", "KRW-ETH"];
const REST_HISTORY_BARS: u32 = 199;
const READINESS_DEADLINE: Duration = Duration::from_secs(120);
const READINESS_REST_REFRESH: Duration = Duration::from_secs(10);
const STABLE_CONNECTION_WINDOW: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct MarketBatch {
    subscription: PublicMarketSubscription,
    raw: RawObjectRef,
    observations: Vec<CandleObservation>,
}

struct DurableSnapshot {
    view: spot_lab::contracts::PaperSessionView,
    events: Vec<PaperSessionEvent>,
}

type FrozenSourceCloses = BTreeMap<ObservationId, (MarketId, CandleInterval, UtcTimestamp)>;

#[expect(
    clippy::too_many_lines,
    reason = "one opt-in incident journey owns isolated-copy fences, recovery, replay and reopen evidence"
)]
#[test]
#[ignore = "contacts public Upbit quotation endpoints and advances only an isolated paper DB copy"]
fn existing_paper_session_evaluates_public_completed_candles_idempotently()
-> Result<(), Box<dyn Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| {
            LabError::Internal("isolated paper E2E TLS provider already installed".into())
        })?;

    let copy_root = required_existing_root("P1_PAPER_COPY_ROOT")?;
    let original_root = required_existing_root("P1_PAPER_ORIGINAL_ROOT")?;
    if copy_root == original_root
        || copy_root.starts_with(&original_root)
        || original_root.starts_with(&copy_root)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "paper E2E copy and original roots overlap",
        )
        .into());
    }
    // A swapped/mistyped pair of environment paths must not open production.
    let source_marker = std::fs::read_to_string(copy_root.join(".p1-isolated-source"))?;
    if PathBuf::from(source_marker.trim()).canonicalize()? != original_root {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "isolated-copy source marker mismatch",
        )
        .into());
    }
    if !copy_root.join("lab.sqlite").is_file() || !copy_root.join("raw").is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "paper E2E copy must contain lab.sqlite and the copied raw-object tree",
        )
        .into());
    }

    let session_id = PaperSessionId::new(SESSION_ID)?;
    // DatabaseOwner is deliberately opened only after the canonical-root fence.
    let owner = DatabaseOwner::open(copy_root.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let baseline_at = UtcTimestamp::now();
    let baseline = runtime.block_on(read_snapshot(&database, session_id.clone(), baseline_at))?;
    assert_original_session_identity(&baseline.view);
    assert_eq!(baseline.view.state, PaperSessionState::Running);
    assert_eq!(
        baseline.view.event_count,
        u64::try_from(baseline.events.len())?
    );

    let result = runtime.block_on(run_public_paper_e2e(
        database.clone(),
        session_id.clone(),
        baseline.view.sequence,
        baseline.view.started_at,
    ));

    // Stop and join the owned public feed before closing the sole DB owner.
    let shutdown = owner.shutdown();
    let evidence = result?;
    shutdown?;

    assert_eq!(
        serde_json::to_value(&baseline.events)?,
        serde_json::to_value(&evidence.after.events[..baseline.events.len()])?,
        "public recovery rewrote the existing nonempty ledger prefix",
    );
    let source_closes = frozen_event_sources(&copy_root, &evidence.events_after_baseline)?;
    assert_three_market_policy_evaluations(
        &evidence.events_after_baseline,
        &source_closes,
        evidence.evaluated_at,
    )?;

    let reopened = DatabaseOwner::open(copy_root)?;
    let reopened_snapshot = runtime.block_on(read_snapshot(
        &reopened.handle(),
        session_id,
        evidence.evaluated_at,
    ))?;
    assert_original_session_identity(&reopened_snapshot.view);
    assert_eq!(
        durable_json(&evidence.after)?,
        durable_json(&reopened_snapshot)?,
        "reopening the isolated DB changed the paper session or event history",
    );
    reopened.shutdown()?;

    println!(
        "P1 public paper E2E PASS: session={}, evaluation_at={}, latest_eval_at={}, sequence={}, events={}, baseline_events={}, preserved_prefix=true, markets={}, stable_ws_seconds={}, reconnects={}, replay_advanced={}, reopen_preserved=true, simulated_only=true",
        SESSION_ID,
        evidence.evaluated_at,
        evidence
            .after
            .view
            .last_evaluated_at
            .ok_or_else(|| io::Error::other("evaluation timestamp missing"))?,
        evidence.after.view.sequence,
        evidence.after.view.event_count,
        baseline.view.event_count,
        MARKETS.join(","),
        STABLE_CONNECTION_WINDOW.as_secs(),
        evidence.reconnect_count,
        evidence.replay_advanced,
    );
    for (market, (h1_prior, h4_prior)) in &evidence.warmup_counts {
        println!(
            "P1 MTF evidence: market={market}, h1_prior_completed={h1_prior}, h4_prior_completed={h4_prior}, required_h1=21, required_h4=60"
        );
    }
    for market in &evidence.after.view.markets {
        println!(
            "P1 paper account: market={}, freshness={:?}, mark_price={:?}, equity={:?}, finalized_at={:?}, source_at={:?}, account_mark_at={:?}, signals={}, virtual_fills={}, supported_verification={:?}, verification_scope={}",
            market.market.code(),
            market.freshness,
            market.mark_price,
            market.equity,
            market.last_finalized_candle_at,
            market.last_source_event_at,
            market.last_account_mark_at,
            market.signal_count,
            market.fill_count,
            market.verification.status,
            market.verification.reason,
        );
    }
    for event in evidence.events_after_baseline {
        let market = event
            .market
            .as_ref()
            .map_or_else(|| "<none>".to_owned(), MarketId::code);
        let close = event
            .candle
            .as_ref()
            .map(|candle| candle.close_time.to_string());
        let outcomes = event
            .signals
            .iter()
            .map(|signal| format!("{:?}:{:?}", signal.intended_side, signal.outcome))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "P1 durable decision: sequence={}, market={}, interval={:?}, candle_close={:?}, outcomes=[{}], virtual_orders={}, virtual_fills={}",
            event.sequence,
            market,
            event.candle.as_ref().map(|candle| candle.interval),
            close,
            outcomes,
            event.orders.len() + event.order_events.len(),
            event.fills.len(),
        );
    }
    Ok(())
}

struct E2eEvidence {
    after: DurableSnapshot,
    events_after_baseline: Vec<PaperSessionEvent>,
    evaluated_at: UtcTimestamp,
    reconnect_count: u64,
    replay_advanced: u32,
    warmup_counts: std::collections::BTreeMap<String, (usize, usize)>,
}

#[expect(
    clippy::too_many_lines,
    reason = "one bounded public-feed owner joins on every outcome and returns durable recovery evidence"
)]
async fn run_public_paper_e2e(
    database: DatabaseHandle,
    session_id: PaperSessionId,
    baseline_sequence: u64,
    started_at: UtcTimestamp,
) -> Result<E2eEvidence, Box<dyn Error>> {
    let (feed, feed_owner) = PublicMarketFeed::start(
        UpbitClient::new()?,
        database.clone(),
        PublicMarketFeedConfig::upbit_production()?,
    )?;

    let run = async {
        let subscriptions = required_subscriptions()?;
        feed.replace_subscriptions(subscriptions.clone())?;
        let count = ProbeCount::try_from(REST_HISTORY_BARS)?;
        let mut batches = Vec::with_capacity(subscriptions.len());
        for subscription in &subscriptions {
            let batch = fetch_and_validate(&feed, subscription, count).await?;
            batches.push(batch);
        }

        await_feed_readiness(&feed, &subscriptions, count, &mut batches).await?;
        let stable_health = await_stable_connection(&feed, &subscriptions).await?;
        println!(
            "P1 public feed stable: connections={}, subscriptions={}, reconnects={}, seconds={}",
            stable_health.status.connection_count,
            stable_health.status.subscription_count,
            stable_health.status.reconnect_count,
            STABLE_CONNECTION_WINDOW.as_secs()
        );
        assert_eq!(stable_health.status.subscription_count, 9);
        assert_eq!(stable_health.status.connection_count, 1);
        assert!(stable_health.status.last_received_at_utc.is_some());
        assert!(stable_health.last_pong_at_utc.is_some());
        assert!(stable_health.status.ready_for_decisions);
        for subscription in &subscriptions {
            feed.ensure_ready(&subscription.market, subscription.interval)?;
        }

        let evaluated_at = UtcTimestamp::now();
        for batch in &batches {
            validate_batch(batch, count, evaluated_at)?;
        }
        let warmup_counts = assert_mtf_history(&batches, started_at)?;
        let fetched_batches = batches
            .iter()
            .map(|batch| (batch.raw.clone(), batch.observations.clone()))
            .collect::<Vec<_>>();
        let advanced = database
            .call("p1_public_paper_recovery", move |store| {
                store.reconcile_running_paper_sessions(&fetched_batches, evaluated_at)
            })
            .await?;
        assert!(
            advanced > 0,
            "the existing RUNNING paper session did not advance"
        );

        let after = read_snapshot(&database, session_id.clone(), evaluated_at).await?;
        assert_original_session_identity(&after.view);
        assert_eq!(after.view.state, PaperSessionState::Running);
        assert!(after.view.last_evaluated_at.is_some());
        assert!(after.view.event_count > 0);
        assert!(after.view.sequence > baseline_sequence);
        assert_eq!(
            after.view.event_count,
            u64::try_from(after.events.len())?,
            "session event count differs from its durable event history",
        );
        assert_market_account_marks(&after.view, evaluated_at);

        let events_after_baseline = after
            .events
            .iter()
            .filter(|event| event.sequence > baseline_sequence)
            .cloned()
            .collect::<Vec<_>>();
        let replay_batches = batches
            .iter()
            .map(|batch| (batch.raw.clone(), batch.observations.clone()))
            .collect::<Vec<_>>();
        let replay_advanced = database
            .call("p1_public_paper_replay", move |store| {
                store.reconcile_running_paper_sessions(&replay_batches, evaluated_at)
            })
            .await?;
        assert_eq!(
            replay_advanced, 0,
            "replaying identical batches advanced the session"
        );
        let replayed = read_snapshot(&database, session_id, evaluated_at).await?;
        assert_eq!(
            durable_json(&after)?,
            durable_json(&replayed)?,
            "replaying identical completed-candle batches changed durable paper state",
        );

        Ok::<_, Box<dyn Error>>(E2eEvidence {
            after,
            events_after_baseline,
            evaluated_at,
            reconnect_count: stable_health.status.reconnect_count,
            replay_advanced,
            warmup_counts,
        })
    }
    .await;

    drop(feed);
    let joined = feed_owner.shutdown().await;
    match (run, joined) {
        (Ok(evidence), Ok(())) => Ok(evidence),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
        (Err(error), Err(join_error)) => Err(io::Error::other(format!(
            "paper E2E failed ({error}); public feed shutdown also failed ({join_error})"
        ))
        .into()),
    }
}

async fn fetch_and_validate(
    feed: &PublicMarketFeed,
    subscription: &PublicMarketSubscription,
    count: ProbeCount,
) -> Result<MarketBatch, Box<dyn Error>> {
    let deadline = Instant::now() + READINESS_DEADLINE;
    let (raw, observations) = loop {
        match feed
            .read_or_bootstrap_observations(&subscription.market, subscription.interval, count)
            .await
        {
            Ok(batch) => break batch,
            Err(error) if is_transient_feed_error(&error) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let batch = MarketBatch {
        subscription: subscription.clone(),
        raw,
        observations,
    };
    validate_batch(&batch, count, UtcTimestamp::now())?;
    Ok(batch)
}

async fn await_feed_readiness(
    feed: &PublicMarketFeed,
    subscriptions: &[PublicMarketSubscription],
    count: ProbeCount,
    batches: &mut [MarketBatch],
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + READINESS_DEADLINE;
    let mut next_rest_refresh = Instant::now();
    loop {
        let health = feed.health();
        let missing = subscriptions
            .iter()
            .filter(|subscription| !feed.is_ready(&subscription.market, subscription.interval))
            .collect::<Vec<_>>();
        if missing.is_empty()
            && health.status.connection_count == 1
            && health.status.subscription_count == 9
            && health.status.last_received_at_utc.is_some()
            && health.last_pong_at_utc.is_some()
            && health.status.ready_for_decisions
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            eprintln!(
                "P1 public readiness timed out: state={:?}, connections={}, subscriptions={}, reconnects={}, received={}, pong={}, ready={}, missing={}",
                health.status.state,
                health.status.connection_count,
                health.status.subscription_count,
                health.status.reconnect_count,
                health.status.last_received_at_utc.is_some(),
                health.last_pong_at_utc.is_some(),
                health.status.ready_for_decisions,
                missing
                    .iter()
                    .map(|item| format!("{}:{:?}", item.market.code(), item.interval))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            return Err(LabError::NetworkUnavailable(
                "public paper feed did not become ready before the bounded E2E deadline".into(),
            )
            .into());
        }

        if Instant::now() >= next_rest_refresh {
            for subscription in missing {
                match fetch_and_validate(feed, subscription, count).await {
                    Ok(batch) => {
                        if let Some(slot) = batches.iter_mut().find(|candidate| {
                            candidate.subscription.market == subscription.market
                                && candidate.subscription.interval == subscription.interval
                        }) {
                            *slot = batch;
                        }
                    }
                    Err(error)
                        if error
                            .downcast_ref::<LabError>()
                            .is_some_and(is_transient_feed_error) =>
                    {
                        eprintln!(
                            "P1 public recovery probe pending: market={}, interval={:?}, error={error}",
                            subscription.market.code(),
                            subscription.interval,
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
            next_rest_refresh = Instant::now() + READINESS_REST_REFRESH;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn is_transient_feed_error(error: &LabError) -> bool {
    matches!(
        error,
        LabError::DataGap(_)
            | LabError::BlockedEvidence(_)
            | LabError::NetworkUnavailable(_)
            | LabError::RateLimited(_)
            | LabError::TemporarilyBlocked(_)
    )
}

async fn await_stable_connection(
    feed: &PublicMarketFeed,
    subscriptions: &[PublicMarketSubscription],
) -> Result<spot_lab::realtime::PublicMarketFeedHealth, Box<dyn Error>> {
    let started = Instant::now();
    let baseline = feed.health();
    let reconnects = baseline.status.reconnect_count;
    let deadline = started + READINESS_DEADLINE;
    loop {
        let current = feed.health();
        if current.status.connection_count != 1
            || current.status.subscription_count != 9
            || current.status.reconnect_count != reconnects
        {
            return Err(LabError::NetworkUnavailable(format!(
                "public connection changed during {} second stability window; state={:?}, connections={}, subscriptions={}, reconnects={} (baseline={reconnects})",
                STABLE_CONNECTION_WINDOW.as_secs(),
                current.status.state,
                current.status.connection_count,
                current.status.subscription_count,
                current.status.reconnect_count,
            ))
            .into());
        }
        let blocked = subscriptions
            .iter()
            .find(|subscription| !feed.is_ready(&subscription.market, subscription.interval));
        if current.status.ready_for_decisions
            && blocked.is_none()
            && started.elapsed() >= STABLE_CONNECTION_WINDOW
        {
            return Ok(current);
        }
        if Instant::now() >= deadline {
            return Err(LabError::NetworkUnavailable(
                "public connection stayed open but inputs did not reconcile within the deadline"
                    .into(),
            )
            .into());
        }
        // A real candle boundary can require REST confirmation during this window.
        // Continue the production feed's reconciliation contract instead of treating
        // a briefly pending completed tip as a socket disconnect.
        if let Some(subscription) = blocked {
            match feed
                .read_or_bootstrap_observations(
                    &subscription.market,
                    subscription.interval,
                    ProbeCount::try_from(REST_HISTORY_BARS)?,
                )
                .await
            {
                Ok(_) => {}
                Err(error) if is_transient_feed_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn required_subscriptions() -> Result<Vec<PublicMarketSubscription>, LabError> {
    let mut subscriptions = Vec::with_capacity(9);
    for market in MARKETS {
        let market = MarketId::parse_upbit(market)?;
        for interval in [CandleInterval::M5, CandleInterval::H1, CandleInterval::H4] {
            subscriptions.push(PublicMarketSubscription::new(market.clone(), interval));
        }
    }
    Ok(subscriptions)
}

fn validate_batch(
    batch: &MarketBatch,
    requested: ProbeCount,
    evaluated_at: UtcTimestamp,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(batch.raw.origin, MarketDataOrigin::ExchangeObserved);
    assert_eq!(batch.raw.http_status, 200);
    assert!(
        batch.raw.transport.is_none(),
        "bootstrap should retain REST provenance"
    );
    assert_eq!(
        batch.observations.len(),
        usize::try_from(requested.get())?,
        "REST history did not return the requested completed-candle count",
    );

    let expected_market = batch.subscription.market.code();
    let mut prior_open = None;
    let mut seen_opens = BTreeSet::new();
    for observation in &batch.observations {
        let candle = &observation.candle;
        assert_eq!(candle.market, expected_market);
        assert_eq!(candle.interval, batch.subscription.interval);
        assert!(candle.completed, "forming candle entered paper history");
        assert!(candle.close_time_utc <= batch.raw.fetched_at);
        assert!(candle.close_time_utc <= evaluated_at);
        assert!(candle.close_time_utc > candle.open_time_utc);
        assert!(seen_opens.insert(candle.open_time_utc));
        if let Some(previous) = prior_open {
            assert!(
                previous < candle.open_time_utc,
                "REST candles are not ascending"
            );
        }
        prior_open = Some(candle.open_time_utc);
    }
    Ok(())
}

fn assert_mtf_history(
    batches: &[MarketBatch],
    started_at: UtcTimestamp,
) -> Result<std::collections::BTreeMap<String, (usize, usize)>, Box<dyn Error>> {
    let mut counts = std::collections::BTreeMap::new();
    for market in MARKETS {
        let h1 = batches
            .iter()
            .find(|batch| {
                batch.subscription.market.code() == market
                    && batch.subscription.interval == CandleInterval::H1
            })
            .ok_or_else(|| io::Error::other("H1 batch missing"))?;
        let h4 = batches
            .iter()
            .find(|batch| {
                batch.subscription.market.code() == market
                    && batch.subscription.interval == CandleInterval::H4
            })
            .ok_or_else(|| io::Error::other("H4 batch missing"))?;
        let h1_prior = h1
            .observations
            .iter()
            .filter(|observation| observation.candle.close_time_utc <= started_at)
            .count();
        let h4_prior = h4
            .observations
            .iter()
            .filter(|observation| observation.candle.close_time_utc <= started_at)
            .count();
        assert!(
            h1_prior >= 21,
            "H1 prior-high/low history is insufficient for {market}"
        );
        assert!(
            h4_prior >= 60,
            "H4 EMA60 history is insufficient for {market}"
        );
        counts.insert(market.to_owned(), (h1_prior, h4_prior));
    }
    Ok(counts)
}

fn assert_market_account_marks(
    view: &spot_lab::contracts::PaperSessionView,
    evaluated_at: UtcTimestamp,
) {
    assert_eq!(view.markets.len(), MARKETS.len());
    for market in &view.markets {
        assert!(
            market.mark_price.is_some(),
            "{} has no account mark price",
            market.market.code()
        );
        assert!(
            market.equity.is_some(),
            "{} has no computed equity",
            market.market.code()
        );
        assert!(market.last_finalized_candle_at.is_some());
        assert!(market.last_source_event_at.is_some());
        assert!(market.last_account_mark_at.is_some());
        assert!(matches!(
            market.freshness,
            PaperFreshness::Live | PaperFreshness::Delayed
        ));
        assert!(
            market
                .last_finalized_candle_at
                .is_some_and(|at| at <= evaluated_at)
        );
        assert!(
            market
                .last_source_event_at
                .is_some_and(|at| at <= evaluated_at)
        );
        assert!(
            market
                .last_account_mark_at
                .is_some_and(|at| at <= evaluated_at)
        );
        assert!(
            market.signal_count > 0,
            "{} has no persisted policy signal",
            market.market.code()
        );
        assert_eq!(
            market.verification.status,
            PaperVerificationStatus::Pass,
            "{} independent supported accounting/event verification failed: {}",
            market.market.code(),
            market.verification.reason,
        );
    }
}

fn assert_three_market_policy_evaluations(
    events: &[PaperSessionEvent],
    source_closes: &FrozenSourceCloses,
    evaluated_at: UtcTimestamp,
) -> Result<(), Box<dyn Error>> {
    let mut evaluated = BTreeSet::new();
    let mut seen_markets = BTreeSet::new();
    for event in events {
        if event.kind != PaperEventKind::CandleProcessed {
            continue;
        }
        let Some(candle) = event.candle.as_ref() else {
            continue;
        };
        assert_eq!(candle.interval, CandleInterval::M5);
        assert!(
            candle.close_time <= evaluated_at,
            "paper event references a future decision bar"
        );
        let Some(market) = event.market.as_ref() else {
            continue;
        };
        let initial_bar = seen_markets.insert(market.code());
        if event.signals.is_empty() {
            // NextBarOpen seeds one execution bar before evaluating its close
            // on the next bar; only that first event may be initialization-only.
            assert!(
                initial_bar,
                "a post-initialization candle lacks policy evaluation"
            );
            assert!(event.orders.is_empty() && event.fills.is_empty());
            continue;
        }
        for signal in &event.signals {
            assert!(
                signal.policy_trace.is_some(),
                "persisted signal lacks policy evaluation trace"
            );
            assert!(
                !signal.source_bar_ids.is_empty(),
                "policy signal has no source-bar provenance"
            );
            for source_id in &signal.source_bar_ids {
                let (source_market, _source_interval, source_close) =
                    source_closes.get(source_id).ok_or_else(|| {
                        io::Error::other(
                            "signal source bar missing from its immutable saved observation",
                        )
                    })?;
                assert_eq!(source_market.code(), market.code());
                assert!(
                    *source_close <= signal.signal_time,
                    "policy signal consumed a source candle closed after its decision candle",
                );
            }
        }
        evaluated.insert(market.code());
    }
    assert_eq!(
        evaluated,
        MARKETS.into_iter().map(str::to_owned).collect(),
        "the existing paper session did not evaluate all three independent accounts",
    );
    Ok(())
}

fn frozen_event_sources(
    copy_root: &std::path::Path,
    events: &[PaperSessionEvent],
) -> Result<FrozenSourceCloses, Box<dyn Error>> {
    // The feed and DatabaseOwner are already joined. This independent reader can
    // never write, and follows exact historical IDs rather than a later REST revision.
    let connection = rusqlite::Connection::open_with_flags(
        copy_root.join("lab.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let ids = events
        .iter()
        .flat_map(|event| event.signals.iter())
        .flat_map(|signal| signal.source_bar_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut sources = BTreeMap::new();
    let mut statement =
        connection.prepare("SELECT observation_json FROM candle_observations WHERE id=?1")?;
    for id in ids {
        let json: String = statement.query_row([id.as_str()], |row| row.get(0))?;
        let observation: CandleObservation = serde_json::from_str(&json)?;
        assert_eq!(observation.id, id);
        assert!(observation.candle.completed);
        assert_eq!(
            observation.content_digest,
            spot_lab::contracts::observation_digest(&observation.candle)?,
        );
        assert_eq!(
            observation.id,
            ObservationId::from_seed(observation.content_digest.as_str()),
        );
        sources.insert(
            id,
            (
                MarketId::parse_upbit(&observation.candle.market)?,
                observation.candle.interval,
                observation.candle.close_time_utc,
            ),
        );
    }
    Ok(sources)
}

async fn read_snapshot(
    database: &DatabaseHandle,
    session_id: PaperSessionId,
    at: UtcTimestamp,
) -> Result<DurableSnapshot, LabError> {
    database
        .call("p1_paper_durable_snapshot", move |store| {
            let view = store.paper_get_session(&session_id, at)?.session;
            let mut events = Vec::new();
            let mut after_sequence = None;
            loop {
                let page = store.paper_list_events(&session_id, after_sequence, 100)?;
                events.extend(page.items);
                after_sequence = page.next_cursor.map(|cursor| cursor.sequence);
                if after_sequence.is_none() {
                    break;
                }
            }
            Ok(DurableSnapshot { view, events })
        })
        .await
}

fn durable_json(snapshot: &DurableSnapshot) -> Result<serde_json::Value, LabError> {
    Ok(serde_json::json!({
        "session": serde_json::to_value(&snapshot.view)?,
        "events": serde_json::to_value(&snapshot.events)?,
    }))
}

fn assert_original_session_identity(view: &spot_lab::contracts::PaperSessionView) {
    assert_eq!(view.session_id.as_str(), SESSION_ID);
    assert_eq!(view.origin_request_id.as_str(), ORIGIN_REQUEST_ID);
    assert_eq!(view.policy_ref.policy_id.as_str(), POLICY_ID);
    assert_eq!(view.policy_ref.revision_id.as_str(), POLICY_REVISION_ID);
    assert_eq!(view.policy_ref.definition_digest.as_str(), POLICY_DIGEST);
    assert_eq!(view.decision_interval, CandleInterval::M5);
    assert_eq!(view.execution_resolution, CandleInterval::M5);
    assert_eq!(view.mode, PaperSessionMode::Simulated);
    assert_eq!(
        view.markets
            .iter()
            .map(|market| market.market.code())
            .collect::<BTreeSet<_>>(),
        MARKETS.into_iter().map(str::to_owned).collect(),
    );
    for market in &view.markets {
        assert_eq!(market.initial_cash.get(), Decimal::from(100_000));
    }
}

fn required_existing_root(name: &str) -> Result<PathBuf, Box<dyn Error>> {
    let raw = std::env::var_os(name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must name an existing directory"),
        )
    })?;
    let path = PathBuf::from(raw);
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be an absolute path"),
        )
        .into());
    }
    let canonical = path.canonicalize()?;
    if !canonical.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be an existing directory"),
        )
        .into());
    }
    Ok(canonical)
}

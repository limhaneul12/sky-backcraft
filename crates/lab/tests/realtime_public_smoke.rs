//! Explicit public-provider smoke; ordinary CI remains hermetic.

use spot_lab::contracts::{CandleInterval, LabError, MarketId, ProbeCount};
use spot_lab::database::DatabaseOwner;
use spot_lab::market_data::UpbitClient;
use spot_lab::realtime::{PublicMarketFeed, PublicMarketFeedConfig, PublicMarketSubscription};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
#[ignore = "contacts public Upbit quotation endpoints; opt in explicitly"]
fn production_public_feed_receives_bootstraps_and_joins() -> Result<(), Box<dyn std::error::Error>>
{
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| LabError::Internal("isolated smoke TLS provider already installed".into()))?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "backcraft-public-ws-smoke-{}-{nonce}",
        std::process::id()
    ));
    let database = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(async {
    let (feed, owner) = PublicMarketFeed::start(
        UpbitClient::new()?,
        database.handle(),
        PublicMarketFeedConfig::upbit_production()?,
    )?;
    let result = async {
        let market = MarketId::parse_upbit("KRW-BTC")?;
        feed.replace_subscriptions([PublicMarketSubscription::new(market.clone(), CandleInterval::M1)])?;
        let (_, observations) = feed.read_or_bootstrap_observations(&market, CandleInterval::M1,
            ProbeCount::try_from(2_u32)?).await?;
        if observations.is_empty() {
            return Err(LabError::DataGap("provider bootstrap returned no completed candles".into()));
        }
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                // Exercise the same bootstrap/parity consumer as JobRuntime; the
                // socket owner intentionally does not own REST publication.
                match feed.read_or_bootstrap_observations(&market, CandleInterval::M1,
                    ProbeCount::try_from(2_u32)?).await {
                    Ok(_) | Err(LabError::DataGap(_) | LabError::BlockedEvidence(_)) => {},
                    Err(error) => return Err(error),
                }
                let health = feed.health();
                if health.status.connection_count == 1 && health.status.last_received_at_utc.is_some()
                    && health.last_pong_at_utc.is_some() && feed.is_ready(&market, CandleInterval::M1) {
                    return Ok::<(), LabError>(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| {
            eprintln!("public-provider deadline health: {:?}", feed.health());
            LabError::NetworkUnavailable("public WS readiness deadline".into())
        })??;
        let health = feed.health();
        println!("public-provider evidence: connections={}, subscriptions={}, received={}, pong={}, per_key_ready={}",
            health.status.connection_count, health.status.subscription_count,
            health.status.last_received_at_utc.is_some(), health.last_pong_at_utc.is_some(),
            feed.is_ready(&market, CandleInterval::M1));
        Ok::<(), LabError>(())
    }.await;
    let joined = owner.shutdown().await;
    drop(feed);
    result.and(joined)
    });
    let closed = database.shutdown();
    outcome?;
    closed?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

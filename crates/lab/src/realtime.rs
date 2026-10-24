//! One bounded, public Upbit market feed shared by persistent runtime consumers.

mod parser;
mod runtime;
mod state;
mod subscription;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::contracts::{
    CandleInterval, CandleObservation, LabError, MarketId, ProbeCount, ProviderAbsenceAnchor,
    ProviderAbsenceSpan, ProviderFeedRecoveryProgress, RawObjectRef, RealtimeConnectionState,
    RealtimeFeedStatus, RealtimeReadinessBlocker, RealtimeSubscriptionReadiness, UtcTimestamp,
    observation_digest,
};
use crate::database::DatabaseHandle;
use crate::market_data::UpbitClient;
use crate::storage::{PersistedProviderRecoveryPage, ProviderFeedRecoveryCursor, RawObjectInput};

use runtime::run_feed;
use state::FeedCache;
use subscription::{canonical_subscriptions, websocket_type_for_interval};

pub use state::LatestMarketPrice;
pub use subscription::PublicMarketSubscription;

struct ProviderRecoveryPage<'a> {
    subscription: &'a PublicMarketSubscription,
    raw: &'a RawObjectRef,
    observations: &'a [CandleObservation],
    cursor_before: Option<ProviderFeedRecoveryCursor>,
    seed_anchor: Option<ProviderAbsenceAnchor>,
    requested_to: Option<UtcTimestamp>,
    target_count: u32,
}

const DEFAULT_MAX_SUBSCRIPTIONS: usize = 200;
const MAX_ALLOWED_SUBSCRIPTIONS: usize = 200;
const DEFAULT_MAX_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_ALLOWED_MESSAGE_BYTES: usize = 64 * 1024;
const DEFAULT_CACHED_BARS_PER_SUBSCRIPTION: usize = 200;
const MAX_CACHED_BARS_PER_SUBSCRIPTION: usize = 200;
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const DEFAULT_RECONNECT_MIN: Duration = Duration::from_millis(500);
const DEFAULT_RECONNECT_MAX: Duration = Duration::from_secs(30);
const DEFAULT_HANDSHAKE_MIN_SPACING: Duration = Duration::from_millis(500);
const MIN_HANDSHAKE_SPACING: Duration = Duration::from_millis(10);
const MAX_HANDSHAKE_SPACING: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const PRODUCTION_WS_ENDPOINT: &str = "wss://api.upbit.com/websocket/v1";
const UPBIT_PUBLIC_HOST_ALLOWLIST: [&str; 1] = ["api.upbit.com"];

/// Validated host/TLS and resource policy for the public quotation feed.
#[derive(Clone)]
pub struct PublicMarketFeedConfig {
    endpoint: reqwest::Url,
    allowed_hosts: Vec<String>,
    max_subscriptions: usize,
    max_message_bytes: usize,
    cached_bars_per_subscription: usize,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    connect_timeout: Duration,
    reconnect_min: Duration,
    reconnect_max: Duration,
    /// Monotonic floor between handshake starts, independent of retry backoff.
    handshake_min_spacing: Duration,
    #[cfg(test)]
    loopback_fixture: bool,
}

impl PublicMarketFeedConfig {
    /// Production Upbit Korea endpoint with TLS-only, exact-host admission.
    ///
    /// # Errors
    /// Returns a configuration error if the compiled endpoint is invalid.
    pub fn upbit_production() -> Result<Self, LabError> {
        let endpoint = reqwest::Url::parse(PRODUCTION_WS_ENDPOINT).map_err(|_| {
            LabError::InvalidConfig("compiled public Upbit endpoint is invalid".into())
        })?;
        Ok(Self {
            endpoint,
            allowed_hosts: vec!["api.upbit.com".into()],
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            cached_bars_per_subscription: DEFAULT_CACHED_BARS_PER_SUBSCRIPTION,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
            handshake_min_spacing: DEFAULT_HANDSHAKE_MIN_SPACING,
            #[cfg(test)]
            loopback_fixture: false,
        })
    }

    /// Select another official Upbit public region endpoint from the exact allowlist.
    ///
    /// # Errors
    /// Rejects a non-TLS URL, a non-Upbit host, credentials, query data, fragments,
    /// an unexpected path or a host not present in `allowed_hosts`.
    pub fn with_production_endpoint(
        mut self,
        endpoint: &str,
        allowed_hosts: &[String],
    ) -> Result<Self, LabError> {
        let parsed = reqwest::Url::parse(endpoint)
            .map_err(|_| LabError::InvalidConfig("invalid public WebSocket endpoint".into()))?;
        validate_production_endpoint(&parsed, allowed_hosts)?;
        self.endpoint = parsed;
        self.allowed_hosts = allowed_hosts.to_vec();
        Ok(self)
    }

    /// Bound the active union size. Upbit accepts at most 200 codes per type.
    ///
    /// # Errors
    /// Rejects zero or values above the reviewed provider limit.
    pub fn with_max_subscriptions(mut self, limit: usize) -> Result<Self, LabError> {
        if !(1..=MAX_ALLOWED_SUBSCRIPTIONS).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "public feed subscriptions must be in 1..={MAX_ALLOWED_SUBSCRIPTIONS}"
            )));
        }
        self.max_subscriptions = limit;
        Ok(self)
    }

    /// Bound a single provider message before JSON decoding.
    ///
    /// # Errors
    /// Rejects zero or values above the raw-object ceiling.
    pub fn with_max_message_bytes(mut self, limit: usize) -> Result<Self, LabError> {
        if !(1..=MAX_ALLOWED_MESSAGE_BYTES).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "public feed message limit must be in 1..={MAX_ALLOWED_MESSAGE_BYTES} bytes"
            )));
        }
        self.max_message_bytes = limit;
        Ok(self)
    }

    /// Bound the latest candle history retained per active market/timeframe.
    ///
    /// # Errors
    /// Rejects zero or values above the reviewed in-memory cache limit.
    pub fn with_cached_bars_per_subscription(mut self, limit: usize) -> Result<Self, LabError> {
        if !(1..=MAX_CACHED_BARS_PER_SUBSCRIPTION).contains(&limit) {
            return Err(LabError::InvalidConfig(format!(
                "public feed cache must be in 1..={MAX_CACHED_BARS_PER_SUBSCRIPTION} bars"
            )));
        }
        self.cached_bars_per_subscription = limit;
        Ok(self)
    }

    fn validate(&self) -> Result<(), LabError> {
        #[cfg(test)]
        if self.loopback_fixture {
            validate_loopback_endpoint(&self.endpoint)?;
        }
        #[cfg(test)]
        if !self.loopback_fixture {
            validate_production_endpoint(&self.endpoint, &self.allowed_hosts)?;
        }
        #[cfg(not(test))]
        validate_production_endpoint(&self.endpoint, &self.allowed_hosts)?;
        // The timeout starts after sending Ping, so it is independent of interval.
        if self.max_subscriptions == 0
            || self.max_subscriptions > MAX_ALLOWED_SUBSCRIPTIONS
            || self.max_message_bytes == 0
            || self.max_message_bytes > MAX_ALLOWED_MESSAGE_BYTES
            || self.cached_bars_per_subscription == 0
            || self.cached_bars_per_subscription > MAX_CACHED_BARS_PER_SUBSCRIPTION
            || self.heartbeat_interval.is_zero()
            || self.heartbeat_interval > Duration::from_secs(120)
            || self.heartbeat_timeout.is_zero()
            || self.heartbeat_timeout > Duration::from_secs(120)
            || self.connect_timeout.is_zero()
            || self.reconnect_min.is_zero()
            || self.reconnect_min > self.reconnect_max
            || self.reconnect_max > Duration::from_secs(120)
            || self.handshake_min_spacing < MIN_HANDSHAKE_SPACING
            || self.handshake_min_spacing > MAX_HANDSHAKE_SPACING
        {
            return Err(LabError::InvalidConfig(
                "public WebSocket resource and timeout policy is invalid".into(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn loopback_fixture(endpoint: &str) -> Result<Self, LabError> {
        let endpoint = reqwest::Url::parse(endpoint)
            .map_err(|_| LabError::InvalidConfig("invalid test WebSocket endpoint".into()))?;
        validate_loopback_endpoint(&endpoint)?;
        Ok(Self {
            endpoint,
            allowed_hosts: Vec::new(),
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            cached_bars_per_subscription: DEFAULT_CACHED_BARS_PER_SUBSCRIPTION,
            heartbeat_interval: Duration::from_millis(80),
            heartbeat_timeout: Duration::from_millis(160),
            connect_timeout: Duration::from_secs(2),
            reconnect_min: Duration::from_millis(20),
            reconnect_max: Duration::from_millis(80),
            handshake_min_spacing: Duration::from_millis(20),
            loopback_fixture: true,
        })
    }
}

fn validate_production_endpoint(
    endpoint: &reqwest::Url,
    allowed_hosts: &[String],
) -> Result<(), LabError> {
    let host = endpoint.host_str().unwrap_or_default().to_ascii_lowercase();
    let allowed = !allowed_hosts.is_empty()
        && allowed_hosts.iter().all(|candidate| {
            let normalized = candidate.to_ascii_lowercase();
            UPBIT_PUBLIC_HOST_ALLOWLIST.contains(&normalized.as_str())
        })
        && allowed_hosts
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&host));
    if endpoint.scheme() != "wss"
        || !allowed
        || endpoint.port().is_some_and(|port| port != 443)
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/websocket/v1"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(LabError::InvalidConfig(
            "public feed endpoint must be an allowlisted Upbit TLS quotation host".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn validate_loopback_endpoint(endpoint: &reqwest::Url) -> Result<(), LabError> {
    let loopback = endpoint.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if endpoint.scheme() != "ws"
        || !loopback
        || endpoint.path() != "/websocket/v1"
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(LabError::InvalidConfig(
            "test WebSocket endpoint must be credential-free loopback".into(),
        ));
    }
    Ok(())
}

/// Secret-free details beyond the shared status projection.
#[derive(Debug, Clone)]
pub struct PublicMarketFeedHealth {
    pub status: RealtimeFeedStatus,
    pub last_provider_at_utc: Option<UtcTimestamp>,
    pub last_pong_at_utc: Option<UtcTimestamp>,
    pub last_data_lag_millis: Option<u64>,
    pub rest_only_intervals: Vec<CandleInterval>,
    pub last_error_category: Option<PublicFeedErrorCategory>,
}

/// Safe bounded error categories; protocol bodies and provider text are not exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicFeedErrorCategory {
    Connect,
    Heartbeat,
    Protocol,
    OversizedMessage,
    Subscription,
    Storage,
    Recovery,
}

struct FeedShared {
    subscriptions: watch::Sender<Vec<PublicMarketSubscription>>,
    health: watch::Sender<PublicMarketFeedHealth>,
    cache: std::sync::Mutex<FeedCache>,
    notify: Arc<Notify>,
    config: PublicMarketFeedConfig,
}

/// Cloneable read/control handle for the one process-wide public market feed.
#[derive(Clone)]
pub struct PublicMarketFeed {
    shared: Arc<FeedShared>,
    client: UpbitClient,
    database: DatabaseHandle,
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
}

impl PublicMarketFeed {
    /// Start the single owned public connection task. It stays disconnected until
    /// a supported market/timeframe subscription is admitted.
    ///
    /// # Errors
    /// Rejects an unsafe endpoint, invalid resource limit or missing Tokio runtime.
    pub fn start(
        client: UpbitClient,
        database: DatabaseHandle,
        config: PublicMarketFeedConfig,
    ) -> Result<(Self, PublicMarketFeedRuntime), LabError> {
        config.validate()?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            LabError::InvalidConfig("public market feed requires the Tokio runtime".into())
        })?;
        let cached_bars_per_subscription = config.cached_bars_per_subscription;
        let status = PublicMarketFeedHealth {
            status: RealtimeFeedStatus {
                state: RealtimeConnectionState::Idle,
                connection_count: 0,
                subscription_count: 0,
                last_received_at_utc: None,
                last_reconciled_at_utc: None,
                reconnect_count: 0,
                dropped_messages: 0,
                ready_for_decisions: false,
                readiness_blockers: Vec::new(),
                subscription_readiness: Vec::new(),
            },
            last_provider_at_utc: None,
            last_pong_at_utc: None,
            last_data_lag_millis: None,
            rest_only_intervals: Vec::new(),
            last_error_category: None,
        };
        let (subscription_tx, subscription_rx) = watch::channel(Vec::new());
        let (health_tx, _health_rx) = watch::channel(status);
        let cancellation = CancellationToken::new();
        let shared = Arc::new(FeedShared {
            subscriptions: subscription_tx,
            health: health_tx,
            cache: std::sync::Mutex::new(FeedCache::new(cached_bars_per_subscription)),
            notify: Arc::new(Notify::new()),
            config,
        });
        let feed = Self {
            shared: Arc::clone(&shared),
            client,
            database,
            reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        let task = runtime.spawn(run_feed(
            Arc::clone(&shared),
            subscription_rx,
            cancellation.clone(),
        ));
        let owner = PublicMarketFeedRuntime {
            cancellation,
            task: Some(task),
            health: shared.health.clone(),
            notify: Arc::clone(&shared.notify),
        };
        Ok((feed, owner))
    }

    /// Replace the canonical union across all persistent paper/MOCK/LIVE sessions.
    ///
    /// D1 remains explicit REST-only because Upbit has no daily candle WebSocket type.
    ///
    /// # Errors
    /// Rejects invalid markets, duplicates after normalization are coalesced, and
    /// subscriptions over the configured limit are rejected before admission.
    pub fn replace_subscriptions(
        &self,
        subscriptions: impl IntoIterator<Item = PublicMarketSubscription>,
    ) -> Result<(), LabError> {
        let canonical =
            canonical_subscriptions(subscriptions, self.shared.config.max_subscriptions)?;
        if self.shared.subscriptions.borrow().as_slice() == canonical.as_slice() {
            return Ok(());
        }
        self.with_cache(|cache| {
            cache.clear_reconciled_for(&canonical);
            Ok(())
        })?;
        self.shared.subscriptions.send_replace(canonical.clone());
        self.shared.health.send_modify(|health| {
            health.status.subscription_count = u32::try_from(canonical.len()).unwrap_or(u32::MAX);
            health.rest_only_intervals = canonical
                .iter()
                .filter(|subscription| websocket_type_for_interval(subscription.interval).is_none())
                .map(|subscription| subscription.interval)
                .collect();
            health
                .rest_only_intervals
                .sort_by_key(|interval| interval.duration());
            health.rest_only_intervals.dedup();
            health.status.ready_for_decisions = false;
            if canonical.is_empty() {
                health.status.state = RealtimeConnectionState::Idle;
                health.status.connection_count = 0;
            } else {
                health.status.state = RealtimeConnectionState::Recovering;
                health.status.connection_count = 0;
            }
        });
        self.shared.notify.notify_one();
        Ok(())
    }

    /// Bootstrap or recover at most one active subscription that needs REST.
    /// The cache cursor advances before I/O so a failing cold key cannot starve
    /// the remaining union. Fresh keys are skipped without materializing bars.
    ///
    /// # Errors
    /// Propagates bounded REST, storage, contract, gap and parity failures.
    pub(crate) async fn reconcile_one_subscription(
        &self,
        completed_count: ProbeCount,
        cancellation: &CancellationToken,
    ) -> Result<Option<PublicMarketSubscription>, LabError> {
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        let subscriptions = self.shared.subscriptions.borrow().clone();
        let selected = self.with_cache(|cache| {
            Ok(cache.next_rest_probe_subscription(
                &subscriptions,
                UtcTimestamp::now(),
                Instant::now(),
            ))
        })?;
        let Some(subscription) = selected else {
            return Ok(None);
        };
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        Box::pin(self.read_or_bootstrap_observations_inner(
            &subscription.market,
            subscription.interval,
            completed_count,
            Some(cancellation),
        ))
        .await?;
        Ok(Some(subscription))
    }

    /// Read bounded completed history from REST, pin accepted history across
    /// provider revisions, and wait for two stable reads before accepting a new tip.
    ///
    /// REST is the completed-history authority; forming WS bars remain
    /// `completed=false` and are never returned as finalized observations.
    ///
    /// # Errors
    /// Propagates REST, storage, contract, gap and parity mismatch failures.
    pub async fn read_or_bootstrap_observations(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        completed_count: ProbeCount,
    ) -> Result<(RawObjectRef, Vec<CandleObservation>), LabError> {
        Box::pin(self.read_or_bootstrap_observations_inner(market, interval, completed_count, None))
            .await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one per-key transaction owns bootstrap, parity, provenance and recovery decisions"
    )]
    async fn read_or_bootstrap_observations_inner(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        completed_count: ProbeCount,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(RawObjectRef, Vec<CandleObservation>), LabError> {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        let subscription = PublicMarketSubscription::new(market.clone(), interval);
        if !self.shared.subscriptions.borrow().contains(&subscription) {
            return Err(LabError::BlockedEvidence(
                "market/timeframe is not part of the active shared feed union".into(),
            ));
        }
        if self.with_cache(|cache| {
            Ok(cache.rest_tip_confirmation_waiting(&subscription, UtcTimestamp::now()))
        })? {
            return Err(awaiting_rest_confirmation(market, interval));
        }
        if self.with_cache(|cache| Ok(cache.rest_retry_waiting(&subscription, Instant::now())))? {
            return Err(awaiting_rest_retry(market, interval));
        }
        if self.with_cache(|cache| {
            Ok(cache.provider_absence_confirmation_waiting(&subscription, Instant::now()))
        })? {
            return Err(awaiting_provider_absence_confirmation(market, interval));
        }
        let (recovery_cursor, seed_anchor, requested_to, accepted_pin) =
            self.provider_recovery_context(&subscription).await?;
        if let Some(observation) = accepted_pin {
            self.with_cache(|cache| cache.seed_accepted_observation(&subscription, observation))?;
        }
        if websocket_type_for_interval(interval).is_none() {
            let (rest_raw, page_observations) = match self
                .client
                .poll_paper_observation_page(
                    market,
                    interval,
                    requested_to,
                    cancellation,
                    &self.database,
                )
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    self.mark_rest_failure(
                        &subscription,
                        PublicFeedErrorCategory::Recovery,
                        false,
                    )?;
                    return Err(error);
                }
            };
            let (mut observations, spans, recovery_progress) = self
                .prepare_provider_observations(ProviderRecoveryPage {
                    subscription: &subscription,
                    raw: &rest_raw,
                    observations: &page_observations,
                    cursor_before: recovery_cursor.clone(),
                    seed_anchor: seed_anchor.clone(),
                    requested_to,
                    target_count: completed_count.get(),
                })
                .await?;
            if let Err(error) = validate_rest_snapshot(&observations, completed_count, &spans) {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(error);
            }
            if observations.iter().any(|row| {
                !row.candle.completed
                    || row.candle.close_time_utc > rest_raw.fetched_at
                    || row.candle.market != market.code()
                    || row.candle.interval != interval
            }) {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(LabError::DataCorrupt(
                    "REST recovery contains unavailable or mismatched candles".into(),
                ));
            }
            let canonical_pins = self
                .canonical_provider_feed_pins(&subscription, &observations)
                .await?;
            let revision = match self.with_cache(|cache| {
                cache.pin_rest_snapshot_with_provider_absence(
                    &subscription,
                    rest_raw.fetched_at,
                    &mut observations,
                    &spans,
                    &canonical_pins,
                )
            }) {
                Ok(revision) => revision,
                Err(error) => {
                    self.mark_rest_failure(
                        &subscription,
                        PublicFeedErrorCategory::Recovery,
                        false,
                    )?;
                    return Err(error);
                }
            };
            log_pinned_revisions(market, interval, revision);
            let tip = observations
                .last()
                .ok_or_else(|| LabError::DataGap("empty REST snapshot".into()))?;
            let tip_confirmed = self.with_cache(|cache| {
                Ok(cache.confirm_rest_tip(&subscription, tip, rest_raw.fetched_at))
            })?;
            if !tip_confirmed {
                self.shared.notify.notify_one();
                return Err(awaiting_rest_confirmation(market, interval));
            }
            self.confirm_provider_recovery_window(&subscription, &observations)
                .await?;
            let stored = self.with_cache(|cache| {
                cache.store_rest_snapshot(&subscription, rest_raw.clone(), &observations, true);
                cache.confirm_rest_snapshot(&subscription, &observations)?;
                Ok(())
            });
            if let Err(error) = stored {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(error);
            }
            self.mark_reconciled(&subscription, UtcTimestamp::now())?;
            let requested = usize::try_from(completed_count.get())
                .map_err(|_| LabError::InvalidConfig("completed_count out of range".into()))?;
            let (_, recent) = self.with_cache(|cache| {
                cache.set_provider_recovery_evidence(
                    &subscription,
                    spans,
                    Some(recovery_progress),
                )?;
                Ok(cache.cached_prefix(&subscription, requested))
            })?;
            return Ok((rest_raw, recent));
        }

        let preflight = self.with_cache(|cache| {
            let requested = usize::try_from(completed_count.get())
                .map_err(|_| LabError::InvalidConfig("completed_count out of range".into()))?;
            let (raw, rows) = cache.cached_prefix(&subscription, requested);
            Ok((
                cache.needs_rest_snapshot(&subscription, UtcTimestamp::now()),
                cache.is_bootstrapped(&subscription),
                raw,
                rows,
            ))
        })?;
        let (needs_rest, bootstrapped, cached_raw, cached_rows) = preflight;
        if !needs_rest {
            let Some(raw) = cached_raw else {
                let error =
                    LabError::DataCorrupt("bootstrapped market feed has no raw reference".into());
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(error);
            };
            return Ok((raw, cached_rows));
        }

        let (rest_raw, page_observations) = match self
            .client
            .poll_paper_observation_page(
                market,
                interval,
                requested_to,
                cancellation,
                &self.database,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(error);
            }
        };
        let (mut rest_observations, spans, recovery_progress) = self
            .prepare_provider_observations(ProviderRecoveryPage {
                subscription: &subscription,
                raw: &rest_raw,
                observations: &page_observations,
                cursor_before: recovery_cursor,
                seed_anchor,
                requested_to,
                target_count: completed_count.get(),
            })
            .await?;
        if let Err(error) = validate_rest_snapshot(&rest_observations, completed_count, &spans) {
            self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
            return Err(error);
        }
        if rest_observations.iter().any(|row| {
            !row.candle.completed
                || row.candle.close_time_utc > rest_raw.fetched_at
                || row.candle.market != market.code()
                || row.candle.interval != interval
        }) {
            self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
            return Err(LabError::DataCorrupt(
                "REST recovery contains unavailable or mismatched candles".into(),
            ));
        }
        let canonical_pins = self
            .canonical_provider_feed_pins(&subscription, &rest_observations)
            .await?;
        let revision = match self.with_cache(|cache| {
            cache.pin_rest_snapshot_with_provider_absence(
                &subscription,
                rest_raw.fetched_at,
                &mut rest_observations,
                &spans,
                &canonical_pins,
            )
        }) {
            Ok(revision) => revision,
            Err(error) => {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(error);
            }
        };
        log_pinned_revisions(market, interval, revision);
        let tip = rest_observations
            .last()
            .ok_or_else(|| LabError::DataGap("empty REST snapshot".into()))?;
        let tip_confirmed = self.with_cache(|cache| {
            Ok(cache.confirm_rest_tip(&subscription, tip, rest_raw.fetched_at))
        })?;
        if !tip_confirmed {
            self.shared.notify.notify_one();
            return Err(awaiting_rest_confirmation(market, interval));
        }
        self.confirm_provider_recovery_window(&subscription, &rest_observations)
            .await?;
        let accepted_history_matches = self.with_cache(|cache| {
            Ok(cache.snapshot_matches_confirmed(&subscription, &rest_observations))
        })?;
        if !accepted_history_matches {
            self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
            if rest_observations
                .last()
                .zip(cached_rows.last())
                .is_some_and(|(rest, previous)| {
                    rest.candle.open_time_utc < previous.candle.open_time_utc
                })
            {
                return Err(LabError::DataGap(
                    "REST recovery snapshot is behind accepted finalized history".into(),
                ));
            }
            return Err(LabError::DataCorrupt(
                "REST recovery changed accepted finalized history".into(),
            ));
        }
        if !bootstrapped {
            self.with_cache(|cache| {
                cache.set_provider_recovery_evidence(
                    &subscription,
                    spans.clone(),
                    Some(recovery_progress.clone()),
                )?;
                cache.store_rest_snapshot(
                    &subscription,
                    rest_raw.clone(),
                    &rest_observations,
                    true,
                );
                Ok(())
            })?;
        }
        let candidates = self.with_cache(|cache| {
            Ok(cache.due_candidates(
                &subscription,
                rest_raw.fetched_at,
                self.shared.config.cached_bars_per_subscription,
            ))
        })?;
        let unobserved_forward_rows = self.with_cache(|cache| {
            Ok(cache.has_unobserved_forward_rows(&subscription, &rest_observations, &candidates))
        })?;
        let incomplete_ws_final = candidates.iter().any(|candidate| {
            !rest_observations.iter().any(|row| {
                row.candle.open_time_utc == candidate.frame.candle.open_time_utc
                    && same_economic_candle(&candidate.frame.candle, &row.candle)
            })
        });
        if unobserved_forward_rows || incomplete_ws_final {
            self.mark_recovery(&subscription, PublicFeedErrorCategory::Recovery, false)?;
            // Upbit emits forming snapshots, not an obligatory terminal frame.
            // Recover finalized economics from HTTP while preserving their actual raw origin.
            let recovered = self.with_cache(|cache| {
                cache.set_provider_recovery_evidence(
                    &subscription,
                    spans.clone(),
                    Some(recovery_progress.clone()),
                )?;
                cache.recover_rest_snapshot(&subscription, rest_raw.clone(), &rest_observations)?;
                Ok(())
            });
            if let Err(error) = recovered {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(error);
            }
            tracing::info!(market = %market.code(), interval = ?interval, unobserved_forward_rows,
                incomplete_ws_final, "public candle input recovered from completed REST snapshot");
            self.mark_reconciled(&subscription, UtcTimestamp::now())?;
            let requested = usize::try_from(completed_count.get())
                .map_err(|_| LabError::InvalidConfig("completed_count out of range".into()))?;
            let (_, rows) =
                self.with_cache(|cache| Ok(cache.cached_prefix(&subscription, requested)))?;
            return Ok((rest_raw, rows));
        }
        if candidates.is_empty() {
            let stored = self.with_cache(|cache| {
                cache.set_provider_recovery_evidence(
                    &subscription,
                    spans.clone(),
                    Some(recovery_progress.clone()),
                )?;
                cache.store_rest_snapshot(
                    &subscription,
                    rest_raw.clone(),
                    &rest_observations,
                    false,
                );
                cache.confirm_rest_snapshot(&subscription, &rest_observations)?;
                Ok(())
            });
            if let Err(error) = stored {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
                return Err(error);
            }
            self.mark_reconciled(&subscription, UtcTimestamp::now())?;
            let requested = usize::try_from(completed_count.get())
                .map_err(|_| LabError::InvalidConfig("completed_count out of range".into()))?;
            let (raw, observations) =
                self.with_cache(|cache| Ok(cache.cached_prefix(&subscription, requested)))?;
            let raw = raw.or(cached_raw).ok_or_else(|| {
                LabError::DataCorrupt("market feed lost its bootstrap raw reference".into())
            })?;
            return Ok((raw, observations));
        }

        for candidate in candidates {
            let Some(rest_observation) = rest_observations.iter().find(|observation| {
                observation.candle.market == candidate.frame.candle.market
                    && observation.candle.interval == candidate.frame.candle.interval
                    && observation.candle.open_time_utc == candidate.frame.candle.open_time_utc
            }) else {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, true)?;
                return Err(LabError::DataGap(
                    "REST confirmation did not contain the pending WebSocket candle".into(),
                ));
            };
            if !same_economic_candle(&candidate.frame.candle, &rest_observation.candle) {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, true)?;
                return Err(LabError::DataGap(
                    "REST and WebSocket candle contents differ; feed readiness is blocked".into(),
                ));
            }
            let raw_frame = candidate.raw_frame.clone();
            let transport = candidate.transport.clone();
            let endpoint = self.shared.config.endpoint.as_str().to_owned();
            let fetched_at = transport.received_at;
            let published = match self
                .database
                .call("publish_public_websocket_raw", move |store| {
                    let published = store.publish_websocket_raw(
                        RawObjectInput {
                            source_url: endpoint,
                            fetched_at,
                            persisted_at: UtcTimestamp::now(),
                            http_status: 101,
                            remaining_req: None,
                            origin: crate::contracts::MarketDataOrigin::ExchangeObserved,
                            body: raw_frame,
                        },
                        transport,
                    )?;
                    store.catalog_raw(&published.object)?;
                    Ok(published)
                })
                .await
            {
                Ok(published) => published,
                Err(error) => {
                    self.mark_recovery(&subscription, PublicFeedErrorCategory::Storage, false)?;
                    return Err(error);
                }
            };
            let mut candle = candidate.frame.candle.clone();
            candle.completed = true;
            let digest = observation_digest(&candle)?;
            let mut raw_ids = rest_observation.raw_object_ids.clone();
            if !raw_ids.contains(&published.object.id) {
                raw_ids.push(published.object.id.clone());
            }
            let websocket_observation = CandleObservation {
                id: crate::contracts::ObservationId::from_seed(digest.as_str()),
                candle,
                content_digest: digest,
                raw_object_ids: raw_ids,
                constituent_ids: Vec::new(),
            };
            let confirmed = self.with_cache(|cache| {
                Ok(cache.mark_confirmed(&subscription, &candidate, websocket_observation))
            })?;
            if !confirmed {
                self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, true)?;
                return Err(LabError::DataGap(
                    "WebSocket candle changed while REST confirmation was in flight".into(),
                ));
            }
        }
        let stored = self.with_cache(|cache| {
            cache.set_provider_recovery_evidence(&subscription, spans, Some(recovery_progress))?;
            cache.store_rest_snapshot(&subscription, rest_raw.clone(), &rest_observations, false);
            cache.confirm_rest_snapshot(&subscription, &rest_observations)?;
            Ok(())
        });
        if let Err(error) = stored {
            self.mark_rest_failure(&subscription, PublicFeedErrorCategory::Recovery, false)?;
            return Err(error);
        }
        self.mark_reconciled(&subscription, UtcTimestamp::now())?;
        let (_cached_raw, cached_observations) = self.with_cache(|cache| {
            let requested = usize::try_from(completed_count.get())
                .map_err(|_| LabError::InvalidConfig("completed_count out of range".into()))?;
            Ok(cache.cached_prefix(&subscription, requested))
        })?;
        Ok((rest_raw, cached_observations))
    }

    async fn prepare_provider_observations(
        &self,
        page: ProviderRecoveryPage<'_>,
    ) -> Result<
        (
            Vec<CandleObservation>,
            Vec<ProviderAbsenceSpan>,
            ProviderFeedRecoveryProgress,
        ),
        LabError,
    > {
        let result = self.prepare_provider_observations_inner(&page).await;
        if let Err(error) = &result
            && !matches!(error, LabError::BlockedEvidence(_))
        {
            self.mark_rest_failure(page.subscription, PublicFeedErrorCategory::Recovery, false)?;
        }
        result
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one recovery transition separates unfinished backfill from confirmed rolling-window refresh before publishing coverage"
    )]
    async fn prepare_provider_observations_inner(
        &self,
        page: &ProviderRecoveryPage<'_>,
    ) -> Result<
        (
            Vec<CandleObservation>,
            Vec<ProviderAbsenceSpan>,
            ProviderFeedRecoveryProgress,
        ),
        LabError,
    > {
        let subscription = page.subscription;
        let raw = page.raw;
        let page_observations = page.observations;
        let cursor_before = &page.cursor_before;
        let seed_anchor = &page.seed_anchor;
        let requested_to = page.requested_to;
        // One market/timeframe owns its admitted scan. Another consumer's
        // smaller output limit cannot replace that in-progress scan identity.
        let target_count = cursor_before
            .as_ref()
            .filter(|cursor| !cursor.is_complete())
            .map_or(page.target_count, |cursor| cursor.target_count);
        let (page_spans, pending_absence) = self
            .observe_and_load_provider_spans(subscription, raw, page_observations)
            .await?;
        let cursor_was_complete = cursor_before
            .as_ref()
            .is_some_and(ProviderFeedRecoveryCursor::is_complete);
        let mut source_window_repair = if cursor_was_complete
            && !self.with_cache(|cache| Ok(cache.is_bootstrapped(subscription)))?
        {
            let market = subscription.market.clone();
            let interval = subscription.interval;
            let started_at = cursor_before
                .as_ref()
                .map(|cursor| cursor.started_at_utc)
                .ok_or_else(|| LabError::Internal("complete source cursor vanished".into()))?;
            let as_of = UtcTimestamp::now();
            self.database
                .call("repair_cold_public_feed_source_window", move |store| {
                    store.repair_provider_feed_recovery_window(&market, interval, started_at, as_of)
                })
                .await?
        } else {
            None
        };
        let previously_confirmed_tip = cursor_before
            .as_ref()
            .and_then(|cursor| cursor.last_recovered_tip.as_ref());
        let missing_history_anchor = if cursor_was_complete
            && !self.with_cache(|cache| Ok(cache.is_bootstrapped(subscription)))?
            && let (Some(accepted), Some(watermark)) = (seed_anchor, previously_confirmed_tip)
        {
            let market = subscription.market.clone();
            let interval = subscription.interval;
            let accepted = accepted.clone();
            let watermark = watermark.clone();
            let needs_backfill = self
                .database
                .call("check_cold_consumer_source_history", move |store| {
                    store.provider_feed_history_needs_backfill(
                        &market, interval, &accepted, &watermark,
                    )
                })
                .await?;
            needs_backfill.then(|| seed_anchor.clone()).flatten()
        } else {
            None
        };
        let tip_aged_out = cursor_was_complete
            && previously_confirmed_tip
                .is_some_and(|anchor| !page_contains_anchor(page_observations, anchor));
        let should_record = cursor_before.is_none()
            || !cursor_was_complete
            || pending_absence
            || tip_aged_out
            || missing_history_anchor.is_some();

        let mut cursor = cursor_before.clone();
        if should_record {
            let current_seed = missing_history_anchor
                .or_else(|| {
                    cursor
                        .as_ref()
                        .filter(|_| cursor_was_complete)
                        .and_then(|cursor| {
                            cursor
                                .last_recovered_tip
                                .clone()
                                .or_else(|| cursor.progress.accepted_anchor.clone())
                        })
                })
                .or(seed_anchor.clone());
            let market = subscription.market.clone();
            let interval = subscription.interval;
            let raw = raw.clone();
            let observations = page_observations.to_vec();
            let as_of = UtcTimestamp::now();
            cursor = Some(
                self.database
                    .call("advance_public_feed_recovery_page", move |store| {
                        store.record_provider_feed_recovery_page(PersistedProviderRecoveryPage {
                            raw,
                            market,
                            interval,
                            requested_to,
                            seed_anchor: current_seed,
                            target_count,
                            observations,
                            pending_absence_confirmation: pending_absence,
                            as_of_utc: as_of,
                        })
                    })
                    .await?,
            );
        }

        let Some(cursor) = cursor else {
            return Err(LabError::Internal(
                "public-feed recovery cursor was not initialized".into(),
            ));
        };
        if !cursor.is_complete() {
            self.with_cache(|cache| {
                cache.set_provider_recovery_evidence(
                    subscription,
                    page_spans,
                    Some(cursor.progress.clone()),
                )
            })?;
            self.mark_recovery(subscription, PublicFeedErrorCategory::Recovery, false)?;
            return Err(if cursor.progress.pending_absence_confirmation {
                awaiting_provider_absence_confirmation(&subscription.market, subscription.interval)
            } else {
                awaiting_provider_backfill(&subscription.market, subscription.interval)
            });
        }

        let observations =
            if cursor_was_complete && !should_record && previously_confirmed_tip.is_some() {
                if let Some(repaired_window) = source_window_repair.take() {
                    merge_recovery_observations(repaired_window, page_observations)?
                } else {
                    // Bootstrap pages stay archived instead of being appended to every
                    // rolling refresh when their source rows are already canonical.
                    page_observations.to_vec()
                }
            } else {
                let market = subscription.market.clone();
                let interval = subscription.interval;
                let started_at = cursor.started_at_utc;
                let staged = self
                    .database
                    .call("read_public_feed_recovery_pages", move |store| {
                        store.provider_feed_recovery_observations(&market, interval, started_at)
                    })
                    .await?;
                let current_window = if cursor_was_complete && !should_record {
                    merge_recovery_observations(staged, page_observations)?
                } else {
                    staged
                };
                if let Some(repaired_window) = source_window_repair.take() {
                    merge_recovery_observations(repaired_window, &current_window)?
                } else {
                    current_window
                }
            };
        let spans =
            load_provider_spans_for_observations(&self.database, subscription, &observations)
                .await?;
        Ok((observations, spans, cursor.progress))
    }

    async fn observe_and_load_provider_spans(
        &self,
        subscription: &PublicMarketSubscription,
        raw: &RawObjectRef,
        observations: &[CandleObservation],
    ) -> Result<(Vec<crate::contracts::ProviderAbsenceSpan>, bool), LabError> {
        let market = subscription.market.clone();
        let interval = subscription.interval;
        let raw_for_observation = raw.clone();
        let rows_for_observation = observations.to_vec();
        let as_of = UtcTimestamp::now();
        let outcome = self
            .database
            .call("observe_public_provider_page", move |store| {
                store.observe_provider_absence_snapshot(
                    &raw_for_observation,
                    &market,
                    interval,
                    &rows_for_observation,
                    as_of,
                )
            })
            .await?;
        let Some(first) = observations.first() else {
            return Ok((Vec::new(), outcome.pending_confirmation));
        };
        let Some(last) = observations.last() else {
            return Ok((Vec::new(), outcome.pending_confirmation));
        };
        let market = subscription.market.clone();
        let start = first.candle.open_time_utc;
        let end = last.candle.close_time_utc;
        let spans = self
            .database
            .call("load_confirmed_public_provider_spans", move |store| {
                store.load_provider_absence_spans(&market, interval, start, end)
            })
            .await?;
        if outcome
            .confirmed_spans
            .iter()
            .any(|confirmed| !spans.iter().any(|span| span.proof_id == confirmed.proof_id))
        {
            return Err(LabError::DataCorrupt(
                "confirmed provider-absence proof was not visible in durable readback".into(),
            ));
        }
        Ok((spans, outcome.pending_confirmation))
    }

    async fn provider_recovery_context(
        &self,
        subscription: &PublicMarketSubscription,
    ) -> Result<
        (
            Option<ProviderFeedRecoveryCursor>,
            Option<ProviderAbsenceAnchor>,
            Option<UtcTimestamp>,
            Option<CandleObservation>,
        ),
        LabError,
    > {
        let market_for_cursor = subscription.market.clone();
        let interval = subscription.interval;
        let cursor = self
            .database
            .call("load_public_feed_recovery_cursor", move |store| {
                store.provider_feed_recovery_cursor(&market_for_cursor, interval)
            })
            .await?;
        if let Some(cursor) = cursor.as_ref() {
            self.restore_distinct_source_watermark(subscription, cursor)
                .await?;
        }
        if let Some(cursor) = cursor.as_ref()
            && !cursor.is_complete()
        {
            let anchor = cursor_backfill_anchor(cursor);
            let accepted_pin = self
                .load_provider_anchor_observation(subscription, anchor.as_ref())
                .await?;
            return Ok((
                Some(cursor.clone()),
                anchor,
                cursor.progress.next_page_to_utc,
                accepted_pin,
            ));
        }
        if let Some(completed) = cursor.as_ref() {
            let market = subscription.market.clone();
            let interval = subscription.interval;
            let consumer_anchor = self
                .database
                .call("load_cold_paper_consumer_anchor", move |store| {
                    store.provider_feed_accepted_anchor(&market, interval)
                })
                .await?;
            let anchor = completed_provider_backfill_anchor(completed, consumer_anchor)?;
            let accepted_pin = self
                .load_provider_anchor_observation(subscription, anchor.as_ref())
                .await?;
            return Ok((cursor.clone(), anchor, None, accepted_pin));
        }

        let cached_anchor = self.with_cache(|cache| Ok(cache.accepted_anchor(subscription)))?;
        let market_for_anchor = subscription.market.clone();
        let interval = subscription.interval;
        let durable_anchor = self
            .database
            .call("load_accepted_paper_feed_anchor", move |store| {
                store.provider_feed_accepted_anchor(&market_for_anchor, interval)
            })
            .await?;
        let anchor = earliest_provider_anchor(cached_anchor.clone(), durable_anchor)?;
        let accepted_pin = if anchor.as_ref() == cached_anchor.as_ref() {
            None
        } else {
            self.load_provider_anchor_observation(subscription, anchor.as_ref())
                .await?
        };
        Ok((None, anchor, None, accepted_pin))
    }

    async fn restore_distinct_source_watermark(
        &self,
        subscription: &PublicMarketSubscription,
        cursor: &ProviderFeedRecoveryCursor,
    ) -> Result<(), LabError> {
        let (Some(consumer), Some(watermark)) = (
            cursor.progress.accepted_anchor.as_ref(),
            cursor.last_recovered_tip.as_ref(),
        ) else {
            return Ok(());
        };
        if consumer == watermark
            || self.with_cache(|cache| Ok(cache.is_bootstrapped(subscription)))?
        {
            return Ok(());
        }
        let observation = self
            .load_provider_anchor_observation(subscription, Some(watermark))
            .await?
            .ok_or_else(|| {
                LabError::DataCorrupt("durable source watermark row is missing".into())
            })?;
        self.with_cache(|cache| cache.seed_source_watermark(subscription, observation))
    }

    async fn load_provider_anchor_observation(
        &self,
        subscription: &PublicMarketSubscription,
        anchor: Option<&ProviderAbsenceAnchor>,
    ) -> Result<Option<CandleObservation>, LabError> {
        let Some(anchor) = anchor else {
            return Ok(None);
        };
        let market = subscription.market.clone();
        let interval = subscription.interval;
        let anchor = anchor.clone();
        self.database
            .call("load_immutable_provider_feed_anchor", move |store| {
                store.provider_feed_accepted_observation(&market, interval, &anchor)
            })
            .await
            .map(Some)
    }

    async fn canonical_provider_feed_pins(
        &self,
        subscription: &PublicMarketSubscription,
        observations: &[CandleObservation],
    ) -> Result<Vec<CandleObservation>, LabError> {
        let market = subscription.market.clone();
        let interval = subscription.interval;
        let observations = observations.to_vec();
        self.database
            .call("load_canonical_public_feed_window_rows", move |store| {
                store.canonical_provider_feed_observations(&market, interval, &observations)
            })
            .await
    }

    async fn confirm_provider_recovery_window(
        &self,
        subscription: &PublicMarketSubscription,
        observations: &[CandleObservation],
    ) -> Result<(), LabError> {
        let market = subscription.market.clone();
        let interval = subscription.interval;
        let observations = observations.to_vec();
        let as_of = UtcTimestamp::now();
        self.database
            .call("confirm_public_feed_recovery_window", move |store| {
                store.confirm_provider_feed_recovery_window(&market, interval, &observations, as_of)
            })
            .await
    }

    /// Whether one active market/timeframe can safely provide runtime inputs.
    /// Aggregate health remains an operator projection; callers gate their own key.
    #[must_use]
    pub fn is_ready(&self, market: &MarketId, interval: CandleInterval) -> bool {
        let subscription = PublicMarketSubscription::new(market.clone(), interval);
        if !self.shared.subscriptions.borrow().contains(&subscription) {
            return false;
        }
        let Ok(cache) = self.shared.cache.lock() else {
            return false;
        };
        if interval == CandleInterval::D1 {
            return cache.subscription_ready(&subscription, UtcTimestamp::now());
        }
        cache.subscription_ready(&subscription, UtcTimestamp::now())
            && self.shared.health.borrow().status.connection_count == 1
            && self.shared.health.borrow().last_pong_at_utc.is_some()
    }

    /// Fail closed when this one market/timeframe lacks a bootstrapped, live feed.
    /// D1 uses the explicit REST-only path and becomes ready after a successful read.
    ///
    /// # Errors
    /// Returns `BLOCKED_EVIDENCE` until the selected input path is ready.
    pub fn ensure_ready(
        &self,
        market: &MarketId,
        interval: CandleInterval,
    ) -> Result<(), LabError> {
        if self.is_ready(market, interval) {
            Ok(())
        } else {
            Err(LabError::BlockedEvidence(format!(
                "public market input is not ready for {} {:?}",
                market.code(),
                interval
            )))
        }
    }

    /// Current secret-free connection, lag and readiness snapshot.
    #[must_use]
    pub fn health(&self) -> PublicMarketFeedHealth {
        let mut health = self.shared.health.borrow().clone();
        let subscriptions = self.shared.subscriptions.borrow().clone();
        let now = UtcTimestamp::now();
        let has_websocket_subscription = subscriptions
            .iter()
            .any(|subscription| subscription.interval != CandleInterval::D1);
        let (readiness, cache_ready, cache_available) = match self.shared.cache.lock() {
            Ok(cache) => (
                cache.subscription_readiness_for_union(&subscriptions, now),
                cache.all_required_ready(&subscriptions, now),
                true,
            ),
            Err(_) => (
                subscriptions
                    .iter()
                    .map(|subscription| RealtimeSubscriptionReadiness {
                        market: subscription.market.clone(),
                        interval: subscription.interval,
                        ready: false,
                        blockers: vec![RealtimeReadinessBlocker::CacheUnavailable],
                        pending_rest_confirmation: false,
                        revision_snapshot_count: 0,
                        last_revision_open_time_utc: None,
                        last_revision_detected_at_utc: None,
                        last_received_at_utc: None,
                        provider_at_utc: None,
                        latest_websocket_open_time_utc: None,
                        latest_rest_close_time_utc: None,
                        last_rest_fetched_at_utc: None,
                        confirmed_provider_absence_spans: Vec::new(),
                        provider_recovery_progress: None,
                    })
                    .collect(),
                false,
                false,
            ),
        };
        let mut blockers = Vec::new();
        if !cache_available {
            blockers.push(RealtimeReadinessBlocker::CacheUnavailable);
        }
        if has_websocket_subscription && health.status.connection_count == 0 {
            blockers.push(RealtimeReadinessBlocker::ConnectionMissing);
        }
        if has_websocket_subscription && health.last_pong_at_utc.is_none() {
            blockers.push(RealtimeReadinessBlocker::HeartbeatUnconfirmed);
        }
        if !subscriptions.is_empty() && readiness.iter().any(|item| !item.ready) {
            blockers.push(RealtimeReadinessBlocker::SubscriptionsNotReady);
        }
        let transport_ready = !has_websocket_subscription
            || (health.status.connection_count == 1 && health.last_pong_at_utc.is_some());
        health.status.ready_for_decisions &= cache_ready && transport_ready;
        if !health.status.ready_for_decisions && blockers.is_empty() {
            blockers.push(RealtimeReadinessBlocker::FeedStateNotReady);
        }
        health.status.readiness_blockers = blockers;
        health.status.subscription_readiness = readiness;
        health
    }

    /// Subscribe to coalesced health snapshots without exposing mutable feed state.
    #[must_use]
    pub fn subscribe_health(&self) -> watch::Receiver<PublicMarketFeedHealth> {
        self.shared.health.subscribe()
    }

    /// Wake signal for persistent consumers. Updates are coalesced state, not a
    /// durable event history or a replay cursor.
    #[must_use]
    pub fn wake(&self) -> Arc<Notify> {
        Arc::clone(&self.shared.notify)
    }

    /// Latest price from the most recently accepted candle frame for a market.
    ///
    /// # Errors
    /// Returns an internal error if the bounded feed cache lock is poisoned.
    pub fn latest_price(&self, market: &MarketId) -> Result<Option<LatestMarketPrice>, LabError> {
        let cache = self
            .shared
            .cache
            .lock()
            .map_err(|_| LabError::Internal("public feed cache lock poisoned".into()))?;
        Ok(cache.latest_price(&market.code()).cloned())
    }

    fn with_cache<T>(
        &self,
        work: impl FnOnce(&mut FeedCache) -> Result<T, LabError>,
    ) -> Result<T, LabError> {
        let mut cache = self
            .shared
            .cache
            .lock()
            .map_err(|_| LabError::Internal("public feed cache lock poisoned".into()))?;
        work(&mut cache)
    }

    fn mark_recovery(
        &self,
        subscription: &PublicMarketSubscription,
        category: PublicFeedErrorCategory,
        dropped: bool,
    ) -> Result<(), LabError> {
        self.with_cache(|cache| {
            cache.mark_recovery_required(subscription);
            Ok(())
        })?;
        self.shared.health.send_modify(|health| {
            health.status.state = RealtimeConnectionState::Recovering;
            health.status.ready_for_decisions = false;
            health.last_error_category = Some(category);
            if dropped {
                health.status.dropped_messages = health.status.dropped_messages.saturating_add(1);
            }
        });
        self.shared.notify.notify_one();
        Ok(())
    }

    fn mark_rest_failure(
        &self,
        subscription: &PublicMarketSubscription,
        category: PublicFeedErrorCategory,
        dropped: bool,
    ) -> Result<(), LabError> {
        self.with_cache(|cache| {
            cache.record_rest_failure(subscription, Instant::now());
            Ok(())
        })?;
        self.mark_recovery(subscription, category, dropped)
    }

    fn mark_reconciled(
        &self,
        subscription: &PublicMarketSubscription,
        at: UtcTimestamp,
    ) -> Result<(), LabError> {
        self.with_cache(|cache| {
            cache.mark_subscription_reconciled(subscription);
            Ok(())
        })?;
        let subscriptions = self.shared.subscriptions.borrow().clone();
        let now = UtcTimestamp::now();
        let all_ready =
            self.with_cache(|cache| Ok(cache.all_required_ready(&subscriptions, now)))?;
        self.shared.health.send_modify(|health| {
            health.status.last_reconciled_at_utc = Some(at);
            health.last_error_category = None;
            health.status.ready_for_decisions = all_ready
                && (health.status.connection_count == 1 && health.last_pong_at_utc.is_some()
                    || subscriptions
                        .iter()
                        .all(|subscription| subscription.interval == CandleInterval::D1));
            health.status.state = if health.status.ready_for_decisions {
                RealtimeConnectionState::Ready
            } else {
                RealtimeConnectionState::Recovering
            };
        });
        self.shared.notify.notify_one();
        Ok(())
    }
}

/// The required public feed handle retained by `JobRuntime`.
pub struct PublicMarketFeedRuntime {
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<(), LabError>>>,
    health: watch::Sender<PublicMarketFeedHealth>,
    notify: Arc<Notify>,
}

impl PublicMarketFeedRuntime {
    /// Cooperatively request shutdown.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Whether the owned task has ended before its owner has joined it.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Await the owned feed task without implicitly requesting cancellation.
    ///
    /// # Errors
    /// Reports task panic/cancellation or a terminal feed lifecycle error.
    pub async fn join(mut self) -> Result<(), LabError> {
        let cancellation_requested = self.cancellation.is_cancelled();
        let task = self
            .task
            .take()
            .ok_or_else(|| LabError::Internal("public feed task already joined".into()))?;
        match task.await {
            Ok(Ok(())) if cancellation_requested => Ok(()),
            Ok(Ok(())) => {
                self.mark_task_failure();
                Err(LabError::Internal(
                    "public feed task exited before cancellation".into(),
                ))
            }
            Ok(Err(error)) => {
                self.mark_task_failure();
                Err(error)
            }
            Err(_) => {
                self.mark_task_failure();
                Err(LabError::Internal(
                    "public feed task panicked or was aborted".into(),
                ))
            }
        }
    }

    /// Request cooperative cancellation and await the connection/task shutdown.
    ///
    /// # Errors
    /// Reports task panic/cancellation or a terminal feed lifecycle error.
    pub async fn shutdown(self) -> Result<(), LabError> {
        self.cancellation.cancel();
        self.join().await
    }

    fn mark_task_failure(&self) {
        self.health.send_modify(|health| {
            health.status.state = RealtimeConnectionState::Error;
            health.status.connection_count = 0;
            health.status.ready_for_decisions = false;
            health.last_error_category = Some(PublicFeedErrorCategory::Recovery);
        });
        self.notify.notify_waiters();
    }
}

impl Drop for PublicMarketFeedRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn load_provider_spans_for_observations(
    database: &DatabaseHandle,
    subscription: &PublicMarketSubscription,
    observations: &[CandleObservation],
) -> Result<Vec<ProviderAbsenceSpan>, LabError> {
    let (Some(first), Some(last)) = (observations.first(), observations.last()) else {
        return Ok(Vec::new());
    };
    let market = subscription.market.clone();
    let interval = subscription.interval;
    let start = first.candle.open_time_utc;
    let end = last.candle.close_time_utc;
    database
        .call("load_confirmed_public_provider_spans", move |store| {
            store.load_provider_absence_spans(&market, interval, start, end)
        })
        .await
}

fn merge_recovery_observations(
    staged: Vec<CandleObservation>,
    latest: &[CandleObservation],
) -> Result<Vec<CandleObservation>, LabError> {
    let mut rows: BTreeMap<UtcTimestamp, CandleObservation> = BTreeMap::new();
    for observation in staged {
        let open = observation.candle.open_time_utc;
        if let Some(existing) = rows.get_mut(&open) {
            if existing.id != observation.id
                || existing.content_digest != observation.content_digest
                || existing.candle != observation.candle
            {
                return Err(LabError::Conflict(
                    "staged public-feed recovery pages disagree at a completed candle time".into(),
                ));
            }
            for raw_id in observation.raw_object_ids {
                if !existing.raw_object_ids.contains(&raw_id) {
                    existing.raw_object_ids.push(raw_id);
                }
            }
        } else {
            rows.insert(open, observation);
        }
    }
    // The latest raw page is independently validated before it reaches this
    // merge. Let its economics supersede staged response economics at repeated
    // opens; FeedCache then pins accepted rows and records provider revisions.
    for observation in latest {
        let open = observation.candle.open_time_utc;
        if let Some(existing) = rows.get_mut(&open) {
            if existing.id != observation.id
                || existing.content_digest != observation.content_digest
                || existing.candle != observation.candle
            {
                *existing = observation.clone();
            } else {
                for raw_id in &observation.raw_object_ids {
                    if !existing.raw_object_ids.contains(raw_id) {
                        existing.raw_object_ids.push(raw_id.clone());
                    }
                }
            }
        } else {
            rows.insert(open, observation.clone());
        }
        if rows.len()
            > usize::try_from(crate::storage::MAX_PROVIDER_RECOVERY_ROWS).unwrap_or(usize::MAX)
        {
            return Err(LabError::ResourceLimit(
                "public-feed merged recovery row limit".into(),
            ));
        }
    }
    Ok(rows.into_values().collect())
}

fn page_contains_anchor(
    observations: &[CandleObservation],
    anchor: &ProviderAbsenceAnchor,
) -> bool {
    observations.iter().any(|observation| {
        observation.candle.open_time_utc == anchor.open_time_utc
            && observation.id == anchor.observation_id
            && observation.content_digest == anchor.content_digest
    })
}

fn earliest_provider_anchor(
    left: Option<ProviderAbsenceAnchor>,
    right: Option<ProviderAbsenceAnchor>,
) -> Result<Option<ProviderAbsenceAnchor>, LabError> {
    match (left, right) {
        (None, anchor) | (anchor, None) => Ok(anchor),
        (Some(left), Some(right)) if left.open_time_utc < right.open_time_utc => Ok(Some(left)),
        (Some(left), Some(right)) if right.open_time_utc < left.open_time_utc => Ok(Some(right)),
        (Some(left), Some(right))
            if left.observation_id == right.observation_id
                && left.content_digest == right.content_digest =>
        {
            Ok(Some(left))
        }
        (Some(_), Some(_)) => Err(LabError::Conflict(
            "feed and paper owners disagree at the same accepted candle boundary".into(),
        )),
    }
}

fn cursor_backfill_anchor(cursor: &ProviderFeedRecoveryCursor) -> Option<ProviderAbsenceAnchor> {
    cursor
        .progress
        .accepted_anchor
        .clone()
        .or_else(|| cursor.last_recovered_tip.clone())
}

fn completed_provider_backfill_anchor(
    cursor: &ProviderFeedRecoveryCursor,
    active_consumer: Option<ProviderAbsenceAnchor>,
) -> Result<Option<ProviderAbsenceAnchor>, LabError> {
    if !cursor.is_complete() {
        return Err(LabError::Conflict(
            "cannot replace an incomplete recovery boundary".into(),
        ));
    }
    // Current checkpoints own the active consumer boundary. A completed epoch's
    // historical seed is not a reason to rescan months of already-consumed data.
    let source = if active_consumer.is_some() {
        cursor
            .last_recovered_tip
            .clone()
            .or_else(|| cursor.progress.accepted_anchor.clone())
    } else {
        cursor_backfill_anchor(cursor)
    };
    earliest_provider_anchor(source, active_consumer)
}

fn awaiting_provider_backfill(market: &MarketId, interval: CandleInterval) -> LabError {
    LabError::BlockedEvidence(format!(
        "PUBLIC_FEED_BACKFILL_IN_PROGRESS: {} {:?} is advancing one bounded historical page",
        market.code(),
        interval
    ))
}

fn same_economic_candle(
    left: &crate::contracts::CandleRecord,
    right: &crate::contracts::CandleRecord,
) -> bool {
    let mut left = left.clone();
    left.completed = right.completed;
    left == *right
}

fn validate_rest_snapshot(
    observations: &[CandleObservation],
    completed_count: ProbeCount,
    spans: &[crate::contracts::ProviderAbsenceSpan],
) -> Result<(), LabError> {
    let requested = usize::try_from(completed_count.get())
        .map_err(|_| LabError::InvalidConfig("completed_count out of range".into()))?;
    if observations.len() < requested {
        return Err(LabError::InsufficientWarmup(format!(
            "REST bootstrap returned {} of {requested} completed candles",
            observations.len()
        )));
    }
    for pair in observations.windows(2) {
        if pair[0].candle.close_time_utc != pair[1].candle.open_time_utc
            && !spans.iter().any(|span| {
                span.market.code() == pair[0].candle.market
                    && span.interval == pair[0].candle.interval
                    && span.start_utc == pair[0].candle.close_time_utc
                    && span.end_utc == pair[1].candle.open_time_utc
                    && span.pre_anchor.open_time_utc == pair[0].candle.open_time_utc
                    && span.post_anchor.open_time_utc == pair[1].candle.open_time_utc
            })
        {
            return Err(LabError::DataGap(
                "REST bootstrap/recovery contains an unconfirmed provider candle gap".into(),
            ));
        }
    }
    Ok(())
}

fn awaiting_rest_confirmation(market: &MarketId, interval: CandleInterval) -> LabError {
    LabError::BlockedEvidence(format!(
        "AWAITING_REST_CONFIRMATION: {} {:?} requires a matching independent REST snapshot at least one second later",
        market.code(),
        interval
    ))
}

fn awaiting_rest_retry(market: &MarketId, interval: CandleInterval) -> LabError {
    LabError::BlockedEvidence(format!(
        "PUBLIC_REST_RETRY_BACKOFF: {} {:?} is waiting for its bounded retry deadline",
        market.code(),
        interval
    ))
}

fn awaiting_provider_absence_confirmation(market: &MarketId, interval: CandleInterval) -> LabError {
    LabError::BlockedEvidence(format!(
        "AWAITING_PROVIDER_ABSENCE_CONFIRMATION: {} {:?} requires an unchanged independent REST page at least one second later",
        market.code(),
        interval
    ))
}

fn log_pinned_revisions(
    market: &MarketId,
    interval: CandleInterval,
    revision: Option<(usize, UtcTimestamp)>,
) {
    if let Some((changed_slots, last_open_time)) = revision {
        tracing::warn!(
            market = %market.code(),
            interval = ?interval,
            changed_slots,
            last_revision_open_time_utc = %last_open_time.to_rfc3339(),
            "public REST snapshot revised accepted history; pinned canonical observations"
        );
    }
}

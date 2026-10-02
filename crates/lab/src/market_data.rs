//! Upbit public market data: request, raw preservation, time normalization.
//!
//! Batch A path only: fetch completed candles, preserve the raw response
//! byte-exact with its hash and fetch timestamp. Completed means
//! `close_time <= fetched_at`; a still-forming candle is never reported.

use crate::contracts::ContentHash;
use crate::contracts::{
    AssetQuantity, CandleInterval, CandleRecord, LabError, MarketDataOrigin, MarketId, PriceKrw,
    ProbeCount, ProbeReport, QuoteAmount, UtcTimestamp,
};
use crate::database::DatabaseHandle;
use crate::storage::RawObjectInput;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub const UPBIT_PUBLIC_BASE: &str = "https://api.upbit.com";

// A probe has at most 200 wire candles. This permits >5 KiB per candle while
// imposing a hard receive-buffer ceiling, including chunked responses.
const MAX_RESPONSE_BYTES: usize = 1_048_576;
// Conservative per-process spacing; other programs on the IP share Upbit's quota.
const PROBE_SPACING: Duration = Duration::from_millis(101);

pub(crate) struct CandleResponse {
    pub source_url: String,
    pub fetched_at: UtcTimestamp,
    pub http_status: u16,
    pub remaining_req: Option<String>,
    pub body: Vec<u8>,
    pub origin: MarketDataOrigin,
}

pub(crate) struct FetchFailure {
    pub error: LabError,
    pub retryable: bool,
}
impl FetchFailure {
    fn permanent(error: LabError) -> Self {
        Self {
            error,
            retryable: false,
        }
    }
}

/// Unparsed `v1/market/all` response: a validation gate input, never
/// archived as raw evidence.
pub(crate) struct MarketCatalogResponse {
    pub http_status: u16,
    pub body: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct UpbitMarketWire {
    market: String,
}

/// Parse the `v1/market/all` catalog response into market codes.
///
/// # Errors
///
/// [`LabError::ContractParse`] on malformed catalog JSON.
pub(crate) fn parse_market_catalog(body: &[u8]) -> Result<Vec<String>, LabError> {
    let entries: Vec<UpbitMarketWire> = serde_json::from_slice(body)?;
    Ok(entries.into_iter().map(|entry| entry.market).collect())
}

/// Thin client over Upbit's public (unauthenticated) REST API only. Private
/// exchange API surfaces are out of scope by contract.
#[derive(Clone)]
pub struct UpbitClient {
    http: reqwest::Client,
    base_url: String,
    next_probe: Arc<Mutex<Instant>>,
    origin: MarketDataOrigin,
}

impl UpbitClient {
    /// Build a client pinned to the `Upbit` public base URL.
    ///
    /// # Errors
    ///
    /// [`LabError::InvalidConfig`] when the underlying HTTP client cannot be
    /// constructed (TLS backend failure).
    pub fn new() -> Result<Self, LabError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("spot-lab/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| LabError::InvalidConfig(format!("http client build: {error}")))?;
        Ok(Self {
            http,
            base_url: UPBIT_PUBLIC_BASE.to_owned(),
            next_probe: Arc::new(Mutex::new(Instant::now())),
            origin: MarketDataOrigin::ExchangeObserved,
        })
    }

    #[cfg(test)]
    pub(crate) fn synthetic_local(base_url: &str) -> Result<Self, LabError> {
        let parsed = reqwest::Url::parse(base_url)
            .map_err(|error| LabError::InvalidConfig(format!("test base URL: {error}")))?;
        if parsed.scheme() != "http"
            || !matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1"))
        {
            return Err(LabError::InvalidConfig(
                "test base URL must be loopback HTTP".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|error| LabError::InvalidConfig(format!("test HTTP client build: {error}")))?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
            next_probe: Arc::new(Mutex::new(Instant::now())),
            origin: MarketDataOrigin::SyntheticTestOnly,
        })
    }

    /// One real network probe. Requests one extra candle so that the newest,
    /// possibly still-forming candle can be dropped while still returning
    /// `completed_count` completed candles.
    ///
    /// # Errors
    ///
    /// [`LabError::NetworkUnavailable`] on transport or HTTP-level failures,
    /// [`LabError::ContractParse`] when the wire payload violates the candle
    /// contract, [`LabError::DataGap`] when fewer than `completed_count`
    /// completed candles exist in the response, and [`LabError::Internal`]
    /// when the raw response cannot be persisted.
    pub async fn fetch_completed_candles(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        completed_count: ProbeCount,
        database: Option<&DatabaseHandle>,
    ) -> Result<ProbeReport, LabError> {
        // No queued probes: clones (including MCP sessions) share one in-flight
        // slot and a monotonic request clock. Dropping the future releases it.
        let mut next_probe = self.next_probe.try_lock().map_err(|_| {
            LabError::CapacityExceeded("another Upbit probe is in flight; no request sent".into())
        })?;
        let now = Instant::now();
        if now < *next_probe {
            return Err(LabError::RateLimited(format!(
                "local candle pacing; try again in {} ms; no request sent",
                (*next_probe - now).as_millis()
            )));
        }
        *next_probe = now + PROBE_SPACING;
        // Freeze the completion cutoff before requesting: a response crossing a
        // candle boundary must not promote an observation fetched while forming.
        let fetched_at = UtcTimestamp::now();
        let response = self
            .request_page(
                market,
                interval,
                completed_count.get() + 1,
                None,
                fetched_at,
                &mut next_probe,
            )
            .await
            .map_err(|failure| failure.error)?;
        let source_url = response.source_url.clone();
        let http_status = response.http_status;
        let remaining_req = response.remaining_req.clone();
        let origin = response.origin;
        let raw_sha256 = ContentHash::of_bytes(&response.body).to_string();
        let (raw, raw_file) = if let Some(database) = database {
            let published = database
                .call("publish_probe_raw", move |store| {
                    let result = store.publish_raw(RawObjectInput {
                        source_url: response.source_url,
                        fetched_at: response.fetched_at,
                        persisted_at: UtcTimestamp::now(),
                        http_status: response.http_status,
                        remaining_req: response.remaining_req,
                        origin: response.origin,
                        body: response.body,
                    })?;
                    store.catalog_raw(&result.object)?;
                    Ok(result)
                })
                .await?;
            (
                published.body,
                Some(format!("{}/body.json.gz", published.object.relative_path)),
            )
        } else {
            (response.body, None)
        };
        if !(200..300).contains(&http_status) {
            return Err(http_error(http_status, &raw_sha256, raw_file.as_deref()));
        }
        let mut candles = parse_candle_bytes(&raw, market, interval, fetched_at)?;
        let requested = usize::try_from(completed_count.get())
            .map_err(|_| LabError::InvalidConfig("completed_count out of range".to_owned()))?;
        candles.retain(|record| record.completed);
        candles.truncate(requested);
        if candles.len() < requested {
            return Err(LabError::DataGap(format!(
                "requested {} completed candles, wire contained only {} at fetched_at={fetched_at} (Upbit omits candles with no trades); raw_sha256={raw_sha256}",
                completed_count.get(),
                candles.len()
            )));
        }
        validate_continuity(&candles, interval)?;

        Ok(ProbeReport {
            market: market.code().clone(),
            interval,
            requested_completed_count: completed_count.get(),
            fetched_at,
            source_url,
            http_status,
            raw_sha256,
            raw_file,
            remaining_req,
            origin,
            candles,
        })
    }

    /// Bounded collection request. Waiting only occurs in the single job runner;
    /// interactive probes still reject immediately. No persistence happens here.
    /// Fetch the live Upbit market catalog (`v1/market/all`). Validation
    /// gate only: the response is bounded and never archived as raw
    /// evidence. Shares the request pacing gate with candle requests.
    pub(crate) async fn fetch_market_catalog(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketCatalogResponse, FetchFailure> {
        let operation = async {
            let mut next_probe = self.next_probe.lock().await;
            tokio::time::sleep_until(*next_probe).await;
            *next_probe = Instant::now() + PROBE_SPACING;
            let url = format!("{}/v1/market/all?isDetails=false", self.base_url);
            let response = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(|error| FetchFailure {
                    retryable: error.is_timeout(),
                    error: LabError::NetworkUnavailable(format!(
                        "upbit market catalog request: {error}"
                    )),
                })?;
            let http_status = response.status().as_u16();
            let remaining_req = response
                .headers()
                .get("Remaining-Req")
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
            if matches!(http_status, 429 | 418)
                || remaining_req.as_deref().is_some_and(|header| {
                    header
                        .split(';')
                        .any(|part| part.trim().split_once('=') == Some(("sec", "0")))
                })
            {
                *next_probe = Instant::now() + Duration::from_secs(1);
            }
            let body = read_body(response).await.map_err(FetchFailure::permanent)?;
            Ok(MarketCatalogResponse { http_status, body })
        };
        tokio::select! {
            () = cancellation.cancelled() => Err(FetchFailure::permanent(LabError::Cancelled("market catalog request cancelled".into()))),
            result = tokio::time::timeout_at(deadline, operation) => result.unwrap_or_else(|_| Err(FetchFailure::permanent(
                LabError::NetworkUnavailable("market catalog deadline elapsed".into())
            ))),
        }
    }

    pub(crate) async fn collect_page(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        to: UtcTimestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CandleResponse, FetchFailure> {
        let operation = async {
            let mut next_probe = self.next_probe.lock().await;
            tokio::time::sleep_until(*next_probe).await;
            *next_probe = Instant::now() + PROBE_SPACING;
            self.request_page(
                market,
                interval,
                200,
                Some(to),
                UtcTimestamp::now(),
                &mut next_probe,
            )
            .await
        };
        tokio::select! {
            () = cancellation.cancelled() => Err(FetchFailure::permanent(LabError::Cancelled("collection request cancelled".into()))),
            result = tokio::time::timeout_at(deadline, operation) => result.unwrap_or_else(|_| Err(FetchFailure::permanent(
                LabError::NetworkUnavailable("collection deadline elapsed".into())
            ))),
        }
    }

    async fn request_page(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        count: u32,
        to: Option<UtcTimestamp>,
        fetched_at: UtcTimestamp,
        next_probe: &mut Instant,
    ) -> Result<CandleResponse, FetchFailure> {
        let mut url = format!(
            "{}/{}?market={}&count={count}",
            self.base_url,
            interval.upbit_endpoint(),
            market.code()
        );
        if let Some(to) = to {
            use std::fmt::Write as _;
            let _ = write!(url, "&to={}", to.to_rfc3339());
        }
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|error| FetchFailure {
                retryable: error.is_timeout(),
                error: LabError::NetworkUnavailable(format!("upbit request: {error}")),
            })?;
        let http_status = response.status().as_u16();
        let remaining_req = response
            .headers()
            .get("Remaining-Req")
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
        if matches!(http_status, 429 | 418)
            || remaining_req.as_deref().is_some_and(|header| {
                header
                    .split(';')
                    .any(|part| part.trim().split_once('=') == Some(("sec", "0")))
            })
        {
            *next_probe = Instant::now() + Duration::from_secs(1);
        }
        let body = read_body(response).await.map_err(FetchFailure::permanent)?;
        Ok(CandleResponse {
            source_url: url,
            fetched_at,
            http_status,
            remaining_req,
            body,
            origin: self.origin,
        })
    }
}

fn http_error(status: u16, hash: &str, raw_file: Option<&str>) -> LabError {
    let detail = format!(
        "upbit HTTP {status}; raw_sha256={hash}; raw_file={}",
        raw_file.unwrap_or("disabled")
    );
    match status {
        429 => LabError::RateLimited(format!(
            "{detail}; candle/IP quota exhausted; wait until the next second before retrying"
        )),
        418 => LabError::TemporarilyBlocked(format!(
            "{detail}; repeated limit violations; inspect the preserved response and wait out the ban before retrying"
        )),
        _ => LabError::NetworkUnavailable(detail),
    }
}

async fn read_body(mut response: reqwest::Response) -> Result<Vec<u8>, LabError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| LabError::NetworkUnavailable(format!("upbit body read: {error}")))?
    {
        if chunk.len() > MAX_RESPONSE_BYTES - bytes.len() {
            return Err(LabError::CapacityExceeded(format!(
                "Upbit response exceeds {MAX_RESPONSE_BYTES} bytes; incomplete body not archived"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn validate_continuity(candles: &[CandleRecord], interval: CandleInterval) -> Result<(), LabError> {
    for pair in candles.windows(2) {
        let distance = pair[0].open_time_utc.0 - pair[1].open_time_utc.0;
        if distance != interval.duration() {
            return Err(LabError::DataGap(format!(
                "missing candle interval between {} and {} (Upbit omits candles with no trades)",
                pair[1].open_time_utc, pair[0].open_time_utc
            )));
        }
    }
    Ok(())
}

/// Wire DTO for Upbit candle endpoints. Unknown exchange fields are ignored;
/// numbers stay exact decimal literals via `arbitrary_precision`.
#[derive(Debug, Deserialize)]
struct UpbitCandleWire {
    market: String,
    candle_date_time_utc: String,
    opening_price: serde_json::Number,
    high_price: serde_json::Number,
    low_price: serde_json::Number,
    trade_price: serde_json::Number,
    candle_acc_trade_volume: serde_json::Number,
    candle_acc_trade_price: serde_json::Number,
}

/// Pure parse + time normalization. `fetched_at` decides completeness:
/// candle `[t, t+step)` is completed iff `t+step <= fetched_at`.
///
/// # Errors
///
/// [`LabError::ContractParse`] on malformed JSON, mismatched market codes or
/// out-of-contract numeric literals.
fn parse_candle_bytes(
    raw: &[u8],
    market: &MarketId,
    interval: CandleInterval,
    fetched_at: UtcTimestamp,
) -> Result<Vec<CandleRecord>, LabError> {
    let records = decode_candles(raw, market, interval, fetched_at)?;
    if records
        .windows(2)
        .any(|pair| pair[0].open_time_utc <= pair[1].open_time_utc)
    {
        return Err(LabError::ContractParse(
            "candles must be distinct and newest first".into(),
        ));
    }
    Ok(records)
}

pub(crate) fn decode_candles(
    raw: &[u8],
    market: &MarketId,
    interval: CandleInterval,
    fetched_at: UtcTimestamp,
) -> Result<Vec<CandleRecord>, LabError> {
    let wire: Vec<UpbitCandleWire> = serde_json::from_slice(raw)?;
    if wire.len() > 200 {
        return Err(LabError::ContractParse(
            "Upbit returned more than 200 candles".into(),
        ));
    }
    let step = interval.duration();
    let expected_market = market.code();
    let mut records = Vec::with_capacity(wire.len());
    for candle in wire {
        if candle.market != expected_market {
            return Err(LabError::ContractParse(format!(
                "candle market {} does not match requested {expected_market}",
                candle.market
            )));
        }
        let open = chrono::NaiveDateTime::parse_from_str(
            &candle.candle_date_time_utc,
            "%Y-%m-%dT%H:%M:%S",
        )
        .map_err(|error| {
            LabError::ContractParse(format!(
                "candle_date_time_utc {:?}: {error}",
                candle.candle_date_time_utc
            ))
        })?
        .and_utc();
        if open.timestamp().rem_euclid(step.num_seconds()) != 0 {
            return Err(LabError::ContractParse(format!(
                "unaligned candle start: {open}"
            )));
        }
        let close_time = open
            .checked_add_signed(step)
            .ok_or_else(|| LabError::ContractParse("candle close timestamp out of range".into()))?;
        let completed = close_time <= fetched_at.0;
        records.push(CandleRecord {
            market: expected_market.clone(),
            interval,
            open_time_utc: UtcTimestamp(open),
            close_time_utc: UtcTimestamp(close_time),
            open: PriceKrw::from_json_number(&candle.opening_price)?,
            high: PriceKrw::from_json_number(&candle.high_price)?,
            low: PriceKrw::from_json_number(&candle.low_price)?,
            close: PriceKrw::from_json_number(&candle.trade_price)?,
            volume: AssetQuantity::from_json_number(&candle.candle_acc_trade_volume)?,
            quote_turnover: QuoteAmount::from_json_number(&candle.candle_acc_trade_price)?,
            completed,
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests;

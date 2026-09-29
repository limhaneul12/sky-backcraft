use super::*;
use crate::contracts::CandleInterval;

/// `SYNTHETIC_TEST_ONLY` fixture in `Upbit` wire shape; newest candle
/// first, still forming at `04:30Z` (its close is `05:00Z`).
const SYNTHETIC_WIRE: &str = r#"[
    {"market":"KRW-BTC","candle_date_time_utc":"2026-09-26T04:00:00","candle_date_time_kst":"2026-09-26T13:00:00","opening_price":160000000,"high_price":160100000,"low_price":159900000,"trade_price":160050000,"candle_acc_trade_price":123.45,"candle_acc_trade_volume":0.771,"unit":60},
    {"market":"KRW-BTC","candle_date_time_utc":"2026-09-26T03:00:00","candle_date_time_kst":"2026-09-26T12:00:00","opening_price":159000000,"high_price":160200000,"low_price":158800000,"trade_price":160000000,"candle_acc_trade_price":456.78,"candle_acc_trade_volume":2.85,"unit":60}
]"#;

#[test]
fn completed_boundary_is_close_time_inclusive() -> Result<(), LabError> {
    let market = MarketId::parse_upbit("KRW-BTC")?;
    // Exactly at close: the [04:00, 05:00) candle is completed.
    let at_close = UtcTimestamp::parse_rfc3339("2026-09-26T05:00:00Z")?;
    let records = decode_candles(
        SYNTHETIC_WIRE.as_bytes(),
        &market,
        CandleInterval::H1,
        at_close,
    )?;
    assert_eq!(records.len(), 2);
    assert!(records[0].completed);
    assert_eq!(
        records[0].close_time_utc.to_rfc3339(),
        "2026-09-26T05:00:00Z"
    );
    assert_eq!(records[0].close.get().to_string(), "160050000");
    // One second before close: newest candle is still forming.
    let before_close = UtcTimestamp::parse_rfc3339("2026-09-26T04:59:59Z")?;
    let records = decode_candles(
        SYNTHETIC_WIRE.as_bytes(),
        &market,
        CandleInterval::H1,
        before_close,
    )?;
    assert!(!records[0].completed);
    assert!(records[1].completed);
    Ok(())
}

/// One boundary journey covers received bytes, classification and archive order.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture journey verifies receive/archive/reject together"
)]
fn rejected_response_preserves_raw_bytes() -> Result<(), Box<dyn std::error::Error>> {
    use crate::database::DatabaseOwner;
    use axum::http::StatusCode;
    use std::io::Read as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let cases = [
        (StatusCode::OK, b"not-json".to_vec(), "CONTRACT_PARSE"),
        (StatusCode::OK, vec![0xff, 0xfe], "CONTRACT_PARSE"),
        (StatusCode::OK, b"[]".to_vec(), "DATA_GAP"),
        (
            StatusCode::OK,
            SYNTHETIC_WIRE
                .replace("T03:00:00", "T02:00:00")
                .into_bytes(),
            "DATA_GAP",
        ),
        (
            StatusCode::TOO_MANY_REQUESTS,
            b"{\"error\":\"quota\"}".to_vec(),
            "RATE_LIMITED",
        ),
        (
            StatusCode::IM_A_TEAPOT,
            b"{\"error\":\"ban duration 60 seconds\"}".to_vec(),
            "TEMPORARILY_BLOCKED",
        ),
        (
            StatusCode::OK,
            vec![b' '; MAX_RESPONSE_BYTES + 1],
            "CAPACITY_EXCEEDED",
        ),
        (StatusCode::OK, SYNTHETIC_WIRE.as_bytes().to_vec(), "OK"),
    ];
    for (index, (status, body, expected)) in cases.into_iter().enumerate() {
        let root = TestDirectory(
            std::env::temp_dir().join(format!("spot-lab-raw-{}-{index}", std::process::id())),
        );
        std::fs::create_dir(&root.0)?;
        let owner = DatabaseOwner::open(root.0.clone())?;
        let database = owner.handle();
        let checked: Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let response_body = body.clone();
            let app = axum::Router::new().fallback(move || {
                let body = response_body.clone();
                async move {
                    (
                        status,
                        [("Remaining-Req", "group=candle; min=1800; sec=0")],
                        body,
                    )
                }
            });
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
            });
            let mut client = UpbitClient::new()?;
            client.base_url = format!("http://{address}");
            client.http = reqwest::Client::builder().no_proxy().build()?;
            let result = client
                .fetch_completed_candles(
                    &MarketId::parse_upbit("KRW-BTC")?,
                    CandleInterval::H1,
                    ProbeCount::default(),
                    Some(&database),
                )
                .await;
            let _ = stop.send(());
            server.await??;
            if expected == "OK" {
                assert_eq!(result?.candles.len(), 2);
            } else {
                assert!(
                    result.unwrap_err().to_string().starts_with(expected),
                    "case {index}"
                );
            }
            // sec=0 (not deprecated min) prevents the next probe on any clone.
            assert!(matches!(
                client
                    .clone()
                    .fetch_completed_candles(
                        &MarketId::parse_upbit("KRW-BTC")?,
                        CandleInterval::H1,
                        ProbeCount::default(),
                        None
                    )
                    .await,
                Err(LabError::RateLimited(_))
            ));
            let directory = root
                .0
                .join("raw")
                .join(&ContentHash::of_bytes(&body).as_str()[..2]);
            if expected == "CAPACITY_EXCEEDED" {
                assert!(!tokio::fs::try_exists(directory).await?);
                return Ok(());
            }
            let mut entries = tokio::fs::read_dir(&directory).await?;
            let entry = entries
                .next_entry()
                .await?
                .ok_or("raw object not published")?;
            assert!(entries.next_entry().await?.is_none());
            let meta: crate::contracts::RawObjectRef = serde_json::from_slice(
                &tokio::fs::read(entry.path().join("metadata.json")).await?,
            )?;
            assert_eq!(meta.http_status, status.as_u16());
            assert_eq!(meta.raw_sha256, ContentHash::of_bytes(&body));
            assert_eq!(
                meta.remaining_req.as_deref(),
                Some("group=candle; min=1800; sec=0")
            );
            let compressed = tokio::fs::read(entry.path().join("body.json.gz")).await?;
            let mut decoded = Vec::new();
            flate2::read::GzDecoder::new(compressed.as_slice()).read_to_end(&mut decoded)?;
            assert_eq!(decoded, body);
            assert!(
                database
                    .call("orphan_readback", |store| store.scan_orphans())
                    .await?
                    .is_empty()
            );
            Ok(())
        });
        owner.shutdown()?;
        checked?;
    }
    Ok(())
}

struct TestDirectory(std::path::PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        // Test-only owned temporary directory, also cleaned when an assertion fails.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn shared_probe_admission_rejects_without_queueing() -> Result<(), LabError> {
    let client = UpbitClient::new()?;
    let clone = client.clone();
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let mut in_flight = client.next_probe.lock().await;
    assert!(matches!(
        clone
            .fetch_completed_candles(&market, CandleInterval::H1, ProbeCount::default(), None)
            .await,
        Err(LabError::CapacityExceeded(_))
    ));
    *in_flight = Instant::now() + Duration::from_secs(60);
    drop(in_flight);
    assert!(matches!(
        clone
            .fetch_completed_candles(&market, CandleInterval::H1, ProbeCount::default(), None)
            .await,
        Err(LabError::RateLimited(_))
    ));
    Ok(())
}

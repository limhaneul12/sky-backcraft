use super::*;
use crate::contracts::{PriceKrw, UtcRange};
use crate::database::DatabaseOwner;
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use rust_decimal::Decimal;
use std::collections::VecDeque;
use std::fs;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
type ScriptedResponses = Arc<Mutex<VecDeque<(StatusCode, Vec<u8>)>>>;

struct TempRoot(std::path::PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-collection-{label}-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create collection test root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone)]
struct ScriptedServer {
    responses: ScriptedResponses,
    seen_to: Arc<Mutex<Vec<Option<String>>>>,
}

struct RunningServer {
    base_url: String,
    seen_to: Arc<Mutex<Vec<Option<String>>>>,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn scripted_response(
    State(script): State<ScriptedServer>,
    uri: Uri,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    let absolute = format!("http://localhost{uri}");
    let to = reqwest::Url::parse(&absolute)
        .expect("local scripted request URI")
        .query_pairs()
        .find(|(key, _)| key == "to")
        .map(|(_, value)| value.into_owned());
    script.seen_to.lock().expect("seen cursor lock").push(to);
    let response = script
        .responses
        .lock()
        .expect("script response lock")
        .pop_front()
        .unwrap_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            b"script exhausted".to_vec(),
        ));
    (
        response.0,
        [("Remaining-Req", "group=candle; min=1800; sec=9")],
        response.1,
    )
}

async fn start_server(
    responses: Vec<(StatusCode, Vec<u8>)>,
) -> Result<RunningServer, std::io::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let seen_to = Arc::new(Mutex::new(Vec::new()));
    let state = ScriptedServer {
        responses: Arc::new(Mutex::new(responses.into())),
        seen_to: seen_to.clone(),
    };
    let app = axum::Router::new()
        .fallback(scripted_response)
        .with_state(state);
    let (stop_tx, stop_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ignored = stop_rx.await;
            })
            .await
    });
    Ok(RunningServer {
        base_url: format!("http://{address}"),
        seen_to,
        stop: stop_tx,
        task: server,
    })
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey covers retry, raw preservation, durable resume and slim format storage"
)]
fn pagination_retry_raw_before_parse_and_durable_resume_cover_more_than_200_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new("resume");
    let owner = DatabaseOwner::open(root.0.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let request = collection_request("request-pagination");
    let request_id = request.request_id.clone();
    let market = request.markets[0].clone();
    let outcome: Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
        let first_page = wire_page(1, 201);
        let retry_body = b"{\"error\":\"temporary\"}".to_vec();
        let malformed_body = b"not-json-after-retry".to_vec();
        let first_server = start_server(vec![
            (StatusCode::OK, first_page),
            (StatusCode::INTERNAL_SERVER_ERROR, retry_body.clone()),
            (StatusCode::OK, malformed_body.clone()),
        ])
        .await?;
        let first_seen = first_server.seen_to.clone();
        let client = UpbitClient::synthetic_local(&first_server.base_url)?;
        let interrupted = tokio::time::timeout(
            Duration::from_secs(10),
            collect(
                &client,
                &database,
                request.clone(),
                &CancellationToken::new(),
            ),
        )
        .await;
        drop(client);
        let _ignored = first_server.stop.send(());
        let stopped = tokio::time::timeout(Duration::from_secs(2), first_server.task)
            .await
            .map_err(|_| "first scripted server did not stop")?;
        stopped??;
        let error = interrupted
            .map_err(|_| "interrupted collection exceeded test deadline")?
            .expect_err("malformed retry must stop collection");
        assert!(matches!(error, LabError::ContractParse(_)));

        let inspect_request = request_id.clone();
        let inspect_market = market.clone();
        let (pages, raw_bodies) = database
            .call("inspect_interrupted_collection", move |store| {
                let pages = store.load_collection_pages(&inspect_request, &inspect_market)?;
                let raw = store.load_collection_raw_objects(&inspect_request)?;
                let bodies = raw
                    .iter()
                    .map(|object| store.raw_objects().read_verified(object))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((pages, bodies))
            })
            .await?;
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].observations.len(), 200);
        assert_eq!(raw_bodies.len(), 3);
        assert_eq!(raw_bodies[1], retry_body);
        assert_eq!(raw_bodies[2], malformed_body);

        let resume_server = start_server(vec![(StatusCode::OK, wire_page(0, 1))]).await?;
        let resume_seen = resume_server.seen_to.clone();
        let resume_client = UpbitClient::synthetic_local(&resume_server.base_url)?;
        let resumed_result = tokio::time::timeout(
            Duration::from_secs(10),
            collect(
                &resume_client,
                &database,
                request.clone(),
                &CancellationToken::new(),
            ),
        )
        .await;
        drop(resume_client);
        let _ignored = resume_server.stop.send(());
        let stopped = tokio::time::timeout(Duration::from_secs(2), resume_server.task)
            .await
            .map_err(|_| "resume scripted server did not stop")?;
        stopped??;
        let resumed = resumed_result.map_err(|_| "resumed collection exceeded test deadline")??;
        assert_eq!(resumed.manifest.status, DatasetStatus::Ready);
        assert_eq!(resumed.observations.len(), 201);
        assert_eq!(resumed.manifest.raw_objects.len(), 4);

        let first_cursors = first_seen.lock().expect("first cursor lock").clone();
        let resume_cursors = resume_seen.lock().expect("resume cursor lock").clone();
        assert_eq!(first_cursors.len(), 3);
        assert_eq!(first_cursors[1], first_cursors[2]);
        assert_eq!(resume_cursors, vec![first_cursors[1].clone()]);
        Ok(())
    });
    owner.shutdown()?;
    let connection = rusqlite::Connection::open_with_flags(
        root.0.join("lab.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let rows = connection
        .prepare(
            "SELECT page_index, length(page_json), instr(page_json, 'slim_v1') \
             FROM collection_pages ORDER BY page_index",
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(rows.len(), 2);
    for (page_index, json_bytes, slim_mark) in rows {
        assert!(
            slim_mark > 0,
            "page {page_index} must store the slim header"
        );
        assert!(
            json_bytes < 2048,
            "page {page_index} stored {json_bytes} bytes; slim headers stay under 2 KiB"
        );
    }
    outcome
}

#[test]
fn duplicate_conflicts_block_admission_and_resampling_preserves_exact_constituents() {
    let request = CollectRequest {
        request_id: RequestId::new("request-conflict").expect("request ID"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range: UtcRange::new(time(0), time(1)).expect("range"),
        data_resolution: CandleInterval::H1,
        warmup_bars: 0,
        completed_only: true,
    };
    let first = observation(CandleInterval::H1, 0, "100", "105", "1", "raw-a");
    let conflict = observation(CandleInterval::H1, 0, "101", "105", "1", "raw-b");
    let snapshot = assemble_dataset(
        request.clone(),
        &[
            page(&request, 0, first, "raw-a"),
            page(&request, 1, conflict, "raw-b"),
        ],
        &[],
        &ReuseInputs::default(),
    )
    .expect("assemble conflicting pages");
    assert_eq!(snapshot.manifest.status, DatasetStatus::BlockedData);
    assert!(snapshot.manifest.quality_issues.iter().any(|issue| {
        issue.kind == QualityKind::DuplicateConflict && issue.severity == QualitySeverity::Error
    }));

    let fine: Vec<_> = (0..12)
        .map(|index| {
            observation(
                CandleInterval::M5,
                index * 5,
                &format!("{}", 100 + index),
                &format!("{}", 101 + index),
                "2",
                &format!("raw-{index}"),
            )
        })
        .collect();
    let aggregated = resample(&fine, CandleInterval::H1).expect("resample full hour");
    assert_eq!(aggregated.len(), 1);
    assert_eq!(aggregated[0].candle.open, fine[0].candle.open);
    assert_eq!(aggregated[0].candle.close, fine[11].candle.close);
    assert_eq!(aggregated[0].candle.volume.get(), Decimal::from(24));
    assert_eq!(aggregated[0].constituent_ids.len(), 12);
    assert_eq!(
        aggregated[0].constituent_ids,
        fine.iter().map(|row| row.id.clone()).collect::<Vec<_>>()
    );
    assert!(matches!(
        resample(&fine[..11], CandleInterval::H1),
        Err(LabError::DataGap(_))
    ));
}

#[test]
fn reuse_merges_stored_rows_reports_counts_and_keeps_duplicates_informational() {
    let request = CollectRequest {
        request_id: RequestId::new("request-reuse").expect("request ID"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range: UtcRange::new(time(0), time(3)).expect("range"),
        data_resolution: CandleInterval::H1,
        warmup_bars: 0,
        completed_only: true,
    };
    let coverage = request
        .range
        .with_warmup(0, CandleInterval::H1)
        .expect("coverage");
    let stored = vec![observation(
        CandleInterval::H1,
        60,
        "100",
        "105",
        "1",
        "raw-stored",
    )];
    let covered: std::collections::BTreeSet<_> =
        stored.iter().map(|row| row.candle.open_time_utc).collect();
    let segments = missing_segments(&coverage, CandleInterval::H1, &covered).expect("segments");
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0], (time(2), time(3)));
    assert_eq!(segments[1], (time(0), time(1)));

    let reuse = ReuseInputs {
        observations: &stored,
        raw_objects: &[raw_ref("raw-stored")],
        api_calls: 2,
    };
    let snapshot = assemble_dataset(
        request.clone(),
        &[
            page(
                &request,
                0,
                observation(CandleInterval::H1, 0, "100", "105", "1", "raw-a"),
                "raw-a",
            ),
            page(
                &request,
                1,
                observation(CandleInterval::H1, 120, "100", "105", "1", "raw-b"),
                "raw-b",
            ),
        ],
        &[],
        &reuse,
    )
    .expect("assemble with reuse");
    assert_eq!(snapshot.manifest.status, DatasetStatus::Ready);
    assert_eq!(snapshot.observations.len(), 3);
    let summary = snapshot.manifest.reuse.expect("reuse summary");
    assert_eq!(summary.reused_observations, 1);
    assert_eq!(summary.fetched_observations, 2);
    assert_eq!(summary.api_calls, 2);
    assert!(
        snapshot
            .manifest
            .raw_objects
            .iter()
            .any(|raw| raw.id == raw_ref("raw-stored").id)
    );

    let duplicate = assemble_dataset(
        request.clone(),
        &[
            page(
                &request,
                0,
                observation(CandleInterval::H1, 0, "100", "105", "1", "raw-a"),
                "raw-a",
            ),
            page(
                &request,
                1,
                observation(CandleInterval::H1, 120, "100", "105", "1", "raw-b"),
                "raw-b",
            ),
            page(
                &request,
                2,
                observation(CandleInterval::H1, 60, "100", "105", "1", "raw-dup"),
                "raw-dup",
            ),
        ],
        &[],
        &reuse,
    )
    .expect("assemble duplicate against reused row");
    assert!(
        duplicate
            .manifest
            .quality_issues
            .iter()
            .any(|issue| issue.kind == QualityKind::DuplicateIdentical)
    );
    assert_eq!(duplicate.manifest.status, DatasetStatus::Ready);
}

fn collection_request(id: &str) -> CollectRequest {
    CollectRequest {
        request_id: RequestId::new(id).expect("request ID"),
        markets: vec![MarketId::parse_upbit("KRW-BTC").expect("market")],
        range: UtcRange::new(time(0), time(201)).expect("range"),
        data_resolution: CandleInterval::H1,
        warmup_bars: 0,
        completed_only: true,
    }
}

fn wire_page(start: i64, end: i64) -> Vec<u8> {
    let rows: Vec<_> = (start..end)
        .rev()
        .map(|hour| {
            let opened = time(hour)
                .0
                .naive_utc()
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            serde_json::json!({
                "market": "KRW-BTC",
                "candle_date_time_utc": opened,
                "opening_price": 100,
                "high_price": 110,
                "low_price": 90,
                "trade_price": 105,
                "candle_acc_trade_volume": 2,
                "candle_acc_trade_price": 210
            })
        })
        .collect();
    serde_json::to_vec(&rows).expect("serialize wire page")
}

fn page(
    request: &CollectRequest,
    page_index: u32,
    observation: CandleObservation,
    raw_seed: &str,
) -> CollectionPage {
    let raw = raw_ref(raw_seed);
    let mut observation = observation;
    observation.raw_object_ids = vec![raw.id.clone()];
    CollectionPage {
        request_id: request.request_id.clone(),
        market: request.markets[0].clone(),
        requested_to: request.range.end(),
        next_to: Some(request.range.start()),
        raw_object: raw,
        observations: vec![observation],
        page_index,
    }
}

fn observation(
    interval: CandleInterval,
    offset_minutes: i64,
    open: &str,
    close: &str,
    volume: &str,
    raw_seed: &str,
) -> CandleObservation {
    let open_time = UtcTimestamp(
        time(0)
            .0
            .checked_add_signed(chrono::Duration::minutes(offset_minutes))
            .expect("fixture timestamp"),
    );
    let candle = CandleRecord {
        market: "KRW-BTC".into(),
        interval,
        open_time_utc: open_time,
        close_time_utc: UtcTimestamp(
            open_time
                .0
                .checked_add_signed(interval.duration())
                .expect("fixture close"),
        ),
        open: price(open),
        high: price("200"),
        low: price("50"),
        close: price(close),
        volume: quantity(volume),
        quote_turnover: amount("210"),
        completed: true,
    };
    let digest = observation_digest(&candle).expect("observation digest");
    CandleObservation {
        id: ObservationId::from_seed(digest.as_str()),
        candle,
        content_digest: digest,
        raw_object_ids: vec![RawObjectId::from_seed(raw_seed)],
        constituent_ids: Vec::new(),
    }
}

fn raw_ref(seed: &str) -> RawObjectRef {
    RawObjectRef {
        id: RawObjectId::from_seed(seed),
        relative_path: format!("raw/{seed}"),
        source_url: format!("https://example.invalid/{seed}/KRW-BTC"),
        fetched_at: time(300),
        persisted_at: time(300),
        http_status: 200,
        remaining_req: None,
        raw_sha256: ContentHash::of_bytes(seed.as_bytes()),
        compressed_sha256: ContentHash::of_bytes(format!("gzip-{seed}").as_bytes()),
        raw_bytes: 1,
        compressed_bytes: 1,
        origin: MarketDataOrigin::SyntheticTestOnly,
    }
}

fn time(hour: i64) -> UtcTimestamp {
    UtcTimestamp(
        UtcTimestamp::parse_rfc3339("2024-01-01T00:00:00Z")
            .expect("base time")
            .0
            .checked_add_signed(chrono::Duration::hours(hour))
            .expect("fixture time"),
    )
}

fn price(value: &str) -> PriceKrw {
    PriceKrw::new(Decimal::from_str(value).expect("price decimal")).expect("price")
}

fn quantity(value: &str) -> AssetQuantity {
    AssetQuantity::new(Decimal::from_str(value).expect("quantity decimal")).expect("quantity")
}

fn amount(value: &str) -> QuoteAmount {
    QuoteAmount::new(Decimal::from_str(value).expect("amount decimal")).expect("amount")
}

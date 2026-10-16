use super::*;
use crate::contracts::{
    AdmissionStatus, EpisodeExitDetails, EpisodeId, EpisodeStatus, EventContext, FillId, MarkKind,
    MarketId, ModelAdmission, OrderId, PlanId, PlanRequest, QuoteAmount, RequestId, ResultQuery,
    Side, SignalId, SignedAmount, StrategyKind, UtcTimestamp, Weight,
};
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

pub(in crate::storage) fn encode_result_page(page: &crate::storage::ResultPage) -> Vec<u8> {
    match page {
        crate::storage::ResultPage::Signals(p) => {
            serde_json::to_vec(&("SIGNALS", p)).expect("encode signals page")
        }
        crate::storage::ResultPage::Orders(p) => {
            serde_json::to_vec(&("ORDERS", p)).expect("encode orders page")
        }
        crate::storage::ResultPage::OrderEvents(p) => {
            serde_json::to_vec(&("ORDER_EVENTS", p)).expect("encode order_events page")
        }
        crate::storage::ResultPage::Fills(p) => {
            serde_json::to_vec(&("FILLS", p)).expect("encode fills page")
        }
        crate::storage::ResultPage::Episodes(p) => {
            serde_json::to_vec(&("EPISODES", p)).expect("encode episodes page")
        }
        crate::storage::ResultPage::Equity(p) => {
            serde_json::to_vec(&("EQUITY", p)).expect("encode equity page")
        }
    }
}

pub(in crate::storage) struct TempRoot(pub(in crate::storage) std::path::PathBuf);

impl TempRoot {
    pub(in crate::storage) fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-sealed-read-{label}-{}-{}",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create sealed-read temp root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.0);
    }
}

pub(in crate::storage) struct ScaledRunConfig<'a> {
    pub(in crate::storage) run_id: &'a str,
    pub(in crate::storage) completed_models: usize,
    pub(in crate::storage) signals_per_model: usize,
    pub(in crate::storage) roundtrips_per_model: usize,
    pub(in crate::storage) marks_per_model: usize,
    pub(in crate::storage) include_blocked_model: bool,
}

pub(in crate::storage) struct SeededScaledRun {
    pub(in crate::storage) run_id: RunId,
    pub(in crate::storage) completed_models: Vec<ModelId>,
    pub(in crate::storage) blocked_model: Option<ModelId>,
}

pub(in crate::storage) struct TamperedChunkGuard {
    chunk_index: i64,
    file_path: PathBuf,
    original_compressed: Vec<u8>,
    original_sha256: String,
    original_event_count: i64,
    original_uncompressed_bytes: i64,
    original_compressed_bytes: i64,
}

impl TamperedChunkGuard {
    pub(in crate::storage) fn rewrite_lines(
        store: &Store,
        run_id: &RunId,
        chunk_index: u64,
        mutate: impl FnOnce(&mut Vec<String>),
    ) -> Self {
        let idx = u64_to_i64(chunk_index).expect("chunk index i64");
        let (rel_path, sha, count, uncomp, comp): (String, String, i64, i64, i64) = store
            .connection
            .query_row(
                "SELECT relative_path,sha256,event_count,uncompressed_bytes,compressed_bytes \
                 FROM run_ledger_chunks WHERE run_id=?1 AND chunk_index=?2",
                params![run_id.as_str(), idx],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .expect("query chunk metadata for rewrite");
        let file_path = store.raw_objects().root.join(&rel_path);
        let original_compressed = fs::read(&file_path).expect("read original chunk file");
        let decompressed = gunzip_bytes(&original_compressed).expect("gunzip original chunk");
        let text = String::from_utf8(decompressed).expect("chunk utf8");
        let mut lines: Vec<String> = text
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect();
        mutate(&mut lines);
        let mut payload = Vec::new();
        for line in &lines {
            payload.extend_from_slice(line.as_bytes());
            payload.push(b'\n');
        }
        let new_compressed = gzip_bytes(&payload).expect("re-gzip mutated chunk");
        let new_sha = ContentHash::of_bytes(&new_compressed);
        let new_count = i64::try_from(lines.len()).expect("new line count");
        let new_uncomp = i64::try_from(payload.len()).expect("new uncompressed bytes");
        let new_comp = i64::try_from(new_compressed.len()).expect("new compressed bytes");
        fs::write(&file_path, &new_compressed).expect("write mutated chunk");
        store
            .connection
            .execute(
                "UPDATE run_ledger_chunks \
                 SET sha256=?1,event_count=?2,uncompressed_bytes=?3,compressed_bytes=?4 \
                 WHERE run_id=?5 AND chunk_index=?6",
                params![
                    new_sha.as_str(),
                    new_count,
                    new_uncomp,
                    new_comp,
                    run_id.as_str(),
                    idx
                ],
            )
            .expect("update mutated chunk metadata");
        Self {
            chunk_index: idx,
            file_path,
            original_compressed,
            original_sha256: sha,
            original_event_count: count,
            original_uncompressed_bytes: uncomp,
            original_compressed_bytes: comp,
        }
    }

    fn corrupt_compressed_byte(store: &Store, run_id: &RunId, chunk_index: u64) -> Self {
        let idx = u64_to_i64(chunk_index).expect("chunk index i64");
        let (rel_path, sha, count, uncomp, comp): (String, String, i64, i64, i64) = store
            .connection
            .query_row(
                "SELECT relative_path,sha256,event_count,uncompressed_bytes,compressed_bytes \
                 FROM run_ledger_chunks WHERE run_id=?1 AND chunk_index=?2",
                params![run_id.as_str(), idx],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .expect("query chunk metadata for byte flip");
        let file_path = store.raw_objects().root.join(&rel_path);
        let original_compressed = fs::read(&file_path).expect("read original chunk file");
        let mut corrupted = original_compressed.clone();
        let mid = corrupted.len() / 2;
        corrupted[mid] ^= 0xFF;
        fs::write(&file_path, &corrupted).expect("write byte-corrupted chunk");
        Self {
            chunk_index: idx,
            file_path,
            original_compressed,
            original_sha256: sha,
            original_event_count: count,
            original_uncompressed_bytes: uncomp,
            original_compressed_bytes: comp,
        }
    }

    pub(in crate::storage) fn restore(self, store: &Store, run_id: &RunId) {
        fs::write(&self.file_path, &self.original_compressed)
            .expect("restore original chunk bytes");
        store
            .connection
            .execute(
                "UPDATE run_ledger_chunks \
                 SET sha256=?1,event_count=?2,uncompressed_bytes=?3,compressed_bytes=?4 \
                 WHERE run_id=?5 AND chunk_index=?6",
                params![
                    self.original_sha256,
                    self.original_event_count,
                    self.original_uncompressed_bytes,
                    self.original_compressed_bytes,
                    run_id.as_str(),
                    self.chunk_index
                ],
            )
            .expect("restore original chunk metadata");
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "single transaction fixture builder seeds consistent multi-model detail rows across all ledger sections"
)]
pub(in crate::storage) fn seed_scaled_multi_model_run(
    store: &mut Store,
    config: &ScaledRunConfig<'_>,
) -> SeededScaledRun {
    let base = crate::reporting::tests::multi_market_fixture();
    let base_model = &base.models[0];
    let base_signal = &base_model.signals[0];
    let base_order = &base_model.orders[0];
    let base_buy_fill = &base_model.fills[0];
    let base_sell_fill = &base_model.fills[1];
    let base_mark = &base_model.account_marks[0];
    let base_episode = &base_model.episodes[0];

    store
        .connection
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .expect("disable foreign keys for synthetic fixture");

    let run_id = RunId::new(config.run_id).expect("valid run id");
    let mut plan = base.plan.clone();
    plan.id = PlanId::new(format!("plan-{}", config.run_id)).expect("valid plan id");
    plan.admissions.clear();

    let mut completed_models = Vec::with_capacity(config.completed_models);
    for m_idx in 0..config.completed_models {
        let model_id = ModelId::new(format!("model-{m_idx:02}")).expect("model id");
        let market = MarketId::parse_upbit(if m_idx % 2 == 0 { "KRW-BTC" } else { "KRW-ETH" })
            .expect("market");
        plan.admissions.push(ModelAdmission {
            model_id: model_id.clone(),
            market,
            strategy: StrategyKind::BuyAndHold,
            policy_ref: None,
            status: AdmissionStatus::Eligible,
            reasons: Vec::new(),
        });
        completed_models.push(model_id);
    }

    let blocked_model = if config.include_blocked_model {
        let blocked_id = ModelId::new("model-blocked").expect("blocked model id");
        plan.admissions.push(ModelAdmission {
            model_id: blocked_id.clone(),
            market: MarketId::parse_upbit("KRW-BTC").expect("market"),
            strategy: StrategyKind::BuyAndHold,
            policy_ref: None,
            status: AdmissionStatus::BlockedData,
            reasons: vec!["DATA_QUALITY_BLOCKED".into()],
        });
        Some(blocked_id)
    } else {
        None
    };

    let mark_source_bar_ids = completed_models
        .iter()
        .enumerate()
        .map(|(m_idx, _)| {
            let market_code = plan.admissions[m_idx].market.code();
            base.datasets
                .iter()
                .flat_map(|dataset| dataset.observations.iter())
                .find(|observation| observation.candle.market.as_str() == market_code.as_str())
                .map(|observation| observation.id.clone())
                .expect("fixture contains a persisted candle for each model market")
        })
        .collect::<Vec<_>>();

    let plan_request = PlanRequest {
        request_id: RequestId::new(format!("req-plan-{}", config.run_id)).expect("plan request id"),
        spec: plan.spec.clone(),
    };
    let mut manifest = base.manifest.clone();
    manifest.run_id = run_id.clone();

    let base_time =
        UtcTimestamp::parse_rfc3339("2024-01-01T00:00:00Z").expect("base fixture timestamp");
    let strategy_json = serde_json::to_string(&base_model.strategy).expect("strategy json");
    let closed_exit_json =
        serde_json::to_string(&EpisodeExitDetails::default()).expect("exit details json");

    for dataset in &base.datasets {
        store
            .begin_collection(&dataset.manifest.request)
            .expect("register fixture collection");
    }
    let tx = store.connection.transaction().expect("begin input seed tx");
    for dataset in &base.datasets {
        let prepared = super::super::PreparedDatasetPublication {
            digests: crate::contracts::dataset_digests(dataset).expect("fixture dataset digests"),
        };
        super::super::insert_dataset_rows(&tx, dataset, &prepared, None)
            .expect("persist complete fixture dataset");
    }
    tx.commit().expect("commit fixture inputs");
    store
        .save_plan(&plan_request, &plan)
        .expect("persist validated fixture plan");
    let tx = store.connection.transaction().expect("begin fact seed tx");
    tx.execute(
        "INSERT INTO jobs(id,request_id,normalized_input_digest,payload_kind,payload_json,\
         current_attempt_number,current_status,created_at_ms) \
         VALUES (?1,?2,?3,'BACKTEST','{}',1,'COMPLETED',0)",
        params![
            manifest.job_id.as_str(),
            format!("req-job-{}", config.run_id),
            plan.input_digest.as_str(),
        ],
    )
    .expect("insert fixture job parent");
    tx.execute(
        "INSERT INTO job_attempts(id,job_id,attempt_number,input_digest,status,state_json,\
         progress_stage,committed_records,last_committed_event_seq,queued_at_ms,started_at_ms,ended_at_ms) \
         VALUES (?1,?2,1,?3,'COMPLETED','{}','completed',0,0,0,0,0)",
        params![
            manifest.attempt_id.as_str(),
            manifest.job_id.as_str(),
            plan.input_digest.as_str(),
        ],
    )
    .expect("insert fixture attempt parent");
    tx.execute(
        "INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,\
         last_committed_event_seq,semantic_digest,manifest_json,ledger_state) \
         VALUES (?1,?2,?3,?4,'COMPLETED',0,0,?5,?6,'DETAIL')",
        params![
            run_id.as_str(),
            manifest.job_id.as_str(),
            manifest.attempt_id.as_str(),
            plan.id.as_str(),
            base.semantic_digest.as_str(),
            serde_json::to_string(&manifest).expect("manifest json"),
        ],
    )
    .expect("insert run row");

    let mut next_seq = 1_u64;
    let mut last_seq_by_model = vec![0_u64; config.completed_models];

    {
        let mut ins_event = tx
            .prepare(
                "INSERT INTO run_events(run_id,event_seq,model_id,event_kind,\
                 accounting_event_time_ms,record_id) VALUES (?1,?2,?3,?4,?5,?6)",
            )
            .expect("prepare run_events");
        let mut ins_signal = tx
            .prepare(
                "INSERT INTO signals(signal_id,run_id,model_id,event_seq,market,\
                 signal_time_ms,decision_available_at_ms,valid_until_ms,record_json) \
                 VALUES (?1,?2,?3,?4,?5,?6,?6,?6,?7)",
            )
            .expect("prepare signals");
        let mut ins_order_event = tx
            .prepare(
                "INSERT INTO order_events(run_id,model_id,event_seq,order_id,status,\
                 effective_at_ms,record_json) VALUES (?1,?2,?3,?4,'FILLED',?5,?6)",
            )
            .expect("prepare order_events");
        let mut ins_order_identity = tx
            .prepare(
                "INSERT OR IGNORE INTO order_identities(run_id,model_id,order_id,parent_signal_id) \
                 VALUES (?1,?2,?3,?4)",
            )
            .expect("prepare order identities");
        let mut ins_order = tx
            .prepare(
                "INSERT INTO orders(run_id,model_id,order_id,final_status,event_seq,\
                 accounting_event_time_ms,record_json) VALUES (?1,?2,?3,'FILLED',?4,?5,?6)",
            )
            .expect("prepare orders");
        let mut ins_fill = tx
            .prepare(
                "INSERT INTO fills(fill_id,run_id,model_id,event_seq,order_id,episode_id,\
                 accounting_mark_seq,price_decimal,qty_decimal,notional_decimal,fee_decimal,\
                 source_bar_id,record_json,liquidity_source_bar_id,liquidity_source_close_time_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6,?4,'100','10',?7,?8,?9,?10,?11,?12)",
            )
            .expect("prepare fills");
        let mut ins_mark = tx
            .prepare(
                "INSERT INTO account_marks(run_id,model_id,event_seq,kind,cash_total_decimal,\
                 cash_free_decimal,cash_reserved_decimal,qty_decimal,price_basis_decimal,\
                 cumulative_fees_decimal,equity_decimal,source_bar_id,record_json) \
                 VALUES (?1,?2,?3,?4,'1000','1000','0','0','0','0',?5,?6,?7)",
            )
            .expect("prepare account_marks");

        for s_idx in 0..config.signals_per_model {
            let offset_mins = i64::try_from(s_idx).expect("signal idx");
            let at = UtcTimestamp(base_time.0 + chrono::Duration::minutes(offset_mins));
            let at_ms = at.0.timestamp_millis();
            for (m_idx, model_id) in completed_models.iter().enumerate() {
                let seq = next_seq;
                next_seq += 1;
                last_seq_by_model[m_idx] = seq;
                let market = &plan.admissions[m_idx].market;
                let sig_id =
                    SignalId::new(format!("sig-{m_idx:02}-{s_idx:04}")).expect("signal id");
                let mut signal = base_signal.clone();
                signal.context = EventContext {
                    run_id: run_id.clone(),
                    model_id: model_id.clone(),
                    market: market.clone(),
                    event_seq: seq,
                    accounting_event_time: at,
                };
                signal.signal_id = sig_id.clone();
                signal.signal_time = at;
                signal.decision_available_at = at;
                signal.valid_until = UtcTimestamp(at.0 + chrono::Duration::minutes(60));
                let json = serde_json::to_string(&signal).expect("signal json");
                let seq_i64 = u64_to_i64(seq).expect("seq i64");
                ins_event
                    .execute(params![
                        run_id.as_str(),
                        seq_i64,
                        model_id.as_str(),
                        "SIGNAL",
                        at_ms,
                        sig_id.as_str()
                    ])
                    .expect("insert signal event");
                ins_signal
                    .execute(params![
                        sig_id.as_str(),
                        run_id.as_str(),
                        model_id.as_str(),
                        seq_i64,
                        market.code(),
                        at_ms,
                        json
                    ])
                    .expect("insert signal row");
            }
        }

        for r_idx in 0..config.roundtrips_per_model {
            let offset_mins = i64::try_from(r_idx).expect("roundtrip idx");
            let at = UtcTimestamp(base_time.0 + chrono::Duration::minutes(offset_mins));
            let at_ms = at.0.timestamp_millis();
            for (m_idx, model_id) in completed_models.iter().enumerate() {
                let market = &plan.admissions[m_idx].market;
                let ep_id = EpisodeId::new(format!("ep-{m_idx:02}-closed")).expect("episode id");
                for (leg_idx, (side, fill_tpl)) in
                    [(Side::Buy, base_buy_fill), (Side::Sell, base_sell_fill)]
                        .into_iter()
                        .enumerate()
                {
                    let order_seq = next_seq;
                    next_seq += 1;
                    let fill_seq = next_seq;
                    next_seq += 1;
                    last_seq_by_model[m_idx] = fill_seq;

                    let ord_id = OrderId::new(format!("ord-{m_idx:02}-{r_idx:04}-{leg_idx}"))
                        .expect("order id");
                    let sig_id =
                        SignalId::new(format!("sig-{m_idx:02}-0000")).expect("parent signal id");
                    ins_order_identity
                        .execute(params![
                            run_id.as_str(),
                            model_id.as_str(),
                            ord_id.as_str(),
                            sig_id.as_str()
                        ])
                        .expect("insert order identity");
                    let mut order = base_order.clone();
                    order.context = EventContext {
                        run_id: run_id.clone(),
                        model_id: model_id.clone(),
                        market: market.clone(),
                        event_seq: order_seq,
                        accounting_event_time: at,
                    };
                    order.order_id = ord_id.clone();
                    order.parent_signal_id = sig_id;
                    order.episode_id = Some(ep_id.clone());
                    order.side = side;
                    order.effective_at = at;
                    order.created_at = at;
                    let order_json = serde_json::to_string(&order).expect("order json");
                    let order_seq_i64 = u64_to_i64(order_seq).expect("order seq");
                    ins_event
                        .execute(params![
                            run_id.as_str(),
                            order_seq_i64,
                            model_id.as_str(),
                            "ORDER_EVENT",
                            at_ms,
                            ord_id.as_str()
                        ])
                        .expect("insert order event");
                    ins_order_event
                        .execute(params![
                            run_id.as_str(),
                            model_id.as_str(),
                            order_seq_i64,
                            ord_id.as_str(),
                            at_ms,
                            &order_json
                        ])
                        .expect("insert order_events row");
                    ins_order
                        .execute(params![
                            run_id.as_str(),
                            model_id.as_str(),
                            ord_id.as_str(),
                            order_seq_i64,
                            at_ms,
                            &order_json
                        ])
                        .expect("insert orders row");

                    let fill_id = FillId::new(format!("fill-{m_idx:02}-{r_idx:04}-{leg_idx}"))
                        .expect("fill id");
                    let mut fill = fill_tpl.clone();
                    fill.context = EventContext {
                        run_id: run_id.clone(),
                        model_id: model_id.clone(),
                        market: market.clone(),
                        event_seq: fill_seq,
                        accounting_event_time: at,
                    };
                    fill.fill_id = fill_id.clone();
                    fill.order_id = ord_id;
                    fill.episode_id = ep_id.clone();
                    fill.side = side;
                    let notional_dec = Decimal::from(
                        1_000_u64
                            + u64::try_from(m_idx * 25 + r_idx * 3 + leg_idx * 40)
                                .expect("notional offset"),
                    );
                    let fee_dec = Decimal::new(
                        i64::try_from(100 + m_idx * 5 + r_idx + leg_idx * 10).expect("fee offset"),
                        2,
                    );
                    let price_cost_dec = Decimal::new(
                        i64::try_from(50 + m_idx * 7 + r_idx * 2).expect("price cost offset")
                            * if leg_idx == 0 { 1 } else { -1 },
                        2,
                    );
                    fill.notional = QuoteAmount::new(notional_dec).expect("notional");
                    fill.fee = QuoteAmount::new(fee_dec).expect("fee");
                    fill.price_cost_attribution =
                        SignedAmount::new(price_cost_dec).expect("price cost");
                    fill.accounting_mark_seq = fill_seq;
                    let fill_json = serde_json::to_string(&fill).expect("fill json");
                    let fill_seq_i64 = u64_to_i64(fill_seq).expect("fill seq");
                    let lsb_close_ms = fill.liquidity_source_close_time.0.timestamp_millis();
                    ins_event
                        .execute(params![
                            run_id.as_str(),
                            fill_seq_i64,
                            model_id.as_str(),
                            "FILL",
                            at_ms,
                            fill_id.as_str()
                        ])
                        .expect("insert fill event");
                    ins_fill
                        .execute(params![
                            fill_id.as_str(),
                            run_id.as_str(),
                            model_id.as_str(),
                            fill_seq_i64,
                            fill.order_id.as_str(),
                            ep_id.as_str(),
                            notional_dec.to_string(),
                            fee_dec.to_string(),
                            fill.source_bar_id.as_str(),
                            fill_json,
                            fill.liquidity_source_bar_id.as_str(),
                            lsb_close_ms
                        ])
                        .expect("insert fill row");
                }
            }
        }

        for k_idx in 0..config.marks_per_model {
            let offset_mins = i64::try_from(k_idx).expect("mark idx");
            let at = UtcTimestamp(base_time.0 + chrono::Duration::minutes(offset_mins));
            let at_ms = at.0.timestamp_millis();
            for (m_idx, model_id) in completed_models.iter().enumerate() {
                let seq = next_seq;
                next_seq += 1;
                last_seq_by_model[m_idx] = seq;
                let market = &plan.admissions[m_idx].market;
                let kind = if k_idx == 0 {
                    MarkKind::Initial
                } else if k_idx + 1 == config.marks_per_model {
                    MarkKind::Terminal
                } else {
                    MarkKind::ExecutionClose
                };
                let (equity_dec, drawdown_dec) = if k_idx == 0 {
                    (Decimal::from(2_000_u64), Decimal::ZERO)
                } else if k_idx + 1 == config.marks_per_model {
                    (
                        Decimal::from(
                            2_150_u64 + u64::try_from(m_idx * 37).expect("terminal offset"),
                        ),
                        Decimal::new(2, 2),
                    )
                } else {
                    match k_idx % 6 {
                        1 | 3 => (
                            Decimal::from(
                                2_400_u64 + u64::try_from(m_idx * 10).expect("peak offset"),
                            ),
                            Decimal::new(1, 2),
                        ),
                        2 | 4 => (
                            Decimal::from(
                                1_750_u64 + u64::try_from(m_idx * 10).expect("drawdown offset"),
                            ),
                            Decimal::new(35, 2),
                        ),
                        _ => (
                            Decimal::from(
                                2_010_u64
                                    + u64::try_from(m_idx * 10 + (k_idx % 5))
                                        .expect("interior offset"),
                            ),
                            Decimal::new(5, 2),
                        ),
                    }
                };
                let source_bar_id = &mark_source_bar_ids[m_idx];
                let mut mark = base_mark.clone();
                mark.context = EventContext {
                    run_id: run_id.clone(),
                    model_id: model_id.clone(),
                    market: market.clone(),
                    event_seq: seq,
                    accounting_event_time: at,
                };
                mark.kind = kind;
                mark.source_bar_id = source_bar_id.clone();
                mark.equity = QuoteAmount::new(equity_dec).expect("mark equity");
                mark.peak_equity = QuoteAmount::new(Decimal::from(2_500_u64)).expect("peak");
                mark.drawdown = Weight::new(drawdown_dec).expect("mark drawdown");
                let kind_str = match kind {
                    MarkKind::Initial => "INITIAL",
                    MarkKind::AfterFill => "AFTER_FILL",
                    MarkKind::ExecutionClose => "EXECUTION_CLOSE",
                    MarkKind::Terminal => "TERMINAL",
                };
                let mark_json = serde_json::to_string(&mark).expect("mark json");
                let seq_i64 = u64_to_i64(seq).expect("mark seq");
                ins_event
                    .execute(params![
                        run_id.as_str(),
                        seq_i64,
                        model_id.as_str(),
                        "ACCOUNT_MARK",
                        at_ms,
                        format!("mark-{seq}")
                    ])
                    .expect("insert mark event");
                ins_mark
                    .execute(params![
                        run_id.as_str(),
                        model_id.as_str(),
                        seq_i64,
                        kind_str,
                        equity_dec.to_string(),
                        source_bar_id.as_str(),
                        mark_json
                    ])
                    .expect("insert mark row");
            }
        }
    }

    for (m_idx, model_id) in completed_models.iter().enumerate() {
        let market = &plan.admissions[m_idx].market;
        let closed_ep_id =
            EpisodeId::new(format!("ep-{m_idx:02}-closed")).expect("closed episode id");
        let mut closed_ep = base_episode.clone();
        closed_ep.episode_id = closed_ep_id.clone();
        closed_ep.run_id = run_id.clone();
        closed_ep.model_id = model_id.clone();
        closed_ep.market = market.clone();
        closed_ep.status = EpisodeStatus::Closed;

        let open_ep_id = EpisodeId::new(format!("ep-{m_idx:02}-open")).expect("open episode id");
        let mut open_ep = base_episode.clone();
        open_ep.episode_id = open_ep_id.clone();
        open_ep.run_id = run_id.clone();
        open_ep.model_id = model_id.clone();
        open_ep.market = market.clone();
        open_ep.status = EpisodeStatus::Open;
        open_ep.closed_at = None;
        open_ep.exit_reason = None;

        for (ep_id, status_str, opened_ms, closed_ms, ep_record, exit_json) in [
            (
                &closed_ep_id,
                "CLOSED",
                base_time.0.timestamp_millis(),
                Some(base_time.0.timestamp_millis() + 60_000),
                &closed_ep,
                Some(closed_exit_json.as_str()),
            ),
            (
                &open_ep_id,
                "OPEN",
                base_time.0.timestamp_millis() + 120_000,
                None,
                &open_ep,
                None,
            ),
        ] {
            tx.execute(
                "INSERT INTO episode_identities(run_id,model_id,episode_id) VALUES (?1,?2,?3)",
                params![run_id.as_str(), model_id.as_str(), ep_id.as_str()],
            )
            .expect("insert episode identity");
            tx.execute(
                "INSERT INTO episodes(run_id,model_id,episode_id,status,opened_at_ms,\
                 closed_at_ms,net_realized_decimal,fees_decimal,record_json,exit_details_json) \
                 VALUES (?1,?2,?3,?4,?5,?6,'10','1',?7,?8)",
                params![
                    run_id.as_str(),
                    model_id.as_str(),
                    ep_id.as_str(),
                    status_str,
                    opened_ms,
                    closed_ms,
                    serde_json::to_string(ep_record).expect("episode json"),
                    exit_json
                ],
            )
            .expect("insert episode row");
        }

        let digest = ContentHash::of_bytes(model_id.as_str().as_bytes());
        tx.execute(
            "INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json,status,\
             status_reason,last_committed_event_seq,signal_count,order_event_count,fill_count,\
             account_mark_count,episode_count,fact_digest,final_digest) \
             VALUES (?1,?2,?3,'BUY_AND_HOLD',?4,'COMPLETED',NULL,?5,?6,?7,?8,?9,2,?10,?10)",
            params![
                run_id.as_str(),
                model_id.as_str(),
                market.code(),
                &strategy_json,
                u64_to_i64(last_seq_by_model[m_idx]).expect("model last seq"),
                i64::try_from(config.signals_per_model).expect("signal count"),
                i64::try_from(config.roundtrips_per_model * 2).expect("order event count"),
                i64::try_from(config.roundtrips_per_model * 2).expect("fill count"),
                i64::try_from(config.marks_per_model).expect("mark count"),
                digest.as_str()
            ],
        )
        .expect("insert completed run_models row");
    }

    if let Some(ref blocked_id) = blocked_model {
        tx.execute(
            "INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json,status,\
             status_reason,last_committed_event_seq,signal_count,order_event_count,fill_count,\
             account_mark_count,episode_count,fact_digest,final_digest) \
             VALUES (?1,?2,'KRW-BTC','BUY_AND_HOLD',?3,'BLOCKED_DATA','DATA_QUALITY_BLOCKED',0,0,0,0,0,0,NULL,NULL)",
            params![run_id.as_str(), blocked_id.as_str(), &strategy_json],
        )
        .expect("insert blocked run_models row");
    }

    let total_seq = next_seq.saturating_sub(1);
    tx.execute(
        "UPDATE runs SET last_committed_event_seq=?1 WHERE id=?2",
        params![
            u64_to_i64(total_seq).expect("total seq i64"),
            run_id.as_str()
        ],
    )
    .expect("update run last_committed_event_seq");

    tx.commit().expect("commit synthetic scaled run");
    let mut foreign_key_violation_count = 0_usize;
    let mut foreign_key_violation_examples = Vec::new();
    {
        let mut statement = store
            .connection
            .prepare("PRAGMA foreign_key_check")
            .expect("prepare fixture foreign key check");
        for violation in statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .expect("query fixture foreign keys")
        {
            let violation = violation.expect("read fixture foreign key violation");
            foreign_key_violation_count += 1;
            if foreign_key_violation_examples.len() < 10 {
                foreign_key_violation_examples.push(violation);
            }
        }
    }
    store
        .connection
        .execute_batch("PRAGMA foreign_keys=ON;")
        .expect("restore foreign keys after complete synthetic fixture");
    assert_eq!(
        foreign_key_violation_count,
        0,
        "scaled fixture has {foreign_key_violation_count} foreign key violations; first {}: {foreign_key_violation_examples:?}",
        foreign_key_violation_examples.len()
    );

    SeededScaledRun {
        run_id,
        completed_models,
        blocked_model,
    }
}

#[test]
fn sealed_line_envelope_compatibility_and_multichunk_verification() {
    let root = TempRoot::new("envelope-multichunk");
    let mut store = Store::open(&root.0).expect("open store");
    let seeded = seed_scaled_multi_model_run(
        &mut store,
        &ScaledRunConfig {
            run_id: "run-envelope-multichunk",
            completed_models: 2,
            signals_per_model: 20,
            roundtrips_per_model: 5,
            marks_per_model: 5_000,
            include_blocked_model: false,
        },
    );
    let stats = store
        .seal_and_compact_run_ledger(&seeded.run_id)
        .expect("seal and compact 2-chunk run");
    assert_eq!(stats.chunks, 2, "10,080 lines must span 2 sealed chunks");

    let opaque_record_line = r#"{"kind":"SIGNAL","model":"model-00","seq":1,"t":0,"r":"opaque"}"#;
    assert!(LedgerLine::from_line(opaque_record_line).is_ok());

    let deeply_nested_record = format!("{}null{}", "[".repeat(129), "]".repeat(129));
    let deeply_nested_line = format!(
        "{{\"kind\":\"SIGNAL\",\"model\":\"model-00\",\"seq\":1,\"t\":0,\"r\":{deeply_nested_record}}}"
    );
    assert!(matches!(
        LedgerLine::from_line(&deeply_nested_line),
        Err(LabError::Internal(message)) if message.contains("recursion limit exceeded")
    ));

    // Baseline & fallback compatibility: non-string lsb / non-int lst on a non-fill line
    // or duplicate JSON keys are tolerated by LedgerLine::from_line with None.
    let guard = TamperedChunkGuard::rewrite_lines(&store, &seeded.run_id, 0, |lines| {
        let line = lines
            .iter_mut()
            .find(|entry| entry.contains("\"kind\":\"SIGNAL\""))
            .expect("signal line in chunk 0");
        *line = line.replacen(
            "\"lsb\":null,\"lst\":null",
            "\"lsb\":12345,\"lst\":\"tolerant\",\"dup\":1,\"dup\":2",
            1,
        );
    });
    let loaded = store
        .load_run_ledger_lines(&seeded.run_id)
        .expect("tolerant lsb/lst must load cleanly")
        .expect("sealed lines present");
    assert_eq!(loaded.len(), usize::try_from(stats.events).expect("events"));
    assert!(
        store
            .query_result(&ResultQuery {
                run_id: seeded.run_id.clone(),
                model_id: Some(seeded.completed_models[0].clone()),
                section: LedgerSection::Signals,
                range: None,
                limit: 10,
                cursor: None,
            })
            .is_ok()
    );
    guard.restore(&store, &seeded.run_id);

    // Corrupting a line in Chunk 1 must reject even when querying Page 1 of Signals in Chunk 0.
    let bad_line_guard = TamperedChunkGuard::rewrite_lines(&store, &seeded.run_id, 1, |lines| {
        let last = lines.last_mut().expect("last line in chunk 1");
        *last =
            "{\"kind\":\"ACCOUNT_MARK\",\"model\":\"model-01\",\"seq\":999999,\"r\":{}}".to_owned();
    });
    assert!(matches!(
        store.query_result(&ResultQuery {
            run_id: seeded.run_id.clone(),
            model_id: Some(seeded.completed_models[0].clone()),
            section: LedgerSection::Signals,
            range: None,
            limit: 5,
            cursor: None,
        }),
        Err(LabError::DataCorrupt(_))
    ));
    bad_line_guard.restore(&store, &seeded.run_id);

    // Flipping a compressed byte in Chunk 1 must reject with InputHashMismatch even on Page 1.
    let bad_byte_guard = TamperedChunkGuard::corrupt_compressed_byte(&store, &seeded.run_id, 1);
    assert!(matches!(
        store.query_result(&ResultQuery {
            run_id: seeded.run_id.clone(),
            model_id: Some(seeded.completed_models[0].clone()),
            section: LedgerSection::Signals,
            range: None,
            limit: 5,
            cursor: None,
        }),
        Err(LabError::InputHashMismatch(_) | LabError::DataCorrupt(_))
    ));
    bad_byte_guard.restore(&store, &seeded.run_id);
}

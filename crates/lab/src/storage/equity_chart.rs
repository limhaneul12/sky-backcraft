//! Bounded, server-owned equity chart projection over the verified run ledger.

use super::ledger_seal::{records_of_kind, visit_run_ledger_source};
use super::{Store, json_error, sql_error};
use crate::contracts::{AccountMark, LabError, LedgerSection, ModelId, RunId};
use rusqlite::{OptionalExtension, params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const MIN_POINTS: u32 = 4;
const MAX_POINTS: u32 = 4_096;
// The HTTP transport owns a 512 KiB response cap. Keep headroom for the MCP
// content envelope instead of authorizing a larger storage payload.
const MAX_SERIALIZED_PROJECTION_BYTES: usize = 352 * 1024;

/// Exact authoritative marks selected for chart rendering.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EquityChartProjection {
    pub run_id: RunId,
    pub model_id: ModelId,
    pub total_count: u64,
    pub records: Vec<AccountMark>,
    pub sample_count: u64,
    pub sampled: bool,
}

impl Store {
    /// Read and validate one model's marks once, then deterministically retain
    /// chart extrema without calculating any financial metric.
    ///
    /// # Errors
    /// Rejects invalid capacity, unknown run/model identities, corrupt ledger
    /// rows or sealed chunks, and projections that exceed the transport budget.
    #[expect(
        clippy::too_many_lines,
        reason = "one projection read preserves generic, typed, domain and count error order across both storage paths"
    )]
    pub fn equity_chart_projection(
        &self,
        run_id: &RunId,
        model_id: &ModelId,
        max_points: u32,
    ) -> Result<EquityChartProjection, LabError> {
        if !(MIN_POINTS..=MAX_POINTS).contains(&max_points) {
            return Err(LabError::ResourceLimit(format!(
                "equity chart max_points must be {MIN_POINTS}..={MAX_POINTS}"
            )));
        }

        let expected_count = self
            .connection
            .query_row(
                "SELECT account_mark_count FROM run_models WHERE run_id=?1 AND model_id=?2",
                params![run_id.as_str(), model_id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| LabError::InvalidConfig("unknown result model".into()))?;
        let expected_count = u64::try_from(expected_count)
            .map_err(|_| LabError::DataCorrupt("negative account mark count".into()))?;

        let mut line_count = 0_u64;
        let mut previous_sequence = None;
        let mut noncanonical = false;
        let mut deserialize_error = None;
        let mut validation_error = None;
        let mut previous_mark_sequence = None;
        let mut sampler = usize::try_from(expected_count)
            .ok()
            .map(|count| StreamingExtremaSampler::new(count, max_points as usize));
        let sealed = visit_run_ledger_source(self, run_id, |line| {
            if line.section != LedgerSection::Equity || line.model_id != *model_id {
                return;
            }
            if previous_sequence.is_some_and(|previous| line.event_seq < previous) {
                noncanonical = true;
            }
            previous_sequence = Some(line.event_seq);
            if noncanonical {
                return;
            }
            line_count = line_count.saturating_add(1);
            if deserialize_error.is_some() {
                return;
            }
            let envelope_sequence = line.event_seq;
            let mark = match serde_json::from_value::<AccountMark>(line.record) {
                Ok(mark) => mark,
                Err(error) => {
                    deserialize_error = Some(json_error(error));
                    return;
                }
            };
            if validation_error.is_none() {
                validation_error = validate_next_mark(
                    run_id,
                    model_id,
                    &mark,
                    Some(envelope_sequence),
                    previous_mark_sequence,
                )
                .err();
                if validation_error.is_none() {
                    previous_mark_sequence = Some(mark.context.event_seq);
                }
            }
            if validation_error.is_none()
                && line_count <= expected_count
                && let Some(sampler) = sampler.as_mut()
            {
                sampler.push(mark);
            }
        })?;
        let (total_count, records) = if sealed {
            if noncanonical {
                let marks = load_compacted_marks_fallback(self, run_id, model_id)?;
                let total_count = u64::try_from(marks.len()).map_err(|_| {
                    LabError::ResourceLimit("account mark count exceeds u64".into())
                })?;
                if total_count != expected_count {
                    return Err(LabError::DataCorrupt(
                        "account mark ledger count differs from model projection".into(),
                    ));
                }
                (total_count, sample_extrema(marks, max_points as usize))
            } else {
                if let Some(error) = deserialize_error {
                    return Err(error);
                }
                if let Some(error) = validation_error {
                    return Err(error);
                }
                if line_count != expected_count {
                    return Err(LabError::DataCorrupt(
                        "account mark ledger count differs from model projection".into(),
                    ));
                }
                let sampler = sampler.ok_or_else(|| {
                    LabError::ResourceLimit("account mark count exceeds usize".into())
                })?;
                (line_count, sampler.finish())
            }
        } else {
            let marks = load_detail_marks(self, run_id, model_id)?;
            let total_count = u64::try_from(marks.len())
                .map_err(|_| LabError::ResourceLimit("account mark count exceeds u64".into()))?;
            if total_count != expected_count {
                return Err(LabError::DataCorrupt(
                    "account mark ledger count differs from model projection".into(),
                ));
            }
            (total_count, sample_extrema(marks, max_points as usize))
        };
        let sample_count = u64::try_from(records.len())
            .map_err(|_| LabError::ResourceLimit("equity sample count exceeds u64".into()))?;
        let projection = EquityChartProjection {
            run_id: run_id.clone(),
            model_id: model_id.clone(),
            total_count,
            sampled: sample_count < total_count,
            sample_count,
            records,
        };
        let encoded = serde_json::to_vec(&projection).map_err(json_error)?;
        if encoded.len() > MAX_SERIALIZED_PROJECTION_BYTES {
            return Err(LabError::ResourceLimit(format!(
                "equity chart projection exceeds {MAX_SERIALIZED_PROJECTION_BYTES} bytes"
            )));
        }
        Ok(projection)
    }
}

fn load_detail_marks(
    store: &Store,
    run_id: &RunId,
    model_id: &ModelId,
) -> Result<Vec<AccountMark>, LabError> {
    let mut statement = store
        .connection
        .prepare(
            "SELECT run_id,model_id,event_seq,source_bar_id,record_json \
             FROM account_marks WHERE run_id=?1 AND model_id=?2 ORDER BY event_seq",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![run_id.as_str(), model_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(sql_error)?;
    let mut marks = Vec::new();
    for row in rows {
        let (indexed_run, indexed_model, indexed_seq, indexed_bar, json) =
            row.map_err(sql_error)?;
        let mark: AccountMark = serde_json::from_str(&json).map_err(|error| {
            LabError::DataCorrupt(format!("malformed account mark record: {error}"))
        })?;
        let indexed_seq = u64::try_from(indexed_seq)
            .map_err(|_| LabError::DataCorrupt("negative account mark sequence".into()))?;
        if indexed_run != run_id.as_str()
            || indexed_model != model_id.as_str()
            || mark.context.run_id != *run_id
            || mark.context.model_id != *model_id
            || mark.context.event_seq != indexed_seq
            || mark.source_bar_id.as_str() != indexed_bar
        {
            return Err(LabError::DataCorrupt(
                "account mark record identity differs from indexed columns".into(),
            ));
        }
        marks.push(mark);
    }
    validate_marks(run_id, model_id, &marks, None)?;
    Ok(marks)
}

fn load_compacted_marks_fallback(
    store: &Store,
    run_id: &RunId,
    model_id: &ModelId,
) -> Result<Vec<AccountMark>, LabError> {
    let lines = store
        .load_run_ledger_lines(run_id)?
        .ok_or_else(|| LabError::DataCorrupt("compacted run has no ledger chunks".into()))?;
    let mut line_sequences = lines
        .iter()
        .filter(|line| line.section == LedgerSection::Equity && line.model_id == *model_id)
        .map(|line| line.event_seq)
        .collect::<Vec<_>>();
    line_sequences.sort_unstable();
    let marks = records_of_kind::<AccountMark>(&lines, model_id, LedgerSection::Equity)?;
    validate_marks(run_id, model_id, &marks, Some(&line_sequences))?;
    Ok(marks)
}

fn validate_marks(
    run_id: &RunId,
    model_id: &ModelId,
    marks: &[AccountMark],
    envelope_sequences: Option<&[u64]>,
) -> Result<(), LabError> {
    if envelope_sequences.is_some_and(|sequences| sequences.len() != marks.len()) {
        return Err(LabError::DataCorrupt(
            "sealed account mark envelope count mismatch".into(),
        ));
    }
    let mut previous = None;
    for (index, mark) in marks.iter().enumerate() {
        previous = Some(validate_next_mark(
            run_id,
            model_id,
            mark,
            envelope_sequences.map(|sequences| sequences[index]),
            previous,
        )?);
    }
    Ok(())
}

fn validate_next_mark(
    run_id: &RunId,
    model_id: &ModelId,
    mark: &AccountMark,
    envelope_sequence: Option<u64>,
    previous: Option<u64>,
) -> Result<u64, LabError> {
    if mark.context.run_id != *run_id || mark.context.model_id != *model_id {
        return Err(LabError::DataCorrupt(
            "account mark record has mismatched run/model identity".into(),
        ));
    }
    if envelope_sequence.is_some_and(|sequence| sequence != mark.context.event_seq) {
        return Err(LabError::DataCorrupt(
            "sealed account mark record differs from envelope sequence".into(),
        ));
    }
    if previous.is_some_and(|sequence| sequence >= mark.context.event_seq) {
        return Err(LabError::DataCorrupt(
            "account mark sequences are not strictly increasing".into(),
        ));
    }
    Ok(mark.context.event_seq)
}

struct StreamingExtremaSampler {
    total_len: usize,
    max_points: usize,
    seen: usize,
    selected: Vec<(usize, AccountMark)>,
    bucket_count: usize,
    interior_len: usize,
    bucket_index: usize,
    bucket_end: usize,
    peak: Option<(usize, AccountMark)>,
    drawdown: Option<(usize, AccountMark)>,
}

impl StreamingExtremaSampler {
    fn new(total_len: usize, max_points: usize) -> Self {
        let interior_budget = max_points - 2;
        let last_index = total_len.saturating_sub(1);
        let interior_len = last_index.saturating_sub(1);
        let bucket_count = if total_len <= max_points {
            0
        } else {
            interior_budget.div_ceil(2)
        };
        let bucket_end = interior_len
            .checked_div(bucket_count)
            .map_or(0, |end| 1 + end);
        Self {
            total_len,
            max_points,
            seen: 0,
            selected: Vec::with_capacity(max_points),
            bucket_count,
            interior_len,
            bucket_index: 0,
            bucket_end,
            peak: None,
            drawdown: None,
        }
    }

    fn push(&mut self, mark: AccountMark) {
        let index = self.seen;
        self.seen += 1;
        if self.total_len <= self.max_points {
            self.selected.push((index, mark));
            return;
        }
        if index == 0 {
            self.selected.push((index, mark));
            return;
        }
        if index + 1 == self.total_len {
            self.flush_bucket();
            self.selected.push((index, mark));
            return;
        }
        while index >= self.bucket_end {
            self.flush_bucket();
            self.bucket_index += 1;
            self.bucket_end = 1 + self.interior_len * (self.bucket_index + 1) / self.bucket_count;
        }
        if self
            .peak
            .as_ref()
            .is_none_or(|(_, peak)| mark.equity >= peak.equity)
        {
            self.peak = Some((index, mark.clone()));
        }
        if self
            .drawdown
            .as_ref()
            .is_none_or(|(_, drawdown)| mark.drawdown >= drawdown.drawdown)
        {
            self.drawdown = Some((index, mark));
        }
    }

    fn flush_bucket(&mut self) {
        let (Some((peak_index, peak)), Some((drawdown_index, drawdown))) =
            (self.peak.take(), self.drawdown.take())
        else {
            return;
        };
        let interior_budget = self.max_points - 2;
        let remaining = interior_budget - (self.selected.len() - 1);
        if remaining == 1 {
            self.selected.push((drawdown_index, drawdown));
        } else if peak_index <= drawdown_index {
            self.selected.push((peak_index, peak));
            if drawdown_index != peak_index {
                self.selected.push((drawdown_index, drawdown));
            }
        } else {
            self.selected.push((drawdown_index, drawdown));
            self.selected.push((peak_index, peak));
        }
    }

    fn finish(mut self) -> Vec<AccountMark> {
        if self.total_len > self.max_points {
            self.flush_bucket();
        }
        self.selected.sort_by_key(|(index, _)| *index);
        self.selected.dedup_by_key(|(index, _)| *index);
        self.selected.truncate(self.max_points);
        self.selected.into_iter().map(|(_, mark)| mark).collect()
    }
}

fn sample_extrema(marks: Vec<AccountMark>, max_points: usize) -> Vec<AccountMark> {
    if marks.len() <= max_points {
        return marks;
    }

    let last_index = marks.len() - 1;
    let interior_budget = max_points - 2;
    let bucket_count = interior_budget.div_ceil(2);
    let interior_len = last_index - 1;
    let mut selected = Vec::with_capacity(max_points);
    selected.push(0);

    for bucket in 0..bucket_count {
        let start = 1 + interior_len * bucket / bucket_count;
        let end = 1 + interior_len * (bucket + 1) / bucket_count;
        if start == end {
            continue;
        }
        let slice = &marks[start..end];
        let Some(peak) = slice
            .iter()
            .enumerate()
            .max_by_key(|(_, mark)| mark.equity)
            .map(|(index, _)| start + index)
        else {
            continue;
        };
        let Some(drawdown) = slice
            .iter()
            .enumerate()
            .max_by_key(|(_, mark)| mark.drawdown)
            .map(|(index, _)| start + index)
        else {
            continue;
        };
        let remaining = interior_budget - (selected.len() - 1);
        if remaining == 1 {
            selected.push(drawdown);
        } else if peak <= drawdown {
            selected.push(peak);
            if drawdown != peak {
                selected.push(drawdown);
            }
        } else {
            selected.push(drawdown);
            selected.push(peak);
        }
    }
    selected.push(last_index);
    selected.sort_unstable();
    selected.dedup();
    selected
        .into_iter()
        .take(max_points)
        .map(|index| marks[index].clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        AccountState, AssetQuantity, EventContext, MarkKind, MarketId, ObservationId, PriceKrw,
        QuoteAmount, SignedAmount, UtcTimestamp, Weight,
    };
    use rusqlite::params;
    use rust_decimal::Decimal;
    use std::fs;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempRoot(std::path::PathBuf);

    impl TempRoot {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "sky-backcraft-equity-chart-{}-{}",
                std::process::id(),
                TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).expect("create fixture root");
            Self(path)
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ignored = fs::remove_dir_all(&self.0);
        }
    }

    fn decimal(text: &str) -> Decimal {
        Decimal::from_str(text).expect("fixture decimal")
    }

    fn mark(sequence: u64, equity: &str, drawdown: &str) -> AccountMark {
        mark_for("model-selected", sequence, equity, drawdown)
    }

    fn mark_for(model_id: &str, sequence: u64, equity: &str, drawdown: &str) -> AccountMark {
        AccountMark {
            context: EventContext {
                run_id: RunId::new("run-equity-chart").expect("run id"),
                model_id: ModelId::new(model_id).expect("model id"),
                market: MarketId::parse_upbit("KRW-XRP").expect("market"),
                event_seq: sequence,
                accounting_event_time: UtcTimestamp::parse_rfc3339("2024-01-01T00:00:00Z")
                    .expect("time"),
            },
            kind: MarkKind::ExecutionClose,
            state: AccountState {
                cash_total: QuoteAmount::new(decimal("100")).expect("cash"),
                cash_free: QuoteAmount::new(decimal("100")).expect("cash"),
                cash_reserved: QuoteAmount::new(Decimal::ZERO).expect("cash"),
                qty: AssetQuantity::new(Decimal::ZERO).expect("qty"),
                price_basis: QuoteAmount::new(Decimal::ZERO).expect("basis"),
                gross_realized: SignedAmount::new(Decimal::ZERO).expect("pnl"),
                cumulative_fees: QuoteAmount::new(Decimal::ZERO).expect("fees"),
            },
            mark_price: PriceKrw::new(decimal("1")).expect("price"),
            source_bar_id: ObservationId::new(format!("bar-{sequence}")).expect("observation id"),
            position_value: QuoteAmount::new(Decimal::ZERO).expect("position"),
            gross_unrealized: SignedAmount::new(Decimal::ZERO).expect("pnl"),
            equity: QuoteAmount::new(decimal(equity)).expect("equity"),
            target_weight: Weight::new(Decimal::ZERO).expect("weight"),
            actual_weight: Weight::new(Decimal::ZERO).expect("weight"),
            peak_equity: QuoteAmount::new(decimal("200")).expect("peak"),
            drawdown: Weight::new(decimal(drawdown)).expect("drawdown"),
        }
    }

    fn fixture_store(root: &TempRoot) -> Store {
        let store = Store::open(&root.0).expect("open fixture store");
        store
            .connection
            .execute_batch("PRAGMA foreign_keys=OFF;")
            .expect("disable fixture foreign keys");
        store
            .connection
            .execute(
                "INSERT INTO runs(id,job_id,attempt_id,plan_id,state,created_at_ms,\
                 last_committed_event_seq,semantic_digest,manifest_json) \
                 VALUES (?1,'job-fixture','attempt-fixture','plan-fixture','COMPLETED',0,106,NULL,'{}')",
                ["run-equity-chart"],
            )
            .expect("insert fixture run");
        for model_id in ["model-selected", "model-other"] {
            store
                .connection
                .execute(
                    "INSERT INTO run_models(run_id,model_id,market,strategy_kind,strategy_json,\
                     status,last_committed_event_seq,account_mark_count) \
                     VALUES ('run-equity-chart',?1,'KRW-XRP','BUY_HOLD','{}','COMPLETED',106,6)",
                    [model_id],
                )
                .expect("insert fixture model");
        }
        for (model_id, base) in [("model-selected", 0_u64), ("model-other", 100_u64)] {
            for index in 1..=6_u64 {
                let sequence = base + index;
                let record = mark_for(
                    model_id,
                    sequence,
                    &(100 + index).to_string(),
                    if index == 3 { "0.4" } else { "0.1" },
                );
                store
                    .connection
                    .execute(
                        "INSERT INTO account_marks(run_id,model_id,event_seq,kind,\
                         cash_total_decimal,cash_free_decimal,cash_reserved_decimal,qty_decimal,\
                         price_basis_decimal,cumulative_fees_decimal,equity_decimal,source_bar_id,record_json) \
                         VALUES (?1,?2,?3,'EXECUTION_CLOSE','100','100','0','0','0','0',?4,?5,?6)",
                        params![
                            record.context.run_id.as_str(),
                            record.context.model_id.as_str(),
                            i64::try_from(sequence).expect("sequence"),
                            record.equity.get().to_string(),
                            record.source_bar_id.as_str(),
                            serde_json::to_string(&record).expect("serialize mark")
                        ],
                    )
                    .expect("insert fixture mark");
            }
        }
        store
    }

    #[test]
    fn sampler_preserves_first_last_bucket_peak_and_deepest_drawdown() {
        let marks = vec![
            mark(1, "100", "0"),
            mark(2, "120", "0"),
            mark(3, "80", "0.25"),
            mark(4, "110", "0.05"),
            mark(5, "150", "0"),
            mark(6, "70", "0.40"),
            mark(7, "105", "0.10"),
            mark(8, "101", "0.12"),
        ];

        let sampled = sample_extrema(marks, 6);
        let sequences = sampled
            .iter()
            .map(|mark| mark.context.event_seq)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2, 3, 5, 6, 8]);
        assert!(
            sampled
                .windows(2)
                .all(|pair| { pair[0].context.event_seq < pair[1].context.event_seq })
        );
    }

    #[test]
    fn validation_rejects_wrong_identity_and_envelope_sequence() {
        let run_id = RunId::new("run-equity-chart").expect("run id");
        let model_id = ModelId::new("model-selected").expect("model id");
        let mut wrong_model = mark(1, "100", "0");
        wrong_model.context.model_id = ModelId::new("model-other").expect("model id");
        assert!(matches!(
            validate_marks(&run_id, &model_id, &[wrong_model], None),
            Err(LabError::DataCorrupt(_))
        ));

        assert!(matches!(
            validate_marks(&run_id, &model_id, &[mark(1, "100", "0")], Some(&[2])),
            Err(LabError::DataCorrupt(_))
        ));
    }

    #[test]
    fn detail_projection_isolates_selected_model_and_preserves_envelope_identity() {
        let root = TempRoot::new();
        let store = fixture_store(&root);
        let run_id = RunId::new("run-equity-chart").expect("run id");
        let model_id = ModelId::new("model-selected").expect("model id");

        let projection = store
            .equity_chart_projection(&run_id, &model_id, 4)
            .expect("project chart");

        assert_eq!(projection.run_id, run_id);
        assert_eq!(projection.model_id, model_id);
        assert_eq!(projection.total_count, 6);
        assert_eq!(projection.sample_count, 4);
        assert!(projection.sampled);
        assert_eq!(projection.records[0].context.event_seq, 1);
        assert_eq!(projection.records[3].context.event_seq, 6);
        assert!(
            projection
                .records
                .iter()
                .all(|record| record.context.model_id == projection.model_id)
        );
    }

    #[test]
    fn detail_projection_rejects_malformed_and_tampered_records() {
        let root = TempRoot::new();
        let store = fixture_store(&root);
        let run_id = RunId::new("run-equity-chart").expect("run id");
        let model_id = ModelId::new("model-selected").expect("model id");

        store
            .connection
            .execute(
                "UPDATE account_marks SET record_json='{' WHERE run_id=?1 AND event_seq=1",
                [run_id.as_str()],
            )
            .expect("corrupt mark JSON");
        assert!(matches!(
            store.equity_chart_projection(&run_id, &model_id, 4),
            Err(LabError::DataCorrupt(_))
        ));

        let tampered = mark_for("model-other", 1, "101", "0.1");
        store
            .connection
            .execute(
                "UPDATE account_marks SET record_json=?1 WHERE run_id=?2 AND event_seq=1",
                params![
                    serde_json::to_string(&tampered).expect("serialize tampered mark"),
                    run_id.as_str()
                ],
            )
            .expect("tamper mark identity");
        assert!(matches!(
            store.equity_chart_projection(&run_id, &model_id, 4),
            Err(LabError::DataCorrupt(_))
        ));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "regression test verifies compacted/detail projections, count mismatch, extrema and unselected-mark corruption"
    )]
    fn compacted_projection_matches_detail_and_rejects_hidden_corruption() {
        use super::super::ledger_seal::tests::{
            ScaledRunConfig, TamperedChunkGuard, TempRoot as SharedTempRoot,
            seed_scaled_multi_model_run,
        };

        let root = SharedTempRoot::new("equity-chart-compacted");
        let mut store = Store::open(&root.0).expect("open store");
        let seeded = seed_scaled_multi_model_run(
            &mut store,
            &ScaledRunConfig {
                run_id: "run-compacted-chart",
                completed_models: 2,
                signals_per_model: 10,
                roundtrips_per_model: 5,
                marks_per_model: 30,
                include_blocked_model: false,
            },
        );
        let capacities = [4_u32, 6, 10, 32];
        let mut detail_projections = Vec::new();
        for model in &seeded.completed_models {
            for &cap in &capacities {
                let proj = store
                    .equity_chart_projection(&seeded.run_id, model, cap)
                    .expect("detail chart projection");
                detail_projections.push((
                    model.clone(),
                    cap,
                    serde_json::to_vec(&proj).expect("encode detail chart"),
                ));
            }
        }

        store
            .seal_and_compact_run_ledger(&seeded.run_id)
            .expect("seal and compact run");

        for (model, cap, expected_bytes) in &detail_projections {
            let proj = store
                .equity_chart_projection(&seeded.run_id, model, *cap)
                .expect("compacted chart projection");
            assert_eq!(
                &serde_json::to_vec(&proj).expect("encode compacted chart"),
                expected_bytes,
                "chart mismatch for {model} at max_points={cap}"
            );
        }

        let selected_model = &seeded.completed_models[0];
        let original_mark_count: i64 = store
            .connection
            .query_row(
                "SELECT account_mark_count FROM run_models WHERE run_id=?1 AND model_id=?2",
                params![seeded.run_id.as_str(), selected_model.as_str()],
                |row| row.get(0),
            )
            .expect("read selected model mark count");
        assert_eq!(original_mark_count, 30);
        store
            .connection
            .execute(
                "UPDATE run_models SET account_mark_count=10 WHERE run_id=?1 AND model_id=?2",
                params![seeded.run_id.as_str(), selected_model.as_str()],
            )
            .expect("lower projected mark count");
        assert!(matches!(
            store.equity_chart_projection(&seeded.run_id, selected_model, 4),
            Err(LabError::DataCorrupt(_))
        ));
        store
            .connection
            .execute(
                "UPDATE run_models SET account_mark_count=?1 WHERE run_id=?2 AND model_id=?3",
                params![
                    original_mark_count,
                    seeded.run_id.as_str(),
                    selected_model.as_str()
                ],
            )
            .expect("restore projected mark count");

        // Verify the 00:05 mark is unselected at max_points=6 and that corrupting
        // its inner record still fails closed.
        let unselected_mark_time =
            crate::contracts::UtcTimestamp::parse_rfc3339("2024-01-01T00:05:00Z")
                .expect("unselected mark timestamp");
        let unselected_mark_time_ms = unselected_mark_time.0.timestamp_millis();
        let unselected_mark_time_marker = format!("\"t\":{unselected_mark_time_ms}");
        let p6 = store
            .equity_chart_projection(&seeded.run_id, &seeded.completed_models[0], 6)
            .expect("compacted chart 6");
        assert!(
            !p6.records
                .iter()
                .any(|mark| mark.context.accounting_event_time == unselected_mark_time),
            "the 00:05 interior mark must not be selected at max_points=6"
        );

        let unselected_guard =
            TamperedChunkGuard::rewrite_lines(&store, &seeded.run_id, 0, |lines| {
                let line = lines
                    .iter_mut()
                    .find(|entry| {
                        entry.contains("\"kind\":\"ACCOUNT_MARK\"")
                            && entry.contains("\"model\":\"model-00\"")
                            && entry.contains(unselected_mark_time_marker.as_str())
                    })
                    .expect("unselected 00:05 model-00 mark line");
                let mut envelope: serde_json::Value =
                    serde_json::from_str(line).expect("unselected mark envelope JSON");
                envelope["r"]["context"]["model_id"] = serde_json::json!("model-01");
                *line = serde_json::to_string(&envelope).expect("encode mismatched mark");
            });
        assert!(matches!(
            store.equity_chart_projection(&seeded.run_id, &seeded.completed_models[0], 6),
            Err(LabError::DataCorrupt(_))
        ));
        unselected_guard.restore(&store, &seeded.run_id);

        // A negative hidden-line sequence invalidates the envelope while retaining its record.
        let hidden_model_guard =
            TamperedChunkGuard::rewrite_lines(&store, &seeded.run_id, 0, |lines| {
                let line = lines
                    .iter_mut()
                    .find(|entry| {
                        entry.contains("\"kind\":\"ACCOUNT_MARK\"")
                            && entry.contains("\"model\":\"model-01\"")
                            && entry.contains(unselected_mark_time_marker.as_str())
                    })
                    .expect("model-01 mark line at 00:05");
                let mut envelope: serde_json::Value =
                    serde_json::from_str(line).expect("hidden mark envelope JSON");
                envelope["seq"] = serde_json::json!(-1);
                *line = serde_json::to_string(&envelope).expect("encode bad hidden envelope");
            });
        assert!(matches!(
            store.equity_chart_projection(&seeded.run_id, &seeded.completed_models[0], 6),
            Err(LabError::DataCorrupt(_))
        ));
        hidden_model_guard.restore(&store, &seeded.run_id);
    }
}

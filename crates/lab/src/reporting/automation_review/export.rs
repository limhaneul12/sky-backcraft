use super::{PreparedReview, count, performance, prepare, saved_evaluation};
use crate::contracts::{
    AUTOMATION_REVIEW_CHUNK_BYTES, AutomationReviewExportChunk, AutomationReviewExportFile,
    AutomationReviewExportManifest, AutomationReviewFile, AutomationReviewSnapshot, ContentHash,
    LabError, MAX_AUTOMATION_REVIEW_BYTES,
};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io::{self, BufWriter, Write as IoWrite};

const HEX: &[u8; 16] = b"0123456789abcdef";

const FILES: [AutomationReviewFile; 6] = [
    AutomationReviewFile::DecisionsCsv,
    AutomationReviewFile::OrdersCsv,
    AutomationReviewFile::FillsCsv,
    AutomationReviewFile::ReportJson,
    AutomationReviewFile::ReportMarkdown,
    AutomationReviewFile::SnapshotJson,
];

struct HashWindowWriter {
    hasher: Sha256,
    bytes: usize,
    window_start: Option<u64>,
    window: Vec<u8>,
    exceeded_limit: bool,
    limit_message: &'static str,
}

struct RenderedFile {
    bytes: usize,
    sha256: ContentHash,
    window: Vec<u8>,
}

impl HashWindowWriter {
    fn new(window_start: Option<u64>, file: AutomationReviewFile) -> Self {
        Self {
            hasher: Sha256::new(),
            bytes: 0,
            window_start,
            window: Vec::new(),
            exceeded_limit: false,
            limit_message: match file {
                AutomationReviewFile::DecisionsCsv
                | AutomationReviewFile::OrdersCsv
                | AutomationReviewFile::FillsCsv => "automation review CSV exceeds byte cap",
                _ => "automation review export file exceeds byte cap",
            },
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), LabError> {
        self.write_all(bytes).map_err(|_| self.limit_error())
    }

    fn limit_error(&self) -> LabError {
        LabError::ResourceLimit(self.limit_message.into())
    }

    fn finish(self) -> Result<RenderedFile, LabError> {
        if self.exceeded_limit {
            return Err(self.limit_error());
        }
        Ok(RenderedFile {
            bytes: self.bytes,
            sha256: ContentHash::try_from(format!("{:x}", self.hasher.finalize()))?,
            window: self.window,
        })
    }
}

impl IoWrite for HashWindowWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let Some(next_bytes) = self.bytes.checked_add(bytes.len()) else {
            self.exceeded_limit = true;
            return Err(io::Error::other(
                "automation review export file exceeds byte cap",
            ));
        };
        if next_bytes > MAX_AUTOMATION_REVIEW_BYTES {
            self.exceeded_limit = true;
            return Err(io::Error::other(
                "automation review export file exceeds byte cap",
            ));
        }

        if let Some(window_start) = self.window_start {
            let input_start = u64::try_from(self.bytes).map_err(io::Error::other)?;
            let input_end = input_start
                .checked_add(u64::try_from(bytes.len()).map_err(io::Error::other)?)
                .ok_or_else(|| io::Error::other("automation review export offset overflow"))?;
            let window_end = window_start.saturating_add(
                u64::try_from(AUTOMATION_REVIEW_CHUNK_BYTES).map_err(io::Error::other)?,
            );
            let capture_start = input_start.max(window_start);
            let capture_end = input_end.min(window_end);
            if capture_start < capture_end {
                let start =
                    usize::try_from(capture_start - input_start).map_err(io::Error::other)?;
                let end = usize::try_from(capture_end - input_start).map_err(io::Error::other)?;
                self.window.extend_from_slice(&bytes[start..end]);
            }
        }
        self.hasher.update(bytes);
        self.bytes = next_bytes;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn file_metadata(file: AutomationReviewFile) -> (&'static str, &'static str) {
    match file {
        AutomationReviewFile::DecisionsCsv => ("decisions.csv", "text/csv; charset=utf-8"),
        AutomationReviewFile::OrdersCsv => ("orders.csv", "text/csv; charset=utf-8"),
        AutomationReviewFile::FillsCsv => ("fills.csv", "text/csv; charset=utf-8"),
        AutomationReviewFile::ReportJson => ("report.json", "application/json"),
        AutomationReviewFile::ReportMarkdown => ("report.md", "text/markdown; charset=utf-8"),
        AutomationReviewFile::SnapshotJson => ("snapshot.json", "application/json"),
    }
}

/// Describe six complete deterministic files without publishing private facts in research artifacts.
/// # Errors
/// Rejects corrupt/incomplete identities, overflow and explicit snapshot/file resource excess.
pub fn export_manifest(
    snapshot: &AutomationReviewSnapshot,
) -> Result<AutomationReviewExportManifest, LabError> {
    let prepared = prepare(snapshot)?;
    let report = performance(&prepared)?;
    let mut files = Vec::new();
    for file in FILES {
        let rendered = render_file(&prepared, &report, file, None)?;
        let (name, media) = file_metadata(file);
        files.push(AutomationReviewExportFile {
            file,
            file_name: name.into(),
            media_type: media.into(),
            bytes: count(rendered.bytes)?,
            sha256: rendered.sha256,
        });
    }
    Ok(AutomationReviewExportManifest {
        schema_version: "1.0".into(),
        session_id: prepared.snapshot.session.session_id.clone(),
        mode: prepared.snapshot.session.mode,
        snapshot_digest: prepared.digest,
        files,
    })
}

/// Read a private, hash-pinned bounded HEX chunk. Updated facts invalidate the read.
/// # Errors
/// Returns a hash mismatch on changed facts/files and rejects out-of-range offsets.
pub fn export_read(
    snapshot: &AutomationReviewSnapshot,
    file: AutomationReviewFile,
    offset: u64,
    expected_sha256: &ContentHash,
    expected_snapshot_digest: &ContentHash,
) -> Result<AutomationReviewExportChunk, LabError> {
    let prepared = prepare(snapshot)?;
    if &prepared.digest != expected_snapshot_digest {
        return Err(LabError::InputHashMismatch(
            "automation review snapshot changed; request a new manifest".into(),
        ));
    }
    let report = performance(&prepared)?;
    let rendered = render_file(&prepared, &report, file, Some(offset))?;
    if &rendered.sha256 != expected_sha256 {
        return Err(LabError::InputHashMismatch(
            "automation review file differs from manifest".into(),
        ));
    }
    let start = usize::try_from(offset).map_err(|_| {
        LabError::InvalidConfig("review export offset exceeds platform range".into())
    })?;
    if start > rendered.bytes {
        return Err(LabError::InvalidConfig(
            "review export offset exceeds file length".into(),
        ));
    }
    let end = start
        .saturating_add(AUTOMATION_REVIEW_CHUNK_BYTES)
        .min(rendered.bytes);
    let chunk = rendered.window;
    if chunk.len() != end - start {
        return Err(LabError::Internal(
            "automation review export window length differs from requested range".into(),
        ));
    }
    let (name, media) = file_metadata(file);
    let mut hex = String::with_capacity(chunk.len() * 2);
    for &byte in &chunk {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 15)]));
    }
    Ok(AutomationReviewExportChunk {
        session_id: prepared.snapshot.session.session_id.clone(),
        mode: prepared.snapshot.session.mode,
        snapshot_digest: prepared.digest,
        file,
        file_name: name.into(),
        media_type: media.into(),
        total_bytes: count(rendered.bytes)?,
        sha256: rendered.sha256,
        offset,
        bytes: count(chunk.len())?,
        chunk_sha256: ContentHash::of_bytes(&chunk),
        encoding: "HEX".into(),
        data_hex: hex,
        next_offset: if end < rendered.bytes {
            Some(count(end)?)
        } else {
            None
        },
    })
}

fn render_file(
    prepared: &PreparedReview,
    report: &crate::contracts::AutomationPerformanceReport,
    file: AutomationReviewFile,
    window_start: Option<u64>,
) -> Result<RenderedFile, LabError> {
    let mut writer = BufWriter::new(HashWindowWriter::new(window_start, file));
    let result = match file {
        AutomationReviewFile::DecisionsCsv => decisions_csv(prepared, &mut writer),
        AutomationReviewFile::OrdersCsv => orders_csv(prepared, &mut writer),
        AutomationReviewFile::FillsCsv => fills_csv(prepared, &mut writer),
        AutomationReviewFile::ReportJson => {
            serde_json::to_writer_pretty(&mut writer, report).map_err(Into::into)
        }
        AutomationReviewFile::SnapshotJson => {
            serde_json::to_writer_pretty(&mut writer, &prepared.snapshot).map_err(Into::into)
        }
        AutomationReviewFile::ReportMarkdown => {
            let markdown = markdown(report)?;
            writer.get_mut().write_bytes(markdown.as_bytes())
        }
    };
    if let Err(error) = result {
        if writer.get_ref().exceeded_limit {
            return Err(writer.get_ref().limit_error());
        }
        return Err(error);
    }
    if let Err(error) = writer.flush() {
        if writer.get_ref().exceeded_limit {
            return Err(writer.get_ref().limit_error());
        }
        return Err(LabError::Internal(format!(
            "flush automation review export: {error}"
        )));
    }
    let sink = match writer.into_inner() {
        Ok(sink) => sink,
        Err(error) => {
            let message = error.error().to_string();
            let writer = error.into_inner();
            if writer.get_ref().exceeded_limit {
                return Err(writer.get_ref().limit_error());
            }
            return Err(LabError::Internal(format!(
                "finish automation review export: {message}"
            )));
        }
    };
    sink.finish()
}

fn csv_row(out: &mut BufWriter<HashWindowWriter>, fields: &[String]) -> Result<(), LabError> {
    for (index, field) in fields.iter().enumerate() {
        if index != 0 {
            write_buffered_bytes(out, b",")?;
        }
        write_buffered_bytes(out, b"\"")?;
        // Keep exact signed decimal fields numeric, but neutralize spreadsheet formulas in text.
        let first = field.trim_start().chars().next();
        if (first.is_some_and(|c| matches!(c, '=' | '+' | '-' | '@'))
            && rust_decimal::Decimal::from_str_exact(field).is_err())
            || field.starts_with(['\t', '\r', '\n'])
        {
            write_buffered_bytes(out, b"'")?;
        }
        let bytes = field.as_bytes();
        let mut start = 0;
        for (index, byte) in bytes.iter().enumerate() {
            if *byte == b'"' {
                write_buffered_bytes(out, &bytes[start..=index])?;
                write_buffered_bytes(out, b"\"")?;
                start = index + 1;
            }
        }
        write_buffered_bytes(out, &bytes[start..])?;
        write_buffered_bytes(out, b"\"")?;
    }
    write_buffered_bytes(out, b"\r\n")
}

fn write_buffered_bytes(
    out: &mut BufWriter<HashWindowWriter>,
    bytes: &[u8],
) -> Result<(), LabError> {
    out.write_all(bytes)
        .map_err(|error| LabError::Internal(format!("write automation review export: {error}")))
}

fn text<T: serde::Serialize>(value: &T) -> Result<String, LabError> {
    Ok(serde_json::to_string(value)?)
}
fn mode(prepared: &PreparedReview) -> Result<String, LabError> {
    Ok(text(&prepared.snapshot.session.mode)?
        .trim_matches('"')
        .into())
}

fn decisions_csv(
    p: &PreparedReview,
    out: &mut BufWriter<HashWindowWriter>,
) -> Result<(), LabError> {
    csv_row(
        out,
        &[
            "session_id",
            "mode",
            "policy_id",
            "revision_id",
            "intent_id",
            "observation_id",
            "decision_time_utc",
            "recorded_at_utc",
            "action",
            "state",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "reasons",
            "indicators_json",
            "policy_trace_json",
            "evaluation_json",
            "evaluation_digest",
        ]
        .map(str::to_owned),
    )?;
    let s = &p.snapshot.session;
    for d in &p.snapshot.decisions {
        let c = &d.candle.candle;
        let e = saved_evaluation(d);
        let reasons = e
            .get("reasons")
            .and_then(serde_json::Value::as_array)
            .map_or_else(
                || {
                    e.get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("SAVED_REASON_NOT_AVAILABLE")
                        .into()
                },
                |v| {
                    v.iter()
                        .map(|r| r.as_str().map_or_else(|| r.to_string(), str::to_owned))
                        .collect::<Vec<_>>()
                        .join("; ")
                },
            );
        csv_row(
            out,
            &[
                s.session_id.to_string(),
                mode(p)?,
                s.policy_ref.policy_id.to_string(),
                s.policy_ref.revision_id.to_string(),
                d.intent_id.to_string(),
                d.observation_id.to_string(),
                d.decision_time_utc.to_rfc3339(),
                d.recorded_at_utc.to_rfc3339(),
                text(&d.action)?.trim_matches('"').into(),
                d.state.clone(),
                c.open.get().to_string(),
                c.high.get().to_string(),
                c.low.get().to_string(),
                c.close.get().to_string(),
                c.volume.get().to_string(),
                reasons,
                text(e.get("indicators").unwrap_or(&serde_json::Value::Null))?,
                text(e.get("policy_trace").unwrap_or(&serde_json::Value::Null))?,
                text(&d.evaluation)?,
                d.evaluation_digest.to_string(),
            ],
        )?;
    }
    Ok(())
}

fn orders_csv(p: &PreparedReview, out: &mut BufWriter<HashWindowWriter>) -> Result<(), LabError> {
    csv_row(
        out,
        &[
            "session_id",
            "mode",
            "client_identifier",
            "intent_id",
            "venue_order_uuid",
            "market",
            "side",
            "state",
            "requested_notional_krw",
            "decision_mark_krw",
            "executed_volume",
            "paid_fee_krw",
            "trade_count",
            "created_at_utc",
            "updated_at_utc",
            "venue_created_at_utc",
        ]
        .map(str::to_owned),
    )?;
    for row in &p.snapshot.orders {
        let o = &row.order;
        csv_row(
            out,
            &[
                p.snapshot.session.session_id.to_string(),
                mode(p)?,
                o.client_identifier.to_string(),
                o.intent_id.to_string(),
                o.venue_order_uuid.clone().unwrap_or_default(),
                o.market.code(),
                text(&o.side)?.trim_matches('"').into(),
                text(&o.state)?.trim_matches('"').into(),
                o.requested_notional_krw.get().to_string(),
                row.decision_mark_krw.get().to_string(),
                o.executed_volume.get().to_string(),
                o.paid_fee_krw.get().to_string(),
                o.trade_count.to_string(),
                o.created_at_utc.to_rfc3339(),
                o.updated_at_utc.to_rfc3339(),
                row.venue_created_at_utc
                    .map(crate::contracts::UtcTimestamp::to_rfc3339)
                    .unwrap_or_default(),
            ],
        )?;
    }
    Ok(())
}

fn fills_csv(p: &PreparedReview, out: &mut BufWriter<HashWindowWriter>) -> Result<(), LabError> {
    csv_row(
        out,
        &[
            "session_id",
            "mode",
            "trade_uuid",
            "client_identifier",
            "market",
            "side",
            "price_krw",
            "volume",
            "funds_krw",
            "stored_trade_time_utc",
            "recorded_at_utc",
            "source_digest",
        ]
        .map(str::to_owned),
    )?;
    for row in &p.snapshot.fills {
        let f = &row.fill;
        csv_row(
            out,
            &[
                p.snapshot.session.session_id.to_string(),
                mode(p)?,
                f.trade_uuid.clone(),
                f.client_identifier.to_string(),
                f.market.code(),
                text(&f.side)?.trim_matches('"').into(),
                f.price_krw.get().to_string(),
                f.volume.get().to_string(),
                f.funds_krw.get().to_string(),
                row.stored_trade_time_utc.to_rfc3339(),
                row.recorded_at_utc.to_rfc3339(),
                row.source_digest.to_string(),
            ],
        )?;
    }
    Ok(())
}

fn metric_text(metric: &crate::contracts::AutomationReviewMetric) -> String {
    metric.value.map_or_else(
        || {
            format!(
                "UNAVAILABLE ({})",
                metric.unavailable_reason.as_deref().unwrap_or("UNKNOWN")
            )
        },
        |v| v.get().to_string(),
    )
}

fn markdown(r: &crate::contracts::AutomationPerformanceReport) -> Result<String, LabError> {
    let mut text = format!(
        "# Strategy automation performance\n\nMode: {:?}\n\nSession: `{}`\n\nPolicy: `{}` / `{}`\n\nSnapshot: `{}` at {}\n\n| Metric | Value |\n| --- | --- |\n| Decisions | {} |\n| Replay warmup records (excluded from signals) | {} |\n| Orders | {} |\n| Fills | {} |\n| Buy funds KRW | {} |\n| Sell funds KRW | {} |\n| Order fees KRW | {} |\n| Net quote cashflow KRW (not P&L) | {} |\n| Realized P&L KRW | {} |\n| Unrealized P&L KRW | {} |\n| Total P&L KRW | {} |\n| Session return ratio | {} |\n| Session drawdown ratio | {} |\n\n## Methodology\n\n{}\n\nFees are reported once per order in orders.csv; do not sum an order fee per individual fill. UTC stored trade time and local receipt time are separate. Empty fields are unavailable, not zero. Monetary CSV values are exact decimal text; import as text when a spreadsheet's numeric precision would round them.\n\n## Coverage\n\n{}\n",
        r.session.mode,
        r.session.session_id,
        r.session.policy_ref.policy_id,
        r.session.policy_ref.revision_id,
        r.snapshot_digest,
        r.snapshot_at_utc.to_rfc3339(),
        r.decision_count,
        r.warmup_record_count,
        r.order_count,
        r.fill_count,
        r.buy_funds_krw.get(),
        r.sell_funds_krw.get(),
        r.paid_fees_krw.get(),
        r.net_quote_cashflow_krw.get(),
        metric_text(&r.realized_pnl_krw),
        metric_text(&r.unrealized_pnl_krw),
        metric_text(&r.total_pnl_krw),
        metric_text(&r.return_ratio),
        metric_text(&r.max_drawdown_ratio),
        r.methodology,
        if r.warnings.is_empty() {
            "No additional recorded coverage warnings.".into()
        } else {
            r.warnings.join("\n\n")
        }
    );
    text.push_str("\n## Evidence references\n\n");
    match (&r.mark_observation_id, &r.mark_content_digest, r.mark_at_utc) {
        (Some(id), Some(digest), Some(time)) => write!(text,"Saved completed mark observation: `{id}`\n\nCandle content SHA-256: `{digest}`\n\nMark close time: {}\n", time.to_rfc3339()).map_err(|_|LabError::Internal("report formatting failed".into()))?,
        _ => text.push_str("Mark observation: unavailable or no open position; no current price was substituted.\n"),
    }
    if let Some(opening) = &r.opening_inventory {
        write!(text,"\nOpening inventory reference: account snapshot {} ({:?}) at {}; base quantity {}; balances SHA-256 `{}`.\n",opening.snapshot_sequence,opening.source_mode,opening.observed_at_utc.to_rfc3339(),opening.base_quantity.get(),opening.balances_digest).map_err(|_|LabError::Internal("report formatting failed".into()))?;
    } else {
        text.push_str("\nOpening inventory reference: unavailable; no zero position is inferred for executed trades.\n");
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::{HashWindowWriter, MAX_AUTOMATION_REVIEW_BYTES};
    use crate::contracts::{AutomationReviewFile, LabError};

    #[test]
    fn hash_window_sink_rejects_the_first_byte_over_the_file_cap() {
        const BUFFER_BYTES: usize = 32 * 1024;
        let buffer = vec![b'x'; BUFFER_BYTES];
        let mut writer = HashWindowWriter::new(None, AutomationReviewFile::SnapshotJson);
        let full_buffers = MAX_AUTOMATION_REVIEW_BYTES / BUFFER_BYTES;
        let remainder = MAX_AUTOMATION_REVIEW_BYTES % BUFFER_BYTES;

        for _ in 0..full_buffers {
            writer
                .write_bytes(&buffer)
                .expect("bounded buffer stays within file cap");
        }
        if remainder > 0 {
            writer
                .write_bytes(&buffer[..remainder])
                .expect("final partial buffer reaches file cap");
        }

        assert!(matches!(
            writer.write_bytes(b"x"),
            Err(LabError::ResourceLimit(_))
        ));
    }
}

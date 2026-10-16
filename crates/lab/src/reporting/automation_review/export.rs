use super::{PreparedReview, count, performance, prepare, saved_evaluation};
use crate::contracts::{
    AUTOMATION_REVIEW_CHUNK_BYTES, AutomationReviewExportChunk, AutomationReviewExportFile,
    AutomationReviewExportManifest, AutomationReviewFile, AutomationReviewSnapshot, ContentHash,
    LabError, MAX_AUTOMATION_REVIEW_BYTES,
};
use std::fmt::Write;

const HEX: &[u8; 16] = b"0123456789abcdef";

const FILES: [AutomationReviewFile; 6] = [
    AutomationReviewFile::DecisionsCsv,
    AutomationReviewFile::OrdersCsv,
    AutomationReviewFile::FillsCsv,
    AutomationReviewFile::ReportJson,
    AutomationReviewFile::ReportMarkdown,
    AutomationReviewFile::SnapshotJson,
];

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
        let data = render(&prepared, &report, file)?;
        let (name, media) = file_metadata(file);
        files.push(AutomationReviewExportFile {
            file,
            file_name: name.into(),
            media_type: media.into(),
            bytes: count(data.len())?,
            sha256: ContentHash::of_bytes(&data),
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
    let data = render(&prepared, &report, file)?;
    let hash = ContentHash::of_bytes(&data);
    if &hash != expected_sha256 {
        return Err(LabError::InputHashMismatch(
            "automation review file differs from manifest".into(),
        ));
    }
    let start = usize::try_from(offset).map_err(|_| {
        LabError::InvalidConfig("review export offset exceeds platform range".into())
    })?;
    if start > data.len() {
        return Err(LabError::InvalidConfig(
            "review export offset exceeds file length".into(),
        ));
    }
    let end = start
        .saturating_add(AUTOMATION_REVIEW_CHUNK_BYTES)
        .min(data.len());
    let chunk = &data[start..end];
    let (name, media) = file_metadata(file);
    let mut hex = String::with_capacity(chunk.len() * 2);
    for byte in chunk {
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
        total_bytes: count(data.len())?,
        sha256: hash,
        offset,
        bytes: count(chunk.len())?,
        chunk_sha256: ContentHash::of_bytes(chunk),
        encoding: "HEX".into(),
        data_hex: hex,
        next_offset: if end < data.len() {
            Some(count(end)?)
        } else {
            None
        },
    })
}

fn render(
    prepared: &PreparedReview,
    report: &crate::contracts::AutomationPerformanceReport,
    file: AutomationReviewFile,
) -> Result<Vec<u8>, LabError> {
    let bytes = match file {
        AutomationReviewFile::DecisionsCsv => decisions_csv(prepared)?,
        AutomationReviewFile::OrdersCsv => orders_csv(prepared)?,
        AutomationReviewFile::FillsCsv => fills_csv(prepared)?,
        AutomationReviewFile::ReportJson => serde_json::to_vec_pretty(report)?,
        AutomationReviewFile::SnapshotJson => serde_json::to_vec_pretty(&prepared.snapshot)?,
        AutomationReviewFile::ReportMarkdown => markdown(report)?.into_bytes(),
    };
    if bytes.len() > MAX_AUTOMATION_REVIEW_BYTES {
        return Err(LabError::ResourceLimit(
            "automation review export file exceeds byte cap".into(),
        ));
    }
    Ok(bytes)
}

fn csv_row(out: &mut String, fields: &[String]) -> Result<(), LabError> {
    for (index, field) in fields.iter().enumerate() {
        if index != 0 {
            out.push(',');
        }
        out.push('"');
        // Keep exact signed decimal fields numeric, but neutralize spreadsheet formulas in text.
        let first = field.trim_start().chars().next();
        if (first.is_some_and(|c| matches!(c, '=' | '+' | '-' | '@'))
            && rust_decimal::Decimal::from_str_exact(field).is_err())
            || field.starts_with(['\t', '\r', '\n'])
        {
            out.push('\'');
        }
        for ch in field.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
    }
    out.push_str("\r\n");
    if out.len() > MAX_AUTOMATION_REVIEW_BYTES {
        return Err(LabError::ResourceLimit(
            "automation review CSV exceeds byte cap".into(),
        ));
    }
    Ok(())
}

fn text<T: serde::Serialize>(value: &T) -> Result<String, LabError> {
    Ok(serde_json::to_string(value)?)
}
fn mode(prepared: &PreparedReview) -> Result<String, LabError> {
    Ok(text(&prepared.snapshot.session.mode)?
        .trim_matches('"')
        .into())
}

fn decisions_csv(p: &PreparedReview) -> Result<Vec<u8>, LabError> {
    let mut out = String::new();
    csv_row(
        &mut out,
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
            &mut out,
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
    Ok(out.into_bytes())
}

fn orders_csv(p: &PreparedReview) -> Result<Vec<u8>, LabError> {
    let mut out = String::new();
    csv_row(
        &mut out,
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
            &mut out,
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
    Ok(out.into_bytes())
}

fn fills_csv(p: &PreparedReview) -> Result<Vec<u8>, LabError> {
    let mut out = String::new();
    csv_row(
        &mut out,
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
            &mut out,
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
    Ok(out.into_bytes())
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

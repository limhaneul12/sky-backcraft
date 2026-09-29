//! Portable F08 review, export, and independent verification.
//!
//! Verification deliberately depends only on contracts and exact decimal
//! arithmetic. Engine replay belongs in a separate adapter once the engine API
//! is stable; it must never be confused with independent accounting proof.

pub mod export;
pub mod metrics;
pub mod verify;

pub use export::{
    ExportManifest, ExportResult, ExportScope, ReadExportResult, ReplayQualification, export_run,
    read_export_package,
};
pub use metrics::{ReviewPayload, build_review};
pub use verify::{semantic_digest, verify_run};

use crate::contracts::{
    EpisodeExitDetails, EpisodeRecord, EpisodeStatus, FillRecord, LabError, OrderRecord,
    ReasonCode, Side, SignalRecord, SignedAmount,
};

/// Convert the immutable signed per-base-unit fill diagnostic to signed KRW.
///
/// The raw field is already embedded in `fill.price`; this helper is presentation-only and must
/// never debit cash or `PnL`.
///
/// # Errors
/// Returns a resource error when exact decimal multiplication overflows.
pub fn fill_price_cost_quote(fill: &FillRecord) -> Result<SignedAmount, LabError> {
    let amount = fill
        .price_cost_attribution
        .get()
        .checked_mul(fill.qty.get())
        .ok_or_else(|| LabError::ResourceLimit("fill price-cost quote amount overflow".into()))?;
    SignedAmount::new(amount)
}

/// Resolve exact closing execution and strategy reasons through immutable fact links.
///
/// # Errors
/// Rejects missing, mismatched, or non-closing fact links.
pub fn episode_exit_details(
    episode: &EpisodeRecord,
    closing: Option<(&FillRecord, &OrderRecord, &SignalRecord)>,
) -> Result<EpisodeExitDetails, LabError> {
    if episode.status == EpisodeStatus::Open {
        if episode.closed_at.is_some() || episode.exit_reason.is_some() || closing.is_some() {
            return Err(LabError::AccountingInvariant(
                "open episode has closing execution facts".into(),
            ));
        }
        return Ok(EpisodeExitDetails::default());
    }

    let (fill, order, signal) = closing.ok_or_else(|| {
        LabError::AccountingInvariant("closed episode lacks linked closing facts".into())
    })?;
    let expected_execution = if fill.artificial_terminal_exit {
        ReasonCode::ArtificialTerminalExit
    } else {
        ReasonCode::MarketFilled
    };
    let links_valid = episode.fill_ids.last() == Some(&fill.fill_id)
        && episode.order_ids.last() == Some(&order.order_id)
        && fill.episode_id == episode.episode_id
        && fill.order_id == order.order_id
        && order.parent_signal_id == signal.signal_id
        && fill.side == Side::Sell
        && episode.closed_at == Some(fill.context.accounting_event_time)
        && episode.exit_reason == Some(expected_execution);
    if !links_valid {
        return Err(LabError::AccountingInvariant(
            "episode closing fill/order/signal links or execution result mismatch".into(),
        ));
    }
    Ok(EpisodeExitDetails {
        execution_result: Some(expected_execution),
        closing_signal_id: Some(signal.signal_id.clone()),
        strategy_exit_reasons: if fill.artificial_terminal_exit {
            Vec::new()
        } else {
            signal.reasons.clone()
        },
    })
}

#[cfg(test)]
pub(crate) mod tests;

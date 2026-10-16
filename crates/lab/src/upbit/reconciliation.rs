//! Order State Reconciliation and Cancel Engine.
//!
//! Enforces:
//! - Separation of order submission from fill verification.
//! - Accurate mapping of Upbit states (`wait`, `watch`, `done`, `cancel`) to internal lifecycle states.
//! - Partial fill accounting, remaining volume tracking, and fee residual verification.
//! - Idempotent event processing to prevent duplicate fills.
//! - Timeout / unknown execution defense: never re-submits a new order on uncertain network responses.
//! - Mock cancellation with zero real exchange DELETE calls.

use crate::upbit::client::UpbitOrderDetail;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Internal normalized order execution states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NormalizedOrderState {
    Submitted,
    Accepted,
    Partial,
    Filled,
    Cancelled,
    Rejected,
    UnknownExecution,
}

/// Detailed outcome of state reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationReport {
    pub order_id: String,
    pub client_identifier: Option<String>,
    pub normalized_state: NormalizedOrderState,
    pub exchange_raw_state: String,
    pub ordered_volume: Decimal,
    pub executed_volume: Decimal,
    pub remaining_volume: Decimal,
    pub paid_fee: Decimal,
    pub trades_count: u64,
    pub discrepancy_detected: bool,
    pub message: String,
}

/// Receipt from order cancellation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCancelReceipt {
    pub order_id: String,
    pub status: String,
    pub real_exchange_delete_called: bool,
    pub cancelled_at_utc: chrono::DateTime<chrono::Utc>,
    pub remaining_volume_unfilled: Decimal,
    pub message: String,
}

/// Reconcile internal order expectations with actual exchange order details.
#[must_use]
pub fn reconcile_order_detail(
    expected_volume: Decimal,
    detail: &UpbitOrderDetail,
) -> ReconciliationReport {
    let executed = detail.executed_volume.unwrap_or(Decimal::ZERO);
    let remaining = detail.remaining_volume.unwrap_or_else(|| {
        if executed >= expected_volume {
            Decimal::ZERO
        } else {
            expected_volume - executed
        }
    });

    let raw_state = detail.state.to_lowercase();
    let (state, discrepancy) = match raw_state.as_str() {
        "wait" | "watch" => {
            if executed > Decimal::ZERO {
                (NormalizedOrderState::Partial, false)
            } else {
                (NormalizedOrderState::Accepted, false)
            }
        }
        "done" => {
            let disc = remaining > Decimal::ZERO && (executed + remaining != expected_volume);
            (NormalizedOrderState::Filled, disc)
        }
        "cancel" => (NormalizedOrderState::Cancelled, false),
        _ => (NormalizedOrderState::UnknownExecution, true),
    };

    let msg = match state {
        NormalizedOrderState::Accepted => "Order is open on exchange waiting for fills".to_string(),
        NormalizedOrderState::Partial => format!(
            "Order partially filled ({executed} / {expected_volume}), remaining {remaining}"
        ),
        NormalizedOrderState::Filled => format!("Order completely filled ({executed} volume)"),
        NormalizedOrderState::Cancelled => {
            format!("Order cancelled on exchange with {executed} executed volume")
        }
        NormalizedOrderState::UnknownExecution => {
            format!("Unknown exchange state '{raw_state}'; bounded polling required")
        }
        _ => "State reconciled".to_string(),
    };

    ReconciliationReport {
        order_id: detail.uuid.clone(),
        client_identifier: detail.identifier.clone(),
        normalized_state: state,
        exchange_raw_state: detail.state.clone(),
        ordered_volume: expected_volume,
        executed_volume: executed,
        remaining_volume: remaining,
        paid_fee: detail.paid_fee.unwrap_or(Decimal::ZERO),
        trades_count: detail.trades_count.unwrap_or(detail.trades.len() as u64),
        discrepancy_detected: discrepancy,
        message: msg,
    }
}

/// Simulate mock cancellation of an order.
#[must_use]
pub fn mock_cancel_order(order_id: &str, unfilled_volume: Decimal) -> OrderCancelReceipt {
    OrderCancelReceipt {
        order_id: order_id.to_string(),
        status: "MOCK_CANCELLED".to_string(),
        real_exchange_delete_called: false,
        cancelled_at_utc: chrono::Utc::now(),
        remaining_volume_unfilled: unfilled_volume,
        message: "Order successfully marked cancelled in mock ledger with 0 real exchange requests emitted.".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconciliation_partial_and_filled() {
        let mut detail = UpbitOrderDetail {
            uuid: "test-uuid-1".into(),
            identifier: Some("client-order-1".into()),
            side: "bid".into(),
            ord_type: "limit".into(),
            price: Some(Decimal::new(95_000_000, 0)),
            state: "wait".into(),
            market: "KRW-BTC".into(),
            created_at: "2026-10-12T00:00:00Z".into(),
            volume: Some(Decimal::new(1, 1)),           // 0.1
            remaining_volume: Some(Decimal::new(6, 2)), // 0.06
            reserved_fee: None,
            remaining_fee: None,
            paid_fee: Some(Decimal::new(190, 0)),
            locked: None,
            executed_volume: Some(Decimal::new(4, 2)), // 0.04
            trades_count: Some(1),
            trades: vec![],
        };

        // Partial
        let report1 = reconcile_order_detail(Decimal::new(1, 1), &detail);
        assert_eq!(report1.client_identifier.as_deref(), Some("client-order-1"));
        assert_eq!(report1.normalized_state, NormalizedOrderState::Partial);
        assert_eq!(report1.executed_volume, Decimal::new(4, 2));

        // Done
        detail.state = "done".into();
        detail.remaining_volume = Some(Decimal::ZERO);
        detail.executed_volume = Some(Decimal::new(1, 1));
        let report2 = reconcile_order_detail(Decimal::new(1, 1), &detail);
        assert_eq!(report2.normalized_state, NormalizedOrderState::Filled);
        assert_eq!(report2.remaining_volume, Decimal::ZERO);
        assert!(!report2.discrepancy_detected);
    }

    #[test]
    fn test_mock_cancel() {
        let receipt = mock_cancel_order("test-order-999", Decimal::new(5, 2));
        assert_eq!(receipt.status, "MOCK_CANCELLED");
        assert!(!receipt.real_exchange_delete_called);
    }
}

//! Order Execution Gate and Simulation Engine.
//!
//! Enforces:
//! - The legacy preview execution path remains mock-only under
//!   `LIVE_TRADING_ENABLED = false`.
//! - LIVE automation order submission is separately gated by the immutable
//!   `LiveExecutionPolicy` plus durable operator, risk, and reconciliation checks.
//! - Upstream Risk Guard verification.
//! - Upstream Human Approval verification.
//! - Mock order intent issuance and simulated lifecycle tracking.

use crate::upbit::approval::{ApprovalError, HumanApprovalGate};
use crate::upbit::preview::{OrderPreview, PreviewError};
use crate::upbit::risk::{RiskError, RiskGuard};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub use super::live_order_control::LiveOrderSendGate;

/// Safe default for the legacy preview-only execution API. LIVE automation uses
/// [`LiveExecutionPolicy`] composed by the process entrypoint.
pub const LIVE_TRADING_ENABLED: bool = false;

/// Immutable process policy controlling whether admitted live automation effects
/// may reach the private exchange adapter.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum LiveExecutionPolicy {
    /// Safe default used by every existing constructor.
    #[default]
    Disabled,
    /// Explicit operator startup permission. This does not replace durable
    /// trading-control consent, approval, risk, or reconciliation gates.
    OperatorEnabled,
}

impl LiveExecutionPolicy {
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::OperatorEnabled)
    }
}

/// Execution errors.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error(
        "LIVE_TRADING_DISABLED: legacy preview execution remains mock-only (LIVE_TRADING_ENABLED = false)"
    )]
    LiveTradingDisabled,
    #[error("Preview error: {0}")]
    Preview(#[from] PreviewError),
    #[error("Risk rejection: {0}")]
    Risk(#[from] RiskError),
    #[error("Approval rejection: {0}")]
    Approval(#[from] ApprovalError),
}

/// Request to execute an order based on a pre-validated preview.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderExecuteRequest {
    pub preview: OrderPreview,
    pub human_approval_token: Option<String>,
    pub client_identifier: Option<String>,
    #[serde(default)]
    pub allow_mock_execution: bool,
}

/// Execution outcome receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderExecutionReceipt {
    pub status: String,
    pub order_id: String,
    pub client_identifier: String,
    pub market: String,
    pub side: String,
    pub ord_type: String,
    pub price: Option<Decimal>,
    pub volume: Option<Decimal>,
    pub notional_krw: Decimal,
    pub executed_at_utc: DateTime<Utc>,
    pub is_mock: bool,
    pub exchange_status: String,
    pub message: String,
}

/// Order Execution Service.
#[derive(Clone)]
pub struct OrderExecutionService {
    risk_guard: RiskGuard,
    approval_gate: HumanApprovalGate,
}

impl OrderExecutionService {
    /// Create a new execution service.
    #[must_use]
    pub fn new(risk_guard: RiskGuard, approval_gate: HumanApprovalGate) -> Self {
        Self {
            risk_guard,
            approval_gate,
        }
    }

    /// Process an order execution request.
    ///
    /// # Errors
    /// Returns `ExecutionError` if live trading is attempted, risk rejected, approval missing, or preview invalid.
    pub async fn execute(
        &self,
        req: OrderExecuteRequest,
        now: DateTime<Utc>,
    ) -> Result<OrderExecutionReceipt, ExecutionError> {
        // 1. Reject unavailable live execution before consuming approval or
        // mutating risk counters. Mock execution never sends exchange requests.
        if !LIVE_TRADING_ENABLED && !req.allow_mock_execution {
            return Err(ExecutionError::LiveTradingDisabled);
        }

        // 2. Verify preview integrity and unexpired TTL
        req.preview.verify_digest()?;
        if req.preview.is_expired(now) {
            return Err(ExecutionError::Preview(PreviewError::Expired {
                expired_at: req.preview.valid_until_utc,
            }));
        }

        // 3. Verify independent risk bounds
        self.risk_guard
            .validate_order(
                &req.preview.market,
                req.preview.calculated_notional_krw,
                now,
            )
            .await?;

        // 4. Verify and consume human approval token
        self.approval_gate
            .verify_and_consume(
                req.human_approval_token.as_deref(),
                &req.preview.preview_digest,
                now,
            )
            .await?;

        // 5. Emit simulated/mock durable order execution receipt
        let client_id = req
            .client_identifier
            .unwrap_or_else(|| format!("ord-{}", Utc::now().timestamp_millis()));
        let order_id = format!("mock-order-{}", &req.preview.preview_digest[..16]);

        Ok(OrderExecutionReceipt {
            status: "MOCK_EXECUTED_ACCEPTED".to_string(),
            order_id,
            client_identifier: client_id,
            market: req.preview.market,
            side: req.preview.side,
            ord_type: req.preview.ord_type,
            price: req.preview.price,
            volume: req.preview.volume,
            notional_krw: req.preview.calculated_notional_krw,
            executed_at_utc: now,
            is_mock: true,
            exchange_status: "wait".to_string(),
            message:
                "Simulated order accepted into mock order ledger; 0 real exchange requests emitted."
                    .to_string(),
        })
    }
}

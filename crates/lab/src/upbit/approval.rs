//! Human Operator Approval Gate for order execution.
//!
//! Enforces:
//! - Separation of analysis from execution: LLM agents cannot self-approve.
//! - Cryptographic binding between approval token and exact `preview_digest`.
//! - Strict expiration (TTL enforcement).
//! - Replay attack prevention: approval tokens are strictly single-use.
//! - Fail-closed behavior on missing, expired, mismatched, or replayed tokens.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Approval verification errors.
#[derive(Debug, thiserror::Error)]
pub enum ApprovalError {
    #[error("APPROVAL_MISSING: Order execution requires a valid human operator approval token")]
    MissingToken,
    #[error("APPROVAL_INVALID: Provided approval token is invalid or unknown")]
    InvalidToken,
    #[error("APPROVAL_EXPIRED: Approval token expired at {expired_at}")]
    Expired { expired_at: DateTime<Utc> },
    #[error(
        "APPROVAL_DIGEST_MISMATCH: Token was issued for preview digest '{token_digest}', but order has digest '{order_digest}'"
    )]
    DigestMismatch {
        token_digest: String,
        order_digest: String,
    },
    #[error("APPROVAL_REPLAY: Token has already been consumed and cannot be reused")]
    TokenAlreadyConsumed,
    #[error("APPROVAL_ISSUANCE_FAILED: {0}")]
    IssuanceFailed(String),
}

#[derive(Clone)]
struct IssuedApproval {
    preview_digest: String,
    valid_until: DateTime<Utc>,
    consumed: bool,
}

/// Thread-safe Human Approval Gate.
#[derive(Clone, Default)]
pub struct HumanApprovalGate {
    tokens: Arc<Mutex<HashMap<String, IssuedApproval>>>,
}

impl HumanApprovalGate {
    /// Issue a test-only approval token bound to an exact preview digest.
    #[cfg(test)]
    pub(crate) async fn issue_token(
        &self,
        preview_digest: &str,
        valid_until: DateTime<Utc>,
    ) -> Result<String, ApprovalError> {
        const MAX_TEST_APPROVALS: usize = 128;
        let nonce = crate::upbit::auth::generate_nonce()
            .map_err(|error| ApprovalError::IssuanceFailed(error.to_string()))?;
        let token = format!("appr-{}", nonce.replace('-', ""));
        let mut lock = self.tokens.lock().await;
        let now = Utc::now();
        lock.retain(|_, approval| !approval.consumed && approval.valid_until >= now);
        if lock.len() >= MAX_TEST_APPROVALS
            && let Some(oldest) = lock
                .iter()
                .min_by_key(|(_, approval)| approval.valid_until)
                .map(|(token, _)| token.clone())
        {
            lock.remove(&oldest);
        }
        lock.insert(
            token.clone(),
            IssuedApproval {
                preview_digest: preview_digest.to_string(),
                valid_until,
                consumed: false,
            },
        );

        Ok(token)
    }

    /// Verify and immediately consume a human approval token.
    ///
    /// # Errors
    /// Returns `ApprovalError` if missing, invalid, expired, mismatched, or already consumed.
    pub async fn verify_and_consume(
        &self,
        token: Option<&str>,
        order_digest: &str,
        now: DateTime<Utc>,
    ) -> Result<(), ApprovalError> {
        let token_str = token.ok_or(ApprovalError::MissingToken)?;
        if token_str.trim().is_empty() {
            return Err(ApprovalError::MissingToken);
        }

        let mut lock = self.tokens.lock().await;
        let entry = lock.get_mut(token_str).ok_or(ApprovalError::InvalidToken)?;

        if entry.consumed {
            return Err(ApprovalError::TokenAlreadyConsumed);
        }

        if now > entry.valid_until {
            return Err(ApprovalError::Expired {
                expired_at: entry.valid_until,
            });
        }

        if entry.preview_digest != order_digest {
            return Err(ApprovalError::DigestMismatch {
                token_digest: entry.preview_digest.clone(),
                order_digest: order_digest.to_string(),
            });
        }

        // Consume token to prevent replay
        entry.consumed = true;
        Ok(())
    }
}

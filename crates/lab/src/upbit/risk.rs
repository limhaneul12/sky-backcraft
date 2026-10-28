//! Independent Risk Guard for execution boundaries.
//!
//! Enforces:
//! - Emergency Stop switch (fails closed immediately if triggered).
//! - Market allowlist (`KRW-BTC`, `KRW-ETH`, `KRW-XRP`).
//! - Maximum single order notional limit.
//! - Maximum daily cumulative order volume ceiling.
//! - Order submission rate limiting / minimum spacing.
//! - Absolute independence from AI / LLM prompt manipulation.

use crate::contracts::LabError;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex as SyncMutex};
use tokio::sync::Mutex;

/// Risk check rejection errors.
#[derive(Debug, thiserror::Error)]
pub enum RiskError {
    #[error("EMERGENCY_STOP: Trading is halted by operator emergency switch")]
    EmergencyStopActive,
    #[error("TRADING_PAUSED: New order admission is paused by server control state")]
    TradingPaused,
    #[error("RISK_REJECTED: Market '{0}' not in allowed execution universe")]
    DisallowedMarket(String),
    #[error(
        "RISK_REJECTED: Order notional {notional} KRW exceeds single order ceiling {limit} KRW"
    )]
    MaxSingleOrderExceeded { notional: Decimal, limit: Decimal },
    #[error(
        "RISK_REJECTED: Daily volume {daily_cumulative} KRW + {notional} KRW exceeds daily limit {limit} KRW"
    )]
    DailyVolumeCeilingExceeded {
        daily_cumulative: Decimal,
        notional: Decimal,
        limit: Decimal,
    },
    #[error(
        "RATE_LIMIT: Order submitted too quickly ({elapsed_ms}ms since last order, minimum {min_interval_ms}ms)"
    )]
    RateLimitExceeded {
        elapsed_ms: u128,
        min_interval_ms: u128,
    },
}

#[cfg(test)]
mod admission_tests {
    use super::{RiskConfig, RiskGuard};

    #[test]
    fn mock_admission_ignores_live_pause_but_honors_emergency_stop() {
        let guard = RiskGuard::new(RiskConfig::default());
        let _pause = guard.latch_order_admission(false);
        assert!(guard.with_mock_order_admission(|| Ok(())).is_ok());

        let _emergency = guard.latch_order_admission(true);
        assert!(guard.with_mock_order_admission(|| Ok(())).is_err());
    }
}

/// Static risk configuration limits.
#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub max_single_order_krw: Decimal,
    pub max_daily_volume_krw: Decimal,
    pub min_order_interval_ms: u128,
    pub allowed_markets: HashSet<String>,
}

impl Default for RiskConfig {
    fn default() -> Self {
        let mut allowed = HashSet::new();
        allowed.insert("KRW-BTC".to_string());
        allowed.insert("KRW-ETH".to_string());
        allowed.insert("KRW-XRP".to_string());
        Self {
            max_single_order_krw: Decimal::new(5_000_000, 0), // 5,000,000 KRW
            max_daily_volume_krw: Decimal::new(50_000_000, 0), // 50,000,000 KRW
            min_order_interval_ms: 3_000,                     // 3 seconds
            allowed_markets: allowed,
        }
    }
}

/// Thread-safe Risk Guard.
#[derive(Clone)]
pub struct RiskGuard {
    config: RiskConfig,
    admission: Arc<SyncMutex<AdmissionState>>,
    state: Arc<Mutex<RiskState>>,
}

struct AdmissionState {
    revision: u64,
    emergency_stop: bool,
    paused: bool,
    pending_safety: BTreeMap<u64, bool>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SafetyLatchToken(u64);

impl SafetyLatchToken {
    #[must_use]
    pub(crate) const fn revision(self) -> u64 {
        self.0
    }
}

struct RiskState {
    last_order_at: Option<DateTime<Utc>>,
    current_day: String,
    daily_volume: Decimal,
}

impl RiskGuard {
    /// Create a new risk guard with default or custom configuration.
    #[must_use]
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            admission: Arc::new(SyncMutex::new(AdmissionState {
                revision: 0,
                emergency_stop: false,
                paused: false,
                pending_safety: BTreeMap::new(),
            })),
            state: Arc::new(Mutex::new(RiskState {
                last_order_at: None,
                current_day: String::new(),
                daily_volume: Decimal::ZERO,
            })),
        }
    }

    /// Trigger the emergency stop switch.
    #[cfg(test)]
    pub(crate) fn trigger_emergency_stop(&self) {
        let _revision = self.latch_order_admission(true);
        tracing::warn!("EMERGENCY STOP TRIGGERED: Trading execution halted immediately");
    }

    /// Clear emergency stop.
    #[cfg(test)]
    pub(crate) fn clear_emergency_stop(&self) {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admission.revision = admission.revision.saturating_add(1);
        admission.emergency_stop = false;
        admission.paused = false;
        admission.pending_safety.clear();
        tracing::info!("Emergency stop cleared by operator");
    }

    /// Check if emergency stop is active.
    #[must_use]
    pub fn is_emergency_stopped(&self) -> bool {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .emergency_stop
    }

    /// Current process-local control revision used to reject out-of-order readback.
    #[must_use]
    pub fn control_revision(&self) -> u64 {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .revision
    }

    /// Serialize a synchronous order-claim transaction with the process-local
    /// Pause/EmergencyStop latch.
    ///
    /// The caller's closure is the claim linearization point and must remain
    /// synchronous and bounded (for example, one SQLite transaction). Never
    /// await or perform venue I/O while this lock is held.
    ///
    /// # Errors
    /// Rejects while order admission is paused, emergency-stopped, or awaiting
    /// authoritative readback of a safety transition.
    pub(crate) fn with_order_admission<T>(
        &self,
        claim: impl FnOnce() -> Result<T, LabError>,
    ) -> Result<T, LabError> {
        let admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.emergency_stop {
            return Err(LabError::BlockedEvidence(
                "EMERGENCY_STOP_BLOCKS_ORDER_CLAIM".into(),
            ));
        }
        if admission.paused || !admission.pending_safety.is_empty() {
            return Err(LabError::BlockedEvidence(
                "TRADING_PAUSE_BLOCKS_ORDER_CLAIM".into(),
            ));
        }
        claim()
    }

    /// Serialize a mock send-claim with the process-local emergency latch while
    /// keeping mock execution independent from the Live trading pause.
    ///
    /// Emergency transitions still fence the closure at the same linearization
    /// point as `with_order_admission`; ordinary Pause transitions do not.
    pub(crate) fn with_mock_order_admission<T>(
        &self,
        claim: impl FnOnce() -> Result<T, LabError>,
    ) -> Result<T, LabError> {
        let admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.emergency_stop
            || admission
                .pending_safety
                .values()
                .any(|emergency| *emergency)
        {
            return Err(LabError::BlockedEvidence(
                "EMERGENCY_STOP_BLOCKS_ORDER_CLAIM".into(),
            ));
        }
        claim()
    }

    /// Check the same fast latch immediately before beginning a venue effect.
    /// The returned success is the effect-start linearization point; no lock is
    /// held while the network request is in flight.
    ///
    /// # Errors
    /// Rejects while order admission is paused, emergency-stopped, or awaiting
    /// authoritative readback of a safety transition.
    pub(crate) fn authorize_order_effect(&self) -> Result<(), LabError> {
        self.with_order_admission(|| Ok(()))
    }

    /// Close new-order admission before publishing a durable safety command.
    #[must_use]
    pub(crate) fn latch_order_admission(&self, emergency: bool) -> SafetyLatchToken {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admission.revision = admission.revision.saturating_add(1);
        if emergency {
            admission.emergency_stop = true;
            admission.paused = false;
        } else if !admission.emergency_stop {
            admission.paused = true;
        }
        let revision = admission.revision;
        admission.pending_safety.insert(revision, emergency);
        SafetyLatchToken(revision)
    }

    /// Apply durable readback only when no newer local safety operation intervened.
    #[must_use]
    pub fn synchronize_order_admission(
        &self,
        expected_revision: u64,
        blocked: bool,
        emergency: bool,
    ) -> bool {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.revision != expected_revision {
            return false;
        }
        if !admission.pending_safety.is_empty() {
            let pending_emergency = admission.pending_safety.values().any(|value| *value);
            if (pending_emergency && !emergency) || (!pending_emergency && !blocked) {
                return false;
            }
            admission.pending_safety.clear();
        }
        admission.emergency_stop = emergency;
        admission.paused = blocked && !emergency;
        true
    }

    /// Resolve the exact safety latch after its durable command is read back.
    #[must_use]
    pub(crate) fn resolve_safety_latch(
        &self,
        token: SafetyLatchToken,
        blocked: bool,
        emergency: bool,
    ) -> bool {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.pending_safety.remove(&token.0).is_none() {
            return false;
        }
        if !admission.pending_safety.is_empty() || admission.revision != token.0 {
            return false;
        }
        admission.emergency_stop = emergency;
        admission.paused = blocked && !emergency;
        true
    }

    /// Evaluate an order proposal against all independent risk rules.
    ///
    /// # Errors
    /// Returns `RiskError` if any risk boundary is violated.
    pub async fn validate_order(
        &self,
        market: &str,
        notional_krw: Decimal,
        now: DateTime<Utc>,
    ) -> Result<(), RiskError> {
        // 1. Emergency stop check
        let (emergency_stop, admission_paused) = {
            let admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                admission.emergency_stop
                    || admission
                        .pending_safety
                        .values()
                        .any(|emergency| *emergency),
                admission.paused || !admission.pending_safety.is_empty(),
            )
        };
        if emergency_stop {
            return Err(RiskError::EmergencyStopActive);
        }
        if admission_paused {
            return Err(RiskError::TradingPaused);
        }

        // 2. Market allowlist check
        if !self.config.allowed_markets.contains(market) {
            return Err(RiskError::DisallowedMarket(market.to_string()));
        }

        // 3. Single order ceiling check
        if notional_krw > self.config.max_single_order_krw {
            return Err(RiskError::MaxSingleOrderExceeded {
                notional: notional_krw,
                limit: self.config.max_single_order_krw,
            });
        }

        // 4. Rate limit and daily volume state check
        let mut state = self.state.lock().await;

        if let Some(last) = state.last_order_at {
            let elapsed = (now - last).num_milliseconds();
            if let Ok(elapsed_ms) = u128::try_from(elapsed)
                && elapsed_ms < self.config.min_order_interval_ms
            {
                return Err(RiskError::RateLimitExceeded {
                    elapsed_ms,
                    min_interval_ms: self.config.min_order_interval_ms,
                });
            }
        }

        let today = now.format("%Y-%m-%d").to_string();
        if state.current_day != today {
            state.current_day = today;
            state.daily_volume = Decimal::ZERO;
        }

        if state.daily_volume + notional_krw > self.config.max_daily_volume_krw {
            return Err(RiskError::DailyVolumeCeilingExceeded {
                daily_cumulative: state.daily_volume,
                notional: notional_krw,
                limit: self.config.max_daily_volume_krw,
            });
        }

        // Record provisional submission
        state.last_order_at = Some(now);
        state.daily_volume += notional_krw;

        Ok(())
    }
}

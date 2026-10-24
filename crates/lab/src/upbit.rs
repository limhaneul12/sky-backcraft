//! Upbit Exchange Integration Module.
//!
//! Submodules:
//! - [`auth`]: Secure credential management, `.env` loading, JWT generation, host allowlist.
//! - [`client`]: Typed private client for read-only accounts/orders and official test orders.
//! - [`preview`]: Order preview engine with tick rules, minimum notional, fees, and intent digests.
//! - [`risk`]: Independent risk guard with emergency stop and volume/frequency ceilings.
//! - [`approval`]: Human operator approval gate with cryptographic binding and replay defense.
//! - [`execution`]: Explicit default-off live execution policy and order gate.
//! - [`reconciliation`]: Order state reconciliation and mock cancellation.
//! - [`realtime`]: Bounded authenticated account/order observations and recovery fence.

pub mod approval;
pub mod auth;
pub mod client;
pub mod control;
pub mod execution;
pub mod live_order_control;
pub mod preview;
pub mod realtime;
pub mod reconciliation;
pub mod risk;

#[cfg(test)]
mod tests;

use approval::HumanApprovalGate;
use auth::{UpbitCredentials, explicit_configuration_present};
use client::{ClientError, UpbitPrivateClient};
use execution::{LiveExecutionPolicy, LiveOrderSendGate, OrderExecutionService};
use risk::{RiskConfig, RiskGuard};
use std::path::Path;

/// Unified Upbit Exchange Service managing credentials, client, risk, and approval gates.
#[derive(Clone)]
pub struct UpbitExchangeService {
    client: UpbitPrivateClient,
    risk_guard: RiskGuard,
    approval_gate: HumanApprovalGate,
    execution_service: OrderExecutionService,
}

impl UpbitExchangeService {
    /// Initialize the exchange service, attempting to load credentials from the environment or `.env`.
    /// Falls back to Mock mode if credentials are not configured.
    ///
    /// # Errors
    /// Rejects malformed explicit credentials, unreadable configured files, or
    /// failure to construct the authenticated HTTP client.
    pub fn init_from_env_or_mock(optional_root: Option<&Path>) -> Result<Self, ClientError> {
        let client = match UpbitCredentials::load_from_env_or_file(optional_root) {
            Ok(credentials) => {
                let client = UpbitPrivateClient::new_live(credentials)?;
                tracing::info!("UpbitPrivateClient initialized in LIVE HTTP READ mode");
                client
            }
            Err(auth::AuthError::MissingCredentials(_)) if !explicit_configuration_present() => {
                tracing::info!("Upbit credentials are absent; using explicit MOCK read mode");
                UpbitPrivateClient::new_mock()
            }
            Err(error) => return Err(ClientError::Auth(error)),
        };

        Ok(Self::from_client(client))
    }

    /// Initialize an authenticated private service with an explicit process policy.
    /// Unlike the default constructor, this path never falls back to MOCK when
    /// credentials are absent or invalid.
    ///
    /// # Errors
    /// Rejects missing/malformed credentials, unreadable configured files, or
    /// failure to construct the authenticated HTTP client.
    pub fn init_from_env_with_policy(
        optional_root: Option<&Path>,
        policy: LiveExecutionPolicy,
    ) -> Result<Self, ClientError> {
        let credentials =
            UpbitCredentials::load_from_env_or_file(optional_root).map_err(ClientError::Auth)?;
        Ok(Self::from_client(UpbitPrivateClient::new_live_with_policy(
            credentials,
            policy,
        )?))
    }

    /// Initialize runtime control with required LIVE credentials, without reading
    /// a `.env` from the invoking working directory.
    ///
    /// # Errors
    /// Rejects absent or invalid credentials and HTTP client construction failures.
    pub fn init_from_env_with_policy_and_send_gate(
        optional_root: Option<&Path>,
        policy: LiveExecutionPolicy,
        send_gate: LiveOrderSendGate,
    ) -> Result<Self, ClientError> {
        let credentials = UpbitCredentials::load_from_env_or_root_file(optional_root)
            .map_err(ClientError::Auth)?;
        Ok(Self::from_client(
            UpbitPrivateClient::new_live_with_policy_and_send_gate(credentials, policy, send_gate)?,
        ))
    }

    /// Initialize managed control as LIVE when configured, otherwise MOCK. The
    /// invoking working directory is never searched for credential files.
    ///
    /// # Errors
    /// Rejects malformed explicit credentials or HTTP client construction failures.
    pub fn init_from_env_or_mock_with_policy_and_send_gate(
        optional_root: Option<&Path>,
        policy: LiveExecutionPolicy,
        send_gate: LiveOrderSendGate,
    ) -> Result<Self, ClientError> {
        let client = match UpbitCredentials::load_from_env_or_root_file(optional_root) {
            Ok(credentials) => UpbitPrivateClient::new_live_with_policy_and_send_gate(
                credentials,
                policy,
                send_gate,
            )?,
            Err(auth::AuthError::MissingCredentials(_))
                if !explicit_configuration_present()
                    && !optional_root.is_some_and(|root| root.join(".env").exists()) =>
            {
                tracing::info!("Upbit credentials are absent; using MOCK runtime-control mode");
                UpbitPrivateClient::new_mock_with_policy_and_send_gate(policy, send_gate)
            }
            Err(error) => return Err(ClientError::Auth(error)),
        };
        Ok(Self::from_client(client))
    }

    /// Construct an isolated credential-free MOCK service with a shared gate.
    #[must_use]
    pub fn new_mock_with_policy_and_send_gate(
        policy: LiveExecutionPolicy,
        send_gate: LiveOrderSendGate,
    ) -> Self {
        Self::from_client(UpbitPrivateClient::new_mock_with_policy_and_send_gate(
            policy, send_gate,
        ))
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn new_mock() -> Self {
        Self::credential_free_mock()
    }

    /// Construct a credential-free exchange projection for public-no-auth routes.
    #[must_use]
    pub fn credential_free_mock() -> Self {
        Self::from_client(UpbitPrivateClient::new_mock())
    }

    fn from_client(client: UpbitPrivateClient) -> Self {
        let risk_guard = RiskGuard::new(RiskConfig::default());
        let approval_gate = HumanApprovalGate::default();
        let execution_service =
            OrderExecutionService::new(risk_guard.clone(), approval_gate.clone());

        Self {
            client,
            risk_guard,
            approval_gate,
            execution_service,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_fixture_client(client: UpbitPrivateClient) -> Self {
        Self::from_client(client)
    }

    /// Access the underlying private client.
    #[must_use]
    pub fn client(&self) -> &UpbitPrivateClient {
        &self.client
    }

    /// Access the risk guard.
    #[must_use]
    pub fn risk_guard(&self) -> &RiskGuard {
        &self.risk_guard
    }

    /// Access the human approval gate.
    #[must_use]
    pub fn approval_gate(&self) -> &HumanApprovalGate {
        &self.approval_gate
    }

    /// Access the execution service.
    #[must_use]
    pub fn execution_service(&self) -> &OrderExecutionService {
        &self.execution_service
    }

    /// Stable source classification for private read projections.
    #[must_use]
    pub fn exchange_read_mode(&self) -> control::ExchangeReadMode {
        if self.client.is_live() {
            control::ExchangeReadMode::LiveRead
        } else {
            control::ExchangeReadMode::Mock
        }
    }

    /// Return the immutable policy shared by private reads, control, and jobs.
    #[must_use]
    pub const fn live_execution_policy(&self) -> LiveExecutionPolicy {
        self.client.live_execution_policy()
    }

    #[must_use]
    pub fn live_order_send_gate(&self) -> Option<LiveOrderSendGate> {
        self.client.live_order_send_gate()
    }

    #[must_use]
    pub fn live_trading_enabled(&self) -> bool {
        self.client.live_trading_enabled()
    }
}

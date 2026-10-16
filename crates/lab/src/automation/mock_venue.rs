//! Persistent deterministic venue facts for the explicit MOCK automation mode.

use super::{VenueAdapter, VenueFuture};
#[cfg(test)]
use crate::contracts::AutomationSessionMode;
use crate::contracts::{
    AutomationOrderIdentifier, AutomationOrderRequest, LabError, MarketId, PriceKrw, UtcTimestamp,
};
use crate::database::DatabaseHandle;
use crate::upbit::client::{ClientError, UpbitAccountBalance, UpbitOrderChance, UpbitOrderDetail};

#[derive(Clone)]
pub(crate) struct PersistentMockVenue {
    database: DatabaseHandle,
}

impl PersistentMockVenue {
    #[must_use]
    pub(crate) fn new(database: DatabaseHandle) -> Self {
        Self { database }
    }

    /// Seed the mock wallet once and capture the initial account read under the
    /// same `DatabaseOwner` used by the strategy ledger.
    #[cfg(test)]
    pub(crate) async fn initialize(&self, now: UtcTimestamp) -> Result<(), LabError> {
        self.database
            .call("seed_automation_mock_venue", move |store| {
                store.automation_seed_mock_venue(now)
            })
            .await?;
        self.snapshot(now).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn snapshot(
        &self,
        now: UtcTimestamp,
    ) -> Result<Vec<UpbitAccountBalance>, LabError> {
        let balances = self
            .database
            .call("read_automation_mock_accounts", |store| {
                store.automation_mock_venue_accounts()
            })
            .await?;
        let captured = balances.clone();
        self.database
            .call("record_automation_mock_account_snapshot", move |store| {
                store.automation_record_account_snapshot(
                    "mock:upbit-default",
                    AutomationSessionMode::Mock,
                    now,
                    now,
                    &captured,
                )
            })
            .await?;
        Ok(balances)
    }
}

impl VenueAdapter for PersistentMockVenue {
    fn is_live(&self) -> bool {
        false
    }

    fn account_identity(&self) -> String {
        "mock:upbit-default".into()
    }

    fn account_balances(&self) -> VenueFuture<'_, Vec<UpbitAccountBalance>> {
        Box::pin(async move {
            self.database
                .call("automation_mock_account_read", |store| {
                    store.automation_mock_venue_accounts()
                })
                .await
                .map_err(|error| client_error(&error))
        })
    }

    fn order_chance<'a>(&'a self, market: &'a MarketId) -> VenueFuture<'a, UpbitOrderChance> {
        Box::pin(async move {
            let market = market.clone();
            self.database
                .call("automation_mock_order_chance", move |store| {
                    store.automation_mock_order_chance(&market)
                })
                .await
                .map_err(|error| client_error(&error))
        })
    }

    fn submit_order<'a>(
        &'a self,
        request: &'a AutomationOrderRequest,
        decision_mark: PriceKrw,
        _send_generation: Option<u64>,
    ) -> VenueFuture<'a, UpbitOrderDetail> {
        Box::pin(async move {
            let request = request.clone();
            self.database
                .call("automation_mock_submit_order", move |store| {
                    store.automation_mock_submit_order(&request, decision_mark, UtcTimestamp::now())
                })
                .await
                .map_err(|error| client_error(&error))
        })
    }

    fn order_by_identifier<'a>(
        &'a self,
        identifier: &'a AutomationOrderIdentifier,
    ) -> VenueFuture<'a, Option<UpbitOrderDetail>> {
        Box::pin(async move {
            let identifier = identifier.clone();
            self.database
                .call("automation_mock_order_readback", move |store| {
                    store.automation_mock_order_by_identifier(&identifier)
                })
                .await
                .map_err(|error| client_error(&error))
        })
    }

    fn cancel_order<'a>(&'a self, uuid: &'a str) -> VenueFuture<'a, UpbitOrderDetail> {
        Box::pin(async move {
            let uuid = uuid.to_owned();
            self.database
                .call("automation_mock_cancel_order", move |store| {
                    store.automation_mock_cancel_order(&uuid, UtcTimestamp::now())
                })
                .await
                .map_err(|error| client_error(&error))
        })
    }
}

fn client_error(error: &LabError) -> ClientError {
    match error {
        LabError::InvalidConfig(_) | LabError::ContractParse(_) | LabError::Conflict(_) => {
            ClientError::InvalidParams(error.to_string())
        }
        LabError::OutcomeUnknown(_) | LabError::NetworkUnavailable(_) => {
            ClientError::Network(error.to_string())
        }
        other => ClientError::Network(other.to_string()),
    }
}

use super::{Store, sql_error};
use crate::contracts::{LabError, UtcTimestamp};
use crate::upbit::control::{
    ControlDecisionContext, PersistedControlOutcome, PersistedControlSnapshot,
    TradingControlAction, TradingControlOutcome, TradingControlReceipt, TradingControlState,
    decide_transition,
};
use crate::upbit::live_order_control::{
    LiveOrderControlAction, LiveOrderControlBegin, LiveOrderControlOutcome,
    LiveOrderControlReceipt, PersistedLiveOrderControlOutcome, PersistedLiveOrderControlSnapshot,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

impl Store {
    pub(crate) fn initialize_live_order_control(
        &mut self,
        boot_epoch: &str,
        available: bool,
        observed_at_utc: &str,
    ) -> Result<(), LabError> {
        if boot_epoch.is_empty() || boot_epoch.len() > 128 {
            return Err(LabError::InvalidConfig(
                "live-order boot epoch exceeds bounds".into(),
            ));
        }
        let transaction = self.connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE live_order_control_intents SET status='DENIED',denied_code='INTERRUPTED_BY_RESTART',observed_at_utc=?1 \
                 WHERE status='PENDING'",
                [observed_at_utc],
            )
            .map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE live_order_control_state SET boot_epoch=?1,sequence=0,live_trading_enabled=0,available=?2,updated_at_utc=?3 \
                 WHERE singleton=1",
                params![boot_epoch, available, observed_at_utc],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    }

    pub(crate) fn live_order_control_snapshot(
        &self,
    ) -> Result<PersistedLiveOrderControlSnapshot, LabError> {
        read_live_order_control_snapshot(&self.connection)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one owner transaction keeps request CAS, OFF fencing, intent receipt and reservation rejection atomic"
    )]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn live_order_control_begin(
        &mut self,
        action: LiveOrderControlAction,
        request_id: &str,
        expected_sequence: u64,
        expected_epoch: &str,
        precondition_denied_code: Option<&str>,
        fail_safe_disable: bool,
        effective_gate_enabled: bool,
        observed_at_utc: &str,
    ) -> Result<LiveOrderControlBegin, LabError> {
        if !action.is_mutation()
            || request_id.is_empty()
            || request_id.len() > 128
            || expected_epoch.is_empty()
            || expected_epoch.len() > 128
        {
            return Err(LabError::InvalidConfig(
                "invalid live-order control mutation identity".into(),
            ));
        }
        let expected_sequence_i64 = i64::try_from(expected_sequence).map_err(|_| {
            LabError::InvalidConfig("expected_sequence exceeds SQLite integer capacity".into())
        })?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let current = read_live_order_control_snapshot(&transaction)?;
        if let Some(existing) = read_live_order_control_intent(&transaction, request_id)? {
            if existing.action != action
                || existing.expected_sequence != expected_sequence
                || existing.expected_epoch != expected_epoch
            {
                if fail_safe_disable && action == LiveOrderControlAction::Disable {
                    let _closed = persist_live_order_control_fail_safe_close(
                        &transaction,
                        &current,
                        request_id,
                        "IDENTITY_CONFLICT_OFF",
                        observed_at_utc,
                    )?;
                    transaction.commit().map_err(sql_error)?;
                }
                return Err(LabError::Conflict(
                    "live-order control request_id was reused for a different command".into(),
                ));
            }
            let snapshot = if fail_safe_disable
                && action == LiveOrderControlAction::Disable
                && current.live_trading_enabled
            {
                persist_live_order_control_fail_safe_close(
                    &transaction,
                    &current,
                    request_id,
                    "REPLAYED_OFF",
                    observed_at_utc,
                )?
            } else {
                current
            };
            if existing.status == "PENDING" {
                transaction.commit().map_err(sql_error)?;
                return Ok(LiveOrderControlBegin::ExistingPending(snapshot));
            }
            transaction.commit().map_err(sql_error)?;
            return Ok(LiveOrderControlBegin::Complete(
                PersistedLiveOrderControlOutcome {
                    snapshot,
                    receipt: existing.receipt()?,
                },
            ));
        }

        let stale_code = if expected_epoch != current.boot_epoch {
            Some("STALE_EPOCH")
        } else if expected_sequence != current.sequence {
            Some("STALE_SEQUENCE")
        } else {
            None
        };
        let unavailable_code = (!current.available).then_some("CONTROL_UNAVAILABLE");
        let denied_code = precondition_denied_code.or(unavailable_code).or(stale_code);
        if let Some(denied_code) = denied_code {
            let snapshot = if fail_safe_disable && action == LiveOrderControlAction::Disable {
                let sequence = current.sequence.checked_add(1).ok_or_else(|| {
                    LabError::ResourceLimit("live-order control sequence exhausted".into())
                })?;
                update_live_order_control_state(
                    &transaction,
                    &current.boot_epoch,
                    sequence,
                    false,
                    current.available,
                    observed_at_utc,
                    current.sequence,
                )?;
                super::automation_store::reject_unsent_live_work_in_control_transaction(
                    &transaction,
                    "LIVE_ORDER_SENDING_DISABLED",
                    UtcTimestamp::parse_rfc3339(observed_at_utc)?,
                    64,
                )?;
                PersistedLiveOrderControlSnapshot {
                    boot_epoch: current.boot_epoch.clone(),
                    sequence,
                    live_trading_enabled: false,
                    available: current.available,
                }
            } else {
                current.clone()
            };
            let receipt = LiveOrderControlReceipt {
                request_id: request_id.to_owned(),
                action,
                outcome: LiveOrderControlOutcome::Denied,
                sequence: snapshot.sequence,
                boot_epoch: snapshot.boot_epoch.clone(),
                observed_at_utc: observed_at_utc.to_owned(),
                denied_code: Some(denied_code.to_owned()),
            };
            insert_live_order_control_intent(
                &transaction,
                request_id,
                action,
                expected_sequence_i64,
                expected_epoch,
                &receipt,
                "DENIED",
            )?;
            transaction.commit().map_err(sql_error)?;
            return Ok(LiveOrderControlBegin::Complete(
                PersistedLiveOrderControlOutcome { snapshot, receipt },
            ));
        }

        if action == LiveOrderControlAction::Enable && effective_gate_enabled {
            let receipt = LiveOrderControlReceipt {
                request_id: request_id.to_owned(),
                action,
                outcome: LiveOrderControlOutcome::Acknowledged,
                sequence: current.sequence,
                boot_epoch: current.boot_epoch.clone(),
                observed_at_utc: observed_at_utc.to_owned(),
                denied_code: None,
            };
            update_live_order_control_state(
                &transaction,
                &current.boot_epoch,
                current.sequence,
                true,
                current.available,
                observed_at_utc,
                current.sequence,
            )?;
            insert_live_order_control_intent(
                &transaction,
                request_id,
                action,
                expected_sequence_i64,
                expected_epoch,
                &receipt,
                "ACKNOWLEDGED",
            )?;
            let snapshot = read_live_order_control_snapshot(&transaction)?;
            transaction.commit().map_err(sql_error)?;
            return Ok(LiveOrderControlBegin::Complete(
                PersistedLiveOrderControlOutcome { snapshot, receipt },
            ));
        }

        let sequence = current.sequence.checked_add(1).ok_or_else(|| {
            LabError::ResourceLimit("live-order control sequence exhausted".into())
        })?;
        let enabled = false;
        update_live_order_control_state(
            &transaction,
            &current.boot_epoch,
            sequence,
            enabled,
            current.available,
            observed_at_utc,
            current.sequence,
        )?;
        if action == LiveOrderControlAction::Disable {
            super::automation_store::reject_unsent_live_work_in_control_transaction(
                &transaction,
                "LIVE_ORDER_SENDING_DISABLED",
                UtcTimestamp::parse_rfc3339(observed_at_utc)?,
                64,
            )?;
        }
        insert_live_order_control_pending(
            &transaction,
            PendingLiveOrderControlIntent {
                request_id,
                action,
                expected_sequence: expected_sequence_i64,
                expected_epoch,
                sequence,
                boot_epoch: &current.boot_epoch,
                observed_at_utc,
            },
        )?;
        let snapshot = PersistedLiveOrderControlSnapshot {
            boot_epoch: current.boot_epoch,
            sequence,
            live_trading_enabled: enabled,
            available: current.available,
        };
        transaction.commit().map_err(sql_error)?;
        Ok(LiveOrderControlBegin::Pending(snapshot))
    }

    pub(crate) fn live_order_control_finish(
        &mut self,
        request_id: &str,
        acknowledged: bool,
        denied_code: Option<&str>,
        observed_at_utc: &str,
    ) -> Result<PersistedLiveOrderControlOutcome, LabError> {
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let existing = read_live_order_control_intent(&transaction, request_id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown live-order control request".into()))?;
        if existing.status != "PENDING" {
            let receipt = existing.receipt()?;
            transaction.commit().map_err(sql_error)?;
            return Ok(PersistedLiveOrderControlOutcome {
                snapshot: read_live_order_control_snapshot(&self.connection)?,
                receipt,
            });
        }
        let is_enable = existing.action == LiveOrderControlAction::Enable;
        let enabled = acknowledged && is_enable;
        let current = read_live_order_control_snapshot(&transaction)?;
        let changed = transaction
            .execute(
                "UPDATE live_order_control_state SET live_trading_enabled=?1,updated_at_utc=?2 \
                 WHERE singleton=1 AND boot_epoch=?3 AND sequence=?4",
                params![
                    enabled,
                    observed_at_utc,
                    current.boot_epoch,
                    i64::try_from(existing.sequence).map_err(|_| {
                        LabError::DataCorrupt("negative live-order control sequence".into())
                    })?
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::Conflict(
                "live-order control changed before request acknowledgement".into(),
            ));
        }
        let outcome = if acknowledged {
            LiveOrderControlOutcome::Acknowledged
        } else {
            LiveOrderControlOutcome::Denied
        };
        let status = if acknowledged {
            "ACKNOWLEDGED"
        } else {
            "DENIED"
        };
        let denied_code = if acknowledged { None } else { denied_code };
        transaction
            .execute(
                "UPDATE live_order_control_intents SET status=?1,observed_at_utc=?2,denied_code=?3 \
                 WHERE request_id=?4 AND status='PENDING'",
                params![status, observed_at_utc, denied_code, request_id],
            )
            .map_err(sql_error)?;
        let receipt = LiveOrderControlReceipt {
            request_id: request_id.to_owned(),
            action: existing.action,
            outcome,
            sequence: existing.sequence,
            boot_epoch: existing.boot_epoch,
            observed_at_utc: observed_at_utc.to_owned(),
            denied_code: denied_code.map(str::to_owned),
        };
        let snapshot = read_live_order_control_snapshot(&transaction)?;
        transaction.commit().map_err(sql_error)?;
        Ok(PersistedLiveOrderControlOutcome { snapshot, receipt })
    }

    pub(crate) fn live_order_control_request(
        &self,
        request_id: &str,
    ) -> Result<Option<PersistedLiveOrderControlOutcome>, LabError> {
        let Some(existing) = read_live_order_control_intent(&self.connection, request_id)? else {
            return Ok(None);
        };
        if existing.status == "PENDING" {
            return Ok(None);
        }
        Ok(Some(PersistedLiveOrderControlOutcome {
            snapshot: read_live_order_control_snapshot(&self.connection)?,
            receipt: existing.receipt()?,
        }))
    }

    #[cfg(test)]
    pub(crate) fn live_order_control_fail_safe_closure(
        &self,
        request_id: &str,
        reason: &str,
    ) -> Result<Option<(String, u64)>, LabError> {
        self.connection
            .query_row(
                "SELECT boot_epoch,sequence FROM live_order_control_fail_safe_closures \
                 WHERE request_id=?1 AND reason=?2 ORDER BY sequence DESC LIMIT 1",
                params![request_id, reason],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(sql_error)?
            .map(|(epoch, sequence)| {
                Ok((
                    epoch,
                    u64::try_from(sequence).map_err(|_| {
                        LabError::DataCorrupt("negative fail-safe closure sequence".into())
                    })?,
                ))
            })
            .transpose()
    }

    pub(crate) fn reject_unsent_live_work_for_live_order_control(
        &mut self,
        reason: &str,
        observed_at_utc: &str,
        limit: usize,
    ) -> Result<usize, LabError> {
        let now = crate::contracts::UtcTimestamp::parse_rfc3339(observed_at_utc)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let rejected = super::automation_store::reject_unsent_live_work_in_control_transaction(
            &transaction,
            reason,
            now,
            limit,
        )?;
        transaction.commit().map_err(sql_error)?;
        Ok(rejected)
    }

    pub(crate) fn trading_control_snapshot(&self) -> Result<PersistedControlSnapshot, LabError> {
        read_snapshot(&self.connection)
    }

    pub(crate) fn apply_trading_control(
        &mut self,
        action: TradingControlAction,
        request_id: &str,
        expected_sequence: u64,
        context: ControlDecisionContext,
        observed_at_utc: &str,
    ) -> Result<PersistedControlOutcome, LabError> {
        let expected_sequence_i64 = i64::try_from(expected_sequence).map_err(|_| {
            LabError::InvalidConfig("expected_sequence exceeds SQLite integer capacity".into())
        })?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        if let Some(receipt) =
            read_receipt(&transaction, request_id, action, expected_sequence_i64)?
        {
            return Ok(PersistedControlOutcome {
                snapshot: read_snapshot(&transaction)?,
                receipt,
            });
        }

        let current = read_snapshot(&transaction)?;
        if current.sequence != expected_sequence {
            return Err(LabError::Conflict(format!(
                "trading control sequence conflict: expected {expected_sequence}, current {}",
                current.sequence
            )));
        }
        let decision = decide_transition(action, &current, context);
        let sequence = if matches!(decision.outcome, TradingControlOutcome::Acknowledged) {
            current.sequence.checked_add(1).ok_or_else(|| {
                LabError::ResourceLimit("trading control sequence exhausted".into())
            })?
        } else {
            current.sequence
        };
        let snapshot = PersistedControlSnapshot {
            state: decision.state,
            sequence,
            observed_at_utc: observed_at_utc.to_owned(),
            order_admission_blocked: decision.order_admission_blocked,
            emergency_latched: decision.emergency_latched,
        };
        if matches!(decision.outcome, TradingControlOutcome::Acknowledged) {
            let sequence_i64 = i64::try_from(sequence).map_err(|_| {
                LabError::ResourceLimit("trading control sequence exceeds SQLite capacity".into())
            })?;
            let changed = transaction
                .execute(
                    "UPDATE trading_control_state SET state = ?1, sequence = ?2, \
                     order_admission_blocked = ?3, emergency_latched = ?4, updated_at_utc = ?5 \
                     WHERE singleton = 1 AND sequence = ?6",
                    params![
                        state_name(snapshot.state),
                        sequence_i64,
                        snapshot.order_admission_blocked,
                        snapshot.emergency_latched,
                        observed_at_utc,
                        expected_sequence_i64,
                    ],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(LabError::Conflict(
                    "trading control state changed during compare-and-swap".into(),
                ));
            }
        }
        let receipt = TradingControlReceipt {
            request_id: request_id.to_owned(),
            action,
            outcome: decision.outcome,
            sequence,
            observed_at_utc: observed_at_utc.to_owned(),
            denied_code: decision.denied_code,
        };
        let outcome = PersistedControlOutcome { snapshot, receipt };
        let outcome_json = serde_json::to_string(&outcome)?;
        transaction
            .execute(
                "INSERT INTO trading_control_receipts(\
                 request_id, action, expected_sequence, resulting_sequence, outcome_json, created_at_utc\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    request_id,
                    action_name(action),
                    expected_sequence_i64,
                    i64::try_from(sequence).map_err(|_| LabError::ResourceLimit(
                        "trading control sequence exceeds SQLite capacity".into()
                    ))?,
                    outcome_json,
                    observed_at_utc,
                ],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok(outcome)
    }

    pub(crate) fn reconcile_trading_control(
        &self,
        request_id: &str,
        action: TradingControlAction,
        expected_sequence: u64,
    ) -> Result<Option<PersistedControlOutcome>, LabError> {
        let expected_sequence = i64::try_from(expected_sequence).map_err(|_| {
            LabError::InvalidConfig("expected_sequence exceeds SQLite integer capacity".into())
        })?;
        let Some(receipt) = read_receipt(&self.connection, request_id, action, expected_sequence)?
        else {
            return Ok(None);
        };
        Ok(Some(PersistedControlOutcome {
            snapshot: read_snapshot(&self.connection)?,
            receipt,
        }))
    }
}

fn read_receipt(
    connection: &rusqlite::Connection,
    request_id: &str,
    action: TradingControlAction,
    expected_sequence: i64,
) -> Result<Option<TradingControlReceipt>, LabError> {
    let replay = connection
        .query_row(
            "SELECT action, expected_sequence, outcome_json \
             FROM trading_control_receipts WHERE request_id = ?1",
            [request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((stored_action, stored_sequence, outcome_json)) = replay else {
        return Ok(None);
    };
    if stored_action != action_name(action) || stored_sequence != expected_sequence {
        return Err(LabError::Conflict(
            "trading control request_id was already used for a different command".into(),
        ));
    }
    serde_json::from_str::<PersistedControlOutcome>(&outcome_json)
        .map(|outcome| Some(outcome.receipt))
        .map_err(|error| {
            LabError::DataCorrupt(format!(
                "stored trading control receipt is invalid JSON: {error}"
            ))
        })
}

fn read_snapshot(connection: &rusqlite::Connection) -> Result<PersistedControlSnapshot, LabError> {
    connection
        .query_row(
            "SELECT state, sequence, order_admission_blocked, emergency_latched, updated_at_utc \
             FROM trading_control_state WHERE singleton = 1",
            [],
            |row| {
                let state = row.get::<_, String>(0)?;
                let sequence = row.get::<_, i64>(1)?;
                let order_admission_blocked = row.get::<_, bool>(2)?;
                let emergency_latched = row.get::<_, bool>(3)?;
                let observed_at_utc = row.get::<_, String>(4)?;
                Ok((
                    state,
                    sequence,
                    order_admission_blocked,
                    emergency_latched,
                    observed_at_utc,
                ))
            },
        )
        .map_err(sql_error)
        .and_then(
            |(state, sequence, order_admission_blocked, emergency_latched, observed_at_utc)| {
                Ok(PersistedControlSnapshot {
                    state: parse_state(&state)?,
                    sequence: u64::try_from(sequence).map_err(|_| {
                        LabError::DataCorrupt("negative trading control sequence".into())
                    })?,
                    observed_at_utc,
                    order_admission_blocked,
                    emergency_latched,
                })
            },
        )
}

const fn action_name(action: TradingControlAction) -> &'static str {
    match action {
        TradingControlAction::Status => "STATUS",
        TradingControlAction::Start => "START",
        TradingControlAction::Resume => "RESUME",
        TradingControlAction::Pause => "PAUSE",
        TradingControlAction::EmergencyStop => "EMERGENCY_STOP",
    }
}

const fn state_name(state: TradingControlState) -> &'static str {
    match state {
        TradingControlState::DisabledByConfig => "DISABLED_BY_CONFIG",
        TradingControlState::Standby => "STANDBY",
        TradingControlState::Running => "RUNNING",
        TradingControlState::Paused => "PAUSED",
        TradingControlState::EmergencyStopActive => "EMERGENCY_STOP_ACTIVE",
    }
}

fn parse_state(value: &str) -> Result<TradingControlState, LabError> {
    match value {
        "STANDBY" => Ok(TradingControlState::Standby),
        "RUNNING" => Ok(TradingControlState::Running),
        "PAUSED" => Ok(TradingControlState::Paused),
        "EMERGENCY_STOP_ACTIVE" => Ok(TradingControlState::EmergencyStopActive),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown persisted trading control state: {value}"
        ))),
    }
}

struct StoredLiveOrderControlIntent {
    request_id: String,
    action: LiveOrderControlAction,
    expected_sequence: u64,
    expected_epoch: String,
    sequence: u64,
    boot_epoch: String,
    status: String,
    observed_at_utc: String,
    denied_code: Option<String>,
}

impl StoredLiveOrderControlIntent {
    fn receipt(&self) -> Result<LiveOrderControlReceipt, LabError> {
        let outcome = match self.status.as_str() {
            "ACKNOWLEDGED" => LiveOrderControlOutcome::Acknowledged,
            "DENIED" => LiveOrderControlOutcome::Denied,
            "PENDING" => {
                return Err(LabError::OutcomeUnknown(
                    "live-order control intent has no durable receipt yet".into(),
                ));
            }
            _ => {
                return Err(LabError::DataCorrupt(
                    "unknown live-order control intent status".into(),
                ));
            }
        };
        Ok(LiveOrderControlReceipt {
            request_id: self.request_id.clone(),
            action: self.action,
            outcome,
            sequence: self.sequence,
            boot_epoch: self.boot_epoch.clone(),
            observed_at_utc: self.observed_at_utc.clone(),
            denied_code: self.denied_code.clone(),
        })
    }
}

fn read_live_order_control_snapshot(
    connection: &Connection,
) -> Result<PersistedLiveOrderControlSnapshot, LabError> {
    connection
        .query_row(
            "SELECT boot_epoch,sequence,live_trading_enabled,available \
             FROM live_order_control_state WHERE singleton=1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            },
        )
        .map_err(sql_error)
        .and_then(|(boot_epoch, sequence, live_trading_enabled, available)| {
            Ok(PersistedLiveOrderControlSnapshot {
                boot_epoch,
                sequence: u64::try_from(sequence).map_err(|_| {
                    LabError::DataCorrupt("negative live-order control sequence".into())
                })?,
                live_trading_enabled,
                available,
            })
        })
}

fn read_live_order_control_intent(
    connection: &Connection,
    request_id: &str,
) -> Result<Option<StoredLiveOrderControlIntent>, LabError> {
    let stored = connection
        .query_row(
            "SELECT action,expected_sequence,expected_epoch,sequence,boot_epoch,status,observed_at_utc,denied_code \
             FROM live_order_control_intents WHERE request_id=?1",
            [request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    stored
        .map(
            |(
                action,
                expected_sequence,
                expected_epoch,
                sequence,
                boot_epoch,
                status,
                observed_at_utc,
                denied_code,
            )| {
                Ok(StoredLiveOrderControlIntent {
                    request_id: request_id.to_owned(),
                    action: parse_live_order_control_action(&action)?,
                    expected_sequence: u64::try_from(expected_sequence).map_err(|_| {
                        LabError::DataCorrupt(
                            "negative live-order control expected sequence".into(),
                        )
                    })?,
                    expected_epoch,
                    sequence: u64::try_from(sequence).map_err(|_| {
                        LabError::DataCorrupt("negative live-order control sequence".into())
                    })?,
                    boot_epoch,
                    status,
                    observed_at_utc,
                    denied_code,
                })
            },
        )
        .transpose()
}

fn update_live_order_control_state(
    transaction: &Transaction<'_>,
    boot_epoch: &str,
    sequence: u64,
    enabled: bool,
    available: bool,
    observed_at_utc: &str,
    expected_sequence: u64,
) -> Result<(), LabError> {
    let sequence = i64::try_from(sequence).map_err(|_| {
        LabError::ResourceLimit("live-order control sequence exceeds SQLite capacity".into())
    })?;
    let expected_sequence = i64::try_from(expected_sequence).map_err(|_| {
        LabError::ResourceLimit("live-order control sequence exceeds SQLite capacity".into())
    })?;
    let changed = transaction
        .execute(
            "UPDATE live_order_control_state SET boot_epoch=?1,sequence=?2,live_trading_enabled=?3,available=?4,updated_at_utc=?5 \
             WHERE singleton=1 AND sequence=?6",
            params![boot_epoch, sequence, enabled, available, observed_at_utc, expected_sequence],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(LabError::Conflict(
            "live-order control state changed during compare-and-swap".into(),
        ));
    }
    Ok(())
}

fn persist_live_order_control_fail_safe_close(
    transaction: &Transaction<'_>,
    current: &PersistedLiveOrderControlSnapshot,
    request_id: &str,
    reason: &str,
    observed_at_utc: &str,
) -> Result<PersistedLiveOrderControlSnapshot, LabError> {
    let sequence = current
        .sequence
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("live-order control sequence exhausted".into()))?;
    update_live_order_control_state(
        transaction,
        &current.boot_epoch,
        sequence,
        false,
        current.available,
        observed_at_utc,
        current.sequence,
    )?;
    super::automation_store::reject_unsent_live_work_in_control_transaction(
        transaction,
        "LIVE_ORDER_SENDING_DISABLED",
        UtcTimestamp::parse_rfc3339(observed_at_utc)?,
        64,
    )?;
    transaction
        .execute(
            "INSERT INTO live_order_control_fail_safe_closures(boot_epoch,sequence,request_id,reason,observed_at_utc) \
             VALUES (?1,?2,?3,?4,?5)",
            params![
                current.boot_epoch,
                i64::try_from(sequence).map_err(|_| {
                    LabError::ResourceLimit("live-order control sequence exceeds SQLite capacity".into())
                })?,
                request_id,
                reason,
                observed_at_utc,
            ],
        )
        .map_err(sql_error)?;
    Ok(PersistedLiveOrderControlSnapshot {
        boot_epoch: current.boot_epoch.clone(),
        sequence,
        live_trading_enabled: false,
        available: current.available,
    })
}

#[derive(Clone, Copy)]
struct PendingLiveOrderControlIntent<'a> {
    request_id: &'a str,
    action: LiveOrderControlAction,
    expected_sequence: i64,
    expected_epoch: &'a str,
    sequence: u64,
    boot_epoch: &'a str,
    observed_at_utc: &'a str,
}

fn insert_live_order_control_pending(
    transaction: &Transaction<'_>,
    intent: PendingLiveOrderControlIntent<'_>,
) -> Result<(), LabError> {
    transaction
        .execute(
            "INSERT INTO live_order_control_intents( \
                 request_id,action,expected_sequence,expected_epoch,sequence,boot_epoch,status,observed_at_utc,denied_code \
             ) VALUES (?1,?2,?3,?4,?5,?6,'PENDING',?7,NULL)",
            params![
                intent.request_id,
                live_order_control_action_name(intent.action),
                intent.expected_sequence,
                intent.expected_epoch,
                i64::try_from(intent.sequence).map_err(|_| {
                    LabError::ResourceLimit("live-order control sequence exceeds SQLite capacity".into())
                })?,
                intent.boot_epoch,
                intent.observed_at_utc,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

fn insert_live_order_control_intent(
    transaction: &Transaction<'_>,
    request_id: &str,
    action: LiveOrderControlAction,
    expected_sequence: i64,
    expected_epoch: &str,
    receipt: &LiveOrderControlReceipt,
    status: &str,
) -> Result<(), LabError> {
    transaction
        .execute(
            "INSERT INTO live_order_control_intents( \
                 request_id,action,expected_sequence,expected_epoch,sequence,boot_epoch,status,observed_at_utc,denied_code \
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                request_id,
                live_order_control_action_name(action),
                expected_sequence,
                expected_epoch,
                i64::try_from(receipt.sequence).map_err(|_| {
                    LabError::ResourceLimit("live-order control sequence exceeds SQLite capacity".into())
                })?,
                receipt.boot_epoch,
                status,
                receipt.observed_at_utc,
                receipt.denied_code,
            ],
        )
        .map_err(sql_error)?;
    Ok(())
}

const fn live_order_control_action_name(action: LiveOrderControlAction) -> &'static str {
    match action {
        LiveOrderControlAction::Enable => "ENABLE",
        LiveOrderControlAction::Disable => "DISABLE",
        LiveOrderControlAction::Status | LiveOrderControlAction::Request => "INVALID",
    }
}

fn parse_live_order_control_action(value: &str) -> Result<LiveOrderControlAction, LabError> {
    match value {
        "ENABLE" => Ok(LiveOrderControlAction::Enable),
        "DISABLE" => Ok(LiveOrderControlAction::Disable),
        _ => Err(LabError::DataCorrupt(format!(
            "unknown persisted live-order control action: {value}"
        ))),
    }
}

//! Bounded subscription selection and durable private transport audit evidence.

use super::{Store, enum_text, json_error, sql_error, timestamp_ms, u64_to_i64};
use crate::contracts::ENGINE_VERSION;
use crate::contracts::{
    CandleInterval, CandleObservation, ContentHash, FrozenPolicyRevision, LabError, MarketId,
    ObservationId, PaperRecoveryCause, PaperSessionState, PaperSessionView,
    PrivateStreamObservation, ProviderAbsenceAnchor, StoredPrivateStreamObservation, UtcTimestamp,
    observation_digest,
};
use crate::paper::{
    MAX_PAPER_MARKETS_PER_SESSION, PAPER_CHECKPOINT_SCHEMA_VERSION, PaperMarketCheckpoint,
};
use rusqlite::{OptionalExtension, params};
use std::collections::BTreeSet;

const MAX_ACTIVE_SESSION_SCAN: i64 = 1024;
const MAX_PRIVATE_STREAM_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const MAX_PRIVATE_PAYLOAD_NODES: usize = 50_000;

impl Store {
    /// Select canonical active requirements without changing session state.
    /// # Errors
    /// Rejects excessive scans, malformed frozen policy or persistence failure.
    pub(crate) fn active_realtime_market_requirements(
        &self,
    ) -> Result<BTreeSet<(MarketId, CandleInterval)>, LabError> {
        let paper_count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM paper_sessions WHERE state IN ('RUNNING','RECOVERY_REQUIRED')",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if paper_count > MAX_ACTIVE_SESSION_SCAN {
            return Err(LabError::ResourceLimit(
                "active paper subscription scan limit".into(),
            ));
        }
        let running_session_ids = self.active_running_paper_session_ids()?;
        let mut requirements = BTreeSet::new();
        let mut strict_loaded_ids = BTreeSet::new();
        for (sql_session_id, session_checkpoints) in self.active_paper_checkpoints_for_feed()? {
            if let Some(running_session_id) = running_session_ids
                .iter()
                .find(|session_id| session_id.as_str() == sql_session_id.as_str())
                && session_checkpoints
                    .first()
                    .is_some_and(|checkpoint| &checkpoint.session_id == running_session_id)
            {
                // Strict loading is reusable only when SQL and view IDs agree.
                strict_loaded_ids.insert(running_session_id.clone());
            }
            for checkpoint in session_checkpoints {
                requirements.insert((
                    checkpoint.market.clone(),
                    checkpoint.plan.spec.decision_interval,
                ));
                for interval in checkpoint.source_cursors.keys().copied() {
                    requirements.insert((checkpoint.market.clone(), interval));
                }
            }
        }
        for poll in
            self.active_running_paper_polls_for_ids(&running_session_ids, &strict_loaded_ids)
        {
            requirements.insert((poll.market, poll.interval));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT market,decision_interval,frozen_policy_json FROM automation_sessions \
             WHERE state IN ('RUNNING','NEEDS_RECONCILIATION') ORDER BY session_id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([MAX_ACTIVE_SESSION_SCAN + 1], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?;
        for (index, row) in rows.enumerate() {
            if i64::try_from(index).unwrap_or(i64::MAX) >= MAX_ACTIVE_SESSION_SCAN {
                return Err(LabError::ResourceLimit(
                    "active automation subscription scan limit".into(),
                ));
            }
            let (market, interval, policy) = row.map_err(sql_error)?;
            let market = MarketId::parse_upbit(&market)?;
            let interval = CandleInterval::parse_code(&interval)?;
            let policy: FrozenPolicyRevision = serde_json::from_str(&policy).map_err(json_error)?;
            policy.definition.validate()?;
            requirements.insert((market.clone(), interval));
            for source in policy.definition.declared_source_intervals() {
                requirements.insert((market.clone(), source));
            }
        }
        Ok(requirements)
    }

    /// Resolve the oldest exact candle already accepted by active paper consumers.
    /// This is the cold-restart backfill seed; failed/latest REST pages never seed it.
    pub(crate) fn provider_feed_accepted_anchor(
        &self,
        market: &MarketId,
        interval: CandleInterval,
    ) -> Result<Option<ProviderAbsenceAnchor>, LabError> {
        let mut candidates = Vec::new();
        for checkpoint in self
            .active_paper_checkpoints_for_feed()?
            .into_iter()
            .flat_map(|(_, checkpoints)| checkpoints)
        {
            if checkpoint.market != *market {
                continue;
            }
            if checkpoint.plan.spec.decision_interval == interval
                && let Some(anchor) = self.anchor_for_cursor(
                    &checkpoint.market,
                    interval,
                    checkpoint.last_finalized_candle_at,
                    checkpoint.last_finalized_candle_digest.as_ref(),
                )?
            {
                candidates.push(anchor);
            }
            if let Some(cursor) = checkpoint.source_cursors.get(&interval)
                && let Some(anchor) = self.anchor_for_cursor(
                    &checkpoint.market,
                    interval,
                    cursor.last_bar_close_utc,
                    cursor.last_bar_digest.as_ref(),
                )?
            {
                candidates.push(anchor);
            }
        }
        candidates.sort_by_key(|anchor| anchor.open_time_utc);
        if let Some(first) = candidates.first()
            && candidates
                .iter()
                .take_while(|candidate| candidate.open_time_utc == first.open_time_utc)
                .any(|candidate| {
                    candidate.observation_id != first.observation_id
                        || candidate.content_digest != first.content_digest
                })
        {
            return Err(LabError::Conflict(
                "active paper checkpoints disagree at one accepted candle boundary".into(),
            ));
        }
        Ok(candidates.into_iter().next())
    }

    /// Load the immutable candle row behind one accepted cursor anchor. The live
    /// provider page can carry revised economics at the same slot; the realtime
    /// cache uses this row to preserve the existing canonical history pin.
    pub(crate) fn provider_feed_accepted_observation(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        anchor: &ProviderAbsenceAnchor,
    ) -> Result<CandleObservation, LabError> {
        let json = self
            .connection
            .query_row(
                "SELECT observation_json FROM candle_observations \
                 WHERE id=?1 AND market=?2 AND interval=?3 AND open_time_ms=?4 \
                 AND content_digest=?5 AND completed=1",
                params![
                    anchor.observation_id.as_str(),
                    market.code(),
                    enum_text(&interval)?,
                    timestamp_ms(anchor.open_time_utc),
                    anchor.content_digest.as_str(),
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| {
                LabError::DataCorrupt(
                    "accepted provider recovery anchor has no immutable candle row".into(),
                )
            })?;
        let observation: CandleObservation = serde_json::from_str(&json).map_err(json_error)?;
        if observation.id != anchor.observation_id
            || observation.candle.market != market.code()
            || observation.candle.interval != interval
            || observation.candle.open_time_utc != anchor.open_time_utc
            || observation.content_digest != anchor.content_digest
            || observation_digest(&observation.candle)? != anchor.content_digest
            || !observation.candle.completed
        {
            return Err(LabError::DataCorrupt(
                "accepted provider recovery anchor row failed identity validation".into(),
            ));
        }
        Ok(observation)
    }

    fn active_paper_checkpoints_for_feed(
        &self,
    ) -> Result<Vec<(String, Vec<PaperMarketCheckpoint>)>, LabError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM paper_sessions WHERE state IN ('RUNNING','RECOVERY_REQUIRED')",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if count > MAX_ACTIVE_SESSION_SCAN {
            return Err(LabError::ResourceLimit(
                "paper feed observer scan limit".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT session_id,state,view_json FROM paper_sessions \
                 WHERE state IN ('RUNNING','RECOVERY_REQUIRED') ORDER BY session_id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([MAX_ACTIVE_SESSION_SCAN + 1], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut checkpoints = Vec::new();
        for (index, row) in rows.enumerate() {
            if i64::try_from(index).unwrap_or(i64::MAX) >= MAX_ACTIVE_SESSION_SCAN {
                return Err(LabError::ResourceLimit(
                    "paper feed observer scan limit".into(),
                ));
            }
            let (sql_session_id, state, view_json) = row.map_err(sql_error)?;
            let view: PaperSessionView = serde_json::from_str(&view_json).map_err(json_error)?;
            let is_running = state == "RUNNING" && view.state == PaperSessionState::Running;
            let is_gap_observer = state == "RECOVERY_REQUIRED"
                && view.state == PaperSessionState::RecoveryRequired
                && matches!(
                    view.recovery_cause,
                    Some(PaperRecoveryCause::FeedGap | PaperRecoveryCause::ProviderGap)
                );
            if !is_running && !is_gap_observer {
                continue;
            }
            let session_checkpoints = load_realtime_paper_checkpoints(&self.connection, &view)?;
            checkpoints.push((sql_session_id, session_checkpoints));
        }
        Ok(checkpoints)
    }

    fn anchor_for_cursor(
        &self,
        market: &MarketId,
        interval: CandleInterval,
        last_close: Option<UtcTimestamp>,
        last_digest: Option<&ContentHash>,
    ) -> Result<Option<ProviderAbsenceAnchor>, LabError> {
        match (last_close, last_digest) {
            (None, None) => Ok(None),
            (Some(close), Some(digest)) => {
                let open = close.0 - interval.duration();
                let mut statement = self
                    .connection
                    .prepare(
                        "SELECT id,observation_json FROM candle_observations \
                         WHERE market=?1 AND interval=?2 AND close_time_ms=?3 AND content_digest=?4 AND completed=1 \
                         ORDER BY id LIMIT 2",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![
                            market.code(),
                            enum_text(&interval)?,
                            timestamp_ms(close),
                            digest.as_str()
                        ],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .map_err(sql_error)?;
                let rows = rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)?;
                let [(id, json)] = rows.as_slice() else {
                    return Err(LabError::DataCorrupt(
                        "active paper cursor has no unique matching completed candle observation"
                            .into(),
                    ));
                };
                let observation: crate::contracts::CandleObservation =
                    serde_json::from_str(json).map_err(json_error)?;
                let observation_id = ObservationId::new(id.clone())?;
                if observation.id != observation_id
                    || observation.candle.market != market.code()
                    || observation.candle.interval != interval
                    || observation.candle.open_time_utc.0 != open
                    || observation.candle.close_time_utc != close
                    || observation.content_digest != *digest
                    || observation_digest(&observation.candle)? != *digest
                    || !observation.candle.completed
                {
                    return Err(LabError::DataCorrupt(
                        "active paper cursor disagrees with its accepted candle observation".into(),
                    ));
                }
                Ok(Some(ProviderAbsenceAnchor {
                    observation_id,
                    open_time_utc: observation.candle.open_time_utc,
                    content_digest: observation.content_digest,
                }))
            }
            _ => Err(LabError::DataCorrupt(
                "active paper cursor has a partial timestamp/digest identity".into(),
            )),
        }
    }

    /// Persist provider facts before readiness acknowledgement or reconciliation.
    /// Identical replay returns the original sequence and first receipt metadata.
    /// # Errors
    /// Rejects malformed/secret-bearing observations, identity conflicts or pressure.
    pub(crate) fn record_private_stream_observation(
        &mut self,
        observation: &PrivateStreamObservation,
    ) -> Result<u64, LabError> {
        validate_observation(observation)?;
        let payload = serde_json::to_string(&observation.payload).map_err(json_error)?;
        let facts = serde_json::to_vec(&(
            observation.kind,
            observation.provider_at_utc,
            &observation.payload,
        ))
        .map_err(json_error)?;
        let digest = ContentHash::of_bytes(&facts);
        let transaction = self.connection.transaction().map_err(sql_error)?;
        let previous = transaction
            .query_row(
                "SELECT sequence,facts_digest FROM private_stream_observations \
             WHERE account_identity=?1 AND event_key=?2",
                params![observation.account_identity, observation.event_key],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some((sequence, expected)) = previous {
            if expected != digest.as_str() {
                return Err(LabError::DataCorrupt(
                    "private stream identity conflicts with previously recorded provider facts"
                        .into(),
                ));
            }
            return u64::try_from(sequence).map_err(|_| {
                LabError::DataCorrupt("negative private stream observation sequence".into())
            });
        }
        transaction
            .execute(
                "INSERT INTO private_stream_observations(\
                account_identity,event_key,generation,kind,provider_at_utc,received_at_utc,\
                facts_digest,payload_json,transport_json,record_digest) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    observation.account_identity,
                    observation.event_key,
                    u64_to_i64(observation.generation)?,
                    enum_text(&observation.kind)?,
                    observation.provider_at_utc.map(|at| at.0.to_rfc3339()),
                    observation.received_at_utc.0.to_rfc3339(),
                    digest.as_str(),
                    payload,
                    observation
                        .transport
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(json_error)?,
                    ""
                ],
            )
            .map_err(sql_error)?;
        let sequence = u64::try_from(transaction.last_insert_rowid())
            .map_err(|_| LabError::DataCorrupt("invalid private stream sequence".into()))?;
        let record_digest = ContentHash::of_bytes(
            &serde_json::to_vec(&(sequence, observation)).map_err(json_error)?,
        );
        let changed = transaction
            .execute(
                "UPDATE private_stream_observations SET record_digest=?1 WHERE sequence=?2",
                params![record_digest.as_str(), u64_to_i64(sequence)?],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(LabError::DataCorrupt(
                "private stream digest publication mismatch".into(),
            ));
        }
        transaction.commit().map_err(sql_error)?;
        Ok(sequence)
    }

    /// Read bounded account-scoped transport history; this is not a fill ledger.
    /// # Errors
    /// Rejects invalid page scope, corrupt persisted facts or storage failure.
    pub(crate) fn private_stream_observations(
        &self,
        account_identity: &str,
        after_sequence: u64,
        limit: u32,
    ) -> Result<Vec<StoredPrivateStreamObservation>, LabError> {
        if account_identity.is_empty()
            || account_identity.len() > 256
            || !(1..=100).contains(&limit)
        {
            return Err(LabError::InvalidConfig(
                "invalid private stream history scope".into(),
            ));
        }
        let mut statement = self.connection.prepare(
            "SELECT sequence,event_key,generation,kind,provider_at_utc,received_at_utc,payload_json,facts_digest,transport_json,record_digest \
             FROM private_stream_observations WHERE account_identity=?1 AND sequence>?2 \
             ORDER BY sequence LIMIT ?3",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    account_identity,
                    u64_to_i64(after_sequence)?,
                    i64::from(limit)
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, String>(9)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        for row in rows {
            let (
                sequence,
                event_key,
                generation,
                kind,
                provider,
                received,
                payload,
                expected_digest,
                transport,
                expected_record_digest,
            ) = row.map_err(sql_error)?;
            bytes = bytes.checked_add(payload.len()).ok_or_else(|| {
                LabError::ResourceLimit("private history byte count overflow".into())
            })?;
            if bytes > MAX_PRIVATE_STREAM_PAYLOAD_BYTES {
                return Err(LabError::ResourceLimit(
                    "private history page exceeds byte budget".into(),
                ));
            }
            let observation = PrivateStreamObservation {
                account_identity: account_identity.into(),
                generation: u64::try_from(generation).map_err(|_| {
                    LabError::DataCorrupt("negative private stream generation".into())
                })?,
                kind: serde_json::from_value(serde_json::Value::String(kind))
                    .map_err(json_error)?,
                event_key,
                provider_at_utc: provider
                    .as_deref()
                    .map(UtcTimestamp::parse_rfc3339)
                    .transpose()?,
                received_at_utc: UtcTimestamp::parse_rfc3339(&received)?,
                transport: transport
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(json_error)?,
                payload: serde_json::from_str(&payload).map_err(json_error)?,
            };
            result.push(verify_stored_observation(
                sequence,
                observation,
                &expected_digest,
                &expected_record_digest,
            )?);
        }
        Ok(result)
    }

    /// Recover the last durable, account-scoped REST coverage watermark.
    /// # Errors
    /// Rejects invalid scope, corrupt journal state or storage failure.
    pub(crate) fn private_stream_latest_reconciliation(
        &self,
        account_identity: &str,
    ) -> Result<Option<StoredPrivateStreamObservation>, LabError> {
        if account_identity.is_empty() || account_identity.len() > 256 {
            return Err(LabError::InvalidConfig(
                "invalid private reconciliation scope".into(),
            ));
        }
        let key = self
            .connection
            .query_row(
                "SELECT event_key FROM private_stream_observations WHERE account_identity=?1 \
             AND kind='REST_RECONCILED' ORDER BY sequence DESC LIMIT 1",
                [account_identity],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        key.map(|key| self.private_stream_observation_by_key(account_identity, &key))
            .transpose()
            .map(Option::flatten)
    }

    /// Read back the exact key after an admitted journal operation lost its reply.
    /// # Errors
    /// Rejects invalid scope, corrupt journal state or storage failures.
    pub(crate) fn private_stream_observation_by_key(
        &self,
        account_identity: &str,
        event_key: &str,
    ) -> Result<Option<StoredPrivateStreamObservation>, LabError> {
        if account_identity.is_empty()
            || account_identity.len() > 256
            || event_key.is_empty()
            || event_key.len() > 256
        {
            return Err(LabError::InvalidConfig(
                "invalid private stream readback identity".into(),
            ));
        }
        let sequence = self.connection.query_row(
            "SELECT sequence FROM private_stream_observations WHERE account_identity=?1 AND event_key=?2",
            params![account_identity, event_key], |row| row.get::<_, i64>(0),
        ).optional().map_err(sql_error)?;
        let Some(sequence) = sequence else {
            return Ok(None);
        };
        let sequence = u64::try_from(sequence)
            .ok()
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| {
                LabError::DataCorrupt("invalid private journal readback sequence".into())
            })?;
        Ok(self
            .private_stream_observations(account_identity, sequence, 1)?
            .into_iter()
            .next())
    }
}

fn load_realtime_paper_checkpoints(
    connection: &rusqlite::Connection,
    view: &PaperSessionView,
) -> Result<Vec<PaperMarketCheckpoint>, LabError> {
    if view.markets.is_empty()
        || view.markets.len() > usize::try_from(MAX_PAPER_MARKETS_PER_SESSION).unwrap_or(usize::MAX)
    {
        return Err(LabError::DataCorrupt(
            "active paper view has an invalid market count".into(),
        ));
    }
    let mut statement = connection
        .prepare(
            "SELECT schema_version,engine_version,checkpoint_digest,checkpoint_json \
             FROM paper_market_checkpoints WHERE session_id=?1 ORDER BY market_order LIMIT ?2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![
                view.session_id.as_str(),
                i64::from(MAX_PAPER_MARKETS_PER_SESSION) + 1,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut checkpoints = Vec::new();
    for row in rows {
        let (schema, engine, expected_digest, json) = row.map_err(sql_error)?;
        if schema != PAPER_CHECKPOINT_SCHEMA_VERSION
            || engine != ENGINE_VERSION
            || ContentHash::of_bytes(json.as_bytes()).as_str() != expected_digest
        {
            return Err(LabError::DataCorrupt(
                "active paper checkpoint schema, engine or digest is invalid".into(),
            ));
        }
        let checkpoint: PaperMarketCheckpoint = serde_json::from_str(&json).map_err(json_error)?;
        let market = view
            .markets
            .iter()
            .find(|snapshot| snapshot.market == checkpoint.market)
            .ok_or_else(|| {
                LabError::DataCorrupt("paper checkpoint market is absent from its view".into())
            })?;
        if checkpoint.session_id != view.session_id
            || checkpoint.policy_ref != view.policy_ref
            || checkpoint.plan.spec.decision_interval != view.decision_interval
        {
            return Err(LabError::DataCorrupt(
                "active paper checkpoint identity differs from its session view".into(),
            ));
        }
        checkpoint.verify_integrity(
            &view.session_id,
            &checkpoint.market,
            &view.policy_ref,
            market.initial_cash,
        )?;
        checkpoints.push(checkpoint);
    }
    if checkpoints.len() != view.markets.len() {
        return Err(LabError::DataCorrupt(
            "active paper checkpoint count differs from its session view".into(),
        ));
    }
    Ok(checkpoints)
}

fn verify_stored_observation(
    sequence: i64,
    observation: PrivateStreamObservation,
    expected_facts: &str,
    expected_record: &str,
) -> Result<StoredPrivateStreamObservation, LabError> {
    validate_observation(&observation)?;
    let sequence = u64::try_from(sequence)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| LabError::DataCorrupt("invalid private stream sequence".into()))?;
    let envelope = serde_json::to_vec(&(sequence, &observation)).map_err(json_error)?;
    if ContentHash::of_bytes(&envelope).as_str() != expected_record {
        return Err(LabError::DataCorrupt(
            "private stream envelope digest mismatch".into(),
        ));
    }
    let facts = serde_json::to_vec(&(
        observation.kind,
        observation.provider_at_utc,
        &observation.payload,
    ))
    .map_err(json_error)?;
    if ContentHash::of_bytes(&facts).as_str() != expected_facts {
        return Err(LabError::DataCorrupt(
            "private stream facts digest mismatch".into(),
        ));
    }
    Ok(StoredPrivateStreamObservation {
        sequence,
        observation,
    })
}

fn validate_observation(value: &PrivateStreamObservation) -> Result<(), LabError> {
    let is_websocket = matches!(
        value.kind,
        crate::contracts::PrivateStreamObservationKind::OrderObserved
            | crate::contracts::PrivateStreamObservationKind::AssetObserved
    );
    if is_websocket != value.transport.is_some() {
        return Err(LabError::InvalidConfig(
            "private observation transport provenance missing or unexpected".into(),
        ));
    }
    if let Some(transport) = &value.transport
        && (transport.connection_epoch != value.generation
            || transport.message_sequence == 0
            || transport.provider_at != value.provider_at_utc
            || transport.received_at != value.received_at_utc)
    {
        return Err(LabError::InvalidConfig(
            "private transport metadata mismatch".into(),
        ));
    }
    if value.account_identity.is_empty()
        || value.account_identity.len() > 256
        || value.account_identity.chars().any(char::is_control)
        || value.event_key.is_empty()
        || value.event_key.len() > 256
        || value.event_key.chars().any(char::is_control)
        || value.generation == 0
    {
        return Err(LabError::InvalidConfig(
            "invalid private stream observation identity".into(),
        ));
    }
    let bytes = serde_json::to_vec(&value.payload).map_err(json_error)?;
    if bytes.len() > MAX_PRIVATE_STREAM_PAYLOAD_BYTES {
        return Err(LabError::ResourceLimit(
            "private stream observation exceeds byte budget".into(),
        ));
    }
    let mut pending = vec![&value.payload];
    let mut visited = 0_usize;
    while let Some(item) = pending.pop() {
        visited += 1;
        if visited > MAX_PRIVATE_PAYLOAD_NODES {
            return Err(LabError::ResourceLimit(
                "private stream observation exceeds node budget".into(),
            ));
        }
        match item {
            serde_json::Value::Object(object) => {
                for (key, item) in object {
                    if [
                        "authorization",
                        "access_key",
                        "secret_key",
                        "access_token",
                        "refresh_token",
                        "api_key",
                        "cookie",
                        "jwt",
                    ]
                    .iter()
                    .any(|denied| key.eq_ignore_ascii_case(denied))
                    {
                        return Err(LabError::InvalidConfig(
                            "credential-bearing fields are forbidden in private stream observations".into()));
                    }
                    pending.push(item);
                }
            }
            serde_json::Value::Array(items) => pending.extend(items),
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => {}
        }
    }
    Ok(())
}

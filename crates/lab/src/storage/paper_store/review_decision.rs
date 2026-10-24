//! Read-time paper provenance and source-backed audit input admission.

use crate::contracts::{
    CandleObservation, ContentHash, FillRecord, LabError, ObservationId, OrderRecord,
    PaperDecisionDataQuality, PaperDecisionReview, PaperObservationProvenance,
    PaperRetainedReceipt, PaperReviewSelection, PaperReviewUnavailableReason, RawObjectRef,
    SavedFactCaptureStatus, SignalRecord, UtcTimestamp,
};
use crate::storage::{Store, json_error, sql_error, u64_to_i64};
use rusqlite::{OptionalExtension, params};
use std::collections::{BTreeMap, BTreeSet};

const MAX_LINKED_PAPER_ORDER_FACTS: usize = 256;
const MAX_LINKED_PAPER_ORDER_BYTES: usize = 512 * 1024;
const MAX_LINKED_PAPER_FILL_FACTS: usize = 512;
const MAX_LINKED_PAPER_FILL_BYTES: usize = 512 * 1024;
const MAX_LINKED_PAPER_SCAN_EVENTS: usize = 50_000;
const MAX_LINKED_PAPER_SCAN_BYTES: usize = 32 * 1024 * 1024;

struct LinkedOrderHistory {
    creation_by_id: BTreeMap<String, OrderRecord>,
    latest_by_id: BTreeMap<String, OrderRecord>,
}

impl Store {
    /// Resolve one saved event without driving feed, reconciliation or evaluation.
    /// # Errors
    /// Rejects invalid prefixes, mismatched signals and corrupt saved identities.
    pub fn paper_review_decision(
        &self,
        selection: &PaperReviewSelection,
    ) -> Result<PaperDecisionReview, LabError> {
        selection.validate()?;
        let page = self.paper_review_events(
            &selection.session_id,
            Some(selection.event_sequence - 1),
            Some(selection.as_of_sequence),
            1,
        )?;
        let event = page.page.items.into_iter().next().ok_or_else(|| {
            LabError::DataCorrupt("selected paper event is absent from its saved prefix".into())
        })?;
        if event.sequence != selection.event_sequence {
            return Err(LabError::DataCorrupt(
                "paper review event identity mismatch".into(),
            ));
        }
        let selected = select_signal(&event.signals, selection)?;
        let policy_ref = selected
            .and_then(|signal| signal.policy_ref.as_ref())
            .unwrap_or(&page.configuration.policy_ref);
        if policy_ref != &page.configuration.policy_ref {
            return Err(LabError::DataCorrupt(
                "paper signal/session policy mismatch".into(),
            ));
        }
        let policy = self
            .load_policy_revision(policy_ref)?
            .ok_or_else(|| {
                LabError::DataCorrupt("saved paper policy revision is unavailable".into())
            })?
            .snapshot;
        let transaction = self.connection.unchecked_transaction().map_err(sql_error)?;
        let raw: String = transaction
            .query_row(
                "SELECT event_json FROM paper_session_events WHERE session_id=?1 AND sequence=?2",
                params![
                    selection.session_id.as_str(),
                    u64_to_i64(selection.event_sequence)?
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let (orders, fills) = match selected {
            Some(signal) => linked_paper_lifecycle_facts(&transaction, selection, signal)?,
            None => (Vec::new(), Vec::new()),
        };
        if selected.is_some() && event.preparation.is_some() {
            return Err(LabError::DataCorrupt(
                "paper event contains both a decision signal and deferred preparation".into(),
            ));
        }
        let (sources, completed) = paper_source_observations(
            &transaction,
            selected,
            page.configuration.decision_interval,
            event.source_event_at,
        )?;
        let quality = decision_quality(selected, completed);
        let recorded_decision = if let Some(preparation) = &event.preparation {
            crate::reporting::decision_explanation::paper_preparation_explanation(
                &page.configuration.policy_ref,
                preparation,
            )
        } else {
            crate::reporting::decision_explanation::paper_recorded_decision(
                selected,
                Some(&page.configuration.policy_ref),
                &orders,
                &fills,
            )?
        };
        transaction.commit().map_err(sql_error)?;
        let non_entry_observations = non_entry_observations(selected);
        Ok(PaperDecisionReview {
            schema_version: "1.0".into(),
            selection: selection.clone(),
            configuration: page.configuration,
            selected_signal_id: selected.map(|signal| signal.signal_id.clone()),
            event,
            original_event_digest: ContentHash::of_bytes(raw.as_bytes()),
            policy,
            source_observations: sources,
            data_quality: quality,
            non_entry_observations,
            recorded_decision: Some(recorded_decision),
        })
    }
}

fn paper_source_observations(
    connection: &rusqlite::Connection,
    signal: Option<&SignalRecord>,
    decision_interval: crate::contracts::CandleInterval,
    source_event_at: Option<UtcTimestamp>,
) -> Result<(Vec<PaperObservationProvenance>, Option<bool>), LabError> {
    let Some(signal) = signal else {
        return Ok((Vec::new(), None));
    };
    if signal.source_bar_ids.len() > 64 {
        return Err(LabError::ResourceLimit(
            "paper signal source link budget exceeded".into(),
        ));
    }
    let mut sources = Vec::new();
    for id in &signal.source_bar_ids {
        if sources
            .iter()
            .any(|source: &PaperObservationProvenance| &source.observation_id == id)
        {
            continue;
        }
        let observation = load_observation(connection, id)?;
        sources.push(provenance(
            connection,
            id,
            observation.as_ref(),
            decision_interval,
            source_event_at,
        )?);
    }
    let completed = signal
        .source_bar_ids
        .first()
        .map(|id| load_observation(connection, id))
        .transpose()?
        .flatten()
        .map(|observation| {
            observation.candle.completed
                && observation.candle.close_time_utc <= signal.decision_available_at
        });
    Ok((sources, completed))
}

fn linked_paper_lifecycle_facts(
    connection: &rusqlite::Connection,
    selection: &PaperReviewSelection,
    signal: &SignalRecord,
) -> Result<(Vec<OrderRecord>, Vec<FillRecord>), LabError> {
    validate_linked_paper_event_json(connection, selection)?;
    let order_facts = load_linked_paper_order_facts(connection, selection, signal)?;
    let history = fold_linked_paper_orders(order_facts)?;
    if history.latest_by_id.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let fills = load_linked_paper_fills(
        connection,
        selection,
        signal,
        &history.latest_by_id,
        &history.creation_by_id,
    )?;
    Ok((history.latest_by_id.into_values().collect(), fills))
}

fn load_linked_paper_order_facts(
    connection: &rusqlite::Connection,
    selection: &PaperReviewSelection,
    signal: &SignalRecord,
) -> Result<Vec<(u64, u64, OrderRecord)>, LabError> {
    let order_limit = i64::try_from(MAX_LINKED_PAPER_ORDER_FACTS + 1)
        .map_err(|_| LabError::ResourceLimit("paper linked-order limit overflow".into()))?;
    let mut order_facts = Vec::<(u64, u64, OrderRecord)>::new();
    let mut order_bytes = 0_usize;
    {
        let mut statement = connection
            .prepare(
                "WITH linked AS (\
                     SELECT e.sequence AS event_sequence, 0 AS source_order, fact.value AS value \
                     FROM paper_session_events e \
                     JOIN json_each(e.event_json,'$.orders') fact \
                     WHERE e.session_id=?1 AND e.sequence BETWEEN ?2 AND ?3 \
                       AND json_extract(fact.value,'$.parent_signal_id')=?4 \
                     UNION ALL \
                     SELECT e.sequence AS event_sequence, 1 AS source_order, fact.value AS value \
                     FROM paper_session_events e \
                     JOIN json_each(e.event_json,'$.order_events') fact \
                     WHERE e.session_id=?1 AND e.sequence BETWEEN ?2 AND ?3 \
                       AND json_extract(fact.value,'$.parent_signal_id')=?4 \
                 ) \
                 SELECT event_sequence,source_order,length(CAST(value AS BLOB)),value \
                 FROM linked ORDER BY event_sequence,source_order,value LIMIT ?5",
            )
            .map_err(sql_error)?;
        let mut rows = statement
            .query(params![
                selection.session_id.as_str(),
                u64_to_i64(selection.event_sequence)?,
                u64_to_i64(selection.as_of_sequence)?,
                signal.signal_id.as_str(),
                order_limit,
            ])
            .map_err(sql_error)?;
        let mut count = 0_usize;
        while let Some(row) = rows.next().map_err(sql_error)? {
            if count == MAX_LINKED_PAPER_ORDER_FACTS {
                return Err(LabError::ResourceLimit(
                    "paper decision linked-order fact cap exceeded".into(),
                ));
            }
            let event_sequence: i64 = row.get(0).map_err(sql_error)?;
            let _source_order: i64 = row.get(1).map_err(sql_error)?;
            let bytes: i64 = row.get(2).map_err(sql_error)?;
            order_bytes = add_linked_fact_bytes(
                order_bytes,
                bytes,
                MAX_LINKED_PAPER_ORDER_BYTES,
                "paper linked-order bytes",
            )?;
            let raw: String = row.get(3).map_err(sql_error)?;
            let event_sequence = sql_sequence(event_sequence)?;
            if event_sequence < selection.event_sequence
                || event_sequence > selection.as_of_sequence
            {
                return Err(LabError::DataCorrupt(
                    "paper linked order escaped the selected event prefix".into(),
                ));
            }
            let order: OrderRecord = serde_json::from_str(&raw)
                .map_err(|_| LabError::DataCorrupt("paper linked order JSON is invalid".into()))?;
            validate_linked_paper_order(signal, &order)?;
            order_facts.push((order.context.event_seq, event_sequence, order));
            count += 1;
        }
    }
    Ok(order_facts)
}

fn fold_linked_paper_orders(
    mut order_facts: Vec<(u64, u64, OrderRecord)>,
) -> Result<LinkedOrderHistory, LabError> {
    order_facts.sort_by(|left, right| {
        (left.0, left.1, left.2.order_id.as_str()).cmp(&(
            right.0,
            right.1,
            right.2.order_id.as_str(),
        ))
    });
    let mut creation_orders = BTreeMap::<String, OrderRecord>::new();
    let mut orders_by_id = BTreeMap::<String, OrderRecord>::new();
    for (_, _, order) in order_facts {
        let key = order.order_id.as_str().to_owned();
        if let Some(creation) = creation_orders.get(&key) {
            if !same_paper_order_identity(creation, &order) {
                return Err(LabError::DataCorrupt(
                    "paper lifecycle changed immutable order identity".into(),
                ));
            }
            let latest = orders_by_id.get(&key).ok_or_else(|| {
                LabError::DataCorrupt("paper order lifecycle has no creation fact".into())
            })?;
            if order.context.event_seq < latest.context.event_seq {
                return Err(LabError::DataCorrupt(
                    "paper order lifecycle sequence moved backwards".into(),
                ));
            }
            if order.context.event_seq == latest.context.event_seq {
                if serde_json::to_vec(latest)? != serde_json::to_vec(&order)? {
                    return Err(LabError::DataCorrupt(
                        "paper order has conflicting lifecycle facts at one sequence".into(),
                    ));
                }
                continue;
            }
        } else {
            if order.status != crate::contracts::OrderStatus::Created
                || order.cumulative_filled_qty.get() != rust_decimal::Decimal::ZERO
            {
                return Err(LabError::DataCorrupt(
                    "paper linked order lacks its earliest CREATED lifecycle fact".into(),
                ));
            }
            creation_orders.insert(key.clone(), order.clone());
        }
        orders_by_id.insert(key, order);
    }
    Ok(LinkedOrderHistory {
        creation_by_id: creation_orders,
        latest_by_id: orders_by_id,
    })
}

fn load_linked_paper_fills(
    connection: &rusqlite::Connection,
    selection: &PaperReviewSelection,
    signal: &SignalRecord,
    orders_by_id: &BTreeMap<String, OrderRecord>,
    creation_orders: &BTreeMap<String, OrderRecord>,
) -> Result<Vec<FillRecord>, LabError> {
    let order_ids = orders_by_id.keys().cloned().collect::<Vec<_>>();
    let order_ids_json = serde_json::to_string(&order_ids).map_err(json_error)?;
    let fill_limit = i64::try_from(MAX_LINKED_PAPER_FILL_FACTS + 1)
        .map_err(|_| LabError::ResourceLimit("paper linked-fill limit overflow".into()))?;
    let mut fills = Vec::new();
    let mut fill_ids = BTreeSet::new();
    let mut fill_bytes = 0_usize;
    {
        let mut statement = connection
            .prepare(
                "WITH requested AS (SELECT value AS order_id FROM json_each(?4)), \
                 linked AS (\
                     SELECT e.sequence AS event_sequence,fact.value AS value \
                     FROM paper_session_events e \
                     JOIN json_each(e.event_json,'$.fills') fact \
                     JOIN requested ON json_extract(fact.value,'$.order_id')=requested.order_id \
                     WHERE e.session_id=?1 AND e.sequence BETWEEN ?2 AND ?3 \
                 ) \
                 SELECT event_sequence,length(CAST(value AS BLOB)),value \
                 FROM linked ORDER BY event_sequence,value LIMIT ?5",
            )
            .map_err(sql_error)?;
        let mut rows = statement
            .query(params![
                selection.session_id.as_str(),
                u64_to_i64(selection.event_sequence)?,
                u64_to_i64(selection.as_of_sequence)?,
                order_ids_json,
                fill_limit,
            ])
            .map_err(sql_error)?;
        let mut count = 0_usize;
        while let Some(row) = rows.next().map_err(sql_error)? {
            if count == MAX_LINKED_PAPER_FILL_FACTS {
                return Err(LabError::ResourceLimit(
                    "paper decision linked-fill fact cap exceeded".into(),
                ));
            }
            let event_sequence: i64 = row.get(0).map_err(sql_error)?;
            let bytes: i64 = row.get(1).map_err(sql_error)?;
            fill_bytes = add_linked_fact_bytes(
                fill_bytes,
                bytes,
                MAX_LINKED_PAPER_FILL_BYTES,
                "paper linked-fill bytes",
            )?;
            let raw: String = row.get(2).map_err(sql_error)?;
            let event_sequence = sql_sequence(event_sequence)?;
            if event_sequence < selection.event_sequence
                || event_sequence > selection.as_of_sequence
            {
                return Err(LabError::DataCorrupt(
                    "paper linked fill escaped the selected event prefix".into(),
                ));
            }
            let fill: FillRecord = serde_json::from_str(&raw)
                .map_err(|_| LabError::DataCorrupt("paper linked fill JSON is invalid".into()))?;
            let Some(order) = orders_by_id.get(fill.order_id.as_str()) else {
                return Err(LabError::DataCorrupt(
                    "paper linked fill points to an absent selected-signal order".into(),
                ));
            };
            let Some(creation) = creation_orders.get(fill.order_id.as_str()) else {
                return Err(LabError::DataCorrupt(
                    "paper linked fill has no earliest order fact".into(),
                ));
            };
            if fill.context.run_id != signal.context.run_id
                || fill.context.model_id != signal.context.model_id
                || fill.context.market != signal.context.market
                || fill.context.event_seq <= creation.context.event_seq
                || fill.side != order.side
                || !fill_ids.insert(fill.fill_id.as_str().to_owned())
            {
                return Err(LabError::DataCorrupt(
                    "paper fill identity does not match its signal/order lifecycle".into(),
                ));
            }
            fills.push(fill);
            count += 1;
        }
    }
    fills.sort_by(|left, right| {
        (left.context.event_seq, &left.fill_id).cmp(&(right.context.event_seq, &right.fill_id))
    });
    Ok(fills)
}

fn validate_linked_paper_event_json(
    connection: &rusqlite::Connection,
    selection: &PaperReviewSelection,
) -> Result<(), LabError> {
    validate_linked_paper_scan_budget(connection, selection)?;
    validate_linked_paper_event_documents(connection, selection)?;
    validate_linked_paper_fact_arrays(connection, selection)
}

fn validate_linked_paper_scan_budget(
    connection: &rusqlite::Connection,
    selection: &PaperReviewSelection,
) -> Result<(), LabError> {
    let scan_limit = i64::try_from(MAX_LINKED_PAPER_SCAN_EVENTS + 1)
        .map_err(|_| LabError::ResourceLimit("paper event scan limit overflow".into()))?;
    let (scan_count, scan_bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*),COALESCE(SUM(event_bytes),0) FROM (\
                 SELECT length(CAST(event_json AS BLOB)) AS event_bytes \
                 FROM paper_session_events \
                 WHERE session_id=?1 AND sequence BETWEEN ?2 AND ?3 \
                 ORDER BY sequence LIMIT ?4\
             )",
            params![
                selection.session_id.as_str(),
                u64_to_i64(selection.event_sequence)?,
                u64_to_i64(selection.as_of_sequence)?,
                scan_limit,
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?;
    let scan_count = usize::try_from(scan_count)
        .map_err(|_| LabError::DataCorrupt("paper event scan count is negative".into()))?;
    let scan_bytes = usize::try_from(scan_bytes)
        .map_err(|_| LabError::DataCorrupt("paper event scan byte count is negative".into()))?;
    if scan_count > MAX_LINKED_PAPER_SCAN_EVENTS || scan_bytes > MAX_LINKED_PAPER_SCAN_BYTES {
        return Err(LabError::ResourceLimit(
            "paper decision lifecycle scan exceeds event or byte budget".into(),
        ));
    }
    Ok(())
}

fn validate_linked_paper_event_documents(
    connection: &rusqlite::Connection,
    selection: &PaperReviewSelection,
) -> Result<(), LabError> {
    let malformed: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM paper_session_events \
             WHERE session_id=?1 AND sequence BETWEEN ?2 AND ?3 \
               AND json_valid(event_json)=0 LIMIT 1)",
            params![
                selection.session_id.as_str(),
                u64_to_i64(selection.event_sequence)?,
                u64_to_i64(selection.as_of_sequence)?,
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if malformed {
        return Err(LabError::DataCorrupt(
            "paper event prefix contains invalid JSON".into(),
        ));
    }
    let event_byte_limit = i64::try_from(crate::contracts::MAX_PAPER_REVIEW_EVENT_BYTES)
        .map_err(|_| LabError::ResourceLimit("paper review event byte cap overflow".into()))?;
    let oversized_event: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM paper_session_events \
             WHERE session_id=?1 AND sequence BETWEEN ?2 AND ?3 \
               AND length(CAST(event_json AS BLOB))>?4 LIMIT 1)",
            params![
                selection.session_id.as_str(),
                u64_to_i64(selection.event_sequence)?,
                u64_to_i64(selection.as_of_sequence)?,
                event_byte_limit,
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if oversized_event {
        return Err(LabError::ResourceLimit(
            "paper lifecycle event exceeds review byte limit".into(),
        ));
    }
    Ok(())
}

fn validate_linked_paper_fact_arrays(
    connection: &rusqlite::Connection,
    selection: &PaperReviewSelection,
) -> Result<(), LabError> {
    let malformed_arrays: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM paper_session_events \
             WHERE session_id=?1 AND sequence BETWEEN ?2 AND ?3 \
               AND (json_type(event_json,'$.orders') IS NOT 'array' \
                    OR json_type(event_json,'$.order_events') IS NOT 'array' \
                    OR json_type(event_json,'$.fills') IS NOT 'array') LIMIT 1)",
            params![
                selection.session_id.as_str(),
                u64_to_i64(selection.event_sequence)?,
                u64_to_i64(selection.as_of_sequence)?,
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let malformed_rows: bool = connection
        .query_row(
            "WITH facts AS (\
                 SELECT fact.type AS item_type, \
                        json_type(fact.value,'$.order_id') AS order_id_type, \
                        json_type(fact.value,'$.parent_signal_id') AS signal_id_type, \
                        NULL AS fill_id_type \
                 FROM paper_session_events e JOIN json_each(e.event_json,'$.orders') fact \
                 WHERE e.session_id=?1 AND e.sequence BETWEEN ?2 AND ?3 \
                 UNION ALL \
                 SELECT fact.type,json_type(fact.value,'$.order_id'), \
                        json_type(fact.value,'$.parent_signal_id'),NULL \
                 FROM paper_session_events e JOIN json_each(e.event_json,'$.order_events') fact \
                 WHERE e.session_id=?1 AND e.sequence BETWEEN ?2 AND ?3 \
                 UNION ALL \
                 SELECT fact.type,json_type(fact.value,'$.order_id'),NULL, \
                        json_type(fact.value,'$.fill_id') \
                 FROM paper_session_events e JOIN json_each(e.event_json,'$.fills') fact \
                 WHERE e.session_id=?1 AND e.sequence BETWEEN ?2 AND ?3 \
             ) \
             SELECT EXISTS(SELECT 1 FROM facts WHERE item_type IS NOT 'object' \
                 OR (fill_id_type IS NULL AND (order_id_type IS NOT 'text' OR signal_id_type IS NOT 'text')) \
                 OR (fill_id_type IS NOT NULL AND (order_id_type IS NOT 'text' OR fill_id_type IS NOT 'text')) LIMIT 1)",
            params![
                selection.session_id.as_str(),
                u64_to_i64(selection.event_sequence)?,
                u64_to_i64(selection.as_of_sequence)?,
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if malformed_arrays || malformed_rows {
        return Err(LabError::DataCorrupt(
            "paper lifecycle event arrays contain malformed records".into(),
        ));
    }
    Ok(())
}

fn validate_linked_paper_order(signal: &SignalRecord, order: &OrderRecord) -> Result<(), LabError> {
    if order.parent_signal_id != signal.signal_id
        || order.context.run_id != signal.context.run_id
        || order.context.model_id != signal.context.model_id
        || order.context.market != signal.context.market
        || order.context.event_seq <= signal.context.event_seq
        || order.created_at < signal.decision_available_at
    {
        return Err(LabError::DataCorrupt(
            "paper order identity does not causally link to selected signal".into(),
        ));
    }
    Ok(())
}

fn same_paper_order_identity(left: &OrderRecord, right: &OrderRecord) -> bool {
    left.order_id == right.order_id
        && left.parent_signal_id == right.parent_signal_id
        && left.side == right.side
        && left.order_type == right.order_type
        && left.requested_price == right.requested_price
        && left.requested_qty == right.requested_qty
        && left.created_at == right.created_at
        && left.effective_at == right.effective_at
        && left.expires_at == right.expires_at
        && left.rule_snapshot_id == right.rule_snapshot_id
        && left.policy_version == right.policy_version
        && left.order_origin == right.order_origin
}

fn add_linked_fact_bytes(
    used: usize,
    raw_bytes: i64,
    limit: usize,
    label: &str,
) -> Result<usize, LabError> {
    let raw_bytes = usize::try_from(raw_bytes)
        .map_err(|_| LabError::DataCorrupt(format!("{label} has a negative size")))?;
    let total = used
        .checked_add(raw_bytes)
        .ok_or_else(|| LabError::ResourceLimit(format!("{label} overflow")))?;
    if total > limit {
        return Err(LabError::ResourceLimit(format!("{label} cap exceeded")));
    }
    Ok(total)
}

fn sql_sequence(value: i64) -> Result<u64, LabError> {
    u64::try_from(value)
        .map_err(|_| LabError::DataCorrupt("paper event sequence is negative".into()))
}

fn decision_quality(
    selected: Option<&SignalRecord>,
    completed: Option<bool>,
) -> PaperDecisionDataQuality {
    let trace = selected.and_then(|signal| signal.policy_trace.as_ref());
    let conditions = trace.and_then(|trace| trace.condition_trace.as_ref());
    let lineage = trace.and_then(|trace| trace.source_provenance.as_ref());
    let capture = |present| {
        if selected.is_none() {
            SavedFactCaptureStatus::NotDecisionEvent
        } else if present {
            SavedFactCaptureStatus::Captured
        } else {
            SavedFactCaptureStatus::NotCaptured
        }
    };
    let mut reasons = vec![
        PaperReviewUnavailableReason::ProviderFirstSeenNotReported,
        PaperReviewUnavailableReason::PaperScenarioDoesNotAttestLiveTradeability,
    ];
    if selected.is_some() && conditions.is_none() {
        reasons.push(PaperReviewUnavailableReason::ConditionTraceNotCaptured);
    }
    if selected.is_some() && lineage.is_none() {
        reasons.push(PaperReviewUnavailableReason::SourceProvenanceNotCaptured);
    }
    let source_capture = if lineage.is_some_and(|items| {
        items.iter().any(|item| {
            item.capture_origin == crate::contracts::PolicySourceCaptureOrigin::NotCaptured
        })
    }) {
        SavedFactCaptureStatus::PartialCapture
    } else {
        capture(lineage.is_some())
    };
    PaperDecisionDataQuality {
        condition_trace: capture(conditions.is_some()),
        source_provenance: source_capture,
        decision_bar_completed_at_logical_time: completed,
        realtime_tradable: None,
        unavailable_reasons: reasons,
    }
}

fn non_entry_observations(
    selected: Option<&SignalRecord>,
) -> Option<crate::contracts::PaperNonEntryObservations> {
    let conditions = selected
        .and_then(|signal| signal.policy_trace.as_ref())
        .and_then(|trace| trace.condition_trace.as_ref());
    selected
        .filter(|signal| signal.outcome != crate::contracts::SignalOutcome::Executed)
        .map(|signal| {
            let declared_conditions = conditions
                .into_iter()
                .flatten()
                .filter(|item| {
                    matches!(
                        item.role,
                        crate::contracts::PolicyConditionRole::Entry
                            | crate::contracts::PolicyConditionRole::Filter
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            crate::contracts::PaperNonEntryObservations {
                saved_outcome: signal.outcome,
                saved_reasons: signal.reasons.clone(),
                state_before: signal.state_before,
                roles_not_declared: declared_conditions.is_empty(),
                declared_conditions,
                causal_rejection_attribution_available: false,
            }
        })
}

pub(super) fn select_signal<'a>(
    signals: &'a [SignalRecord],
    selection: &PaperReviewSelection,
) -> Result<Option<&'a SignalRecord>, LabError> {
    if let Some(id) = &selection.signal_id {
        let mut matches = signals.iter().filter(|signal| &signal.signal_id == id);
        let selected = matches.next().ok_or_else(|| {
            LabError::InvalidConfig("selected signal is absent from the saved event".into())
        })?;
        if matches.next().is_some() {
            return Err(LabError::DataCorrupt(
                "saved event repeats selected signal identity".into(),
            ));
        }
        return Ok(Some(selected));
    }
    match signals {
        [] => Ok(None),
        [signal] => Ok(Some(signal)),
        _ => Err(LabError::InvalidConfig(
            "multiple saved signals require an explicit signal_id".into(),
        )),
    }
}

pub(super) fn load_observation(
    connection: &rusqlite::Connection,
    id: &ObservationId,
) -> Result<Option<CandleObservation>, LabError> {
    let size: Option<i64> = connection
        .query_row(
            "SELECT length(CAST(observation_json AS BLOB)) FROM candle_observations WHERE id=?1",
            [id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(size) = size else { return Ok(None) };
    if !(0..=131_072).contains(&size) {
        return Err(LabError::ResourceLimit(
            "saved source observation exceeds review byte budget".into(),
        ));
    }
    let raw: String = connection
        .query_row(
            "SELECT observation_json FROM candle_observations WHERE id=?1",
            [id.as_str()],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let observation: CandleObservation = serde_json::from_str(&raw).map_err(json_error)?;
    if &observation.id != id {
        return Err(LabError::DataCorrupt(
            "source observation SQL/JSON identity mismatch".into(),
        ));
    }
    crate::storage::validate_observation_identity(&observation)?;
    if observation.raw_object_ids.len() + observation.constituent_ids.len() > 64 {
        return Err(LabError::ResourceLimit(
            "saved source links exceed review budget".into(),
        ));
    }
    Ok(Some(observation))
}

fn provenance(
    connection: &rusqlite::Connection,
    id: &ObservationId,
    observation: Option<&CandleObservation>,
    interval: crate::contracts::CandleInterval,
    recorded_at: Option<UtcTimestamp>,
) -> Result<PaperObservationProvenance, LabError> {
    let mut reasons = vec![PaperReviewUnavailableReason::ProviderFirstSeenNotReported];
    let Some(observation) = observation else {
        reasons.push(PaperReviewUnavailableReason::SourceObservationMissing);
        return Ok(PaperObservationProvenance {
            observation_id: id.clone(),
            interval,
            content_digest: None,
            open_time: None,
            close_time: None,
            raw_object_ids: Vec::new(),
            constituent_ids: Vec::new(),
            first_retained_receipt: None,
            receipt_cutoff_utc: recorded_at,
            provider_first_seen_at_utc: None,
            unavailable_reasons: reasons,
        });
    };
    let receipt = if let Some(at) = recorded_at {
        let raw: Option<String> = connection.query_row(
            "SELECT r.object_json FROM observation_raw_objects o JOIN raw_objects r ON r.id=o.raw_object_id WHERE o.observation_id=?1 AND r.fetched_at_ms<=?2 AND r.persisted_at_ms<=?2 ORDER BY r.fetched_at_ms,r.id LIMIT 1",
            params![id.as_str(), at.0.timestamp_millis()], |row| row.get(0),
        ).optional().map_err(sql_error)?;
        raw.map(|raw| {
            let object: RawObjectRef = serde_json::from_str(&raw).map_err(json_error)?;
            if object.fetched_at > at || object.persisted_at > at {
                return Err(LabError::DataCorrupt(
                    "retained receipt SQL/JSON timestamp mismatch".into(),
                ));
            }
            Ok::<_, LabError>(PaperRetainedReceipt {
                raw_object_id: object.id,
                fetched_at_utc: object.fetched_at,
                persisted_at_utc: object.persisted_at,
            })
        })
        .transpose()?
    } else {
        None
    };
    if receipt.is_none() {
        reasons.push(PaperReviewUnavailableReason::ReceiptNotRetainedBeforeEvent);
    }
    Ok(PaperObservationProvenance {
        observation_id: id.clone(),
        interval: observation.candle.interval,
        content_digest: Some(observation.content_digest.clone()),
        open_time: Some(observation.candle.open_time_utc),
        close_time: Some(observation.candle.close_time_utc),
        raw_object_ids: observation.raw_object_ids.clone(),
        constituent_ids: observation.constituent_ids.clone(),
        first_retained_receipt: receipt,
        receipt_cutoff_utc: recorded_at,
        provider_first_seen_at_utc: None,
        unavailable_reasons: reasons,
    })
}

#[cfg(test)]
#[path = "review_decision/tests.rs"]
mod tests;

//! Bounded reads of one explicitly selected saved decision source.

mod paper_lifecycle;

use super::Store;
#[cfg(test)]
use crate::contracts::DecisionAnalysisPeriod;
use crate::contracts::{
    AutomationEvent, AutomationEventKind, AutomationFillView, AutomationIntentId,
    AutomationOrderIdentifier, AutomationOrderView, AutomationReviewSnapshot, ContentHash,
    DecisionAnalysisComparisonTarget, DecisionAnalysisDecisionDetail, DecisionAnalysisDecisionId,
    DecisionAnalysisDecisionRow, DecisionAnalysisFence, DecisionAnalysisMode,
    DecisionAnalysisObservationClock, DecisionAnalysisObservationScope,
    DecisionAnalysisPeriodPlacement, DecisionAnalysisPopulation, DecisionAnalysisQuery,
    DecisionAnalysisSelection, DecisionAnalysisSource, DecisionAnalysisSourceSnapshot, LabError,
    MAX_DECISION_ANALYSIS_FACTS, MAX_DECISION_ANALYSIS_INPUT_BYTES, PaperEventKind,
    PolicyRevisionRef, SavedFactCaptureStatus, SignalRecord, UtcTimestamp,
};
use rusqlite::params;
#[cfg(test)]
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

const PAPER_ANALYSIS_PAGE_SIZE: u32 = 100;
const MAX_DECISION_ANALYSIS_STORAGE_MILLIS: u64 = 15_000;

#[derive(Debug)]
struct DecisionAnalysisReadBudget {
    started: Instant,
}

impl DecisionAnalysisReadBudget {
    fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }

    fn check_elapsed(&self) -> Result<(), LabError> {
        if self.started.elapsed().as_millis() > u128::from(MAX_DECISION_ANALYSIS_STORAGE_MILLIS) {
            return Err(LabError::ResourceLimit(
                "decision analysis storage read exceeded its 15-second owner budget; choose a smaller saved session and request a fresh overview".into(),
            ));
        }
        Ok(())
    }

    fn deadline(&self) -> Result<Instant, LabError> {
        self.started
            .checked_add(Duration::from_millis(MAX_DECISION_ANALYSIS_STORAGE_MILLIS))
            .ok_or_else(|| LabError::ResourceLimit("decision analysis deadline overflow".into()))
    }
}

/// One explicitly clocked non-decision saved observation.
#[derive(Debug, Clone)]
pub(crate) struct DecisionAnalysisObservation {
    pub kind: String,
    pub basis_time: DecisionAnalysisObservationClock,
    pub scope: DecisionAnalysisObservationScope,
    pub at: Option<UtcTimestamp>,
}

/// Complete, bounded facts needed to produce every decision-analysis projection.
#[derive(Debug, Clone)]
pub(crate) struct DecisionAnalysisSourceData {
    pub source_snapshot: DecisionAnalysisSourceSnapshot,
    pub decisions: Vec<DecisionAnalysisDecisionDetail>,
    /// At most one latest actual pre-period record per market, only loaded for previous compare.
    pub predecessor_candidates: Vec<DecisionAnalysisDecisionDetail>,
    pub observations: Vec<DecisionAnalysisObservation>,
}

#[derive(Debug, Clone)]
pub(crate) struct DecisionAnalysisReadSnapshot {
    pub selected: DecisionAnalysisSourceData,
    pub compared: Option<DecisionAnalysisSourceData>,
}

#[derive(Default)]
struct AutomationDecisionProjection {
    decisions: Vec<DecisionAnalysisDecisionDetail>,
    predecessor_candidates: Vec<DecisionAnalysisDecisionDetail>,
    observations: Vec<DecisionAnalysisObservation>,
    fact_count: usize,
    explanation_bytes: usize,
}

struct AutomationRecordProjection {
    detail: Option<DecisionAnalysisDecisionDetail>,
    warmup_observation: Option<DecisionAnalysisObservation>,
    linked_facts: usize,
    explanation_bytes: usize,
}

#[derive(Debug, Clone)]
struct PaperDecisionCandidate {
    event_sequence: u64,
    signal: SignalRecord,
}

struct PaperEventPrefix {
    events: Vec<crate::contracts::PaperSessionEvent>,
    observations: Vec<DecisionAnalysisObservation>,
    source_facts: usize,
}

struct PaperDecisionProjection {
    decisions: Vec<DecisionAnalysisDecisionDetail>,
    predecessor_candidates: Vec<DecisionAnalysisDecisionDetail>,
    linked_facts: usize,
    explanation_bytes: usize,
}

#[derive(Debug, Default)]
struct AutomationLifecycleIndex {
    orders_by_intent: HashMap<AutomationIntentId, Vec<AutomationOrderView>>,
    client_ids_by_intent: HashMap<AutomationIntentId, BTreeSet<AutomationOrderIdentifier>>,
    fills_by_client: HashMap<String, Vec<AutomationFillView>>,
    events_by_intent: HashMap<AutomationIntentId, Vec<AutomationEvent>>,
}

impl AutomationLifecycleIndex {
    fn from_snapshot(snapshot: &AutomationReviewSnapshot) -> Self {
        let mut index = Self::default();
        let mut intents_by_client: HashMap<String, BTreeSet<AutomationIntentId>> = HashMap::new();
        for review_order in &snapshot.orders {
            let order = &review_order.order;
            index
                .orders_by_intent
                .entry(order.intent_id.clone())
                .or_default()
                .push(order.clone());
            index
                .client_ids_by_intent
                .entry(order.intent_id.clone())
                .or_default()
                .insert(order.client_identifier.clone());
            intents_by_client
                .entry(order.client_identifier.as_str().to_owned())
                .or_default()
                .insert(order.intent_id.clone());
        }
        let mut intents_by_observation: HashMap<String, BTreeSet<AutomationIntentId>> =
            HashMap::new();
        let mut intents_by_raw_id = HashMap::new();
        for decision in &snapshot.decisions {
            intents_by_observation
                .entry(decision.observation_id.as_str().to_owned())
                .or_default()
                .insert(decision.intent_id.clone());
            intents_by_raw_id.insert(
                decision.intent_id.as_str().to_owned(),
                decision.intent_id.clone(),
            );
        }
        for event in &snapshot.events {
            let mut matched = BTreeSet::new();
            if let Some(intent_id) = event
                .detail
                .get("intent_id")
                .and_then(serde_json::Value::as_str)
                && let Some(intent_id) = intents_by_raw_id.get(intent_id)
            {
                matched.insert(intent_id.clone());
            }
            if let Some(client_id) = event
                .detail
                .get("client_identifier")
                .and_then(serde_json::Value::as_str)
                && let Some(intent_ids) = intents_by_client.get(client_id)
            {
                matched.extend(intent_ids.iter().cloned());
            }
            if let Some(observation_id) = event
                .detail
                .get("observation_id")
                .and_then(serde_json::Value::as_str)
                && let Some(intent_ids) = intents_by_observation.get(observation_id)
            {
                matched.extend(intent_ids.iter().cloned());
            }
            for intent_id in matched {
                index
                    .events_by_intent
                    .entry(intent_id)
                    .or_default()
                    .push(event.clone());
            }
        }
        for fill in &snapshot.fills {
            index
                .fills_by_client
                .entry(fill.fill.client_identifier.as_str().to_owned())
                .or_default()
                .push(fill.fill.clone());
        }
        index
    }

    fn take_for_intent(
        &mut self,
        intent_id: &AutomationIntentId,
    ) -> (
        Vec<AutomationOrderView>,
        Vec<AutomationFillView>,
        Vec<AutomationEvent>,
    ) {
        let orders = self.orders_by_intent.remove(intent_id).unwrap_or_default();
        let client_ids = self
            .client_ids_by_intent
            .remove(intent_id)
            .unwrap_or_default();
        let fills = client_ids
            .iter()
            .filter_map(|client_id| self.fills_by_client.get(client_id.as_str()))
            .flatten()
            .cloned()
            .collect();
        let events = self.events_by_intent.remove(intent_id).unwrap_or_default();
        (orders, fills, events)
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct DecisionAnalysisTestFixture {
    pub paper_population: DecisionAnalysisPopulation,
    pub paper_before_period: DecisionAnalysisDecisionId,
    pub paper_previous: DecisionAnalysisDecisionId,
    pub paper_selected: DecisionAnalysisDecisionId,
    pub automation_population: DecisionAnalysisPopulation,
    pub automation_before_period: DecisionAnalysisDecisionId,
    pub automation_previous: DecisionAnalysisDecisionId,
    pub automation_selected: DecisionAnalysisDecisionId,
    pub automation_client_identifier: AutomationOrderIdentifier,
}

#[cfg(test)]
struct PaperSignalFixture {
    signal_id: crate::contracts::SignalId,
    run_id: String,
    model_id: String,
    event_sequence: u64,
    market: crate::contracts::MarketId,
    strategy: crate::contracts::StrategyKind,
    policy_ref: PolicyRevisionRef,
    decision_time: UtcTimestamp,
    raw_weight: Decimal,
    constrained_weight: Decimal,
    reason: crate::contracts::ReasonCode,
}

impl Store {
    pub(crate) fn decision_analysis_snapshot(
        &mut self,
        query: &DecisionAnalysisQuery,
    ) -> Result<DecisionAnalysisReadSnapshot, LabError> {
        let mut budget = DecisionAnalysisReadBudget::new();
        let (selected, compared) = match query {
            DecisionAnalysisQuery::Overview { population } => (
                self.load_decision_population(population, None, "selected", false, &mut budget)?,
                None,
            ),
            DecisionAnalysisQuery::Decisions { selection, .. }
            | DecisionAnalysisQuery::Decision { selection, .. } => (
                self.load_decision_population(
                    &selection.population,
                    Some(&selection.fence),
                    "selected",
                    false,
                    &mut budget,
                )?,
                None,
            ),
            DecisionAnalysisQuery::Compare {
                selection, against, ..
            } => {
                let selected = self.load_decision_population(
                    &selection.population,
                    Some(&selection.fence),
                    "selected",
                    matches!(against, DecisionAnalysisComparisonTarget::Previous),
                    &mut budget,
                )?;
                let compared = match against {
                    DecisionAnalysisComparisonTarget::Previous => None,
                    DecisionAnalysisComparisonTarget::Selected { selection, .. } => {
                        Some(self.load_decision_population(
                            &selection.population,
                            Some(&selection.fence),
                            "compared",
                            false,
                            &mut budget,
                        )?)
                    }
                };
                (selected, compared)
            }
        };
        budget.check_elapsed()?;
        Ok(DecisionAnalysisReadSnapshot { selected, compared })
    }

    fn load_decision_population(
        &mut self,
        population: &DecisionAnalysisPopulation,
        expected_fence: Option<&DecisionAnalysisFence>,
        side: &str,
        include_predecessor_context: bool,
        budget: &mut DecisionAnalysisReadBudget,
    ) -> Result<DecisionAnalysisSourceData, LabError> {
        match &population.source {
            DecisionAnalysisSource::Paper { session_id } => self.load_paper_population(
                population,
                session_id,
                expected_fence,
                side,
                include_predecessor_context,
                budget,
            ),
            DecisionAnalysisSource::Automation { session_id } => self.load_automation_population(
                population,
                session_id,
                expected_fence,
                side,
                include_predecessor_context,
                budget,
            ),
        }
    }

    fn load_paper_population(
        &mut self,
        population: &DecisionAnalysisPopulation,
        session_id: &crate::contracts::PaperSessionId,
        expected_fence: Option<&DecisionAnalysisFence>,
        side: &str,
        include_predecessor_context: bool,
        budget: &mut DecisionAnalysisReadBudget,
    ) -> Result<DecisionAnalysisSourceData, LabError> {
        let expected_as_of = match expected_fence {
            Some(DecisionAnalysisFence::Paper { as_of_sequence }) => Some(*as_of_sequence),
            Some(DecisionAnalysisFence::Automation { .. }) => {
                return Err(LabError::InvalidConfig(
                    "paper source requires a paper event-sequence fence".into(),
                ));
            }
            None => None,
        };
        let mut page_limit = PAPER_ANALYSIS_PAGE_SIZE;
        budget.check_elapsed()?;
        let first = self.read_paper_page(session_id, None, expected_as_of, &mut page_limit)?;
        let as_of_sequence = first.as_of_sequence;
        validate_paper_population(
            population,
            &first.configuration.policy_ref,
            &first.configuration.markets,
        )?;
        if expected_as_of.is_some_and(|expected| expected != as_of_sequence) {
            return Err(LabError::InputHashMismatch(format!(
                "paper {side} snapshot fence differs for session {session_id}"
            )));
        }
        let configuration = first.configuration.clone();
        let mode = match first.mode {
            crate::contracts::PaperSessionMode::Simulated => DecisionAnalysisMode::Simulated,
        };
        let selection = DecisionAnalysisSelection {
            population: population.clone(),
            fence: DecisionAnalysisFence::Paper { as_of_sequence },
        };
        let source_snapshot = DecisionAnalysisSourceSnapshot {
            selection,
            mode,
            policy_ref: configuration.policy_ref.clone(),
        };
        // Admit cumulative original bytes before retaining the remaining decoded review pages.
        let (event_digests, event_bytes) =
            self.paper_event_digests(session_id, as_of_sequence, budget)?;
        let prefix =
            self.collect_paper_prefix(session_id, as_of_sequence, first, &mut page_limit, budget)?;
        validate_paper_event_digests(&event_digests, &prefix.events)?;
        let index =
            paper_lifecycle::PaperLifecycleIndex::from_events(&prefix.events, budget.deadline()?)?;
        let projection = project_paper_prefix(
            &prefix.events,
            &event_digests,
            &index,
            population,
            &source_snapshot.policy_ref,
            include_predecessor_context,
            budget,
        )?;
        let mut input_bytes = event_bytes;
        add_input_bytes(
            &mut input_bytes,
            super::serialized_json_len(&configuration)?,
        )?;
        input_bytes = input_bytes.saturating_add(projection.explanation_bytes);
        let fact_count = prefix.source_facts.saturating_add(projection.linked_facts);
        if input_bytes > MAX_DECISION_ANALYSIS_INPUT_BYTES
            || fact_count > MAX_DECISION_ANALYSIS_FACTS
        {
            return Err(analysis_limit("paper decision-analysis projection"));
        }
        Ok(DecisionAnalysisSourceData {
            source_snapshot,
            decisions: projection.decisions,
            predecessor_candidates: projection.predecessor_candidates,
            observations: prefix.observations,
        })
    }

    fn read_paper_page(
        &mut self,
        session_id: &crate::contracts::PaperSessionId,
        after_sequence: Option<u64>,
        as_of_sequence: Option<u64>,
        page_limit: &mut u32,
    ) -> Result<crate::contracts::PaperSessionReviewPage, LabError> {
        loop {
            match self.paper_review_events(session_id, after_sequence, as_of_sequence, *page_limit)
            {
                Err(LabError::ResourceLimit(_)) if *page_limit > 1 => {
                    *page_limit = (*page_limit / 2).max(1);
                }
                result => return result,
            }
        }
    }

    fn collect_paper_prefix(
        &mut self,
        session_id: &crate::contracts::PaperSessionId,
        as_of_sequence: u64,
        first_page: crate::contracts::PaperSessionReviewPage,
        page_limit: &mut u32,
        budget: &mut DecisionAnalysisReadBudget,
    ) -> Result<PaperEventPrefix, LabError> {
        let mut events = Vec::new();
        let mut observations = Vec::new();
        let mut source_facts = 0_usize;
        let mut after = first_page
            .page
            .next_cursor
            .as_ref()
            .map(|cursor| cursor.sequence);
        absorb_paper_page(
            first_page.page.items,
            &mut events,
            &mut source_facts,
            &mut observations,
            budget,
        )?;
        while let Some(after_sequence) = after {
            budget.check_elapsed()?;
            let page = self.read_paper_page(
                session_id,
                Some(after_sequence),
                Some(as_of_sequence),
                page_limit,
            )?;
            after = page.page.next_cursor.as_ref().map(|cursor| cursor.sequence);
            absorb_paper_page(
                page.page.items,
                &mut events,
                &mut source_facts,
                &mut observations,
                budget,
            )?;
        }
        if u64::try_from(events.len()).ok() != Some(as_of_sequence) {
            return Err(LabError::DataCorrupt(
                "paper event vector differs from its fixed sequence prefix".into(),
            ));
        }
        Ok(PaperEventPrefix {
            events,
            observations,
            source_facts,
        })
    }

    fn paper_event_digests(
        &self,
        session_id: &crate::contracts::PaperSessionId,
        as_of_sequence: u64,
        budget: &DecisionAnalysisReadBudget,
    ) -> Result<(HashMap<u64, ContentHash>, usize), LabError> {
        if as_of_sequence
            > u64::try_from(MAX_DECISION_ANALYSIS_FACTS)
                .map_err(|_| LabError::ResourceLimit("paper event count limit overflow".into()))?
        {
            return Err(analysis_limit("paper source prefix event count"));
        }
        let cutoff = i64::try_from(as_of_sequence).map_err(|_| {
            LabError::ResourceLimit("paper analysis fence exceeds SQLite range".into())
        })?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT sequence,length(CAST(event_json AS BLOB)),event_json \
                 FROM paper_session_events WHERE session_id=?1 AND sequence<=?2 \
                 ORDER BY sequence",
            )
            .map_err(super::sql_error)?;
        let mut rows = statement
            .query(params![session_id.as_str(), cutoff])
            .map_err(super::sql_error)?;
        let capacity = usize::try_from(as_of_sequence).map_err(|_| {
            LabError::ResourceLimit("paper analysis fence exceeds memory range".into())
        })?;
        let mut digests = HashMap::with_capacity(capacity);
        let mut bytes = 0_usize;
        while let Some(row) = rows.next().map_err(super::sql_error)? {
            budget.check_elapsed()?;
            let sequence: i64 = row.get(0).map_err(super::sql_error)?;
            let stored_bytes: i64 = row.get(1).map_err(super::sql_error)?;
            let event_json: String = row.get(2).map_err(super::sql_error)?;
            let stored_bytes = usize::try_from(stored_bytes)
                .map_err(|_| LabError::DataCorrupt("negative paper event byte length".into()))?;
            if event_json.len() != stored_bytes {
                return Err(LabError::DataCorrupt(
                    "paper event JSON byte length differs from its stored length".into(),
                ));
            }
            bytes = admit_paper_event_json_bytes(bytes, stored_bytes)?;
            let sequence = u64::try_from(sequence)
                .map_err(|_| LabError::DataCorrupt("negative paper event sequence".into()))?;
            if digests.len() >= MAX_DECISION_ANALYSIS_FACTS {
                return Err(analysis_limit("paper source prefix event count"));
            }
            if digests
                .insert(sequence, ContentHash::of_bytes(event_json.as_bytes()))
                .is_some()
            {
                return Err(LabError::DataCorrupt(
                    "paper event prefix repeats a durable sequence".into(),
                ));
            }
        }
        if u64::try_from(digests.len()).ok() != Some(as_of_sequence) {
            return Err(LabError::DataCorrupt(
                "paper raw event digest count differs from its fixed sequence prefix".into(),
            ));
        }
        Ok((digests, bytes))
    }

    fn load_automation_population(
        &mut self,
        population: &DecisionAnalysisPopulation,
        session_id: &crate::contracts::AutomationSessionId,
        expected_fence: Option<&DecisionAnalysisFence>,
        side: &str,
        include_predecessor_context: bool,
        budget: &mut DecisionAnalysisReadBudget,
    ) -> Result<DecisionAnalysisSourceData, LabError> {
        let expected_digest = match expected_fence {
            Some(DecisionAnalysisFence::Automation { snapshot_digest }) => Some(snapshot_digest),
            Some(DecisionAnalysisFence::Paper { .. }) => {
                return Err(LabError::InvalidConfig(
                    "automation source requires an automation snapshot-digest fence".into(),
                ));
            }
            None => None,
        };
        budget.check_elapsed()?;
        let snapshot = self.automation_review_snapshot(session_id)?;
        if snapshot.session.session_id != *session_id
            || snapshot.frozen_policy.reference != snapshot.session.policy_ref
        {
            return Err(LabError::DataCorrupt(
                "automation decision-analysis snapshot identity differs".into(),
            ));
        }
        if population
            .market
            .as_ref()
            .is_some_and(|market| market != &snapshot.session.market)
        {
            return Err(LabError::InvalidConfig(
                "requested market does not match the saved automation session market".into(),
            ));
        }
        validate_policy_filter(population, &snapshot.session.policy_ref)?;
        let manifest = crate::reporting::automation_review::export_manifest(&snapshot)?;
        budget.check_elapsed()?;
        if expected_digest.is_some_and(|expected| expected != &manifest.snapshot_digest) {
            return Err(LabError::InputHashMismatch(format!(
                "automation {side} source/session {session_id} changed after its overview; refresh that selection"
            )));
        }
        let fence = DecisionAnalysisFence::Automation {
            snapshot_digest: manifest.snapshot_digest,
        };
        let selection = DecisionAnalysisSelection {
            population: population.clone(),
            fence,
        };
        let mode = match snapshot.session.mode {
            crate::contracts::AutomationSessionMode::Mock => DecisionAnalysisMode::Mock,
            crate::contracts::AutomationSessionMode::Live => DecisionAnalysisMode::Live,
        };
        let source_snapshot = DecisionAnalysisSourceSnapshot {
            selection,
            mode,
            policy_ref: snapshot.session.policy_ref.clone(),
        };
        let encoded_bytes = validate_automation_source_bounds(&snapshot)?;
        budget.check_elapsed()?;
        let projection =
            automation_decisions(&snapshot, population, include_predecessor_context, budget)?;
        budget.check_elapsed()?;
        let mut observations = projection.observations;
        append_automation_event_observations(&mut observations, &snapshot)?;
        let fact_count = projection.fact_count.saturating_add(observations.len());
        let decoded_bytes = encoded_bytes.saturating_add(projection.explanation_bytes);
        if fact_count > MAX_DECISION_ANALYSIS_FACTS
            || decoded_bytes > MAX_DECISION_ANALYSIS_INPUT_BYTES
        {
            return Err(analysis_limit("automation decision-analysis projection"));
        }
        Ok(DecisionAnalysisSourceData {
            source_snapshot,
            decisions: projection.decisions,
            predecessor_candidates: projection.predecessor_candidates,
            observations,
        })
    }
}

fn validate_automation_source_bounds(
    snapshot: &AutomationReviewSnapshot,
) -> Result<usize, LabError> {
    let fact_count = snapshot
        .decisions
        .len()
        .saturating_add(snapshot.orders.len())
        .saturating_add(snapshot.fills.len())
        .saturating_add(snapshot.events.len())
        .saturating_add(snapshot.candles.len())
        .saturating_add(snapshot.fee_accruals.len());
    if fact_count > MAX_DECISION_ANALYSIS_FACTS {
        return Err(analysis_limit("automation source fact count"));
    }
    let encoded_bytes = super::serialized_json_len(snapshot)?;
    if encoded_bytes > MAX_DECISION_ANALYSIS_INPUT_BYTES {
        return Err(analysis_limit("automation decoded source bytes"));
    }
    Ok(encoded_bytes)
}

fn validate_paper_population(
    population: &DecisionAnalysisPopulation,
    saved_policy_ref: &PolicyRevisionRef,
    saved_markets: &[crate::contracts::PaperMarketStart],
) -> Result<(), LabError> {
    validate_policy_filter(population, saved_policy_ref)?;
    if population
        .market
        .as_ref()
        .is_some_and(|market| !saved_markets.iter().any(|saved| &saved.market == market))
    {
        return Err(LabError::InvalidConfig(
            "requested market is absent from the saved paper session configuration".into(),
        ));
    }
    Ok(())
}

fn validate_paper_event_digests(
    digests: &HashMap<u64, ContentHash>,
    events: &[crate::contracts::PaperSessionEvent],
) -> Result<(), LabError> {
    if digests.len() != events.len()
        || events
            .iter()
            .any(|event| !digests.contains_key(&event.sequence))
    {
        return Err(LabError::DataCorrupt(
            "paper raw event digests differ from the decoded fixed prefix".into(),
        ));
    }
    Ok(())
}

fn admit_paper_event_json_bytes(total: usize, event_bytes: usize) -> Result<usize, LabError> {
    let total = total.checked_add(event_bytes).ok_or_else(|| {
        LabError::ResourceLimit("paper stored event JSON byte count overflow".into())
    })?;
    if total > MAX_DECISION_ANALYSIS_INPUT_BYTES {
        return Err(analysis_limit("paper stored event JSON bytes"));
    }
    Ok(total)
}

fn validate_policy_filter(
    population: &DecisionAnalysisPopulation,
    saved_policy_ref: &PolicyRevisionRef,
) -> Result<(), LabError> {
    if population
        .policy_ref
        .as_ref()
        .is_some_and(|policy_ref| policy_ref != saved_policy_ref)
    {
        return Err(LabError::InvalidConfig(
            "requested policy revision differs from the saved session configuration".into(),
        ));
    }
    Ok(())
}

fn absorb_paper_page(
    events: Vec<crate::contracts::PaperSessionEvent>,
    prefix: &mut Vec<crate::contracts::PaperSessionEvent>,
    fact_count: &mut usize,
    observations: &mut Vec<DecisionAnalysisObservation>,
    budget: &mut DecisionAnalysisReadBudget,
) -> Result<(), LabError> {
    for event in events {
        budget.check_elapsed()?;
        *fact_count = fact_count
            .saturating_add(1)
            .saturating_add(event.signals.len())
            .saturating_add(event.orders.len())
            .saturating_add(event.order_events.len())
            .saturating_add(event.fills.len())
            .saturating_add(event.account_marks.len())
            .saturating_add(event.episodes.len());
        if *fact_count > MAX_DECISION_ANALYSIS_FACTS {
            return Err(analysis_limit("paper source prefix fact count"));
        }
        observations.extend(paper_observations(&event)?);
        prefix.push(event);
    }
    Ok(())
}

fn project_paper_prefix(
    events: &[crate::contracts::PaperSessionEvent],
    event_digests: &HashMap<u64, ContentHash>,
    index: &paper_lifecycle::PaperLifecycleIndex<'_>,
    population: &DecisionAnalysisPopulation,
    policy_ref: &PolicyRevisionRef,
    include_predecessor_context: bool,
    budget: &mut DecisionAnalysisReadBudget,
) -> Result<PaperDecisionProjection, LabError> {
    let mut decisions = Vec::new();
    let mut predecessor_candidates = BTreeMap::new();
    let mut linked_facts = 0_usize;
    let mut explanation_bytes = 0_usize;
    for event in events {
        budget.check_elapsed()?;
        for signal in &event.signals {
            if population
                .market
                .as_ref()
                .is_some_and(|market| market != &signal.context.market)
            {
                continue;
            }
            if include_predecessor_context && signal.decision_available_at < population.period.start
            {
                let candidate = PaperDecisionCandidate {
                    event_sequence: event.sequence,
                    signal: signal.clone(),
                };
                let replace = predecessor_candidates
                    .get(&signal.context.market)
                    .is_none_or(|existing: &PaperDecisionCandidate| {
                        compare_paper_candidates(&candidate, existing).is_gt()
                    });
                if replace {
                    predecessor_candidates.insert(signal.context.market.clone(), candidate);
                }
                continue;
            }
            if placement(population, Some(signal.decision_available_at)).is_none() {
                continue;
            }
            let event_digest = event_digests.get(&event.sequence).ok_or_else(|| {
                LabError::DataCorrupt("paper event digest is absent from its prefix".into())
            })?;
            let detail = paper_decision_detail(
                index,
                event.sequence,
                signal,
                policy_ref,
                event_digest,
                population,
            )?;
            linked_facts = linked_facts
                .saturating_add(detail.explanation.orders.len())
                .saturating_add(detail.explanation.fills.len());
            add_input_bytes(
                &mut explanation_bytes,
                super::serialized_json_len(&detail.explanation)?,
            )?;
            decisions.push(detail);
        }
    }

    let mut predecessors = Vec::new();
    for candidate in predecessor_candidates.into_values() {
        budget.check_elapsed()?;
        if candidate.signal.decision_available_at >= population.period.start {
            return Err(LabError::DataCorrupt(
                "paper predecessor does not precede selected period".into(),
            ));
        }
        let event_digest = event_digests
            .get(&candidate.event_sequence)
            .ok_or_else(|| {
                LabError::DataCorrupt("paper predecessor digest is absent from its prefix".into())
            })?;
        let mut context_population = population.clone();
        context_population.period = crate::contracts::DecisionAnalysisPeriod {
            start: candidate.signal.decision_available_at,
            end: population.period.start,
        };
        let detail = paper_decision_detail(
            index,
            candidate.event_sequence,
            &candidate.signal,
            policy_ref,
            event_digest,
            &context_population,
        )?;
        linked_facts = linked_facts
            .saturating_add(detail.explanation.orders.len())
            .saturating_add(detail.explanation.fills.len());
        add_input_bytes(
            &mut explanation_bytes,
            super::serialized_json_len(&detail.explanation)?,
        )?;
        predecessors.push(detail);
    }
    Ok(PaperDecisionProjection {
        decisions,
        predecessor_candidates: predecessors,
        linked_facts,
        explanation_bytes,
    })
}

fn paper_decision_detail(
    index: &paper_lifecycle::PaperLifecycleIndex<'_>,
    event_sequence: u64,
    signal: &SignalRecord,
    policy_ref: &PolicyRevisionRef,
    event_digest: &ContentHash,
    population: &DecisionAnalysisPopulation,
) -> Result<DecisionAnalysisDecisionDetail, LabError> {
    let explanation = index.explanation(signal, policy_ref)?;
    let period_placement = placement(population, explanation.decision_time).ok_or_else(|| {
        LabError::DataCorrupt("out-of-period paper signal escaped selection".into())
    })?;
    let row = paper_analysis_row(
        event_sequence,
        signal,
        policy_ref,
        event_digest,
        &explanation,
        period_placement,
    );
    Ok(DecisionAnalysisDecisionDetail { row, explanation })
}

fn paper_observations(
    event: &crate::contracts::PaperSessionEvent,
) -> Result<Vec<DecisionAnalysisObservation>, LabError> {
    let Some(kind) = paper_observation_kind(event.kind)? else {
        return Ok(Vec::new());
    };
    let event_time = event.finalized_at.or(event.source_event_at);
    match event.kind {
        PaperEventKind::DecisionDeferred => paper_deferred_observation(event, kind, event_time),
        PaperEventKind::Pause
        | PaperEventKind::Resume
        | PaperEventKind::Stop
        | PaperEventKind::RecoveryRequired => {
            paper_session_control_observation(event, kind, event_time)
        }
        PaperEventKind::ProviderRecovery => paper_recovery_observations(event, kind, event_time),
        PaperEventKind::CandleProcessed => Ok(Vec::new()),
    }
}

fn paper_observation_kind(kind: PaperEventKind) -> Result<Option<String>, LabError> {
    match kind {
        PaperEventKind::CandleProcessed => Ok(None),
        PaperEventKind::DecisionDeferred
        | PaperEventKind::Pause
        | PaperEventKind::Resume
        | PaperEventKind::Stop
        | PaperEventKind::RecoveryRequired
        | PaperEventKind::ProviderRecovery => serde_json::to_value(kind)?
            .as_str()
            .map(|value| Some(value.to_owned()))
            .ok_or_else(|| LabError::DataCorrupt("paper event kind is not a string".into())),
    }
}

fn paper_deferred_observation(
    event: &crate::contracts::PaperSessionEvent,
    kind: String,
    event_time: Option<UtcTimestamp>,
) -> Result<Vec<DecisionAnalysisObservation>, LabError> {
    let (scope, basis_time, at) = if let Some(preparation) = &event.preparation {
        if event
            .market
            .as_ref()
            .is_some_and(|market| market != &preparation.market)
        {
            return Err(LabError::DataCorrupt(
                "paper deferred event and preparation market identities differ".into(),
            ));
        }
        (
            DecisionAnalysisObservationScope::Market {
                market: preparation.market.clone(),
            },
            DecisionAnalysisObservationClock::PreparationDecisionTime,
            Some(preparation.decision_time),
        )
    } else if let Some(market) = &event.market {
        (
            DecisionAnalysisObservationScope::Market {
                market: market.clone(),
            },
            DecisionAnalysisObservationClock::PaperSourceOrFinalizedTime,
            event_time,
        )
    } else {
        (
            DecisionAnalysisObservationScope::Unknown,
            DecisionAnalysisObservationClock::PaperSourceOrFinalizedTime,
            event_time,
        )
    };
    Ok(vec![paper_observation_record(kind, basis_time, scope, at)])
}

fn paper_session_control_observation(
    event: &crate::contracts::PaperSessionEvent,
    kind: String,
    event_time: Option<UtcTimestamp>,
) -> Result<Vec<DecisionAnalysisObservation>, LabError> {
    if event.market.is_some() {
        return Err(LabError::DataCorrupt(
            "whole-session paper control event has a market identity".into(),
        ));
    }
    Ok(vec![paper_observation_record(
        kind,
        DecisionAnalysisObservationClock::PaperSourceOrFinalizedTime,
        DecisionAnalysisObservationScope::SessionWide,
        event_time,
    )])
}

fn paper_recovery_observations(
    event: &crate::contracts::PaperSessionEvent,
    kind: String,
    event_time: Option<UtcTimestamp>,
) -> Result<Vec<DecisionAnalysisObservation>, LabError> {
    let mut markets = event
        .provider_recovery
        .as_ref()
        .into_iter()
        .flat_map(|recovery| recovery.checkpoint_transitions.iter())
        .map(|transition| transition.market.clone())
        .collect::<BTreeSet<_>>();
    if let Some(event_market) = &event.market {
        if !markets.is_empty() && !markets.contains(event_market) {
            return Err(LabError::DataCorrupt(
                "paper provider-recovery market conflicts with its checkpoint transitions".into(),
            ));
        }
        markets.insert(event_market.clone());
    }
    if markets.is_empty() {
        return Ok(vec![paper_observation_record(
            kind,
            DecisionAnalysisObservationClock::PaperSourceOrFinalizedTime,
            DecisionAnalysisObservationScope::Unknown,
            event_time,
        )]);
    }
    Ok(markets
        .into_iter()
        .map(|market| {
            paper_observation_record(
                kind.clone(),
                DecisionAnalysisObservationClock::PaperSourceOrFinalizedTime,
                DecisionAnalysisObservationScope::Market { market },
                event_time,
            )
        })
        .collect())
}

fn paper_observation_record(
    kind: String,
    basis_time: DecisionAnalysisObservationClock,
    scope: DecisionAnalysisObservationScope,
    at: Option<UtcTimestamp>,
) -> DecisionAnalysisObservation {
    DecisionAnalysisObservation {
        kind,
        basis_time,
        scope,
        at,
    }
}

fn compare_paper_candidates(
    left: &PaperDecisionCandidate,
    right: &PaperDecisionCandidate,
) -> std::cmp::Ordering {
    left.signal
        .decision_available_at
        .cmp(&right.signal.decision_available_at)
        .then_with(|| left.event_sequence.cmp(&right.event_sequence))
        .then_with(|| {
            left.signal
                .signal_id
                .as_str()
                .cmp(right.signal.signal_id.as_str())
        })
}

fn paper_analysis_row(
    event_sequence: u64,
    signal: &SignalRecord,
    policy_ref: &PolicyRevisionRef,
    event_digest: &ContentHash,
    explanation: &crate::contracts::RecordedDecisionExplanation,
    period_placement: DecisionAnalysisPeriodPlacement,
) -> DecisionAnalysisDecisionRow {
    DecisionAnalysisDecisionRow {
        decision_id: DecisionAnalysisDecisionId::Paper {
            event_sequence,
            signal_id: signal.signal_id.clone(),
        },
        market: signal.context.market.clone(),
        decision_time: explanation.decision_time,
        period_placement,
        policy_ref: policy_ref.clone(),
        saved_fact_digest: event_digest.clone(),
        strategy_action: explanation.strategy_action,
        constrained_action: explanation.constrained_action,
        execution_state: explanation.execution_state,
        reasons: explanation.reasons.clone(),
    }
}

fn automation_decisions(
    snapshot: &AutomationReviewSnapshot,
    population: &DecisionAnalysisPopulation,
    include_predecessor_context: bool,
    budget: &mut DecisionAnalysisReadBudget,
) -> Result<AutomationDecisionProjection, LabError> {
    let mut lifecycle = AutomationLifecycleIndex::from_snapshot(snapshot);
    let mut decisions = Vec::with_capacity(snapshot.decisions.len());
    let mut predecessor_candidates = Vec::new();
    let mut observations = Vec::new();
    let mut bytes = 0_usize;
    let mut fact_count = 0_usize;
    for decision in &snapshot.decisions {
        budget.check_elapsed()?;
        fact_count = fact_count.saturating_add(1);
        let decision_in_period = placement(population, Some(decision.decision_time_utc)).is_some();
        let warmup_candidate = has_saved_policy_warmup_marker(&decision.evaluation);
        if !decision_in_period && !warmup_candidate {
            continue;
        }
        let projection = project_automation_record(
            snapshot,
            decision,
            &mut lifecycle,
            DecisionAnalysisPeriodPlacement::InPeriod,
        )?;
        fact_count = fact_count.saturating_add(projection.linked_facts);
        if fact_count > MAX_DECISION_ANALYSIS_FACTS {
            return Err(analysis_limit("automation decision-analysis rows"));
        }
        add_input_bytes(&mut bytes, projection.explanation_bytes)?;
        if let Some(warmup) = projection.warmup_observation
            && placement(population, warmup.at).is_some()
        {
            observations.push(warmup);
        }
        if decision_in_period && let Some(detail) = projection.detail {
            decisions.push(detail);
        }
    }

    if include_predecessor_context {
        let mut prior_decisions = snapshot
            .decisions
            .iter()
            .filter(|decision| {
                decision.decision_time_utc < population.period.start
                    && !has_saved_policy_warmup_marker(&decision.evaluation)
            })
            .collect::<Vec<_>>();
        prior_decisions.sort_by(|left, right| {
            right
                .decision_time_utc
                .cmp(&left.decision_time_utc)
                .then_with(|| right.intent_id.cmp(&left.intent_id))
        });
        for decision in prior_decisions {
            budget.check_elapsed()?;
            fact_count = fact_count.saturating_add(1);
            let projection = project_automation_record(
                snapshot,
                decision,
                &mut lifecycle,
                DecisionAnalysisPeriodPlacement::InPeriod,
            )?;
            fact_count = fact_count.saturating_add(projection.linked_facts);
            if fact_count > MAX_DECISION_ANALYSIS_FACTS {
                return Err(analysis_limit("automation predecessor linkage facts"));
            }
            add_input_bytes(&mut bytes, projection.explanation_bytes)?;
            if projection.warmup_observation.is_some() {
                continue;
            }
            let detail = projection.detail.ok_or_else(|| {
                LabError::DataCorrupt("automation predecessor projection has no decision".into())
            })?;
            let decision_time = detail.row.decision_time.ok_or_else(|| {
                LabError::DataCorrupt("automation predecessor has no saved decision time".into())
            })?;
            if decision_time >= population.period.start {
                return Err(LabError::DataCorrupt(
                    "automation predecessor does not precede selected period".into(),
                ));
            }
            predecessor_candidates.push(detail);
            break;
        }
    }
    budget.check_elapsed()?;
    Ok(AutomationDecisionProjection {
        decisions,
        predecessor_candidates,
        observations,
        fact_count,
        explanation_bytes: bytes,
    })
}

fn project_automation_record(
    snapshot: &AutomationReviewSnapshot,
    decision: &crate::contracts::AutomationDecisionRecord,
    lifecycle: &mut AutomationLifecycleIndex,
    period_placement: DecisionAnalysisPeriodPlacement,
) -> Result<AutomationRecordProjection, LabError> {
    let (orders, fills, events) = lifecycle.take_for_intent(&decision.intent_id);
    let linked_facts = orders.len() + fills.len() + events.len();
    let explanation = crate::reporting::decision_explanation::automation_recorded_decision(
        &snapshot.session.policy_ref,
        decision,
        &orders,
        &fills,
        &events,
    )?;
    let explanation_bytes = super::serialized_json_len(&explanation)?;
    if explanation.condition_trace_capture == SavedFactCaptureStatus::NotDecisionEvent {
        if !orders.is_empty() || !fills.is_empty() {
            return Err(LabError::DataCorrupt(
                "saved POLICY_WARMUP intent has linked order or fill facts".into(),
            ));
        }
        return Ok(AutomationRecordProjection {
            detail: None,
            warmup_observation: Some(warmup_observation(decision, &events)?),
            linked_facts,
            explanation_bytes,
        });
    }
    let row = automation_decision_row(
        decision,
        &snapshot.session.market,
        &snapshot.session.policy_ref,
        &explanation,
        period_placement,
    );
    Ok(AutomationRecordProjection {
        detail: Some(DecisionAnalysisDecisionDetail { row, explanation }),
        warmup_observation: None,
        linked_facts,
        explanation_bytes,
    })
}

fn automation_decision_row(
    decision: &crate::contracts::AutomationDecisionRecord,
    market: &crate::contracts::MarketId,
    policy_ref: &PolicyRevisionRef,
    explanation: &crate::contracts::RecordedDecisionExplanation,
    period_placement: DecisionAnalysisPeriodPlacement,
) -> DecisionAnalysisDecisionRow {
    DecisionAnalysisDecisionRow {
        decision_id: DecisionAnalysisDecisionId::Automation {
            intent_id: decision.intent_id.clone(),
        },
        market: market.clone(),
        decision_time: explanation.decision_time,
        period_placement,
        policy_ref: policy_ref.clone(),
        saved_fact_digest: decision.evaluation_digest.clone(),
        strategy_action: explanation.strategy_action,
        constrained_action: explanation.constrained_action,
        execution_state: explanation.execution_state,
        reasons: explanation.reasons.clone(),
    }
}

fn warmup_observation(
    decision: &crate::contracts::AutomationDecisionRecord,
    events: &[AutomationEvent],
) -> Result<DecisionAnalysisObservation, LabError> {
    if !has_saved_policy_warmup_marker(&decision.evaluation) {
        return Err(LabError::DataCorrupt(
            "automation NotDecisionEvent classification has no saved POLICY_WARMUP marker".into(),
        ));
    }
    let event_time = events
        .iter()
        .find(|event| {
            event.kind == AutomationEventKind::SignalObserved
                && event
                    .detail
                    .get("intent_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(decision.intent_id.as_str())
        })
        .map_or(decision.recorded_at_utc, |event| event.at_utc);
    Ok(DecisionAnalysisObservation {
        kind: "POLICY_WARMUP".into(),
        basis_time: DecisionAnalysisObservationClock::AutomationEventTime,
        scope: DecisionAnalysisObservationScope::Market {
            market: crate::contracts::MarketId::parse_upbit(&decision.candle.candle.market)?,
        },
        at: Some(event_time),
    })
}

fn has_saved_policy_warmup_marker(evaluation: &serde_json::Value) -> bool {
    ["phase", "kind"]
        .iter()
        .any(|key| evaluation.get(key).and_then(serde_json::Value::as_str) == Some("POLICY_WARMUP"))
}

fn is_automation_observation(kind: AutomationEventKind) -> bool {
    matches!(
        kind,
        AutomationEventKind::Started
            | AutomationEventKind::Paused
            | AutomationEventKind::Resumed
            | AutomationEventKind::Stopped
            | AutomationEventKind::CandleObserved
            | AutomationEventKind::DecisionDeferred
            | AutomationEventKind::NeedsReconciliation
            | AutomationEventKind::RecoveryBlocked
            | AutomationEventKind::BaselineRecorded
            | AutomationEventKind::BaselineBlocked
    )
}

fn append_automation_event_observations(
    observations: &mut Vec<DecisionAnalysisObservation>,
    snapshot: &AutomationReviewSnapshot,
) -> Result<(), LabError> {
    let candles_by_id: BTreeMap<&str, &crate::contracts::CandleObservation> = snapshot
        .candles
        .iter()
        .map(|candle| (candle.id.as_str(), candle))
        .collect();
    let orders_by_client_id: BTreeMap<&str, &crate::contracts::AutomationOrderView> = snapshot
        .orders
        .iter()
        .map(|order| (order.order.client_identifier.as_str(), &order.order))
        .collect();
    for event in &snapshot.events {
        if is_automation_observation(event.kind) {
            observations.push(DecisionAnalysisObservation {
                kind: serde_json::to_value(event.kind)?
                    .as_str()
                    .ok_or_else(|| {
                        LabError::DataCorrupt("automation event kind is not a string".into())
                    })?
                    .to_owned(),
                basis_time: DecisionAnalysisObservationClock::AutomationEventTime,
                scope: automation_observation_scope_indexed(
                    event,
                    snapshot,
                    &candles_by_id,
                    &orders_by_client_id,
                )?,
                at: Some(event.at_utc),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
fn automation_observation_scope(
    event: &AutomationEvent,
    snapshot: &AutomationReviewSnapshot,
) -> Result<DecisionAnalysisObservationScope, LabError> {
    let candles_by_id: BTreeMap<&str, &crate::contracts::CandleObservation> = snapshot
        .candles
        .iter()
        .map(|candle| (candle.id.as_str(), candle))
        .collect();
    let orders_by_client_id: BTreeMap<&str, &crate::contracts::AutomationOrderView> = snapshot
        .orders
        .iter()
        .map(|order| (order.order.client_identifier.as_str(), &order.order))
        .collect();
    automation_observation_scope_indexed(event, snapshot, &candles_by_id, &orders_by_client_id)
}

fn automation_observation_scope_indexed(
    event: &AutomationEvent,
    snapshot: &AutomationReviewSnapshot,
    candles_by_id: &BTreeMap<&str, &crate::contracts::CandleObservation>,
    orders_by_client_id: &BTreeMap<&str, &crate::contracts::AutomationOrderView>,
) -> Result<DecisionAnalysisObservationScope, LabError> {
    let direct_market = if let Some(value) = event.detail.get("market") {
        let code = value.as_str().ok_or_else(|| {
            LabError::DataCorrupt("automation observation market is not text".into())
        })?;
        Some(crate::contracts::MarketId::parse_upbit(code)?)
    } else {
        None
    };
    if matches!(
        event.kind,
        AutomationEventKind::Started
            | AutomationEventKind::Paused
            | AutomationEventKind::Resumed
            | AutomationEventKind::Stopped
    ) {
        if direct_market.is_some() {
            return Err(LabError::DataCorrupt(
                "whole-session automation control event has a market identity".into(),
            ));
        }
        return Ok(DecisionAnalysisObservationScope::SessionWide);
    }

    let mut markets = BTreeSet::new();
    if let Some(market) = direct_market {
        markets.insert(market);
    }
    if let Some(observation_id) = event
        .detail
        .get("observation_id")
        .and_then(serde_json::Value::as_str)
        && let Some(candle) = candles_by_id.get(observation_id)
    {
        markets.insert(crate::contracts::MarketId::parse_upbit(
            &candle.candle.market,
        )?);
    }
    if let Some(client_identifier) = event
        .detail
        .get("client_identifier")
        .and_then(serde_json::Value::as_str)
        && let Some(order) = orders_by_client_id.get(client_identifier)
    {
        markets.insert(order.market.clone());
    }
    if markets.len() > 1
        || markets
            .iter()
            .any(|market| market != &snapshot.session.market)
    {
        return Err(LabError::DataCorrupt(
            "automation observation has contradictory saved market identities".into(),
        ));
    }
    Ok(markets
        .into_iter()
        .next()
        .map_or(DecisionAnalysisObservationScope::Unknown, |market| {
            DecisionAnalysisObservationScope::Market { market }
        }))
}

fn placement(
    population: &DecisionAnalysisPopulation,
    decision_time: Option<UtcTimestamp>,
) -> Option<DecisionAnalysisPeriodPlacement> {
    match decision_time {
        Some(time) if time >= population.period.start && time < population.period.end => {
            Some(DecisionAnalysisPeriodPlacement::InPeriod)
        }
        None => Some(DecisionAnalysisPeriodPlacement::Unplaced),
        Some(_) => None,
    }
}

fn add_input_bytes(total: &mut usize, bytes: usize) -> Result<(), LabError> {
    *total = total.saturating_add(bytes);
    if *total > MAX_DECISION_ANALYSIS_INPUT_BYTES {
        return Err(analysis_limit("decoded decision-analysis source bytes"));
    }
    Ok(())
}

fn analysis_limit(scope: &str) -> LabError {
    LabError::ResourceLimit(format!(
        "{scope} exceeds the complete saved-source decision-analysis bound; choose a smaller saved session and request a fresh overview"
    ))
}

#[cfg(test)]
impl Store {
    /// Seed a temporary-database fixture for MCP and storage decision-analysis tests.
    ///
    /// It creates one stopped simulated paper session with two equal-time signals in one event,
    /// and one stopped MOCK automation session with a linked partial order/fill plus a later HOLD.
    /// It performs no provider, job, schedule, or venue calls.
    pub(crate) fn seed_decision_analysis_fixture(
        &mut self,
    ) -> Result<DecisionAnalysisTestFixture, LabError> {
        self.seed_decision_analysis_fixture_with_namespace("default")
    }

    /// Seed the same deterministic facts under distinct durable identities for comparison tests.
    #[expect(
        clippy::too_many_lines,
        reason = "one test-only cross-source fixture keeps persisted policy, signal, intent, and lifecycle identities linked for MCP acceptance"
    )]
    pub(crate) fn seed_decision_analysis_fixture_with_namespace(
        &mut self,
        namespace: &str,
    ) -> Result<DecisionAnalysisTestFixture, LabError> {
        if namespace.is_empty()
            || namespace.len() > 24
            || !namespace
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(LabError::InvalidConfig(
                "decision-analysis test namespace must contain 1..=24 lowercase letters, digits, or hyphens".into(),
            ));
        }
        let now = analysis_time("2026-10-14T00:00:00Z")?;
        let market = crate::contracts::MarketId::parse_upbit("KRW-BTC")?;
        let builtin_definitions = crate::contracts::builtin_policy_definitions()?;
        self.seed_builtin_policies_once(&builtin_definitions, now)?;

        let paper_policy = self
            .paper_start_options()?
            .policy_options
            .into_iter()
            .find(|option| option.supported)
            .ok_or_else(|| LabError::Internal("supported paper test policy missing".into()))?;
        let paper_session = self
            .paper_write(
                &crate::contracts::PaperSessionWrite::Start {
                    request_id: crate::contracts::RequestId::new(format!(
                        "analysis-paper-start-{namespace}"
                    ))?,
                    policy_ref: paper_policy.reference.clone(),
                    decision_interval: crate::contracts::CandleInterval::H1,
                    execution_resolution: crate::contracts::CandleInterval::H1,
                    latency_ms: 0,
                    execution: crate::contracts::ExecutionPolicy::NextBarOpen {
                        participation_cap: crate::contracts::Weight::new(Decimal::ONE)?,
                    },
                    markets: vec![
                        analysis_paper_market_start(market.clone())?,
                        analysis_paper_market_start(crate::contracts::MarketId::parse_upbit(
                            "KRW-ETH",
                        )?)?,
                    ],
                },
                now,
            )?
            .session;
        let paper_session_id = paper_session.session_id;
        let paper_before_period =
            crate::contracts::SignalId::new(format!("analysis-signal-before-{namespace}"))?;
        let paper_previous =
            crate::contracts::SignalId::new(format!("analysis-signal-a-{namespace}"))?;
        let paper_selected =
            crate::contracts::SignalId::new(format!("analysis-signal-b-{namespace}"))?;
        let paper_decision_sequence = paper_session
            .sequence
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("paper fixture sequence overflow".into()))?;
        let paper_time = analysis_time("2026-10-14T01:00:00Z")?;
        let paper_before_time = analysis_time("2026-10-14T00:30:00Z")?;
        let decision_event = crate::contracts::PaperSessionEvent {
            sequence: paper_decision_sequence,
            kind: PaperEventKind::CandleProcessed,
            market: Some(market.clone()),
            candle: None,
            source_event_at: Some(paper_time),
            finalized_at: Some(paper_time),
            control: None,
            provider_recovery: None,
            preparation: None,
            signals: vec![
                analysis_paper_signal(PaperSignalFixture {
                    signal_id: paper_before_period.clone(),
                    run_id: format!("analysis-run-before-{namespace}"),
                    model_id: format!("analysis-model-before-{namespace}"),
                    event_sequence: paper_decision_sequence,
                    market: market.clone(),
                    strategy: paper_policy.family,
                    policy_ref: paper_policy.reference.clone(),
                    decision_time: paper_before_time,
                    raw_weight: Decimal::new(1, 1),
                    constrained_weight: Decimal::new(1, 1),
                    reason: crate::contracts::ReasonCode::HoldState,
                })?,
                analysis_paper_signal(PaperSignalFixture {
                    signal_id: paper_previous.clone(),
                    run_id: format!("analysis-run-{namespace}"),
                    model_id: format!("analysis-model-{namespace}"),
                    event_sequence: paper_decision_sequence,
                    market: market.clone(),
                    strategy: paper_policy.family,
                    policy_ref: paper_policy.reference.clone(),
                    decision_time: paper_time,
                    raw_weight: Decimal::new(5, 1),
                    constrained_weight: Decimal::new(1, 1),
                    reason: crate::contracts::ReasonCode::BandEntry,
                })?,
                analysis_paper_signal(PaperSignalFixture {
                    signal_id: paper_selected.clone(),
                    run_id: format!("analysis-run-{namespace}"),
                    model_id: format!("analysis-model-{namespace}"),
                    event_sequence: paper_decision_sequence,
                    market: market.clone(),
                    strategy: paper_policy.family,
                    policy_ref: paper_policy.reference.clone(),
                    decision_time: paper_time,
                    raw_weight: Decimal::new(1, 1),
                    constrained_weight: Decimal::new(1, 1),
                    reason: crate::contracts::ReasonCode::RuleBlocked,
                })?,
            ],
            orders: Vec::new(),
            order_events: Vec::new(),
            fills: Vec::new(),
            account_marks: Vec::new(),
            episodes: Vec::new(),
        };
        seed_analysis_paper_event(self, &paper_session_id, &decision_event)?;

        let eth_market = crate::contracts::MarketId::parse_upbit("KRW-ETH")?;
        let preparation_time = analysis_time("2026-10-14T01:30:00Z")?;
        let preparation_event = crate::contracts::PaperSessionEvent {
            sequence: paper_decision_sequence + 1,
            kind: PaperEventKind::DecisionDeferred,
            market: Some(eth_market.clone()),
            candle: None,
            source_event_at: Some(preparation_time),
            finalized_at: Some(preparation_time),
            control: None,
            provider_recovery: None,
            preparation: Some(crate::contracts::DecisionPreparation {
                status: crate::contracts::DecisionPreparationStatus::AwaitingPolicySources,
                market: eth_market.clone(),
                interval: crate::contracts::CandleInterval::H1,
                observation_id: crate::contracts::ObservationId::new(format!(
                    "analysis-eth-preparation-{namespace}"
                ))?,
                decision_time: preparation_time,
                completed_bars_seen: 0,
                required_bars: 2,
            }),
            signals: Vec::new(),
            orders: Vec::new(),
            order_events: Vec::new(),
            fills: Vec::new(),
            account_marks: Vec::new(),
            episodes: Vec::new(),
        };
        seed_analysis_paper_event(self, &paper_session_id, &preparation_event)?;

        let recovery_time = analysis_time("2026-10-14T01:45:00Z")?;
        let recovery_target_completed = analysis_time("2026-10-14T02:00:00Z")?;
        let recovery_event = crate::contracts::PaperSessionEvent {
            sequence: paper_decision_sequence + 2,
            kind: PaperEventKind::ProviderRecovery,
            market: None,
            candle: None,
            source_event_at: Some(recovery_time),
            finalized_at: Some(recovery_time),
            control: None,
            provider_recovery: Some(crate::contracts::PaperProviderRecovery {
                phase: crate::contracts::PaperProviderRecoveryPhase::Progress,
                processed_at: recovery_time,
                target_completed_at: recovery_target_completed,
                proof_ids: vec![ContentHash::of_bytes(
                    format!("analysis-recovery-{namespace}").as_bytes(),
                )],
                checkpoint_transitions: [market.clone(), eth_market]
                    .into_iter()
                    .map(|market| crate::contracts::PaperCheckpointTransition {
                        market,
                        before_digest: ContentHash::of_bytes(b"analysis-before"),
                        after_digest: ContentHash::of_bytes(b"analysis-after"),
                        from_close: None,
                        to_close: Some(recovery_target_completed),
                    })
                    .collect(),
            }),
            preparation: None,
            signals: Vec::new(),
            orders: Vec::new(),
            order_events: Vec::new(),
            fills: Vec::new(),
            account_marks: Vec::new(),
            episodes: Vec::new(),
        };
        seed_analysis_paper_event(self, &paper_session_id, &recovery_event)?;
        let _paper_stopped = self
            .paper_write(
                &crate::contracts::PaperSessionWrite::Stop {
                    request_id: crate::contracts::RequestId::new(format!(
                        "analysis-paper-stop-{namespace}"
                    ))?,
                    session_id: paper_session_id.clone(),
                    expected_sequence: recovery_event.sequence,
                },
                analysis_time("2026-10-14T01:50:00Z")?,
            )?
            .session;

        let (policy_id, family, definition) = crate::contracts::builtin_policy_definitions()?
            .into_iter()
            .next()
            .ok_or_else(|| {
                LabError::Internal("automation test policy definition missing".into())
            })?;
        let automation_policy = crate::contracts::FrozenPolicyRevision {
            reference: PolicyRevisionRef {
                policy_id,
                revision_id: crate::contracts::PolicyRevisionId::new(
                    "decision-analysis-test-revision",
                )?,
                definition_digest: ContentHash::of_value(&definition)?,
            },
            revision_number: 1,
            parent_revision_id: None,
            family,
            origin: crate::contracts::PolicyOrigin::Builtin,
            definition,
        };
        let automation_session_id = crate::contracts::AutomationSessionId::new(format!(
            "decision-analysis-automation-{namespace}"
        ))?;
        let automation_created = analysis_time("2026-10-13T23:00:00Z")?;
        let risk = crate::contracts::AutomationRiskPolicy {
            activation_loss_limit_krw: None,
            max_order_notional_krw: crate::contracts::QuoteAmount::new(Decimal::from(50_000))?,
            max_daily_notional_krw: crate::contracts::QuoteAmount::new(Decimal::from(500_000))?,
            max_asset_exposure_krw: crate::contracts::QuoteAmount::new(Decimal::from(100_000))?,
            max_daily_loss_krw: crate::contracts::QuoteAmount::new(Decimal::from(10_000))?,
            max_open_orders: 2,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 3_600,
            max_feed_age_seconds: 3_600,
        };
        self.connection
            .execute(
                "INSERT INTO automation_sessions(\
                     session_id,start_request_id,account_identity,mode,state,sequence,policy_ref_json,\
                     frozen_policy_json,market,decision_interval,risk_json,checkpoint_json,created_at_utc,updated_at_utc\
                ) VALUES (?1,?2,'mock:upbit-default','MOCK','RUNNING',1,?3,?4,\
                           'KRW-BTC','h1',?5,'{}',?6,?6)",
                params![
                    automation_session_id.as_str(),
                    format!("analysis-automation-start-{namespace}"),
                    serde_json::to_string(&automation_policy.reference)?,
                    serde_json::to_string(&automation_policy)?,
                    serde_json::to_string(&risk)?,
                    automation_created.to_rfc3339(),
                ],
            )
            .map_err(super::sql_error)?;
        let automation_before_period = crate::contracts::AutomationIntentId::new(format!(
            "analysis-intent-before-period-{namespace}"
        ))?;
        let automation_warmup = crate::contracts::AutomationIntentId::new(format!(
            "analysis-intent-warmup-{namespace}"
        ))?;
        let automation_previous =
            crate::contracts::AutomationIntentId::new(format!("analysis-intent-a-{namespace}"))?;
        let automation_selected =
            crate::contracts::AutomationIntentId::new(format!("analysis-intent-b-{namespace}"))?;
        let before_period_observation =
            analysis_automation_observation("2026-10-13T23:00:00Z", "2026-10-14T00:00:00Z", 99)?;
        let before_period_time = before_period_observation.candle.close_time_utc;
        let before_period_evaluation = analysis_evaluation(before_period_time, "ANALYSIS_PRIOR");
        self.automation_record_decision(
            &automation_session_id,
            &before_period_observation,
            "{}",
            &automation_before_period,
            "HOLD",
            &before_period_evaluation,
            analysis_time("2026-10-14T00:00:01Z")?,
        )?;
        let warmup_observation =
            analysis_automation_observation("2026-10-14T00:00:00Z", "2026-10-14T01:00:00Z", 99)?;
        let warmup_time = warmup_observation.candle.close_time_utc;
        let warmup_evaluation = analysis_warmup_evaluation(warmup_time);
        self.automation_record_decision(
            &automation_session_id,
            &warmup_observation,
            "{}",
            &automation_warmup,
            "HOLD",
            &warmup_evaluation,
            analysis_time("2026-10-14T01:00:01Z")?,
        )?;
        let first_observation =
            analysis_automation_observation("2026-10-14T01:00:00Z", "2026-10-14T02:00:00Z", 100)?;
        let first_time = first_observation.candle.close_time_utc;
        let first_evaluation = analysis_evaluation(first_time, "ANALYSIS_ENTRY");
        self.automation_record_decision(
            &automation_session_id,
            &first_observation,
            "{}",
            &automation_previous,
            "BUY",
            &first_evaluation,
            analysis_time("2026-10-14T02:00:01Z")?,
        )?;
        seed_analysis_partial_order(self, &automation_session_id, &automation_previous)?;
        let second_observation =
            analysis_automation_observation("2026-10-14T02:00:00Z", "2026-10-14T03:00:00Z", 101)?;
        let second_time = second_observation.candle.close_time_utc;
        let second_evaluation = analysis_evaluation(second_time, "ANALYSIS_HOLD");
        self.automation_record_decision(
            &automation_session_id,
            &second_observation,
            "{}",
            &automation_selected,
            "HOLD",
            &second_evaluation,
            analysis_time("2026-10-14T03:00:01Z")?,
        )?;
        self.connection
            .execute(
                "UPDATE automation_sessions SET state='STOPPED',updated_at_utc=?1 WHERE session_id=?2",
                params![
                    analysis_time("2026-10-14T03:00:01Z")?.to_rfc3339(),
                    automation_session_id.as_str(),
                ],
            )
            .map_err(super::sql_error)?;

        let period = DecisionAnalysisPeriod {
            start: analysis_time("2026-10-14T01:00:00Z")?,
            end: analysis_time("2026-10-14T04:00:00Z")?,
        };
        let automation_client_identifier =
            AutomationOrderIdentifier::from_intent(&automation_previous);
        Ok(DecisionAnalysisTestFixture {
            paper_population: DecisionAnalysisPopulation {
                source: DecisionAnalysisSource::Paper {
                    session_id: paper_session_id,
                },
                period: period.clone(),
                market: Some(market.clone()),
                policy_ref: Some(paper_policy.reference),
            },
            paper_before_period: DecisionAnalysisDecisionId::Paper {
                event_sequence: paper_decision_sequence,
                signal_id: paper_before_period,
            },
            paper_previous: DecisionAnalysisDecisionId::Paper {
                event_sequence: paper_decision_sequence,
                signal_id: paper_previous,
            },
            paper_selected: DecisionAnalysisDecisionId::Paper {
                event_sequence: paper_decision_sequence,
                signal_id: paper_selected,
            },
            automation_population: DecisionAnalysisPopulation {
                source: DecisionAnalysisSource::Automation {
                    session_id: automation_session_id,
                },
                period,
                market: Some(market),
                policy_ref: Some(automation_policy.reference),
            },
            automation_before_period: DecisionAnalysisDecisionId::Automation {
                intent_id: automation_before_period,
            },
            automation_previous: DecisionAnalysisDecisionId::Automation {
                intent_id: automation_previous,
            },
            automation_selected: DecisionAnalysisDecisionId::Automation {
                intent_id: automation_selected,
            },
            automation_client_identifier,
        })
    }
}

#[cfg(test)]
fn analysis_time(value: &str) -> Result<UtcTimestamp, LabError> {
    UtcTimestamp::parse_rfc3339(value)
}

#[cfg(test)]
fn analysis_zero_costs() -> Result<crate::contracts::CostPolicy, LabError> {
    let zero = crate::contracts::BasisPoints::new(Decimal::ZERO)?;
    Ok(crate::contracts::CostPolicy {
        buy_fee_bps: zero,
        sell_fee_bps: zero,
        maker_fee_bps: zero,
        half_spread_bps: zero,
        slippage_bps: zero,
        impact_bps: zero,
        assumption_label: "decision-analysis-test-only".into(),
        dynamic: None,
    })
}

#[cfg(test)]
fn analysis_paper_market_start(
    market: crate::contracts::MarketId,
) -> Result<crate::contracts::PaperMarketStart, LabError> {
    Ok(crate::contracts::PaperMarketStart {
        market,
        initial_cash: crate::contracts::QuoteAmount::new(Decimal::from(1_000))?,
        costs: analysis_zero_costs()?,
        market_rules: crate::contracts::PaperMarketRuleScenario::SimulatedScenario {
            min_notional: crate::contracts::QuoteAmount::new(Decimal::ONE)?,
            quantity_step: crate::contracts::AssetQuantity::new(Decimal::new(1, 1))?,
            ticks: vec![crate::contracts::TickBand {
                lower_bound: crate::contracts::QuoteAmount::new(Decimal::ZERO)?,
                tick: crate::contracts::PriceKrw::new(Decimal::ONE)?,
            }],
            assumption_label: "decision-analysis-test-only".into(),
        },
    })
}

#[cfg(test)]
fn seed_analysis_paper_event(
    store: &mut Store,
    session_id: &crate::contracts::PaperSessionId,
    event: &crate::contracts::PaperSessionEvent,
) -> Result<(), LabError> {
    let event_time = event
        .finalized_at
        .or(event.source_event_at)
        .ok_or_else(|| {
            LabError::InvalidConfig("paper test event requires a saved event time".into())
        })?;
    let mut view = store.paper_get_session(session_id, event_time)?.session;
    let expected_sequence = view
        .sequence
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("paper fixture sequence overflow".into()))?;
    if event.sequence != expected_sequence {
        return Err(LabError::DataCorrupt(
            "paper test event does not advance the persisted sequence by one".into(),
        ));
    }
    let kind = serde_json::to_value(event.kind)?
        .as_str()
        .ok_or_else(|| LabError::Internal("paper test event kind is not text".into()))?
        .to_owned();
    store
        .connection
        .execute(
            "INSERT INTO paper_session_events(session_id,sequence,market,kind,occurred_at_utc,event_json) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                session_id.as_str(),
                i64::try_from(event.sequence).map_err(|_| {
                    LabError::ResourceLimit("paper fixture sequence exceeds SQLite range".into())
                })?,
                event.market.as_ref().map(crate::contracts::MarketId::code),
                kind,
                event_time.to_rfc3339(),
                serde_json::to_string(event)?,
            ],
        )
        .map_err(super::sql_error)?;
    view.sequence = event.sequence;
    view.event_count = view
        .event_count
        .checked_add(1)
        .ok_or_else(|| LabError::ResourceLimit("paper fixture event count overflow".into()))?;
    view.last_evaluated_at = Some(event_time);
    let updated = store
        .connection
        .execute(
            "UPDATE paper_sessions SET sequence=?1,event_count=?2,view_json=?3 WHERE session_id=?4",
            params![
                i64::try_from(view.sequence).map_err(|_| {
                    LabError::ResourceLimit("paper fixture sequence exceeds SQLite range".into())
                })?,
                i64::try_from(view.event_count).map_err(|_| {
                    LabError::ResourceLimit("paper fixture event count exceeds SQLite range".into())
                })?,
                serde_json::to_string(&view)?,
                session_id.as_str(),
            ],
        )
        .map_err(super::sql_error)?;
    if updated != 1 {
        return Err(LabError::DataCorrupt(
            "paper fixture session update did not match exactly one row".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn analysis_paper_signal(
    fixture: PaperSignalFixture,
) -> Result<crate::contracts::SignalRecord, LabError> {
    let PaperSignalFixture {
        signal_id,
        run_id,
        model_id,
        event_sequence,
        market,
        strategy,
        policy_ref,
        decision_time,
        raw_weight,
        constrained_weight,
        reason,
    } = fixture;
    Ok(crate::contracts::SignalRecord {
        context: crate::contracts::EventContext {
            run_id: crate::contracts::RunId::new(run_id)?,
            model_id: crate::contracts::ModelId::new(model_id)?,
            market,
            event_seq: event_sequence,
            accounting_event_time: decision_time,
        },
        signal_id,
        strategy,
        strategy_version: "decision-analysis-test".into(),
        policy_ref: Some(policy_ref),
        policy_trace: None,
        decision_trace: None,
        actual_weight_at_decision: Some(crate::contracts::Weight::new(Decimal::new(1, 1))?),
        source_bar_ids: Vec::new(),
        signal_time: decision_time,
        decision_available_at: decision_time,
        valid_until: UtcTimestamp(decision_time.0 + chrono::Duration::hours(1)),
        state_before: crate::contracts::PositionState::Cash,
        state_after: crate::contracts::PositionState::Cash,
        raw_target_weight: crate::contracts::Weight::new(raw_weight)?,
        constrained_target_weight: crate::contracts::Weight::new(constrained_weight)?,
        intended_side: None,
        reasons: vec![reason],
        indicators: crate::contracts::IndicatorSnapshot {
            version: "decision-analysis-test".into(),
            ema: None,
            bar_sigma: None,
            annual_vol: None,
            rsi: None,
            entry_high: None,
            exit_low: None,
            completed_bars_seen: 0,
            policy_values: Vec::new(),
        },
        evidence_effect: None,
        outcome: crate::contracts::SignalOutcome::NoAction,
    })
}

#[cfg(test)]
fn analysis_automation_observation(
    open_at: &str,
    close_at: &str,
    close_price: i64,
) -> Result<crate::contracts::CandleObservation, LabError> {
    let open_time_utc = analysis_time(open_at)?;
    let close_time_utc = analysis_time(close_at)?;
    let close = crate::contracts::PriceKrw::new(Decimal::from(close_price))?;
    let candle = crate::contracts::CandleRecord {
        market: "KRW-BTC".into(),
        interval: crate::contracts::CandleInterval::H1,
        open_time_utc,
        close_time_utc,
        open: close,
        high: close,
        low: close,
        close,
        volume: crate::contracts::AssetQuantity::new(Decimal::ONE)?,
        quote_turnover: crate::contracts::QuoteAmount::new(Decimal::from(close_price))?,
        completed: true,
    };
    let content_digest = crate::contracts::observation_digest(&candle)?;
    Ok(crate::contracts::CandleObservation {
        id: crate::contracts::ObservationId::from_seed(content_digest.as_str()),
        candle,
        content_digest,
        raw_object_ids: Vec::new(),
        constituent_ids: Vec::new(),
    })
}

#[cfg(test)]
fn analysis_evaluation(decision_time: UtcTimestamp, reason: &str) -> serde_json::Value {
    let raw_target_weight = if reason == "ANALYSIS_ENTRY" {
        "0.5"
    } else {
        "0.1"
    };
    serde_json::json!({
        "decision_time_utc": decision_time,
        "reasons": [reason],
        "indicators": {"ema": "101.25", "volatility": "0.02"},
        "actual_weight_at_decision": "0.1",
        "raw_target_weight": raw_target_weight,
        "constrained_target_weight": "0.1",
    })
}

#[cfg(test)]
fn analysis_warmup_evaluation(decision_time: UtcTimestamp) -> serde_json::Value {
    serde_json::json!({
        "phase": "POLICY_WARMUP",
        "decision_time": decision_time,
        "decision_time_utc": decision_time,
        "completed_bars_seen": 1,
        "policy_evaluation": null,
    })
}

#[cfg(test)]
fn seed_analysis_partial_order(
    store: &mut Store,
    session_id: &crate::contracts::AutomationSessionId,
    intent_id: &AutomationIntentId,
) -> Result<(), LabError> {
    let client_identifier = AutomationOrderIdentifier::from_intent(intent_id);
    store
        .connection
        .execute(
            "INSERT INTO automation_orders(\
                 client_identifier,intent_id,session_id,account_identity,order_request_json,\
                 rules_snapshot_json,request_digest,market,side,state,requested_notional_krw,\
                 decision_mark_krw,reserve_quote_krw,reserve_asset,reserved_asset_volume,\
                 reserved_fee_krw,remaining_fee_krw,executed_volume,remaining_volume,paid_fee_krw,\
                 locked_amount,locked_currency,trades_count,venue_created_at_utc,created_at_utc,updated_at_utc\
             ) VALUES (?1,?2,?3,'mock:upbit-default','{}','{}',?4,'KRW-BTC','BID',\
                       'PARTIALLY_FILLED','1000','101','1001','BTC','0','1','0.8','2','3','0.2',\
                       '800','KRW',1,'2026-10-14T02:00:02Z','2026-10-14T02:00:02Z','2026-10-14T02:30:00Z')",
            params![
                client_identifier.as_str(),
                intent_id.as_str(),
                session_id.as_str(),
                ContentHash::of_bytes(b"decision-analysis-order").as_str(),
            ],
        )
        .map_err(super::sql_error)?;
    store
        .connection
        .execute(
            "UPDATE automation_intents SET state='ORDER_SUBMITTED' WHERE intent_id=?1",
            [intent_id.as_str()],
        )
        .map_err(super::sql_error)?;
    store
        .connection
        .execute(
            "INSERT INTO automation_fills(\
                 trade_uuid,client_identifier,session_id,market,side,price_krw,volume,funds_krw,\
                 trade_time_utc,source_digest,created_at_utc\
                 ) VALUES (?1,?2,?3,'KRW-BTC','BID','101','2','202',\
                       '2026-10-14T02:30:00Z',?4,'2026-10-14T02:30:01Z')",
            params![
                format!("analysis-partial-fill-{}", intent_id.as_str()),
                client_identifier.as_str(),
                session_id.as_str(),
                ContentHash::of_bytes(intent_id.as_str().as_bytes()).as_str(),
            ],
        )
        .map_err(super::sql_error)?;
    store
        .connection
        .execute(
            "INSERT INTO automation_fee_accruals(\
                 client_identifier,sequence,delta_paid_fee_krw,cumulative_paid_fee_krw,observed_at_utc\
                 ) VALUES (?1,1,'0.2','0.2','2026-10-14T02:30:01Z')",
            [client_identifier.as_str()],
        )
        .map_err(super::sql_error)?;
    let sequence: i64 = store
        .connection
        .query_row(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM automation_session_events WHERE session_id=?1",
            [session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(super::sql_error)?;
    store
        .connection
        .execute(
            "INSERT INTO automation_session_events(session_id,sequence,kind,at_utc,detail_json) \
             VALUES (?1,?2,'ORDER_ADMITTED','2026-10-14T02:00:02Z',?3)",
            params![
                session_id.as_str(),
                sequence,
                serde_json::json!({
                    "intent_id": intent_id,
                    "client_identifier": client_identifier,
                    "notional_krw": "1000",
                })
                .to_string(),
            ],
        )
        .map_err(super::sql_error)?;
    Ok(())
}

#[cfg(test)]
mod tests;

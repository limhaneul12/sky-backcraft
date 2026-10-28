//! Per-read indexing of one already bounded immutable paper event prefix.

use crate::contracts::{
    FillRecord, LabError, OrderRecord, PaperSessionEvent, PolicyRevisionRef,
    RecordedDecisionExplanation, SignalRecord,
};
use crate::storage::Store;
use std::collections::BTreeMap;
use std::time::Instant;

type OrderFacts<'a> = Vec<(u64, u64, &'a OrderRecord)>;
type FillFacts<'a> = Vec<(u64, &'a FillRecord)>;

pub(super) struct PaperLifecycleIndex<'a> {
    signals: BTreeMap<String, (u64, &'a SignalRecord)>,
    orders: BTreeMap<String, OrderFacts<'a>>,
    fills: BTreeMap<String, FillFacts<'a>>,
    as_of_sequence: u64,
    deadline: Instant,
}

impl<'a> PaperLifecycleIndex<'a> {
    pub(super) fn from_events(
        events: &'a [PaperSessionEvent],
        deadline: Instant,
    ) -> Result<Self, LabError> {
        check_deadline(deadline)?;
        let mut signals = BTreeMap::new();
        let mut orders: BTreeMap<String, OrderFacts<'a>> = BTreeMap::new();
        let mut parents = BTreeMap::new();
        let mut pending_fills = Vec::new();
        let mut previous_sequence = 0;
        for event in events {
            check_deadline(deadline)?;
            if event.sequence <= previous_sequence {
                return Err(LabError::DataCorrupt(
                    "paper prefix event order is not strictly increasing".into(),
                ));
            }
            previous_sequence = event.sequence;
            if event.preparation.is_some() && !event.signals.is_empty() {
                return Err(LabError::DataCorrupt(
                    "paper event contains both a decision signal and deferred preparation".into(),
                ));
            }
            for signal in &event.signals {
                check_deadline(deadline)?;
                if signals
                    .insert(
                        signal.signal_id.as_str().to_owned(),
                        (event.sequence, signal),
                    )
                    .is_some()
                {
                    return Err(LabError::DataCorrupt(
                        "paper prefix repeats a saved signal identity".into(),
                    ));
                }
            }
            for order in event.orders.iter().chain(&event.order_events) {
                check_deadline(deadline)?;
                let parent = order.parent_signal_id.as_str();
                let id = order.order_id.as_str();
                if let Some(old_parent) = parents.insert(id.to_owned(), parent.to_owned())
                    && old_parent != parent
                {
                    return Err(LabError::DataCorrupt(
                        "paper order changed its parent signal identity".into(),
                    ));
                }
                orders.entry(parent.to_owned()).or_default().push((
                    order.context.event_seq,
                    event.sequence,
                    order,
                ));
            }
            pending_fills.extend(event.fills.iter().map(|fill| (event.sequence, fill)));
        }
        let mut fills: BTreeMap<String, FillFacts<'a>> = BTreeMap::new();
        for (sequence, fill) in pending_fills {
            check_deadline(deadline)?;
            let parent = parents.get(fill.order_id.as_str()).ok_or_else(|| {
                LabError::DataCorrupt("paper prefix fill has no recorded order identity".into())
            })?;
            fills
                .entry(parent.clone())
                .or_default()
                .push((sequence, fill));
        }
        for parent in orders.keys() {
            if !signals.contains_key(parent) {
                return Err(LabError::DataCorrupt(
                    "paper prefix order has no recorded parent signal".into(),
                ));
            }
        }
        Ok(Self {
            signals,
            orders,
            fills,
            as_of_sequence: previous_sequence,
            deadline,
        })
    }

    pub(super) fn explanation(
        &self,
        signal: &SignalRecord,
        policy_ref: &PolicyRevisionRef,
    ) -> Result<RecordedDecisionExplanation, LabError> {
        check_deadline(self.deadline)?;
        let key = signal.signal_id.as_str();
        let (event_sequence, saved_signal) = self.signals.get(key).ok_or_else(|| {
            LabError::DataCorrupt("selected signal is absent from its indexed paper prefix".into())
        })?;
        // The index owns the immutable source references; do not project a caller's altered copy.
        if *saved_signal != signal {
            return Err(LabError::DataCorrupt(
                "selected signal differs from its indexed original fact".into(),
            ));
        }
        let orders = self
            .orders
            .get(key)
            .into_iter()
            .flatten()
            .map(|(internal, event, order)| (*internal, *event, (*order).clone()))
            .collect();
        let fills = self
            .fills
            .get(key)
            .into_iter()
            .flatten()
            .map(|(event, fill)| (*event, (*fill).clone()))
            .collect();
        let explanation = Store::paper_recorded_decision_from_lifecycle(
            saved_signal,
            policy_ref,
            *event_sequence,
            self.as_of_sequence,
            orders,
            fills,
        )?;
        check_deadline(self.deadline)?;
        Ok(explanation)
    }
}

fn check_deadline(deadline: Instant) -> Result<(), LabError> {
    if Instant::now() >= deadline {
        return Err(LabError::ResourceLimit(
            "paper decision analysis exceeded its shared loading deadline; no partial totals were returned".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

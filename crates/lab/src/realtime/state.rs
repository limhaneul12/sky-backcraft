use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::contracts::{
    CandleInterval, CandleObservation, ContentHash, LabError, PriceKrw, ProviderAbsenceAnchor,
    ProviderAbsenceSpan, ProviderFeedRecoveryProgress, RawObjectRef, RawWebSocketTransport,
    UtcTimestamp, observation_digest,
};

use super::parser::ValidatedCandleFrame;
use super::subscription::PublicMarketSubscription;

const MAX_PENDING_CANDIDATES_PER_SUBSCRIPTION: usize = 2;
const MIN_REST_TIP_CONFIRMATION_SECONDS: i64 = 1;
const MIN_REST_RETRY_SECONDS: u64 = 1;
const MAX_REST_RETRY_SECONDS: u64 = 30;

#[derive(Debug, Clone)]
struct PendingRestTipVerification {
    content_digest: ContentHash,
    first_fetched_at_utc: UtcTimestamp,
}

#[derive(Debug, Clone, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "these independent flags preserve the existing feed-readiness predicate"
)]
struct SubscriptionReadinessFacts {
    rest_bootstrapped: bool,
    rest_only_ready: bool,
    recovery_required: bool,
    stream_live: bool,
    reconciled: bool,
    websocket_is_fresh: bool,
    provider_clock_valid: bool,
    rest_is_fresh: bool,
    rest_confirms_accepted_tip: bool,
    pending_rest_confirmation: bool,
    last_received_at_utc: Option<UtcTimestamp>,
    provider_at_utc: Option<UtcTimestamp>,
    latest_websocket_open_time_utc: Option<UtcTimestamp>,
    latest_rest_close_time_utc: Option<UtcTimestamp>,
    last_rest_fetched_at_utc: Option<UtcTimestamp>,
    revision_snapshot_count: u64,
    last_revision_open_time_utc: Option<UtcTimestamp>,
    last_revision_detected_at_utc: Option<UtcTimestamp>,
    confirmed_provider_absence_spans: Vec<ProviderAbsenceSpan>,
    provider_recovery_progress: Option<ProviderFeedRecoveryProgress>,
}

impl SubscriptionReadinessFacts {
    const fn is_ready(&self, interval: CandleInterval) -> bool {
        if matches!(interval, CandleInterval::D1) {
            return self.rest_bootstrapped
                && self.rest_only_ready
                && !self.recovery_required
                && self.reconciled
                && self.rest_is_fresh;
        }
        self.rest_bootstrapped
            && !self.recovery_required
            && self.stream_live
            && self.reconciled
            && self.provider_clock_valid
            && (self.websocket_is_fresh || (self.rest_is_fresh && self.rest_confirms_accepted_tip))
    }

    fn blockers(
        &self,
        interval: CandleInterval,
    ) -> Vec<crate::contracts::RealtimeReadinessBlocker> {
        use crate::contracts::RealtimeReadinessBlocker as Blocker;

        let mut blockers = Vec::new();
        if !self.rest_bootstrapped {
            blockers.push(Blocker::RestBootstrapRequired);
        }
        if self.recovery_required {
            blockers.push(Blocker::RecoveryRequired);
        }
        if matches!(interval, CandleInterval::D1) {
            if !self.rest_only_ready || !self.reconciled || !self.rest_is_fresh {
                blockers.push(Blocker::RestOnlyInputUnavailable);
            }
            return blockers;
        }
        if !self.stream_live {
            blockers.push(Blocker::StreamNotConfirmed);
        }
        if !self.reconciled {
            blockers.push(Blocker::ReconciliationPending);
        }
        let rest_confirms_latest = self.rest_is_fresh && self.rest_confirms_accepted_tip;
        if !self.websocket_is_fresh && !rest_confirms_latest {
            if self.latest_websocket_open_time_utc.is_none() {
                blockers.push(Blocker::WebsocketCandleMissing);
            } else {
                blockers.push(Blocker::WebsocketCandleStale);
            }
        }
        if self.latest_websocket_open_time_utc.is_some() && !self.provider_clock_valid {
            blockers.push(Blocker::ProviderClockInvalid);
        }
        if !self.pending_rest_confirmation && !self.websocket_is_fresh && !rest_confirms_latest {
            blockers.push(Blocker::RestConfirmationMissing);
        }
        blockers
    }
}

#[derive(Debug, Clone)]
pub struct LatestMarketPrice {
    pub market: String,
    pub price: PriceKrw,
    pub provider_at: UtcTimestamp,
    pub received_at: UtcTimestamp,
}

#[derive(Debug, Clone)]
pub(super) struct PendingCandle {
    pub frame: ValidatedCandleFrame,
    pub raw_frame: Vec<u8>,
    pub transport: RawWebSocketTransport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameDisposition {
    New,
    Updated,
    Duplicate,
    Gap,
    OutOfOrder,
}

#[derive(Debug, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "these booleans represent independent bounded subscription lifecycle and readiness facts"
)]
pub(super) struct SubscriptionCache {
    current: Option<PendingCandle>,
    closed_candidates: VecDeque<PendingCandle>,
    confirmed: VecDeque<CandleObservation>,
    confirmed_open_time: Option<UtcTimestamp>,
    rest_prefix: VecDeque<CandleObservation>,
    accepted_anchor_pin: Option<CandleObservation>,
    rest_raw: Option<RawObjectRef>,
    rest_bootstrapped: bool,
    rest_only_ready: bool,
    stream_live: bool,
    reconciled: bool,
    recovery_required: bool,
    last_message_sequence: Option<u64>,
    pending_rest_tip: Option<PendingRestTipVerification>,
    provider_absence_confirmation_until: Option<Instant>,
    rest_retry_failures: u8,
    rest_retry_at: Option<Instant>,
    last_revision_fingerprint: Option<ContentHash>,
    revision_snapshot_count: u64,
    last_revision_open_time_utc: Option<UtcTimestamp>,
    last_revision_detected_at_utc: Option<UtcTimestamp>,
    confirmed_provider_absence_spans: Vec<ProviderAbsenceSpan>,
    provider_recovery_progress: Option<ProviderFeedRecoveryProgress>,
}

#[derive(Debug)]
pub(super) struct FeedCache {
    bars_per_subscription: usize,
    subscriptions: HashMap<String, SubscriptionCache>,
    latest_prices: HashMap<String, LatestMarketPrice>,
    last_rest_probe_key: Option<String>,
}

impl FeedCache {
    pub fn new(bars_per_subscription: usize) -> Self {
        Self {
            bars_per_subscription,
            subscriptions: HashMap::new(),
            latest_prices: HashMap::new(),
            last_rest_probe_key: None,
        }
    }

    pub fn apply_frame(
        &mut self,
        frame: ValidatedCandleFrame,
        raw_frame: Vec<u8>,
        transport: RawWebSocketTransport,
    ) -> FrameDisposition {
        let key = subscription_key(&frame.subscription);
        let market_code = frame.candle.market.clone();
        let latest = LatestMarketPrice {
            market: market_code.clone(),
            price: frame.candle.close,
            provider_at: frame.provider_at,
            received_at: frame.received_at,
        };
        let cache = self.subscriptions.entry(key).or_default();
        if cache
            .last_message_sequence
            .is_some_and(|last| transport.message_sequence <= last)
        {
            cache.recovery_required = true;
            cache.reconciled = false;
            return FrameDisposition::OutOfOrder;
        }
        cache.last_message_sequence = Some(transport.message_sequence);
        let confirmed_open_time = cache.confirmed_open_time;
        cache.closed_candidates.retain(|candidate| {
            confirmed_open_time
                .is_none_or(|confirmed| candidate.frame.candle.open_time_utc > confirmed)
        });

        let incoming = PendingCandle {
            frame,
            raw_frame,
            transport,
        };
        let disposition = match cache.current.as_ref() {
            None => {
                cache.current = Some(incoming.clone());
                cache.stream_live = confirms_subscription(incoming.frame.stream_kind);
                FrameDisposition::New
            }
            Some(current)
                if incoming.frame.candle.open_time_utc == current.frame.candle.open_time_utc =>
            {
                if incoming.frame.provider_at < current.frame.provider_at {
                    cache.recovery_required = true;
                    cache.reconciled = false;
                    FrameDisposition::OutOfOrder
                } else if incoming.frame.candle == current.frame.candle {
                    // Duplicate economic state may carry a later receive timestamp.
                    cache.current = Some(incoming.clone());
                    cache.stream_live = confirms_subscription(incoming.frame.stream_kind);
                    FrameDisposition::Duplicate
                } else if is_monotonic_candle_update(&current.frame, &incoming.frame) {
                    cache.current = Some(incoming.clone());
                    cache.stream_live = confirms_subscription(incoming.frame.stream_kind);
                    FrameDisposition::Updated
                } else {
                    cache.recovery_required = true;
                    cache.reconciled = false;
                    FrameDisposition::OutOfOrder
                }
            }
            Some(current)
                if incoming.frame.candle.open_time_utc < current.frame.candle.open_time_utc =>
            {
                cache.recovery_required = true;
                cache.reconciled = false;
                FrameDisposition::OutOfOrder
            }
            Some(current) => {
                // A stalled REST consumer must not poison the socket across epochs.
                // Coalesce only unconfirmed frames; explicit REST gap recovery owns finality.
                let previous = current.clone();
                let previous_is_unconfirmed = cache
                    .confirmed_open_time
                    .is_none_or(|confirmed| previous.frame.candle.open_time_utc > confirmed);
                let saturated = previous_is_unconfirmed
                    && cache.closed_candidates.len() >= MAX_PENDING_CANDIDATES_PER_SUBSCRIPTION;
                if saturated {
                    cache.closed_candidates.pop_front();
                }
                let contiguous =
                    incoming.frame.candle.open_time_utc == previous.frame.candle.close_time_utc;
                if previous_is_unconfirmed {
                    cache.closed_candidates.push_back(previous);
                }
                cache.current = Some(incoming.clone());
                cache.stream_live = confirms_subscription(incoming.frame.stream_kind);
                if contiguous && !saturated {
                    FrameDisposition::New
                } else {
                    cache.recovery_required = true;
                    cache.reconciled = false;
                    FrameDisposition::Gap
                }
            }
        };
        if !matches!(disposition, FrameDisposition::OutOfOrder) {
            match self.latest_prices.get(&market_code) {
                Some(current) if current.provider_at > latest.provider_at => {}
                _ => {
                    self.latest_prices.insert(market_code, latest);
                }
            }
        }
        disposition
    }

    /// Replace the bounded REST evidence prefix without advancing finality.
    /// Call `confirm_rest_snapshot` only after the external tip gate succeeds.
    pub fn store_rest_snapshot(
        &mut self,
        subscription: &PublicMarketSubscription,
        raw: RawObjectRef,
        observations: &[CandleObservation],
        recovery_complete: bool,
    ) {
        let maximum_bars = self.bars_per_subscription;
        let key = subscription_key(subscription);
        let cache = self.subscriptions.entry(key).or_default();
        cache.rest_prefix = observations
            .iter()
            .rev()
            .take(maximum_bars)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        cache.rest_raw = Some(raw);
        cache.rest_bootstrapped = true;
        cache.rest_only_ready = subscription.interval == CandleInterval::D1;
        let confirmed_open_time = cache.confirmed_open_time;
        cache.closed_candidates.retain(|candidate| {
            confirmed_open_time
                .is_none_or(|confirmed| candidate.frame.candle.open_time_utc > confirmed)
        });
        if recovery_complete {
            cache.recovery_required = false;
            cache.reconciled = true;
        }
    }

    /// Pin provider revisions to the exact observations already accepted by this
    /// process, while rejecting structural gaps or timestamp/interval changes.
    /// Returns a summary only when a new revision snapshot is first observed.
    #[cfg(test)]
    pub fn pin_rest_snapshot(
        &mut self,
        subscription: &PublicMarketSubscription,
        fetched_at: UtcTimestamp,
        observations: &mut [CandleObservation],
    ) -> Result<Option<(usize, UtcTimestamp)>, LabError> {
        self.pin_rest_snapshot_with_provider_absence(
            subscription,
            fetched_at,
            observations,
            &[],
            &[],
        )
    }

    /// Pin completed rows after exact provider-absence spans cover every structural gap.
    #[expect(
        clippy::too_many_lines,
        reason = "one bounded cache transition validates span coverage before pinning immutable history and recording revisions"
    )]
    pub fn pin_rest_snapshot_with_provider_absence(
        &mut self,
        subscription: &PublicMarketSubscription,
        fetched_at: UtcTimestamp,
        observations: &mut [CandleObservation],
        confirmed_spans: &[ProviderAbsenceSpan],
        canonical_pins: &[CandleObservation],
    ) -> Result<Option<(usize, UtcTimestamp)>, LabError> {
        let Some(first) = observations.first() else {
            return Err(LabError::DataGap("empty REST snapshot".into()));
        };
        let Some(latest) = observations.last() else {
            return Err(LabError::DataGap("empty REST snapshot".into()));
        };
        if observations.iter().any(|observation| {
            let expected_close = observation
                .candle
                .open_time_utc
                .0
                .checked_add_signed(subscription.interval.duration());
            !observation.candle.completed
                || observation.candle.close_time_utc > fetched_at
                || observation.candle.market != subscription.market.code()
                || observation.candle.interval != subscription.interval
                || observation
                    .candle
                    .open_time_utc
                    .0
                    .timestamp()
                    .rem_euclid(subscription.interval.duration().num_seconds())
                    != 0
                || expected_close != Some(observation.candle.close_time_utc.0)
        }) {
            return Err(LabError::DataCorrupt(
                "REST snapshot contains an unavailable or mismatched candle".into(),
            ));
        }
        for pair in observations.windows(2) {
            if pair[0].candle.close_time_utc != pair[1].candle.open_time_utc
                && !confirmed_spans.iter().any(|span| {
                    span.market == subscription.market
                        && span.interval == subscription.interval
                        && span.start_utc == pair[0].candle.close_time_utc
                        && span.end_utc == pair[1].candle.open_time_utc
                        && span.pre_anchor.open_time_utc == pair[0].candle.open_time_utc
                        && span.post_anchor.open_time_utc == pair[1].candle.open_time_utc
                })
            {
                return Err(LabError::DataGap(
                    "REST snapshot contains an unconfirmed provider candle gap".into(),
                ));
            }
        }
        let first_open = first.candle.open_time_utc;
        let latest_open = latest.candle.open_time_utc;
        let key = subscription_key(subscription);
        let cache = self.subscriptions.entry(key).or_default();
        cache.confirmed_provider_absence_spans = confirmed_spans.to_vec();

        let mut pinned_by_open = BTreeMap::new();
        for observation in &cache.rest_prefix {
            pinned_by_open.insert(observation.candle.open_time_utc, observation.clone());
        }
        // Match cached_prefix's precedence: an accepted WebSocket-confirmed row
        // may carry additional raw provenance for the same immutable observation.
        for observation in &cache.confirmed {
            pinned_by_open.insert(observation.candle.open_time_utc, observation.clone());
        }
        if let Some(observation) = cache.accepted_anchor_pin.as_ref() {
            pinned_by_open.insert(observation.candle.open_time_utc, observation.clone());
        }
        let candidate_opens = observations
            .iter()
            .map(|observation| observation.candle.open_time_utc)
            .collect::<std::collections::BTreeSet<_>>();
        let mut canonical_pins_by_open = BTreeMap::new();
        for observation in canonical_pins {
            validate_canonical_seed(subscription, observation)?;
            let open = observation.candle.open_time_utc;
            if !candidate_opens.contains(&open) {
                return Err(LabError::DataCorrupt(
                    "canonical SQL pin is outside the current REST source window".into(),
                ));
            }
            if pinned_by_open.get(&open).is_some_and(|existing| {
                existing.id != observation.id
                    || existing.content_digest != observation.content_digest
                    || existing.candle != observation.candle
            }) {
                return Err(LabError::Conflict(
                    "canonical SQL row conflicts with accepted in-memory candle history".into(),
                ));
            }
            if canonical_pins_by_open
                .insert(open, observation.clone())
                .is_some()
            {
                return Err(LabError::Conflict(
                    "multiple canonical SQL pins occupy one candle slot".into(),
                ));
            }
        }
        let history_boundary = cache
            .confirmed_open_time
            .into_iter()
            .chain(cache.rest_prefix.back().map(|row| row.candle.open_time_utc))
            .chain(cache.confirmed.back().map(|row| row.candle.open_time_utc))
            .max();
        if history_boundary.is_some() && pinned_by_open.is_empty() {
            return Err(LabError::DataCorrupt(
                "accepted finalized boundary has no pinned observation".into(),
            ));
        }
        // A cold restored anchor is one known point, not a complete history
        // window. Older dedicated pins also do not widen the retained REST window.
        let oldest_complete_history = if cache.rest_bootstrapped {
            cache
                .rest_prefix
                .front()
                .map(|row| row.candle.open_time_utc)
        } else {
            None
        };
        if let Some(boundary) = history_boundary {
            if latest_open < boundary {
                return Err(LabError::DataGap(
                    "REST snapshot tip regressed behind accepted finalized history".into(),
                ));
            }
            if !observations
                .iter()
                .any(|row| row.candle.open_time_utc == boundary)
            {
                return Err(LabError::DataCorrupt(
                    "REST snapshot omitted the accepted finalized boundary".into(),
                ));
            }
        }

        let mut revision_bytes = Vec::new();
        let mut revision_slots = 0_usize;
        let mut latest_revision_open = None;
        for observation in observations.iter_mut() {
            let open_time = observation.candle.open_time_utc;
            let pinned = if let Some(canonical) = canonical_pins_by_open.get(&open_time) {
                Some(canonical)
            } else if history_boundary.is_none_or(|boundary| open_time > boundary) {
                None
            } else {
                pinned_by_open.get(&open_time)
            };
            let Some(pinned) = pinned else {
                if history_boundary.is_some_and(|boundary| {
                    oldest_complete_history
                        .is_some_and(|oldest| open_time >= oldest && open_time <= boundary)
                }) {
                    return Err(LabError::DataCorrupt(
                        "REST snapshot introduced an unexplained accepted-history slot".into(),
                    ));
                }
                continue;
            };
            if observation.candle.market != pinned.candle.market
                || observation.candle.interval != pinned.candle.interval
                || observation.candle.open_time_utc != pinned.candle.open_time_utc
                || observation.candle.close_time_utc != pinned.candle.close_time_utc
                || !observation.candle.completed
                || !pinned.candle.completed
            {
                return Err(LabError::DataCorrupt(
                    "REST snapshot changed accepted candle slot structure".into(),
                ));
            }
            let economics_changed = observation.content_digest != pinned.content_digest;
            if !economics_changed && !same_candle_economics(&observation.candle, &pinned.candle) {
                return Err(LabError::DataCorrupt(
                    "REST snapshot digest disagrees with accepted candle economics".into(),
                ));
            }
            if economics_changed {
                revision_bytes.extend_from_slice(open_time.to_rfc3339().as_bytes());
                revision_bytes.push(0);
                revision_bytes.extend_from_slice(pinned.content_digest.as_str().as_bytes());
                revision_bytes.push(0);
                revision_bytes.extend_from_slice(observation.content_digest.as_str().as_bytes());
                revision_bytes.push(0xff);
                revision_slots = revision_slots.saturating_add(1);
                latest_revision_open = Some(open_time);
            }
            if economics_changed
                || observation.id != pinned.id
                || observation.raw_object_ids != pinned.raw_object_ids
                || observation.constituent_ids != pinned.constituent_ids
            {
                *observation = pinned.clone();
            }
        }

        if let Some(boundary) = history_boundary {
            for open_time in pinned_by_open
                .range(first_open..=boundary)
                .map(|(open, _)| *open)
            {
                if !observations
                    .iter()
                    .any(|row| row.candle.open_time_utc == open_time)
                {
                    return Err(LabError::DataCorrupt(
                        "REST snapshot omitted an accepted candle within its overlap".into(),
                    ));
                }
            }
        }

        if revision_slots == 0 {
            return Ok(None);
        }
        let fingerprint = ContentHash::of_bytes(&revision_bytes);
        if cache.last_revision_fingerprint.as_ref() == Some(&fingerprint) {
            return Ok(None);
        }
        cache.last_revision_fingerprint = Some(fingerprint);
        cache.revision_snapshot_count = cache.revision_snapshot_count.saturating_add(1);
        cache.last_revision_open_time_utc = latest_revision_open;
        cache.last_revision_detected_at_utc = Some(fetched_at);
        Ok(Some((
            revision_slots,
            latest_revision_open.unwrap_or(latest_open),
        )))
    }

    /// Whether a prior REST tip observation still needs its one-second spacing
    /// before another independent read may be admitted.
    pub fn rest_tip_confirmation_waiting(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
    ) -> bool {
        self.subscriptions
            .get(&subscription_key(subscription))
            .and_then(|cache| cache.pending_rest_tip.as_ref())
            .is_some_and(|pending| {
                now.0.signed_duration_since(pending.first_fetched_at_utc.0)
                    < chrono::Duration::seconds(MIN_REST_TIP_CONFIRMATION_SECONDS)
            })
    }

    /// Require two independent REST reads with an unchanged economic digest and
    /// at least one second between their actual fetch timestamps for a new tip.
    pub fn confirm_rest_tip(
        &mut self,
        subscription: &PublicMarketSubscription,
        tip: &CandleObservation,
        fetched_at: UtcTimestamp,
    ) -> bool {
        let key = subscription_key(subscription);
        let cache = self.subscriptions.entry(key).or_default();
        let accepted_tip = cache
            .confirmed_open_time
            .into_iter()
            .chain(cache.rest_prefix.back().map(|row| row.candle.open_time_utc))
            .chain(cache.confirmed.back().map(|row| row.candle.open_time_utc))
            .max();
        if accepted_tip.is_some_and(|accepted| tip.candle.open_time_utc <= accepted) {
            cache.pending_rest_tip = None;
            return true;
        }
        if let Some(pending) = cache.pending_rest_tip.as_ref()
            && pending.content_digest == tip.content_digest
            && fetched_at
                .0
                .signed_duration_since(pending.first_fetched_at_utc.0)
                >= chrono::Duration::seconds(MIN_REST_TIP_CONFIRMATION_SECONDS)
        {
            cache.pending_rest_tip = None;
            return true;
        }
        if cache
            .pending_rest_tip
            .as_ref()
            .is_none_or(|pending| pending.content_digest != tip.content_digest)
        {
            cache.pending_rest_tip = Some(PendingRestTipVerification {
                content_digest: tip.content_digest.clone(),
                first_fetched_at_utc: fetched_at,
            });
        }
        false
    }

    /// Advance the accepted REST watermark only after the caller has passed the
    /// two-snapshot tip gate and stored the pinned completed prefix.
    pub fn confirm_rest_snapshot(
        &mut self,
        subscription: &PublicMarketSubscription,
        observations: &[CandleObservation],
    ) -> Result<(), LabError> {
        let latest = observations
            .last()
            .ok_or_else(|| LabError::DataGap("empty REST snapshot".into()))?;
        let latest_open = latest.candle.open_time_utc;
        let cache = self
            .subscriptions
            .entry(subscription_key(subscription))
            .or_default();
        let accepted_tip = cache
            .confirmed_open_time
            .into_iter()
            .chain(cache.rest_prefix.back().map(|row| row.candle.open_time_utc))
            .chain(cache.confirmed.back().map(|row| row.candle.open_time_utc))
            .max();
        if accepted_tip.is_some_and(|accepted| latest_open < accepted) {
            return Err(LabError::DataGap(
                "REST snapshot tip regressed behind accepted finalized history".into(),
            ));
        }
        cache.confirmed_open_time =
            Some(accepted_tip.map_or(latest_open, |tip| tip.max(latest_open)));
        cache.pending_rest_tip = None;
        cache.rest_retry_failures = 0;
        cache.rest_retry_at = None;
        cache
            .closed_candidates
            .retain(|candidate| candidate.frame.candle.open_time_utc > latest_open);
        Ok(())
    }

    /// Accept a bounded, contiguous completed REST recovery without inventing WS provenance.
    /// The caller validates provider finality; previously accepted overlapping history is immutable.
    pub fn recover_rest_snapshot(
        &mut self,
        subscription: &PublicMarketSubscription,
        raw: RawObjectRef,
        observations: &[CandleObservation],
    ) -> Result<(), LabError> {
        let first = observations
            .first()
            .ok_or_else(|| LabError::DataGap("empty REST recovery".into()))?;
        let latest = observations
            .last()
            .ok_or_else(|| LabError::DataGap("empty REST recovery".into()))?;
        if let Some(cache) = self.subscriptions.get(&subscription_key(subscription))
            && let Some(previous) = cache.rest_prefix.back()
            && (latest.candle.open_time_utc < previous.candle.open_time_utc
                || first.candle.open_time_utc > previous.candle.close_time_utc)
        {
            return Err(LabError::DataGap(
                "REST recovery does not cover the previous finalized boundary".into(),
            ));
        }
        if !self.snapshot_matches_confirmed(subscription, observations) {
            return Err(LabError::DataCorrupt(
                "REST recovery changed confirmed finalized history".into(),
            ));
        }
        self.store_rest_snapshot(subscription, raw, observations, true);
        if let Some(cache) = self.subscriptions.get_mut(&subscription_key(subscription)) {
            let latest_open = latest.candle.open_time_utc;
            cache.confirmed_open_time = Some(
                cache
                    .confirmed_open_time
                    .map_or(latest_open, |current| current.max(latest_open)),
            );
            cache.pending_rest_tip = None;
            cache.rest_retry_failures = 0;
            cache.rest_retry_at = None;
            let confirmed_open_time = cache.confirmed_open_time;
            cache.closed_candidates.retain(|item| {
                confirmed_open_time
                    .is_none_or(|confirmed| item.frame.candle.open_time_utc > confirmed)
            });
            if !cache
                .confirmed
                .iter()
                .any(|row| row.candle.open_time_utc == latest_open)
            {
                if cache.confirmed.len() >= self.bars_per_subscription {
                    cache.confirmed.pop_front();
                }
                cache.confirmed.push_back(latest.clone());
            }
        }
        Ok(())
    }

    pub fn cached_prefix(
        &self,
        subscription: &PublicMarketSubscription,
        requested: usize,
    ) -> (Option<RawObjectRef>, Vec<CandleObservation>) {
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return (None, Vec::new());
        };
        let mut rows = BTreeMap::new();
        for observation in &cache.rest_prefix {
            rows.insert(observation.candle.open_time_utc, observation.clone());
        }
        for observation in &cache.confirmed {
            rows.insert(observation.candle.open_time_utc, observation.clone());
        }
        let mut observations = rows.into_values().collect::<Vec<_>>();
        if observations.len() > requested {
            observations.drain(..observations.len() - requested);
        }
        (cache.rest_raw.clone(), observations)
    }

    pub fn accepted_anchor(
        &self,
        subscription: &PublicMarketSubscription,
    ) -> Option<ProviderAbsenceAnchor> {
        let cache = self.subscriptions.get(&subscription_key(subscription))?;
        let boundary = cache
            .confirmed_open_time
            .into_iter()
            .chain(cache.rest_prefix.back().map(|row| row.candle.open_time_utc))
            .chain(cache.confirmed.back().map(|row| row.candle.open_time_utc))
            .chain(
                cache
                    .accepted_anchor_pin
                    .as_ref()
                    .map(|row| row.candle.open_time_utc),
            )
            .max()?;
        cache
            .confirmed
            .iter()
            .chain(cache.rest_prefix.iter())
            .chain(cache.accepted_anchor_pin.iter())
            .find(|row| row.candle.open_time_utc == boundary)
            .map(|row| ProviderAbsenceAnchor {
                observation_id: row.id.clone(),
                open_time_utc: row.candle.open_time_utc,
                content_digest: row.content_digest.clone(),
            })
    }

    /// Seed one immutable consumer anchor after a cold restart. This keeps a
    /// provider revision at that slot pinnable even when the prior feed cache
    /// has been lost, without extending the bounded REST prefix.
    pub fn seed_accepted_observation(
        &mut self,
        subscription: &PublicMarketSubscription,
        observation: CandleObservation,
    ) -> Result<(), LabError> {
        validate_canonical_seed(subscription, &observation)?;
        let cache = self
            .subscriptions
            .entry(subscription_key(subscription))
            .or_default();
        if cache.accepted_anchor_pin.as_ref().is_some_and(|existing| {
            existing.candle.open_time_utc > observation.candle.open_time_utc
                || (existing.candle.open_time_utc == observation.candle.open_time_utc
                    && (existing.id != observation.id
                        || existing.content_digest != observation.content_digest
                        || existing.candle != observation.candle))
        }) || cache
            .rest_prefix
            .iter()
            .chain(cache.confirmed.iter())
            .any(|existing| {
                existing.candle.open_time_utc == observation.candle.open_time_utc
                    && (existing.id != observation.id
                        || existing.content_digest != observation.content_digest
                        || existing.candle != observation.candle)
            })
        {
            return Err(LabError::Conflict(
                "cold-start accepted candle anchor conflicts with cached finalized history".into(),
            ));
        }
        cache.confirmed_open_time = Some(
            cache
                .confirmed_open_time
                .map_or(observation.candle.open_time_utc, |current| {
                    current.max(observation.candle.open_time_utc)
                }),
        );
        if cache
            .accepted_anchor_pin
            .as_ref()
            .is_none_or(|existing| observation.candle.open_time_utc > existing.candle.open_time_utc)
        {
            cache.accepted_anchor_pin = Some(observation);
        }
        Ok(())
    }

    /// Restore the distinct durable REST watermark without completing bootstrap.
    /// The older consumer boundary retains its dedicated pin; the source row
    /// stays in the existing bounded REST prefix and never gains WS provenance.
    pub fn seed_source_watermark(
        &mut self,
        subscription: &PublicMarketSubscription,
        observation: CandleObservation,
    ) -> Result<(), LabError> {
        validate_canonical_seed(subscription, &observation)?;
        let maximum = self.bars_per_subscription;
        let cache = self
            .subscriptions
            .entry(subscription_key(subscription))
            .or_default();
        if cache.rest_bootstrapped {
            return Ok(());
        }
        if let Some(existing) = cache
            .rest_prefix
            .iter()
            .find(|row| row.candle.open_time_utc == observation.candle.open_time_utc)
        {
            return if existing.id == observation.id
                && existing.content_digest == observation.content_digest
                && existing.candle == observation.candle
            {
                Ok(())
            } else {
                Err(LabError::Conflict(
                    "durable REST watermark conflicts with its canonical cache row".into(),
                ))
            };
        }
        let position = cache
            .rest_prefix
            .iter()
            .position(|row| row.candle.open_time_utc > observation.candle.open_time_utc)
            .unwrap_or(cache.rest_prefix.len());
        cache.rest_prefix.insert(position, observation);
        while cache.rest_prefix.len() > maximum {
            cache.rest_prefix.pop_front();
        }
        Ok(())
    }

    pub fn set_provider_recovery_evidence(
        &mut self,
        subscription: &PublicMarketSubscription,
        spans: Vec<ProviderAbsenceSpan>,
        progress: Option<ProviderFeedRecoveryProgress>,
    ) -> Result<(), LabError> {
        if spans.len() > 256
            || spans.iter().any(|span| {
                span.market != subscription.market || span.interval != subscription.interval
            })
        {
            return Err(LabError::DataCorrupt(
                "provider recovery evidence is outside the subscription scope".into(),
            ));
        }
        let cache = self
            .subscriptions
            .entry(subscription_key(subscription))
            .or_default();
        cache.confirmed_provider_absence_spans = spans;
        cache.provider_absence_confirmation_until = if progress
            .as_ref()
            .is_some_and(|progress| progress.pending_absence_confirmation)
        {
            Some(Instant::now() + Duration::from_secs(1))
        } else {
            None
        };
        cache.provider_recovery_progress = progress;
        Ok(())
    }

    pub fn provider_absence_confirmation_waiting(
        &self,
        subscription: &PublicMarketSubscription,
        now: Instant,
    ) -> bool {
        self.subscriptions
            .get(&subscription_key(subscription))
            .and_then(|cache| cache.provider_absence_confirmation_until)
            .is_some_and(|retry_at| now < retry_at)
    }

    pub fn needs_rest_snapshot(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
    ) -> bool {
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return true;
        };
        if cache.pending_rest_tip.is_some() {
            return !self.rest_tip_confirmation_waiting(subscription, now);
        }
        !cache.rest_bootstrapped
            || cache.recovery_required
            || self.has_due_candidate(subscription, now)
            || self.silent_rest_probe_due(subscription, now)
    }

    /// Pick one REST bootstrap/recovery key in canonical-union round-robin order.
    /// The cursor advances before I/O so a failed cold key cannot starve the rest.
    pub fn next_rest_probe_subscription(
        &mut self,
        subscriptions: &[PublicMarketSubscription],
        now: UtcTimestamp,
        retry_now: Instant,
    ) -> Option<PublicMarketSubscription> {
        if subscriptions.is_empty() {
            return None;
        }
        let start = self
            .last_rest_probe_key
            .as_ref()
            .and_then(|last| {
                subscriptions
                    .iter()
                    .position(|subscription| subscription_key(subscription) == *last)
            })
            .map_or(0, |index| (index + 1) % subscriptions.len());
        for offset in 0..subscriptions.len() {
            let index = (start + offset) % subscriptions.len();
            let subscription = &subscriptions[index];
            if self.rest_retry_waiting(subscription, retry_now) {
                continue;
            }
            if self.provider_absence_confirmation_waiting(subscription, retry_now) {
                continue;
            }
            if self.needs_rest_snapshot(subscription, now) {
                self.last_rest_probe_key = Some(subscription_key(subscription));
                return Some(subscription.clone());
            }
        }
        None
    }

    /// Whether this key's bounded monotonic REST retry delay has not elapsed.
    pub fn rest_retry_waiting(
        &self,
        subscription: &PublicMarketSubscription,
        now: Instant,
    ) -> bool {
        self.subscriptions
            .get(&subscription_key(subscription))
            .and_then(|cache| cache.rest_retry_at)
            .is_some_and(|retry_at| now < retry_at)
    }

    /// Record one actual failed REST/reconciliation attempt with capped exponential delay.
    pub fn record_rest_failure(&mut self, subscription: &PublicMarketSubscription, now: Instant) {
        let cache = self
            .subscriptions
            .entry(subscription_key(subscription))
            .or_default();
        cache.rest_retry_failures = cache.rest_retry_failures.saturating_add(1);
        let exponent = u32::from(cache.rest_retry_failures.saturating_sub(1).min(5));
        let seconds = MIN_REST_RETRY_SECONDS
            .checked_shl(exponent)
            .unwrap_or(u64::MAX)
            .min(MAX_REST_RETRY_SECONDS);
        cache.rest_retry_at = Some(now + Duration::from_secs(seconds));
    }

    /// Trigger an explicit REST freshness check when a candle boundary passes
    /// without a new WebSocket observation. The REST snapshot timestamp prevents
    /// repeated polling within the same expected interval.
    pub fn silent_rest_probe_due(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
    ) -> bool {
        if subscription.interval == CandleInterval::D1 {
            return false;
        }
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return false;
        };
        if !cache.rest_bootstrapped
            || cache.recovery_required
            || self.has_due_candidate(subscription, now)
        {
            return false;
        }
        let latest_close = cache
            .current
            .as_ref()
            .map(|current| current.frame.candle.close_time_utc)
            .into_iter()
            .chain(
                cache
                    .rest_prefix
                    .back()
                    .map(|observation| observation.candle.close_time_utc),
            )
            .max();
        let Some(latest_close) = latest_close else {
            return false;
        };
        let probe_anchor = cache
            .rest_raw
            .as_ref()
            .map_or(latest_close, |raw| raw.fetched_at.max(latest_close));
        next_interval_boundary(latest_close, probe_anchor, subscription.interval)
            .is_some_and(|boundary| now >= boundary)
    }

    /// Whether REST introduced a candle newer than the prior REST prefix without
    /// a corresponding due WebSocket candidate to confirm it.
    pub fn has_unobserved_forward_rows(
        &self,
        subscription: &PublicMarketSubscription,
        observations: &[CandleObservation],
        candidates: &[PendingCandle],
    ) -> bool {
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return false;
        };
        let previous_latest = cache
            .rest_prefix
            .back()
            .map(|observation| observation.candle.open_time_utc);
        observations.iter().any(|observation| {
            previous_latest.is_none_or(|previous| observation.candle.open_time_utc > previous)
                && !candidates.iter().any(|candidate| {
                    candidate.frame.candle.open_time_utc == observation.candle.open_time_utc
                })
        })
    }

    /// Check that every retained WebSocket-confirmed bar still appears unchanged
    /// in a fresh REST snapshot and that the snapshot has not fallen behind it.
    pub fn snapshot_matches_confirmed(
        &self,
        subscription: &PublicMarketSubscription,
        observations: &[CandleObservation],
    ) -> bool {
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return false;
        };
        let Some(latest_rest) = observations.last() else {
            return false;
        };
        if cache.rest_prefix.back().is_some_and(|previous| {
            previous.candle.open_time_utc > latest_rest.candle.open_time_utc
        }) {
            return false;
        }
        if cache.rest_prefix.iter().any(|previous| {
            observations
                .first()
                .is_some_and(|first| previous.candle.open_time_utc >= first.candle.open_time_utc)
                && !observations
                    .iter()
                    .find(|row| row.candle.open_time_utc == previous.candle.open_time_utc)
                    .is_some_and(|row| same_candle_economics(&previous.candle, &row.candle))
        }) {
            return false;
        }
        let Some(confirmed_open_time) = cache.confirmed_open_time else {
            return true;
        };
        let Some(latest_rest) = observations.last() else {
            return false;
        };
        if latest_rest.candle.open_time_utc < confirmed_open_time {
            return false;
        }
        let Some(latest_confirmed) = cache
            .confirmed
            .iter()
            .find(|confirmed| confirmed.candle.open_time_utc == confirmed_open_time)
            .or_else(|| {
                cache
                    .rest_prefix
                    .iter()
                    .find(|rest| rest.candle.open_time_utc == confirmed_open_time)
            })
            .or_else(|| {
                cache
                    .accepted_anchor_pin
                    .iter()
                    .find(|anchor| anchor.candle.open_time_utc == confirmed_open_time)
            })
        else {
            return false;
        };
        if !observations
            .iter()
            .find(|rest| rest.candle.open_time_utc == confirmed_open_time)
            .is_some_and(|rest| same_candle_economics(&latest_confirmed.candle, &rest.candle))
        {
            return false;
        }
        cache.confirmed.iter().all(|confirmed| {
            let open_time = confirmed.candle.open_time_utc;
            if observations
                .first()
                .is_some_and(|first| open_time < first.candle.open_time_utc)
            {
                return true;
            }
            observations
                .iter()
                .find(|rest| rest.candle.open_time_utc == open_time)
                .is_some_and(|rest| same_candle_economics(&confirmed.candle, &rest.candle))
        })
    }

    pub fn is_bootstrapped(&self, subscription: &PublicMarketSubscription) -> bool {
        self.subscriptions
            .get(&subscription_key(subscription))
            .is_some_and(|cache| cache.rest_bootstrapped)
    }

    pub fn subscription_ready(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
    ) -> bool {
        self.readiness_facts(subscription, now)
            .is_ready(subscription.interval)
    }

    pub fn subscription_readiness(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
    ) -> crate::contracts::RealtimeSubscriptionReadiness {
        let facts = self.readiness_facts(subscription, now);
        crate::contracts::RealtimeSubscriptionReadiness {
            market: subscription.market.clone(),
            interval: subscription.interval,
            ready: facts.is_ready(subscription.interval),
            blockers: facts.blockers(subscription.interval),
            last_received_at_utc: facts.last_received_at_utc,
            provider_at_utc: facts.provider_at_utc,
            latest_websocket_open_time_utc: facts.latest_websocket_open_time_utc,
            latest_rest_close_time_utc: facts.latest_rest_close_time_utc,
            last_rest_fetched_at_utc: facts.last_rest_fetched_at_utc,
            pending_rest_confirmation: facts.pending_rest_confirmation,
            revision_snapshot_count: facts.revision_snapshot_count,
            last_revision_open_time_utc: facts.last_revision_open_time_utc,
            last_revision_detected_at_utc: facts.last_revision_detected_at_utc,
            confirmed_provider_absence_spans: facts
                .confirmed_provider_absence_spans
                .iter()
                .map(crate::contracts::ProviderAbsenceSummary::from)
                .collect(),
            provider_recovery_progress: facts
                .provider_recovery_progress
                .as_ref()
                .map(crate::contracts::ProviderRecoveryProgressSummary::from),
        }
    }

    pub fn subscription_readiness_for_union(
        &self,
        subscriptions: &[PublicMarketSubscription],
        now: UtcTimestamp,
    ) -> Vec<crate::contracts::RealtimeSubscriptionReadiness> {
        subscriptions
            .iter()
            .map(|subscription| self.subscription_readiness(subscription, now))
            .collect()
    }

    fn readiness_facts(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
    ) -> SubscriptionReadinessFacts {
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return SubscriptionReadinessFacts::default();
        };
        let current = cache.current.as_ref();
        let (last_received_at_utc, provider_at_utc, latest_websocket_open_time_utc) = current
            .map(|current| {
                (
                    Some(current.frame.received_at),
                    Some(current.frame.provider_at),
                    Some(current.frame.candle.open_time_utc),
                )
            })
            .unwrap_or_default();
        let (websocket_is_fresh, provider_clock_valid) =
            current.map_or((false, false), |current| {
                let age = now.0.signed_duration_since(current.frame.received_at.0);
                let provider_lag = current
                    .frame
                    .received_at
                    .0
                    .signed_duration_since(current.frame.provider_at.0);
                let freshness_limit = subscription
                    .interval
                    .duration()
                    .max(chrono::Duration::minutes(1));
                (
                    age >= chrono::Duration::zero() && age <= freshness_limit,
                    provider_lag >= chrono::Duration::zero()
                        && provider_lag <= subscription.interval.duration(),
                )
            });
        let rest_is_fresh = cache.rest_raw.as_ref().is_some_and(|raw| {
            let rest_age = now.0.signed_duration_since(raw.fetched_at.0);
            let freshness_limit = subscription
                .interval
                .duration()
                .max(chrono::Duration::minutes(1));
            rest_age >= chrono::Duration::zero() && rest_age <= freshness_limit
        });
        let rest_confirms_accepted_tip = cache.confirmed_open_time.is_some_and(|confirmed_open| {
            cache
                .rest_prefix
                .back()
                .is_some_and(|rest| rest.candle.open_time_utc == confirmed_open)
        });
        SubscriptionReadinessFacts {
            rest_bootstrapped: cache.rest_bootstrapped,
            rest_only_ready: cache.rest_only_ready,
            recovery_required: cache.recovery_required,
            stream_live: cache.stream_live,
            reconciled: cache.reconciled,
            websocket_is_fresh,
            provider_clock_valid,
            rest_is_fresh,
            rest_confirms_accepted_tip,
            pending_rest_confirmation: cache.pending_rest_tip.is_some(),
            last_received_at_utc,
            provider_at_utc,
            latest_websocket_open_time_utc,
            latest_rest_close_time_utc: cache
                .rest_prefix
                .back()
                .map(|observation| observation.candle.close_time_utc),
            last_rest_fetched_at_utc: cache.rest_raw.as_ref().map(|raw| raw.fetched_at),
            revision_snapshot_count: cache.revision_snapshot_count,
            last_revision_open_time_utc: cache.last_revision_open_time_utc,
            last_revision_detected_at_utc: cache.last_revision_detected_at_utc,
            confirmed_provider_absence_spans: cache.confirmed_provider_absence_spans.clone(),
            provider_recovery_progress: cache.provider_recovery_progress.clone(),
        }
    }

    pub fn all_required_ready(
        &self,
        subscriptions: &[PublicMarketSubscription],
        now: UtcTimestamp,
    ) -> bool {
        !subscriptions.is_empty()
            && subscriptions
                .iter()
                .all(|subscription| self.subscription_ready(subscription, now))
    }

    pub fn due_candidates(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
        maximum: usize,
    ) -> Vec<PendingCandle> {
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return Vec::new();
        };
        let mut candidates = cache
            .closed_candidates
            .iter()
            .filter(|candidate| candidate.frame.candle.close_time_utc <= now)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(current) = cache
            .current
            .as_ref()
            .filter(|candidate| candidate.frame.candle.close_time_utc <= now)
            && !candidates.iter().any(|candidate| {
                candidate.frame.candle.open_time_utc == current.frame.candle.open_time_utc
            })
        {
            candidates.push(current.clone());
        }
        candidates.retain(|candidate| {
            cache
                .confirmed_open_time
                .is_none_or(|confirmed| candidate.frame.candle.open_time_utc > confirmed)
        });
        candidates.sort_by_key(|candidate| candidate.frame.candle.open_time_utc);
        candidates.truncate(maximum);
        candidates
    }

    pub fn has_due_candidate(
        &self,
        subscription: &PublicMarketSubscription,
        now: UtcTimestamp,
    ) -> bool {
        let Some(cache) = self.subscriptions.get(&subscription_key(subscription)) else {
            return false;
        };
        let is_unconfirmed = |candidate: &PendingCandle| {
            cache
                .confirmed_open_time
                .is_none_or(|confirmed| candidate.frame.candle.open_time_utc > confirmed)
        };
        cache.closed_candidates.iter().any(|candidate| {
            candidate.frame.candle.close_time_utc <= now && is_unconfirmed(candidate)
        }) || cache.current.as_ref().is_some_and(|candidate| {
            candidate.frame.candle.close_time_utc <= now && is_unconfirmed(candidate)
        })
    }

    pub fn mark_confirmed(
        &mut self,
        subscription: &PublicMarketSubscription,
        candidate: &PendingCandle,
        observation: CandleObservation,
    ) -> bool {
        let maximum_bars = self.bars_per_subscription;
        let cache = self
            .subscriptions
            .entry(subscription_key(subscription))
            .or_default();
        if cache.current.as_ref().is_none_or(|current| {
            current.transport.connection_epoch != candidate.transport.connection_epoch
                || (current.frame.candle.open_time_utc == candidate.frame.candle.open_time_utc
                    && current.transport.message_sequence > candidate.transport.message_sequence
                    && !same_candle_economics(&current.frame.candle, &candidate.frame.candle))
        }) {
            cache.recovery_required = true;
            cache.reconciled = false;
            return false;
        }
        cache.closed_candidates.retain(|item| {
            !(item.frame.candle.open_time_utc == candidate.frame.candle.open_time_utc
                && item.transport.connection_epoch == candidate.transport.connection_epoch
                && item.transport.message_sequence == candidate.transport.message_sequence)
        });
        cache.confirmed_open_time = Some(
            cache
                .confirmed_open_time
                .map_or(candidate.frame.candle.open_time_utc, |current| {
                    current.max(candidate.frame.candle.open_time_utc)
                }),
        );
        let confirmed_open_time = cache.confirmed_open_time;
        cache.closed_candidates.retain(|item| {
            confirmed_open_time.is_none_or(|confirmed| item.frame.candle.open_time_utc > confirmed)
        });
        if cache.confirmed.len() == maximum_bars {
            cache.confirmed.pop_front();
        }
        if !cache
            .confirmed
            .iter()
            .any(|existing| existing.id == observation.id)
        {
            cache.confirmed.push_back(observation);
        }
        cache.reconciled = true;
        true
    }

    pub fn mark_subscription_reconciled(&mut self, subscription: &PublicMarketSubscription) {
        if let Some(cache) = self.subscriptions.get_mut(&subscription_key(subscription)) {
            cache.reconciled = true;
            cache.recovery_required = false;
        }
    }

    pub fn mark_recovery_required(&mut self, subscription: &PublicMarketSubscription) {
        if let Some(cache) = self.subscriptions.get_mut(&subscription_key(subscription)) {
            cache.recovery_required = true;
            cache.reconciled = false;
        }
    }

    pub fn clear_reconciled_for(&mut self, subscriptions: &[PublicMarketSubscription]) {
        let required = subscriptions
            .iter()
            .map(subscription_key)
            .collect::<std::collections::HashSet<_>>();
        self.subscriptions.retain(|key, _| required.contains(key));
        for subscription in subscriptions {
            {
                let cache = self
                    .subscriptions
                    .entry(subscription_key(subscription))
                    .or_default();
                cache.reconciled = false;
                cache.rest_only_ready = false;
                cache.stream_live = false;
                cache.recovery_required = cache.rest_bootstrapped;
                cache.last_message_sequence = None;
                cache.current = None;
                cache.closed_candidates.clear();
            }
        }
        let markets = subscriptions
            .iter()
            .filter(|subscription| {
                super::subscription::websocket_type_for_interval(subscription.interval).is_some()
            })
            .map(|subscription| subscription.market.code())
            .collect::<std::collections::HashSet<_>>();
        self.latest_prices
            .retain(|market, _| markets.contains(market));
    }

    pub fn latest_price(&self, market: &str) -> Option<&LatestMarketPrice> {
        self.latest_prices.get(market)
    }
}

fn subscription_key(subscription: &PublicMarketSubscription) -> String {
    format!(
        "{}:{}",
        subscription.market.code(),
        interval_tag(subscription.interval)
    )
}

fn next_interval_boundary(
    latest_close: UtcTimestamp,
    probe_anchor: UtcTimestamp,
    interval: CandleInterval,
) -> Option<UtcTimestamp> {
    let duration = interval.duration();
    let duration_seconds = duration.num_seconds();
    if duration_seconds <= 0 {
        return None;
    }
    let elapsed_seconds = probe_anchor
        .0
        .signed_duration_since(latest_close.0)
        .num_seconds()
        .max(0);
    let steps = elapsed_seconds
        .div_euclid(duration_seconds)
        .saturating_add(1);
    let steps = i32::try_from(steps).ok()?;
    let offset = duration.checked_mul(steps)?;
    latest_close.0.checked_add_signed(offset).map(UtcTimestamp)
}

const fn interval_tag(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::M1 => "M1",
        CandleInterval::M5 => "M5",
        CandleInterval::M15 => "M15",
        CandleInterval::H1 => "H1",
        CandleInterval::H4 => "H4",
        CandleInterval::D1 => "D1",
    }
}

fn is_monotonic_candle_update(
    previous: &ValidatedCandleFrame,
    next: &ValidatedCandleFrame,
) -> bool {
    let prior = &previous.candle;
    let incoming = &next.candle;
    prior.market == incoming.market
        && prior.interval == incoming.interval
        && prior.open_time_utc == incoming.open_time_utc
        && prior.open == incoming.open
        && incoming.high >= prior.high
        && incoming.low <= prior.low
        && incoming.volume.get() >= prior.volume.get()
        && incoming.quote_turnover.get() >= prior.quote_turnover.get()
}

fn same_candle_economics(
    left: &crate::contracts::CandleRecord,
    right: &crate::contracts::CandleRecord,
) -> bool {
    let mut left = left.clone();
    left.completed = right.completed;
    left == *right
}

const fn confirms_subscription(kind: super::parser::CandleStreamKind) -> bool {
    matches!(
        kind,
        super::parser::CandleStreamKind::Snapshot | super::parser::CandleStreamKind::Realtime
    )
}

fn validate_canonical_seed(
    subscription: &PublicMarketSubscription,
    observation: &CandleObservation,
) -> Result<(), LabError> {
    if observation.candle.market != subscription.market.code()
        || observation.candle.interval != subscription.interval
        || !observation.candle.completed
        || observation.candle.close_time_utc
            != UtcTimestamp(
                observation
                    .candle
                    .open_time_utc
                    .0
                    .checked_add_signed(subscription.interval.duration())
                    .ok_or_else(|| {
                        LabError::DataCorrupt("accepted candle slot overflows".into())
                    })?,
            )
        || observation_digest(&observation.candle)? != observation.content_digest
    {
        return Err(LabError::DataCorrupt(
            "cold-start accepted candle anchor failed identity validation".into(),
        ));
    }
    Ok(())
}

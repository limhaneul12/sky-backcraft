use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::Timelike;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::contracts::{
    AssetQuantity, ContentHash, MarketDataOrigin, PriceKrw, ProviderAbsenceSnapshotRef,
    QuoteAmount, RawObjectId, RawWebSocketTransport,
};

use super::parser::{CandleStreamKind, ValidatedCandleFrame, decode_candle_frame};
use super::state::{FeedCache, FrameDisposition};
use super::subscription::{
    PublicMarketSubscription, canonical_subscriptions, websocket_subscription_message,
};
use super::*;

type TestError = Box<dyn Error + Send + Sync>;

#[test]
fn websocket_decimal_frames_remain_exact_and_unfinalized() -> Result<(), LabError> {
    let received_at = UtcTimestamp::parse_rfc3339("2026-01-01T00:00:31Z")?;
    let wire = candle_frame(
        "candle.60m",
        "KRW-BTC",
        "2026-01-01T00:00:00",
        "123456789012345.12345678",
        "123456789012346.12345678",
        "123456789012344.12345678",
        "123456789012345.98765432",
        "0.0000000123456789",
        "1234.567890123456789",
        received_at.0.timestamp_millis(),
    );
    let decoded = decode_candle_frame(wire.as_bytes(), received_at, 4096)?;
    assert_eq!(decoded.candle.interval, CandleInterval::H1);
    assert!(!decoded.candle.completed);
    assert_eq!(decoded.stream_kind, CandleStreamKind::Realtime);
    assert_eq!(
        decoded.candle.open.get().to_string(),
        "123456789012345.12345678"
    );
    assert_eq!(
        decoded.candle.volume.get().to_string(),
        "0.0000000123456789"
    );
    assert_eq!(
        decoded.candle.quote_turnover.get().to_string(),
        "1234.567890123456789"
    );
    assert_eq!(
        decoded.candle.close_time_utc.to_rfc3339(),
        "2026-01-01T01:00:00Z"
    );
    Ok(())
}

#[test]
fn subscription_union_deduplicates_and_keeps_daily_rest_only() -> Result<(), LabError> {
    let btc = MarketId::parse_upbit("KRW-BTC")?;
    let eth = MarketId::parse_upbit("KRW-ETH")?;
    let union = canonical_subscriptions(
        [
            PublicMarketSubscription::new(btc.clone(), CandleInterval::M1),
            PublicMarketSubscription::new(btc.clone(), CandleInterval::M1),
            PublicMarketSubscription::new(btc.clone(), CandleInterval::H1),
            PublicMarketSubscription::new(eth, CandleInterval::D1),
        ],
        20,
    )?;
    assert_eq!(union.len(), 3);
    assert_eq!(
        union
            .iter()
            .filter(|subscription| subscription.interval == CandleInterval::D1)
            .count(),
        1
    );
    let message = websocket_subscription_message(&union, "spot-lab-public-test")?;
    let payload: serde_json::Value = serde_json::from_str(&message)?;
    let objects = payload
        .as_array()
        .ok_or_else(|| LabError::ContractParse("subscription request must be an array".into()))?;
    let types = objects
        .iter()
        .filter_map(|object| object.get("type").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    assert!(types.contains(&"candle.1m"));
    assert!(types.contains(&"candle.60m"));
    assert!(!types.iter().any(|stream_type| stream_type.contains("1d")));
    Ok(())
}

#[test]
fn bounded_state_handles_duplicate_reorder_and_gap_explicitly() -> Result<(), LabError> {
    let mut cache = FeedCache::new(4);
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let epoch = 1;
    assert_eq!(
        cache.apply_frame(
            state_frame("2026-01-01T00:00:00Z", "100", "1")?,
            b"frame-1".to_vec(),
            transport(epoch, 1, "2026-01-01T00:30:00Z")?,
        ),
        FrameDisposition::New
    );
    assert_eq!(
        cache.apply_frame(
            state_frame("2026-01-01T00:00:00Z", "100", "1")?,
            b"frame-2".to_vec(),
            transport(epoch, 2, "2026-01-01T00:31:00Z")?,
        ),
        FrameDisposition::Duplicate
    );
    assert_eq!(
        cache.apply_frame(
            state_frame("2026-01-01T01:00:00Z", "101", "2")?,
            b"frame-3".to_vec(),
            transport(epoch, 3, "2026-01-01T01:30:00Z")?,
        ),
        FrameDisposition::New
    );
    assert_eq!(
        cache.apply_frame(
            state_frame("2026-01-01T00:00:00Z", "100", "1")?,
            b"old-frame".to_vec(),
            transport(epoch, 4, "2026-01-01T00:32:00Z")?,
        ),
        FrameDisposition::OutOfOrder
    );
    assert_eq!(
        cache.apply_frame(
            state_frame("2026-01-01T03:00:00Z", "103", "3")?,
            b"gap-frame".to_vec(),
            transport(epoch, 5, "2026-01-01T03:30:00Z")?,
        ),
        FrameDisposition::Gap
    );
    assert_eq!(
        cache
            .due_candidates(&subscription, time("2026-01-01T04:00:00Z")?, 4)
            .len(),
        3
    );
    assert_eq!(
        cache
            .latest_price("KRW-BTC")
            .map(|row| row.price.get().to_string()),
        Some("103".into())
    );
    Ok(())
}

#[test]
fn stalled_confirmation_backlog_recovers_without_disconnect_or_epoch_poisoning()
-> Result<(), LabError> {
    let mut cache = FeedCache::new(4);
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    for (sequence, open_at) in [
        "2026-01-01T00:00:00Z",
        "2026-01-01T01:00:00Z",
        "2026-01-01T02:00:00Z",
        "2026-01-01T03:00:00Z",
    ]
    .into_iter()
    .enumerate()
    {
        let disposition = cache.apply_frame(
            state_frame(open_at, "100", "1")?,
            vec![],
            transport(
                1,
                u64::try_from(sequence)
                    .map_err(|_| LabError::Internal("fixture sequence".into()))?
                    + 1,
                open_at,
            )?,
        );
        if sequence == 3 {
            assert_eq!(disposition, FrameDisposition::Gap);
        }
    }
    assert!(
        cache
            .due_candidates(&subscription, time("2026-01-01T04:00:00Z")?, 10)
            .len()
            <= 3
    );
    cache.clear_reconciled_for(std::slice::from_ref(&subscription));
    assert!(
        cache
            .due_candidates(&subscription, time("2026-01-01T04:00:00Z")?, 10)
            .is_empty()
    );
    assert_eq!(
        cache.apply_frame(
            state_frame("2026-01-01T04:00:00Z", "100", "1")?,
            vec![],
            transport(2, 1, "2026-01-01T04:00:31Z")?
        ),
        FrameDisposition::New
    );
    Ok(())
}

#[test]
fn bounded_rest_gap_recovery_is_idempotent_and_rejects_history_rewrites() -> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let mut cache = FeedCache::new(4);
    let first = rest_observation(&state_frame("2026-01-01T00:00:00Z", "100", "1")?, "http-0")?;
    cache.store_rest_snapshot(
        &subscription,
        rest_raw("initial", time("2026-01-01T01:00:01Z")?),
        std::slice::from_ref(&first),
        true,
    );
    let forward = rest_observation(&state_frame("2026-01-01T01:00:00Z", "101", "2")?, "http-1")?;
    let rows = vec![first.clone(), forward];
    assert!(cache.has_unobserved_forward_rows(&subscription, &rows, &[]));
    let raw = rest_raw("recovered", time("2026-01-01T02:00:01Z")?);
    cache.recover_rest_snapshot(&subscription, raw.clone(), &rows)?;
    cache.recover_rest_snapshot(&subscription, raw.clone(), &rows)?;
    assert_eq!(
        serde_json::to_value(cache.cached_prefix(&subscription, 4).1)?,
        serde_json::to_value(&rows)?
    );
    assert!(!cache.has_due_candidate(&subscription, raw.fetched_at));
    let mut revised = rows.clone();
    revised[0] = rest_observation(&state_frame("2026-01-01T00:00:00Z", "99", "1")?, "revised")?;
    assert!(matches!(
        cache.recover_rest_snapshot(&subscription, raw.clone(), &revised),
        Err(LabError::DataCorrupt(_))
    ));
    let uncovered = rest_observation(&state_frame("2026-01-01T03:00:00Z", "103", "3")?, "http-3")?;
    assert!(matches!(
        cache.recover_rest_snapshot(&subscription, raw, &[uncovered]),
        Err(LabError::DataGap(_))
    ));
    assert_eq!(
        serde_json::to_value(cache.cached_prefix(&subscription, 4).1)?,
        serde_json::to_value(&rows)?
    );
    Ok(())
}

#[test]
fn new_rest_tip_waits_for_a_matching_independent_read_with_minimum_spacing() -> Result<(), LabError>
{
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let tip = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "100", "1")?,
        "same-raw",
    )?;
    let revised_tip = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "99", "1")?,
        "same-revised-raw",
    )?;
    let first_fetch = time("2026-01-01T01:00:00Z")?;
    let mut cache = FeedCache::new(4);

    assert!(!cache.confirm_rest_tip(&subscription, &tip, first_fetch));
    assert!(!cache.is_bootstrapped(&subscription));
    assert!(cache.cached_prefix(&subscription, 4).1.is_empty());
    assert!(cache.rest_tip_confirmation_waiting(
        &subscription,
        UtcTimestamp(first_fetch.0 + chrono::Duration::milliseconds(999))
    ));
    assert!(!cache.needs_rest_snapshot(
        &subscription,
        UtcTimestamp(first_fetch.0 + chrono::Duration::milliseconds(999))
    ));

    let revised_fetch = UtcTimestamp(first_fetch.0 + chrono::Duration::seconds(1));
    assert!(!cache.confirm_rest_tip(&subscription, &revised_tip, revised_fetch));
    assert!(cache.rest_tip_confirmation_waiting(
        &subscription,
        UtcTimestamp(revised_fetch.0 + chrono::Duration::milliseconds(999))
    ));
    let stable_fetch = UtcTimestamp(revised_fetch.0 + chrono::Duration::seconds(1));
    // This is the same observation identity/raw evidence as the preceding read;
    // only the independent fetch clock distinguishes the second REST request.
    assert!(cache.confirm_rest_tip(&subscription, &revised_tip, stable_fetch));
    assert!(!cache.rest_tip_confirmation_waiting(&subscription, stable_fetch));

    cache.store_rest_snapshot(
        &subscription,
        rest_raw("stable-tip", stable_fetch),
        std::slice::from_ref(&revised_tip),
        true,
    );
    cache.confirm_rest_snapshot(&subscription, std::slice::from_ref(&revised_tip))?;
    assert!(cache.is_bootstrapped(&subscription));
    assert_eq!(
        cache.cached_prefix(&subscription, 4).1[0].id,
        revised_tip.id
    );
    Ok(())
}

#[test]
fn rest_history_revision_is_pinned_and_repeated_snapshot_does_not_increment_diagnostic()
-> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let first = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "100", "1")?,
        "accepted-0",
    )?;
    let second = rest_observation(
        &state_frame("2026-01-01T01:00:00Z", "101", "2")?,
        "accepted-1",
    )?;
    let accepted = [first.clone(), second.clone()];
    let accepted_at = time("2026-01-01T02:00:01Z")?;
    let mut cache = FeedCache::new(4);
    cache.store_rest_snapshot(
        &subscription,
        rest_raw("accepted", accepted_at),
        &accepted,
        true,
    );
    cache.confirm_rest_snapshot(&subscription, &accepted)?;

    let changed = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "99", "1")?,
        "provider-revision",
    )?;
    let mut revised_snapshot = [changed.clone(), second.clone()];
    let revision_at = time("2026-01-01T02:05:01Z")?;
    let revision = cache.pin_rest_snapshot(&subscription, revision_at, &mut revised_snapshot)?;
    assert_eq!(revision, Some((1, first.candle.open_time_utc)));
    assert_eq!(revised_snapshot[0].id, first.id);
    assert_eq!(revised_snapshot[0].raw_object_ids, first.raw_object_ids);
    assert_eq!(revised_snapshot[0].candle.close, first.candle.close);
    assert!(cache.snapshot_matches_confirmed(&subscription, &revised_snapshot));

    let mut repeated_snapshot = [changed, second.clone()];
    assert!(
        cache
            .pin_rest_snapshot(
                &subscription,
                time("2026-01-01T02:06:01Z")?,
                &mut repeated_snapshot
            )?
            .is_none()
    );
    assert_eq!(repeated_snapshot[0].id, first.id);
    let mut provenance_only = first.clone();
    provenance_only.raw_object_ids = vec![RawObjectId::from_seed("later-identical-economics")];
    let mut provenance_snapshot = [provenance_only, second.clone()];
    assert!(
        cache
            .pin_rest_snapshot(
                &subscription,
                time("2026-01-01T02:06:30Z")?,
                &mut provenance_snapshot,
            )?
            .is_none()
    );
    assert_eq!(provenance_snapshot[0].raw_object_ids, first.raw_object_ids);
    let diagnostics = cache.subscription_readiness(&subscription, revision_at);
    assert_eq!(diagnostics.revision_snapshot_count, 1);
    assert_eq!(
        diagnostics.last_revision_open_time_utc,
        Some(first.candle.open_time_utc)
    );

    let mut missing_boundary = [first.clone()];
    assert!(matches!(
        cache.pin_rest_snapshot(
            &subscription,
            time("2026-01-01T02:07:01Z")?,
            &mut missing_boundary,
        ),
        Err(LabError::DataGap(_))
    ));
    let shifted = rest_observation(
        &state_frame("2026-01-01T00:01:00Z", "100", "1")?,
        "shifted-slot",
    )?;
    let mut shifted_snapshot = [shifted, second];
    assert!(matches!(
        cache.pin_rest_snapshot(
            &subscription,
            time("2026-01-01T02:08:01Z")?,
            &mut shifted_snapshot,
        ),
        Err(LabError::DataCorrupt(_))
    ));
    Ok(())
}

#[test]
fn cold_restart_provider_gap_revision_pins_the_accepted_anchor_economics() -> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let accepted = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "100", "1")?,
        "accepted-anchor",
    )?;
    let revised = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "99", "1")?,
        "provider-revision",
    )?;
    let post = rest_observation(
        &state_frame("2026-01-01T03:00:00Z", "103", "3")?,
        "post-gap-anchor",
    )?;
    let read_digest = ContentHash::of_bytes(b"same verified raw gap page");
    let span = ProviderAbsenceSpan {
        proof_id: ContentHash::of_bytes(b"two-read exact provider absence proof"),
        market: subscription.market.clone(),
        interval: subscription.interval,
        start_utc: time("2026-01-01T01:00:00Z")?,
        end_utc: time("2026-01-01T03:00:00Z")?,
        pre_anchor: ProviderAbsenceAnchor {
            observation_id: revised.id.clone(),
            open_time_utc: revised.candle.open_time_utc,
            content_digest: revised.content_digest.clone(),
        },
        post_anchor: ProviderAbsenceAnchor {
            observation_id: post.id.clone(),
            open_time_utc: post.candle.open_time_utc,
            content_digest: post.content_digest.clone(),
        },
        snapshots: [
            ProviderAbsenceSnapshotRef {
                raw_object_id: RawObjectId::from_seed("gap-read-1"),
                fetched_at_utc: time("2026-01-01T04:00:02Z")?,
                body_digest: read_digest.clone(),
            },
            ProviderAbsenceSnapshotRef {
                raw_object_id: RawObjectId::from_seed("gap-read-2"),
                fetched_at_utc: time("2026-01-01T04:00:03Z")?,
                body_digest: read_digest,
            },
        ],
    };

    // Simulate a cold feed cache while retaining the database-backed consumer
    // anchor loaded by provider_recovery_context.
    let mut cache = FeedCache::new(4);
    cache.seed_accepted_observation(&subscription, accepted.clone())?;
    let mut rows = merge_recovery_observations(
        vec![accepted.clone(), post.clone()],
        &[revised.clone(), post.clone()],
    )?;
    let revision = cache.pin_rest_snapshot_with_provider_absence(
        &subscription,
        time("2026-01-01T04:00:04Z")?,
        &mut rows,
        std::slice::from_ref(&span),
        &[],
    )?;

    assert_eq!(revision, Some((1, accepted.candle.open_time_utc)));
    assert_eq!(rows[0].id, accepted.id);
    assert_eq!(rows[0].content_digest, accepted.content_digest);
    assert_eq!(rows[0].candle.close, accepted.candle.close);
    assert!(cache.snapshot_matches_confirmed(&subscription, &rows));

    // Once both endpoints are accepted, a revision to the other endpoint must
    // traverse staged merge and coverage validation before pinning both originals.
    cache.store_rest_snapshot(
        &subscription,
        rest_raw("accepted-gap", time("2026-01-01T04:00:04Z")?),
        &rows,
        true,
    );
    cache.confirm_rest_snapshot(&subscription, &rows)?;
    let revised_post = rest_observation(
        &state_frame("2026-01-01T03:00:00Z", "102", "3")?,
        "post-gap-revision",
    )?;
    let mut merged = merge_recovery_observations(rows, &[revised, revised_post])?;
    let revision = cache.pin_rest_snapshot_with_provider_absence(
        &subscription,
        time("2026-01-01T04:00:05Z")?,
        &mut merged,
        &[span],
        &[],
    )?;
    assert_eq!(revision, Some((2, post.candle.open_time_utc)));
    assert_eq!(merged[0].id, accepted.id);
    assert_eq!(merged[1].id, post.id);
    assert_eq!(merged[1].content_digest, post.content_digest);
    assert_eq!(merged[1].candle, post.candle);
    Ok(())
}

#[test]
fn accepted_anchor_pin_advances_to_a_new_recovery_epoch() -> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let previous = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "100", "1")?,
        "previous-epoch-anchor",
    )?;
    let accepted = rest_observation(
        &state_frame("2026-01-01T02:00:00Z", "102", "2")?,
        "new-epoch-anchor",
    )?;
    let revised = rest_observation(
        &state_frame("2026-01-01T02:00:00Z", "101", "2")?,
        "new-epoch-provider-revision",
    )?;
    let post = rest_observation(
        &state_frame("2026-01-01T03:00:00Z", "103", "3")?,
        "new-epoch-post-anchor",
    )?;
    let mut cache = FeedCache::new(4);
    cache.seed_accepted_observation(&subscription, previous)?;
    cache.seed_accepted_observation(&subscription, accepted.clone())?;

    let mut rows = [revised, post];
    let revision =
        cache.pin_rest_snapshot(&subscription, time("2026-01-01T04:00:01Z")?, &mut rows)?;
    assert_eq!(revision, Some((1, accepted.candle.open_time_utc)));
    assert_eq!(rows[0].id, accepted.id);
    assert_eq!(rows[0].content_digest, accepted.content_digest);
    assert_eq!(rows[0].candle.close, accepted.candle.close);
    Ok(())
}

#[test]
fn confirmation_from_a_closed_epoch_cannot_resurrect_dropped_candidates() -> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let mut cache = FeedCache::new(4);
    let frame = state_frame("2026-01-01T00:00:00Z", "100", "1")?;
    let observation = rest_observation(&frame, "old-http")?;
    cache.apply_frame(frame, vec![], transport(1, 1, "2026-01-01T00:30:00Z")?);
    let candidate = cache
        .due_candidates(&subscription, time("2026-01-01T01:00:00Z")?, 4)
        .pop()
        .ok_or_else(|| LabError::Internal("missing fixture candidate".into()))?;
    cache.clear_reconciled_for(std::slice::from_ref(&subscription));
    cache.apply_frame(
        state_frame("2026-01-01T01:00:00Z", "101", "2")?,
        vec![],
        transport(2, 1, "2026-01-01T01:30:00Z")?,
    );
    assert!(!cache.mark_confirmed(&subscription, &candidate, observation));
    Ok(())
}

#[test]
fn silent_interval_requires_periodic_rest_probe_and_preserves_cached_prefix() -> Result<(), LabError>
{
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let mut cache = FeedCache::new(4);
    let first_frame = state_frame("2026-01-01T00:00:00Z", "100", "1")?;
    let first_observation = rest_observation(&first_frame, "rest-bar-0")?;
    let initial_raw = rest_raw("rest-snapshot-0", time("2026-01-01T01:00:00Z")?);
    cache.store_rest_snapshot(
        &subscription,
        initial_raw,
        std::slice::from_ref(&first_observation),
        true,
    );
    cache.apply_frame(
        first_frame,
        b"ws-bar-0".to_vec(),
        transport(1, 1, "2026-01-01T00:30:00Z")?,
    );
    let candidate = cache
        .due_candidates(&subscription, time("2026-01-01T01:00:00Z")?, 2)
        .pop()
        .ok_or_else(|| LabError::DataGap("expected first WebSocket close candidate".into()))?;
    assert!(cache.mark_confirmed(&subscription, &candidate, first_observation.clone()));
    assert!(
        cache.snapshot_matches_confirmed(&subscription, std::slice::from_ref(&first_observation))
    );

    assert!(!cache.needs_rest_snapshot(&subscription, time("2026-01-01T01:59:59Z")?));
    assert!(cache.silent_rest_probe_due(&subscription, time("2026-01-01T02:00:00Z")?));

    let refreshed_raw = rest_raw("rest-snapshot-1", time("2026-01-01T02:00:01Z")?);
    cache.store_rest_snapshot(
        &subscription,
        refreshed_raw.clone(),
        std::slice::from_ref(&first_observation),
        false,
    );
    assert!(!cache.needs_rest_snapshot(&subscription, time("2026-01-01T02:00:02Z")?));
    assert!(cache.subscription_ready(&subscription, time("2026-01-01T02:00:02Z")?));
    let (cached_raw, cached_rows) = cache.cached_prefix(&subscription, 4);
    assert_eq!(cached_raw.map(|raw| raw.id), Some(refreshed_raw.id.clone()));
    assert_eq!(cached_rows.len(), 1);
    assert_eq!(cached_rows[0].id, first_observation.id);

    assert!(cache.silent_rest_probe_due(&subscription, time("2026-01-01T03:00:00Z")?));
    let forward_frame = state_frame("2026-01-01T01:00:00Z", "101", "2")?;
    let forward_observation = rest_observation(&forward_frame, "rest-bar-1")?;
    assert!(cache.has_unobserved_forward_rows(
        &subscription,
        &[first_observation.clone(), forward_observation],
        &[],
    ));
    let (still_cached_raw, still_cached_rows) = cache.cached_prefix(&subscription, 4);
    assert_eq!(
        still_cached_raw.map(|raw| raw.id),
        Some(refreshed_raw.id.clone())
    );
    assert_eq!(still_cached_rows.len(), 1);
    assert_eq!(still_cached_rows[0].id, first_observation.id);
    Ok(())
}

#[test]
fn cold_subscription_diagnostics_explain_blockers_and_probe_selection_is_fair()
-> Result<(), LabError> {
    let now = time("2026-01-01T00:00:00Z")?;
    let first =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::M5);
    let second =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-ETH")?, CandleInterval::M5);
    let subscriptions = [first.clone(), second.clone()];
    let mut cache = FeedCache::new(4);
    let retry_now = Instant::now();

    let diagnostics = cache.subscription_readiness_for_union(&subscriptions, now);
    assert_eq!(diagnostics.len(), subscriptions.len());
    assert!(!diagnostics[0].ready);
    assert!(
        diagnostics[0]
            .blockers
            .contains(&crate::contracts::RealtimeReadinessBlocker::RestBootstrapRequired)
    );
    assert!(
        diagnostics[0]
            .blockers
            .contains(&crate::contracts::RealtimeReadinessBlocker::WebsocketCandleMissing)
    );

    assert_eq!(
        cache.next_rest_probe_subscription(&subscriptions, now, retry_now),
        Some(first.clone())
    );
    assert_eq!(
        cache.next_rest_probe_subscription(&subscriptions, now, retry_now),
        Some(second.clone())
    );
    assert_eq!(
        cache.next_rest_probe_subscription(&subscriptions, now, retry_now),
        Some(first)
    );
    Ok(())
}

#[test]
fn rest_retry_backoff_is_per_key_fair_resettable_and_capped() -> Result<(), LabError> {
    let btc = PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let eth = PublicMarketSubscription::new(MarketId::parse_upbit("KRW-ETH")?, CandleInterval::H1);
    let subscriptions = [btc.clone(), eth.clone()];
    let wall_now = time("2026-01-01T00:00:00Z")?;
    let start = Instant::now();
    let mut cache = FeedCache::new(4);

    cache.record_rest_failure(&btc, start);
    assert!(cache.rest_retry_waiting(&btc, start + Duration::from_millis(999)));
    assert_eq!(
        cache.next_rest_probe_subscription(
            &subscriptions,
            wall_now,
            start + Duration::from_millis(999),
        ),
        Some(eth.clone())
    );
    cache.record_rest_failure(&eth, start + Duration::from_millis(999));
    let btc_retry_at = start + Duration::from_secs(1);
    assert_eq!(
        cache.next_rest_probe_subscription(&subscriptions, wall_now, btc_retry_at),
        Some(btc.clone())
    );

    cache.record_rest_failure(&btc, btc_retry_at);
    assert!(cache.rest_retry_waiting(&btc, btc_retry_at + Duration::from_secs(1)));
    let accepted = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "100", "1")?,
        "accepted-after-retry",
    )?;
    cache.store_rest_snapshot(
        &btc,
        rest_raw("accepted-after-retry", time("2026-01-01T01:00:01Z")?),
        std::slice::from_ref(&accepted),
        true,
    );
    cache.confirm_rest_snapshot(&btc, std::slice::from_ref(&accepted))?;
    assert!(!cache.rest_retry_waiting(&btc, btc_retry_at + Duration::from_secs(1)));

    let mut capped = FeedCache::new(4);
    let mut failure_at = start;
    for delay_secs in [1_u64, 2, 4, 8, 16, 30, 30] {
        capped.record_rest_failure(&btc, failure_at);
        let deadline = failure_at + Duration::from_secs(delay_secs);
        let just_before_deadline = deadline
            .checked_sub(Duration::from_nanos(1))
            .ok_or_else(|| LabError::Internal("retry deadline underflow".into()))?;
        assert!(capped.rest_retry_waiting(&btc, just_before_deadline));
        assert!(!capped.rest_retry_waiting(&btc, deadline));
        failure_at = deadline;
    }
    Ok(())
}

#[test]
fn confirmed_current_frames_do_not_accumulate_as_zombies_across_rollovers() -> Result<(), LabError>
{
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let mut cache = FeedCache::new(4);

    let first_frame = state_frame("2026-01-01T00:00:00Z", "100", "1")?;
    let first_transport = RawWebSocketTransport {
        connection_epoch: 1,
        message_sequence: 1,
        provider_at: Some(first_frame.provider_at),
        received_at: first_frame.received_at,
    };
    assert_eq!(
        cache.apply_frame(first_frame, b"ws-frame-0".to_vec(), first_transport),
        FrameDisposition::New
    );
    let first_candidate = cache
        .due_candidates(&subscription, time("2026-01-01T01:00:00Z")?, 2)
        .pop()
        .ok_or_else(|| LabError::DataGap("expected first finalized candle".into()))?;
    let first_observation = rest_observation(&first_candidate.frame, "rollover-rest-0")?;
    assert!(cache.mark_confirmed(&subscription, &first_candidate, first_observation));

    let unconfirmed_rollovers = [
        "2026-01-01T01:00:00Z",
        "2026-01-01T02:00:00Z",
        "2026-01-01T03:00:00Z",
    ];
    for (index, open) in unconfirmed_rollovers.into_iter().enumerate() {
        let frame = state_frame(open, "100", "1")?;
        let transport = RawWebSocketTransport {
            connection_epoch: 1,
            message_sequence: u64::try_from(index + 2).expect("fixture sequence fits u64"),
            provider_at: Some(frame.provider_at),
            received_at: frame.received_at,
        };
        assert_eq!(
            cache.apply_frame(frame, format!("ws-frame-{index}").into_bytes(), transport),
            FrameDisposition::New
        );
    }
    let due = cache.due_candidates(&subscription, time("2026-01-01T04:00:00Z")?, 4);
    assert_eq!(due.len(), 3);
    assert!(due.iter().all(|candidate| {
        candidate.frame.candle.open_time_utc != time("2026-01-01T00:00:00Z").expect("fixture time")
    }));

    Ok(())
}

#[test]
fn production_endpoint_uses_only_allowlisted_tls_hosts() -> Result<(), LabError> {
    let config = PublicMarketFeedConfig::upbit_production()?;
    let allowed = vec!["api.upbit.com".to_owned()];
    assert!(
        config
            .clone()
            .with_production_endpoint("wss://api.upbit.com/websocket/v1", &allowed)
            .is_ok()
    );
    assert!(
        config
            .clone()
            .with_production_endpoint(
                "wss://sg-api.upbit.com/websocket/v1",
                &["sg-api.upbit.com".to_owned()]
            )
            .is_err()
    );
    assert!(
        config
            .with_production_endpoint(
                "wss://example.invalid/websocket/v1",
                &["example.invalid".to_owned()]
            )
            .is_err()
    );
    assert!(
        PublicMarketFeedConfig::upbit_production()?
            .with_production_endpoint(
                "ws://api.upbit.com/websocket/v1",
                &["api.upbit.com".to_owned()]
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn heartbeat_bounds_validate_timeout_after_ping_for_production_and_fixture() -> Result<(), LabError>
{
    let production = PublicMarketFeedConfig::upbit_production()?;
    assert_eq!(production.heartbeat_interval, Duration::from_secs(20));
    assert_eq!(production.heartbeat_timeout, Duration::from_secs(10));
    assert_eq!(production.handshake_min_spacing, Duration::from_millis(500));
    production.validate()?;

    let fixture = PublicMarketFeedConfig::loopback_fixture("ws://127.0.0.1:12345/websocket/v1")?;
    assert_eq!(fixture.heartbeat_interval, Duration::from_millis(80));
    assert_eq!(fixture.heartbeat_timeout, Duration::from_millis(160));
    assert_eq!(fixture.handshake_min_spacing, Duration::from_millis(20));
    fixture.validate()?;

    let mut zero_timeout = fixture.clone();
    zero_timeout.heartbeat_timeout = Duration::ZERO;
    assert!(zero_timeout.validate().is_err());
    let mut excessive_timeout = production.clone();
    excessive_timeout.heartbeat_timeout = Duration::from_secs(121);
    assert!(excessive_timeout.validate().is_err());
    let mut too_fast_handshake = fixture.clone();
    too_fast_handshake.handshake_min_spacing = Duration::from_millis(9);
    assert!(too_fast_handshake.validate().is_err());
    let mut excessive_handshake_spacing = fixture.clone();
    excessive_handshake_spacing.handshake_min_spacing = Duration::from_secs(6);
    assert!(excessive_handshake_spacing.validate().is_err());
    let mut invalid_fixture_resources = fixture;
    invalid_fixture_resources.max_subscriptions = 0;
    assert!(invalid_fixture_resources.validate().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn handshake_spacing_keeps_one_deadline_while_subscription_union_changes()
-> Result<(), TestError> {
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let updates = [
        vec![PublicMarketSubscription::new(
            market.clone(),
            CandleInterval::M5,
        )],
        vec![PublicMarketSubscription::new(
            market.clone(),
            CandleInterval::M15,
        )],
        vec![PublicMarketSubscription::new(
            market.clone(),
            CandleInterval::H1,
        )],
        vec![PublicMarketSubscription::new(market, CandleInterval::H4)],
    ];
    let initial = vec![PublicMarketSubscription::new(
        MarketId::parse_upbit("KRW-BTC")?,
        CandleInterval::M1,
    )];
    let (updates_tx, mut updates_rx) = watch::channel(initial.clone());
    let cancellation = tokio_util::sync::CancellationToken::new();
    let minimum_spacing = Duration::from_millis(300);
    let last_attempt = tokio::time::Instant::now();
    let waiter = tokio::spawn(async move {
        let mut subscriptions = initial;
        let stopped = super::runtime::wait_for_handshake_spacing(
            Some(last_attempt),
            minimum_spacing,
            &mut updates_rx,
            &mut subscriptions,
            &cancellation,
        )
        .await?;
        Ok::<_, LabError>((last_attempt.elapsed(), subscriptions, stopped))
    });
    tokio::task::yield_now().await;

    for next in &updates {
        tokio::time::advance(Duration::from_millis(40)).await;
        updates_tx.send(next.clone())?;
        tokio::task::yield_now().await;
    }
    let before_deadline = minimum_spacing
        .checked_sub(Duration::from_millis(161))
        .ok_or_else(|| io::Error::other("fixture spacing must exceed the pre-deadline offset"))?;
    tokio::time::advance(before_deadline).await;
    assert!(!waiter.is_finished());
    tokio::time::advance(Duration::from_millis(1)).await;
    let (elapsed, subscriptions, stopped) =
        tokio::time::timeout(Duration::from_secs(1), waiter).await???;
    assert!(!stopped);
    assert!(elapsed >= minimum_spacing);
    assert_eq!(elapsed, minimum_spacing);
    assert_eq!(subscriptions, updates[3]);
    Ok(())
}

#[tokio::test]
async fn local_subscription_churn_spaces_actual_websocket_handshakes() -> Result<(), TestError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = local_endpoint(address);
    let (attempts_tx, mut attempts_rx) = mpsc::channel(2);
    let server = tokio::spawn(async move {
        let mut accepted_count = 0_usize;
        for _ in 0..2 {
            let (stream, _) =
                tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
            let accepted_at = tokio::time::Instant::now();
            let mut socket = accept_async(stream).await?;
            let request = next_text(&mut socket).await?;
            accepted_count += 1;
            attempts_tx
                .send((accepted_at, request))
                .await
                .map_err(|_| io::Error::other("handshake receiver closed"))?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Ping(payload))) => {
                        socket.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
        }
        Ok::<usize, TestError>(accepted_count)
    });

    let root = TestRoot::new("subscription-handshake-spacing")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let mut config = PublicMarketFeedConfig::loopback_fixture(&endpoint)?;
    config.handshake_min_spacing = Duration::from_millis(120);
    let (feed, runtime) = PublicMarketFeed::start(UpbitClient::new()?, owner.handle(), config)?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    feed.replace_subscriptions([PublicMarketSubscription::new(
        market.clone(),
        CandleInterval::M1,
    )])?;
    let (first_at, first_request) =
        tokio::time::timeout(Duration::from_secs(2), attempts_rx.recv())
            .await?
            .ok_or_else(|| io::Error::other("first handshake missing"))?;
    assert!(first_request.contains("candle.1m"));

    for interval in [
        CandleInterval::M5,
        CandleInterval::M15,
        CandleInterval::H1,
        CandleInterval::H4,
    ] {
        feed.replace_subscriptions([PublicMarketSubscription::new(market.clone(), interval)])?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (second_at, second_request) =
        tokio::time::timeout(Duration::from_secs(2), attempts_rx.recv())
            .await?
            .ok_or_else(|| io::Error::other("second handshake missing"))?;
    assert!(
        second_at.duration_since(first_at) >= Duration::from_millis(90),
        "subscription change bypassed the minimum handshake spacing"
    );
    assert!(second_request.contains("candle.240m"));
    assert!(!second_request.contains("candle.5m"));
    runtime.shutdown().await?;
    assert_eq!(server.await??, 2);
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn local_websocket_reconnects_pongs_and_joins_on_shutdown() -> Result<(), TestError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = local_endpoint(address);
    let (subscriptions_tx, mut subscriptions_rx) = mpsc::channel(4);
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut closed_tx = Some(closed_tx);
        for connection_index in 0..2 {
            let (stream, _) =
                tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
            let mut socket = accept_async(stream).await?;
            let request = next_text(&mut socket).await?;
            subscriptions_tx
                .send(request)
                .await
                .map_err(|_| io::Error::other("subscription receiver closed"))?;
            if connection_index == 0 {
                let ping = next_ping(&mut socket).await?;
                socket.send(Message::Pong(ping.into())).await?;
                socket.send(Message::Text(test_candle()?.into())).await?;
                // Drop the first connection abruptly to exercise reconnect.
                drop(socket);
            } else {
                loop {
                    match socket.next().await {
                        Some(Ok(Message::Close(_))) => {
                            if let Some(sender) = closed_tx.take() {
                                let _sent = sender.send(());
                            }
                            break;
                        }
                        Some(Err(_)) | None => break,
                        Some(Ok(_)) => {}
                    }
                }
            }
        }
        Ok::<(), TestError>(())
    });

    let root = TestRoot::new("reconnect-heartbeat")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let (feed, runtime) = PublicMarketFeed::start(
        UpbitClient::new()?,
        owner.handle(),
        PublicMarketFeedConfig::loopback_fixture(&endpoint)?,
    )?;
    feed.replace_subscriptions([PublicMarketSubscription::new(
        MarketId::parse_upbit("KRW-BTC")?,
        CandleInterval::H1,
    )])?;
    let first: serde_json::Value = serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(3), subscriptions_rx.recv())
            .await?
            .ok_or_else(|| io::Error::other("first subscription missing"))?,
    )?;
    assert_eq!(first[0]["ticket"], "spot-lab-public-1");
    wait_for(|| feed.health().last_pong_at_utc.is_some()).await?;
    wait_for(|| feed.health().status.reconnect_count > 0).await?;
    let second: serde_json::Value = serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(3), subscriptions_rx.recv())
            .await?
            .ok_or_else(|| io::Error::other("reconnect subscription missing"))?,
    )?;
    assert_eq!(second[0]["ticket"], "spot-lab-public-2");
    assert!(feed.health().last_provider_at_utc.is_some());
    assert!(feed.health().last_data_lag_millis.is_some());
    runtime.shutdown().await?;
    tokio::time::timeout(Duration::from_secs(2), closed_rx).await??;
    server.await??;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn local_oversized_frame_invalidates_readiness_and_owned_task_stops() -> Result<(), TestError>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = local_endpoint(address);
    let (oversized_tx, oversized_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
        let mut first = accept_async(first).await?;
        let _request = next_text(&mut first).await?;
        first.send(Message::Binary(vec![b'x'; 129].into())).await?;
        let _sent = oversized_tx.send(());
        // The client must reconnect after rejecting the bounded frame.
        let (second, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
        let mut second = accept_async(second).await?;
        let _request = next_text(&mut second).await?;
        loop {
            match second.next().await {
                Some(Ok(Message::Close(_))) => {
                    let _sent = closed_tx.send(());
                    break;
                }
                Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
        Ok::<(), TestError>(())
    });

    let root = TestRoot::new("oversized-frame")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let config =
        PublicMarketFeedConfig::loopback_fixture(&endpoint)?.with_max_message_bytes(128)?;
    let (feed, runtime) = PublicMarketFeed::start(UpbitClient::new()?, owner.handle(), config)?;
    feed.replace_subscriptions([PublicMarketSubscription::new(
        MarketId::parse_upbit("KRW-BTC")?,
        CandleInterval::M1,
    )])?;
    tokio::time::timeout(Duration::from_secs(3), oversized_rx).await??;
    wait_for(|| {
        feed.health().last_error_category == Some(PublicFeedErrorCategory::OversizedMessage)
    })
    .await?;
    assert!(!feed.health().status.ready_for_decisions);
    wait_for(|| feed.health().status.reconnect_count > 0).await?;
    runtime.shutdown().await?;
    tokio::time::timeout(Duration::from_secs(2), closed_rx).await??;
    server.await??;
    owner.shutdown()?;
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "one isolated REST and WebSocket fixture proves persisted exchange observation provenance"
)]
async fn check_rest_confirmation(last_update_matches: bool) -> Result<(), TestError> {
    let open_time = chrono::Utc::now()
        .with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .and_then(|value| value.checked_sub_signed(chrono::Duration::minutes(1)))
        .ok_or_else(|| io::Error::other("test candle time is out of range"))?;
    let open_wire = open_time.format("%Y-%m-%dT%H:%M:%S").to_string();
    let provider_at = UtcTimestamp(
        chrono::Utc::now()
            .with_nanosecond(0)
            .ok_or_else(|| io::Error::other("test provider time is out of range"))?,
    );
    let rest_listener = TcpListener::bind("127.0.0.1:0").await?;
    let rest_address = rest_listener.local_addr()?;
    let body = serde_json::json!([{
        "market": "KRW-BTC",
        "candle_date_time_utc": open_wire.clone(),
        "opening_price": 100,
        "high_price": 110,
        "low_price": 90,
        "trade_price": 105,
        "candle_acc_trade_volume": 2.5,
        "candle_acc_trade_price": 250
    }])
    .to_string();
    let app = axum::Router::new().fallback(move || {
        let body = body.clone();
        async move { body }
    });
    let (rest_stop, rest_stopped) = oneshot::channel();
    let rest_server = tokio::spawn(async move {
        axum::serve(rest_listener, app)
            .with_graceful_shutdown(async {
                let _ignored = rest_stopped.await;
            })
            .await
    });

    let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
    let ws_address = ws_listener.local_addr()?;
    let ws_endpoint = local_endpoint(ws_address);
    let (closed_tx, closed_rx) = oneshot::channel();
    let ws_frame = candle_frame(
        "candle.1m",
        "KRW-BTC",
        &open_wire,
        "100",
        "110",
        "90",
        if last_update_matches { "105" } else { "104" },
        "2.5",
        "250",
        provider_at.0.timestamp_millis(),
    );
    // This target candle is already closed; its provider clock stays fresh for readiness.
    let server_frame = ws_frame.clone();
    let ws_server = tokio::spawn(async move {
        let (stream, _) =
            tokio::time::timeout(Duration::from_secs(3), ws_listener.accept()).await??;
        let mut socket = accept_async(stream).await?;
        let _request = next_text(&mut socket).await?;
        socket.send(Message::Text(server_frame.into())).await?;
        loop {
            match socket.next().await {
                Some(Ok(Message::Ping(payload))) => {
                    socket.send(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Close(_))) => {
                    let _sent = closed_tx.send(());
                    break;
                }
                Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
        Ok::<(), TestError>(())
    });

    let root = TestRoot::new("ws-observation-parity")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let rest_client = UpbitClient::synthetic_local(&format!("http://{rest_address}"))?;
    let (feed, runtime) = PublicMarketFeed::start(
        rest_client,
        owner.handle(),
        PublicMarketFeedConfig::loopback_fixture(&ws_endpoint)?,
    )?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    feed.replace_subscriptions([PublicMarketSubscription::new(
        market.clone(),
        CandleInterval::M1,
    )])?;
    let expected_websocket_price = Decimal::from(if last_update_matches { 105 } else { 104 });
    wait_for(|| {
        feed.latest_price(&market).is_ok_and(|price| {
            price.is_some_and(|value| value.price.get() == expected_websocket_price)
        })
    })
    .await?;
    let count = ProbeCount::try_from(1_u32)?;
    assert!(matches!(
        feed.read_or_bootstrap_observations(&market, CandleInterval::M1, count)
            .await,
        Err(LabError::BlockedEvidence(reason)) if reason.contains("AWAITING_REST_CONFIRMATION")
    ));
    assert!(
        feed.health()
            .status
            .subscription_readiness
            .iter()
            .any(|readiness| readiness.pending_rest_confirmation)
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (rest_raw, observations) = feed
        .read_or_bootstrap_observations(&market, CandleInterval::M1, count)
        .await?;
    assert_eq!(observations.len(), 1);
    assert!(observations[0].candle.completed);
    assert_eq!(observations[0].candle.close.get().to_string(), "105");
    assert!(observations[0].raw_object_ids.contains(&rest_raw.id));
    let provenance = observations[0]
        .raw_object_ids
        .iter()
        .map(|id| read_raw_metadata(&root.0, id))
        .collect::<Result<Vec<_>, _>>()?;
    let mut rest_reads = provenance
        .iter()
        .filter(|raw| raw.http_status == 200)
        .collect::<Vec<_>>();
    rest_reads.sort_by_key(|raw| raw.fetched_at);
    assert_eq!(
        rest_reads.len(),
        2,
        "both independent REST witnesses are retained"
    );
    assert_ne!(rest_reads[0].id, rest_reads[1].id);
    assert!(
        rest_reads[1].fetched_at.0 - rest_reads[0].fetched_at.0 >= chrono::Duration::seconds(1)
    );
    assert_eq!(
        provenance
            .iter()
            .filter(|raw| raw.http_status == 101)
            .count(),
        usize::from(last_update_matches)
    );
    assert_eq!(provenance.len(), if last_update_matches { 3 } else { 2 });
    wait_for(|| feed.health().last_pong_at_utc.is_some()).await?;
    feed.ensure_ready(&market, CandleInterval::M1)?;

    if last_update_matches {
        let websocket_raw = provenance
            .iter()
            .find(|raw| raw.http_status == 101)
            .ok_or_else(|| io::Error::other("WebSocket raw provenance missing"))?;
        assert_eq!(websocket_raw.http_status, 101);
        let transport = websocket_raw
            .transport
            .as_ref()
            .ok_or_else(|| io::Error::other("WebSocket transport metadata missing"))?;
        assert_eq!(transport.connection_epoch, 1);
        assert_eq!(transport.message_sequence, 1);
        assert_eq!(transport.provider_at, Some(provider_at));
        assert_eq!(transport.received_at, websocket_raw.fetched_at);
        assert_eq!(
            owner
                .handle()
                .call("websocket_raw_body", {
                    let websocket_raw = websocket_raw.clone();
                    move |store| store.raw_objects().read_verified(&websocket_raw)
                })
                .await?,
            ws_frame.as_bytes()
        );
    }

    runtime.shutdown().await?;
    tokio::time::timeout(Duration::from_secs(2), closed_rx).await??;
    ws_server.await??;
    let _sent = rest_stop.send(());
    rest_server.await??;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn exact_rest_parity_publishes_the_actual_websocket_observation() -> Result<(), TestError> {
    check_rest_confirmation(true).await
}

#[tokio::test]
async fn rest_final_recovers_a_last_forming_update_without_fabricated_ws_provenance()
-> Result<(), TestError> {
    check_rest_confirmation(false).await
}

fn rest_observation(
    frame: &ValidatedCandleFrame,
    raw_id_seed: &str,
) -> Result<CandleObservation, LabError> {
    let mut candle = frame.candle.clone();
    candle.completed = true;
    let digest = observation_digest(&candle)?;
    Ok(CandleObservation {
        id: crate::contracts::ObservationId::from_seed(digest.as_str()),
        candle,
        content_digest: digest,
        raw_object_ids: vec![RawObjectId::from_seed(raw_id_seed)],
        constituent_ids: Vec::new(),
    })
}

fn rest_raw(label: &str, fetched_at: UtcTimestamp) -> RawObjectRef {
    let digest = ContentHash::of_bytes(label.as_bytes());
    RawObjectRef {
        id: RawObjectId::from_seed(label),
        relative_path: format!("raw/{}", digest.as_str()),
        source_url: "https://api.upbit.com/v1/candles/minutes/60".into(),
        fetched_at,
        persisted_at: fetched_at,
        http_status: 200,
        remaining_req: None,
        raw_sha256: digest.clone(),
        compressed_sha256: digest,
        raw_bytes: u64::try_from(label.len()).expect("fixture label length fits u64"),
        compressed_bytes: u64::try_from(label.len()).expect("fixture label length fits u64"),
        origin: MarketDataOrigin::ExchangeObserved,
        transport: None,
    }
}

#[test]
fn cold_sql_pins_keep_warm_cache_bounds_and_report_raw_revisions() -> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let mut canonical_rows = (0..=20)
        .map(|hour| {
            let open = format!("2026-01-01T{hour:02}:00:00Z");
            rest_observation(
                &state_frame(&open, "100", "1")?,
                &format!("canonical-row-{hour}"),
            )
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    let canonical_one = canonical_rows[1].clone();
    let canonical_nine = canonical_rows[9].clone();
    let revision_one = rest_observation(
        &state_frame("2026-01-01T01:00:00Z", "101", "1")?,
        "raw-revision-one",
    )?;
    let revision_nine = rest_observation(
        &state_frame("2026-01-01T09:00:00Z", "102", "1")?,
        "raw-revision-nine",
    )?;
    canonical_rows[1] = revision_one;
    canonical_rows[9] = revision_nine;

    let fetched_at = time("2026-01-01T22:00:01Z")?;
    let mut cache = FeedCache::new(32);
    cache.store_rest_snapshot(
        &subscription,
        rest_raw("warm-window", fetched_at),
        &canonical_rows[10..],
        true,
    );
    cache.confirm_rest_snapshot(&subscription, &canonical_rows[10..])?;

    let sql_pins = vec![canonical_one.clone(), canonical_nine.clone()];
    let revision = cache.pin_rest_snapshot_with_provider_absence(
        &subscription,
        fetched_at,
        &mut canonical_rows,
        &[],
        &sql_pins,
    )?;
    assert_eq!(revision, Some((2, canonical_nine.candle.open_time_utc)));
    assert_eq!(canonical_rows[1].id, canonical_one.id);
    assert_eq!(canonical_rows[9].id, canonical_nine.id);
    assert!(cache.is_bootstrapped(&subscription));
    assert_eq!(cache.cached_prefix(&subscription, 32).1.len(), 11);

    // The pre-existing complete REST range still rejects an omitted slot even
    // when an absence witness covers the incoming structural gap.
    let span = crate::contracts::ProviderAbsenceSpan {
        proof_id: ContentHash::of_bytes(b"cache-gap-fixture"),
        market: subscription.market.clone(),
        interval: subscription.interval,
        start_utc: time("2026-01-01T10:00:00Z")?,
        end_utc: time("2026-01-01T11:00:00Z")?,
        pre_anchor: crate::contracts::ProviderAbsenceAnchor {
            observation_id: canonical_rows[9].id.clone(),
            open_time_utc: canonical_rows[9].candle.open_time_utc,
            content_digest: canonical_rows[9].content_digest.clone(),
        },
        post_anchor: crate::contracts::ProviderAbsenceAnchor {
            observation_id: canonical_rows[11].id.clone(),
            open_time_utc: canonical_rows[11].candle.open_time_utc,
            content_digest: canonical_rows[11].content_digest.clone(),
        },
        snapshots: [
            ProviderAbsenceSnapshotRef {
                raw_object_id: RawObjectId::from_seed("cache-gap-read-one"),
                fetched_at_utc: time("2026-01-01T22:00:01Z")?,
                body_digest: ContentHash::of_bytes(b"same fixture response"),
            },
            ProviderAbsenceSnapshotRef {
                raw_object_id: RawObjectId::from_seed("cache-gap-read-two"),
                fetched_at_utc: time("2026-01-01T22:00:02Z")?,
                body_digest: ContentHash::of_bytes(b"same fixture response"),
            },
        ],
    };
    let mut missing_warm_slot = canonical_rows
        .into_iter()
        .enumerate()
        .filter_map(|(index, row)| (index != 10).then_some(row))
        .collect::<Vec<_>>();
    assert!(matches!(
        cache.pin_rest_snapshot_with_provider_absence(
            &subscription,
            time("2026-01-01T22:00:02Z")?,
            &mut missing_warm_slot,
            &[span],
            &sql_pins,
        ),
        Err(LabError::DataCorrupt(_))
    ));
    Ok(())
}

#[test]
fn provider_clock_skew_tolerates_bounded_forward_drift_and_rejects_excessive_future_timestamps()
-> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let rest_row = rest_observation(
        &state_frame("2026-01-01T00:00:00Z", "100", "1")?,
        "rest-bootstrap-row",
    )?;
    let fetched_at = time("2026-01-01T01:00:01Z")?;
    let mut cache = FeedCache::new(8);
    cache.store_rest_snapshot(
        &subscription,
        rest_raw("rest-bootstrap", fetched_at),
        std::slice::from_ref(&rest_row),
        true,
    );
    cache.confirm_rest_snapshot(&subscription, std::slice::from_ref(&rest_row))?;

    let mut minor_forward_skew = state_frame("2026-01-01T01:00:00Z", "101", "1")?;
    minor_forward_skew.received_at = time("2026-01-01T01:00:02Z")?;
    minor_forward_skew.provider_at = time("2026-01-01T01:00:05Z")?;
    cache.apply_frame(
        minor_forward_skew,
        b"{}".to_vec(),
        transport(1, 1, "2026-01-01T01:00:02Z")?,
    );
    let readiness = cache.subscription_readiness(&subscription, time("2026-01-01T01:00:03Z")?);
    assert!(readiness.ready);
    assert!(readiness.blockers.is_empty());

    let mut excessive_forward_skew = state_frame("2026-01-01T01:00:00Z", "102", "2")?;
    excessive_forward_skew.received_at = time("2026-01-01T01:00:02Z")?;
    excessive_forward_skew.provider_at = time("2026-01-01T01:00:35Z")?;
    cache.apply_frame(
        excessive_forward_skew,
        b"{}".to_vec(),
        transport(1, 2, "2026-01-01T01:00:02Z")?,
    );
    let blocked = cache.subscription_readiness(&subscription, time("2026-01-01T01:00:03Z")?);
    assert!(!blocked.ready);
    assert!(
        blocked
            .blockers
            .contains(&crate::contracts::RealtimeReadinessBlocker::ProviderClockInvalid)
    );
    Ok(())
}

fn state_frame(open_at: &str, close: &str, volume: &str) -> Result<ValidatedCandleFrame, LabError> {
    let open_time = time(open_at)?;
    let interval = CandleInterval::H1;
    let open = PriceKrw::new(Decimal::from_str_exact("100")?)?;
    let high = PriceKrw::new(Decimal::from_str_exact("110")?)?;
    let low = PriceKrw::new(Decimal::from_str_exact("90")?)?;
    let close = PriceKrw::new(Decimal::from_str_exact(close)?)?;
    let open_timestamp = open_time.0.timestamp();
    let provider_at = UtcTimestamp(
        chrono::DateTime::<chrono::Utc>::from_timestamp(open_timestamp + 30, 0)
            .ok_or_else(|| LabError::InvalidConfig("state fixture timestamp is invalid".into()))?,
    );
    Ok(ValidatedCandleFrame {
        subscription: PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, interval),
        candle: crate::contracts::CandleRecord {
            market: "KRW-BTC".into(),
            interval,
            open_time_utc: open_time,
            close_time_utc: UtcTimestamp(open_time.0 + interval.duration()),
            open,
            high,
            low,
            close,
            volume: AssetQuantity::new(Decimal::from_str_exact(volume)?)?,
            quote_turnover: QuoteAmount::new(Decimal::from_str_exact("100")?)?,
            completed: false,
        },
        provider_at,
        received_at: UtcTimestamp(
            chrono::DateTime::<chrono::Utc>::from_timestamp(open_timestamp + 31, 0).ok_or_else(
                || LabError::InvalidConfig("state receipt timestamp is invalid".into()),
            )?,
        ),
        stream_kind: CandleStreamKind::Realtime,
    })
}

fn transport(
    epoch: u64,
    sequence: u64,
    received_at: &str,
) -> Result<RawWebSocketTransport, LabError> {
    Ok(RawWebSocketTransport {
        connection_epoch: epoch,
        message_sequence: sequence,
        provider_at: Some(time(received_at)?),
        received_at: time(received_at)?,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the helper mirrors distinct typed fields in the Upbit candle wire fixture"
)]
fn candle_frame(
    stream_type: &str,
    code: &str,
    open_at: &str,
    opening_price: &str,
    high_price: &str,
    low_price: &str,
    trade_price: &str,
    volume: &str,
    quote_turnover: &str,
    provider_timestamp_ms: i64,
) -> String {
    format!(
        r#"{{"type":"{stream_type}","code":"{code}","candle_date_time_utc":"{open_at}","opening_price":{opening_price},"high_price":{high_price},"low_price":{low_price},"trade_price":{trade_price},"candle_acc_trade_volume":{volume},"candle_acc_trade_price":{quote_turnover},"timestamp":{provider_timestamp_ms},"stream_type":"REALTIME"}}"#
    )
}

fn test_candle() -> Result<String, LabError> {
    let at = UtcTimestamp::parse_rfc3339("2026-01-01T00:30:00Z")?
        .0
        .timestamp_millis();
    Ok(candle_frame(
        "candle.60m",
        "KRW-BTC",
        "2026-01-01T00:00:00",
        "100",
        "110",
        "90",
        "105",
        "2.5",
        "250",
        at,
    ))
}

fn local_endpoint(address: SocketAddr) -> String {
    format!("ws://{address}/websocket/v1")
}

async fn next_text<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> Result<String, TestError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match tokio::time::timeout(Duration::from_secs(2), socket.next()).await? {
            Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(Box::new(error)),
            None => return Err(io::Error::other("WebSocket closed before subscription").into()),
        }
    }
}

async fn next_ping<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> Result<Vec<u8>, TestError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match tokio::time::timeout(Duration::from_secs(2), socket.next()).await? {
            Some(Ok(Message::Ping(payload))) => return Ok(payload.to_vec()),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(Box::new(error)),
            None => return Err(io::Error::other("WebSocket closed before heartbeat").into()),
        }
    }
}

async fn wait_for(condition: impl Fn() -> bool) -> Result<(), TestError> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

struct TestRoot(std::path::PathBuf);

impl TestRoot {
    fn new(label: &str) -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "spot-lab-public-ws-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

fn time(value: &str) -> Result<UtcTimestamp, LabError> {
    UtcTimestamp::parse_rfc3339(value)
}

fn read_raw_metadata(
    root: &std::path::Path,
    id: &crate::contracts::RawObjectId,
) -> io::Result<RawObjectRef> {
    let raw_root = root.join("raw");
    for shard in std::fs::read_dir(raw_root)? {
        let shard = shard?;
        for object in std::fs::read_dir(shard.path())? {
            let object = object?;
            let metadata = std::fs::read(object.path().join("metadata.json"))?;
            let raw: RawObjectRef = serde_json::from_slice(&metadata).map_err(io::Error::other)?;
            if &raw.id == id {
                return Ok(raw);
            }
        }
    }
    Err(io::Error::other("raw metadata not found"))
}

#[tokio::test]
async fn cold_context_restores_consumer_and_source_watermark_without_marking_ready()
-> Result<(), TestError> {
    let root = TestRoot::new("cold-dual-watermark")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let (accepted, middle, watermark) = owner
        .handle()
        .call("seed_dual_watermark", |store| {
            let market = MarketId::parse_upbit("KRW-BTC")?;
            let interval = CandleInterval::H1;
            let at = time("2026-01-01T03:00:01Z")?;
            let (raw, rows) = recovery_fixture_page(
                store,
                &[
                    "2026-01-01T02:00:00",
                    "2026-01-01T01:00:00",
                    "2026-01-01T00:00:00",
                ],
                at,
            )?;
            store.record_provider_feed_recovery_page(
                crate::storage::PersistedProviderRecoveryPage {
                    raw,
                    market: market.clone(),
                    interval,
                    requested_to: None,
                    seed_anchor: Some(ProviderAbsenceAnchor {
                        observation_id: rows[0].id.clone(),
                        open_time_utc: rows[0].candle.open_time_utc,
                        content_digest: rows[0].content_digest.clone(),
                    }),
                    target_count: 3,
                    observations: rows.clone(),
                    pending_absence_confirmation: false,
                    as_of_utc: at,
                },
            )?;
            store.confirm_provider_feed_recovery_window(&market, interval, &rows[..1], at)?;
            store.confirm_provider_feed_recovery_window(&market, interval, &rows[2..], at)?;
            Ok((rows[0].clone(), rows[1].clone(), rows[2].clone()))
        })
        .await?;
    let (feed, runtime) = PublicMarketFeed::start(
        UpbitClient::new()?,
        owner.handle(),
        PublicMarketFeedConfig::loopback_fixture("ws://127.0.0.1:1/websocket/v1")?,
    )?;
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let (_, _, _, consumer) = feed.provider_recovery_context(&subscription).await?;
    let revised = rest_observation(
        &state_frame("2026-01-01T02:00:00Z", "99", "1")?,
        "revised-durable-source-tip",
    )?;
    let mut rows = vec![accepted.clone(), middle.clone(), revised];
    feed.with_cache(|cache| {
        cache.seed_accepted_observation(
            &subscription,
            consumer.ok_or_else(|| LabError::DataCorrupt("consumer anchor missing".into()))?,
        )?;
        cache.pin_rest_snapshot_with_provider_absence(
            &subscription,
            time("2026-01-01T03:00:03Z")?,
            &mut rows,
            &[],
            &[],
        )?;
        Ok(())
    })?;
    assert_eq!(rows[0].id, accepted.id);
    assert_eq!(rows[1].id, middle.id);
    assert_eq!(rows[2].id, watermark.id);
    assert_eq!(rows[2].content_digest, watermark.content_digest);
    assert!(!feed.with_cache(|cache| Ok(cache.is_bootstrapped(&subscription)))?);
    assert!(!feed.is_ready(&subscription.market, subscription.interval));
    feed.confirm_provider_recovery_window(&subscription, &rows[2..])
        .await?;
    runtime.shutdown().await?;
    owner.shutdown()?;
    Ok(())
}

fn recovery_fixture_page(
    store: &mut crate::storage::Store,
    opens: &[&str],
    at: UtcTimestamp,
) -> Result<(RawObjectRef, Vec<CandleObservation>), LabError> {
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let body = serde_json::to_vec(
        &opens
            .iter()
            .map(|open| {
                serde_json::json!({
                    "market":"KRW-BTC","candle_date_time_utc":open,
                    "opening_price":100,"high_price":101,"low_price":99,"trade_price":100,
                    "candle_acc_trade_volume":1,"candle_acc_trade_price":100
                })
            })
            .collect::<Vec<_>>(),
    )?;
    let published = store.publish_raw(crate::storage::RawObjectInput {
        source_url: "http://127.0.0.1:1/v1/candles/minutes/60?market=KRW-BTC&count=200".into(),
        fetched_at: at,
        persisted_at: at,
        http_status: 200,
        remaining_req: None,
        origin: crate::contracts::MarketDataOrigin::SyntheticTestOnly,
        body,
    })?;
    store.catalog_raw(&published.object)?;
    let rows =
        crate::market_data::decode_candles(&published.body, &market, CandleInterval::H1, at)?
            .into_iter()
            .rev()
            .map(|candle| {
                let digest = observation_digest(&candle)?;
                Ok(CandleObservation {
                    id: crate::contracts::ObservationId::from_seed(digest.as_str()),
                    candle,
                    content_digest: digest,
                    raw_object_ids: vec![published.object.id.clone()],
                    constituent_ids: vec![],
                })
            })
            .collect::<Result<Vec<_>, LabError>>()?;
    Ok((published.object, rows))
}

#[test]
fn completed_cold_seed_tracks_current_consumers_without_rescanning_old_bootstrap()
-> Result<(), TestError> {
    let anchor = |at: &str| -> Result<ProviderAbsenceAnchor, LabError> {
        Ok(ProviderAbsenceAnchor {
            observation_id: crate::contracts::ObservationId::from_seed(at),
            open_time_utc: time(at)?,
            content_digest: crate::contracts::ContentHash::of_bytes(at.as_bytes()),
        })
    };
    let historical = anchor("2025-01-01T00:00:00Z")?;
    let current = anchor("2025-04-01T00:00:00Z")?;
    let watermark = anchor("2025-04-01T00:05:00Z")?;
    let cursor = ProviderFeedRecoveryCursor {
        market: MarketId::parse_upbit("KRW-BTC")?,
        interval: CandleInterval::M5,
        progress: crate::contracts::ProviderFeedRecoveryProgress {
            accepted_anchor: Some(historical.clone()),
            next_page_to_utc: None,
            pages_scanned: 1,
            rows_scanned: 200,
            page_limit: 64,
            row_limit: 12_800,
            pending_absence_confirmation: false,
        },
        target_count: 199,
        started_at_utc: historical.open_time_utc,
        last_recovered_tip: Some(watermark.clone()),
    };
    let selected = completed_provider_backfill_anchor(&cursor, Some(current.clone()))?
        .ok_or("missing current consumer boundary")?;
    assert_eq!(selected, current);
    assert!((watermark.open_time_utc.0 - historical.open_time_utc.0).num_minutes() / 5 > 12_800);
    assert_eq!(
        (watermark.open_time_utc.0 - selected.open_time_utc.0).num_minutes() / 5,
        1
    );
    assert_eq!(
        completed_provider_backfill_anchor(&cursor, Some(historical.clone()))?,
        Some(historical.clone())
    );
    assert_eq!(
        completed_provider_backfill_anchor(&cursor, None)?,
        Some(historical.clone())
    );
    let mut incomplete = cursor.clone();
    incomplete.progress.next_page_to_utc = Some(current.open_time_utc);
    assert_eq!(
        cursor_backfill_anchor(&incomplete),
        Some(historical.clone())
    );
    assert!(matches!(
        completed_provider_backfill_anchor(&incomplete, Some(current)),
        Err(LabError::Conflict(_))
    ));
    assert_eq!(cursor.progress.accepted_anchor, Some(historical));
    assert_eq!(cursor.last_recovered_tip, Some(watermark));
    Ok(())
}

#[tokio::test]
async fn shared_consumers_keep_the_in_progress_recovery_target() -> Result<(), TestError> {
    let root = TestRoot::new("shared-recovery-target")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let (first, first_rows, second, second_rows) = owner
        .handle()
        .call("shared_recovery_pages", |store| {
            let opens = ["2026-01-01T03:00:00", "2026-01-01T00:00:00"];
            let (first, rows) =
                recovery_fixture_page(store, &opens, time("2026-01-01T04:00:01Z")?)?;
            let (second, second_rows) =
                recovery_fixture_page(store, &opens, time("2026-01-01T04:00:02Z")?)?;
            Ok((first, rows, second, second_rows))
        })
        .await?;
    let (feed, runtime) = PublicMarketFeed::start(
        UpbitClient::new()?,
        owner.handle(),
        PublicMarketFeedConfig::loopback_fixture("ws://127.0.0.1:1/websocket/v1")?,
    )?;
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    assert!(matches!(
        feed.prepare_provider_observations(ProviderRecoveryPage {
            subscription: &subscription,
            raw: &first,
            observations: &first_rows,
            cursor_before: None,
            seed_anchor: None,
            requested_to: None,
            target_count: 199,
        })
        .await,
        Err(LabError::BlockedEvidence(_))
    ));
    let cursor = owner
        .handle()
        .call("read_shared_recovery_cursor", |store| {
            store.provider_feed_recovery_cursor(
                &MarketId::parse_upbit("KRW-BTC")?,
                CandleInterval::H1,
            )
        })
        .await?
        .ok_or("missing shared cursor")?;
    assert_eq!(cursor.target_count, 199);
    // The automation reader needs only two output bars while the coordinator's
    // already-admitted recovery is still filling its larger bootstrap target.
    assert!(
        matches!(feed.prepare_provider_observations(ProviderRecoveryPage {
        subscription:&subscription,raw:&second,observations:&second_rows,cursor_before:Some(cursor),
        seed_anchor:None,requested_to:None,target_count:2,
    }).await,Err(LabError::BlockedEvidence(reason)) if reason.contains("PUBLIC_FEED_BACKFILL_IN_PROGRESS"))
    );
    let cursor = owner
        .handle()
        .call("read_continued_shared_cursor", |store| {
            store.provider_feed_recovery_cursor(
                &MarketId::parse_upbit("KRW-BTC")?,
                CandleInterval::H1,
            )
        })
        .await?
        .ok_or("missing continued cursor")?;
    assert_eq!(cursor.target_count, 199);
    assert_eq!(cursor.progress.pages_scanned, 2);
    runtime.shutdown().await?;
    owner.shutdown()?;
    Ok(())
}

#[test]
fn accepted_singleton_pins_do_not_expand_the_complete_rest_window() -> Result<(), LabError> {
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let rows = (0..5)
        .map(|hour| {
            rest_observation(
                &state_frame(&format!("2026-01-01T0{hour}:00:00Z"), "100", "1")?,
                "window-fixture",
            )
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    let at = time("2026-01-01T05:00:01Z")?;
    let mut complete = FeedCache::new(4);
    complete.store_rest_snapshot(
        &subscription,
        rest_raw("complete-window", at),
        &rows[2..],
        true,
    );
    complete.seed_accepted_observation(&subscription, rows[0].clone())?;
    let mut older = rows[1..].to_vec();
    complete.pin_rest_snapshot(&subscription, at, &mut older)?;
    assert_eq!(older[0].id, rows[1].id);
    // An observed singleton outside the retained window does not assert that
    // every intermediate slot was retained. A hole inside a complete window
    // still requires fail-closed handling (provider-proof contradiction is upstream).
    let mut sparse = FeedCache::new(4);
    sparse.store_rest_snapshot(
        &subscription,
        rest_raw("known-sparse-window", at),
        &[rows[2].clone(), rows[4].clone()],
        true,
    );
    let mut unexplained = rows[2..].to_vec();
    assert!(
        matches!(sparse.pin_rest_snapshot(&subscription,at,&mut unexplained),Err(LabError::DataCorrupt(reason)) if reason.contains("unexplained accepted-history slot"))
    );
    Ok(())
}

#[test]
fn warm_rest_window_allows_a_contiguous_new_tail_but_rejects_future_holes() -> Result<(), LabError>
{
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let rows = (0..=4)
        .map(|hour| {
            rest_observation(
                &state_frame(&format!("2026-01-01T0{hour}:00:00Z"), "100", "1")?,
                "rollover-window",
            )
        })
        .collect::<Result<Vec<_>, LabError>>()?;
    let fetched_at = time("2026-01-01T05:00:01Z")?;
    let mut cache = FeedCache::new(8);
    cache.store_rest_snapshot(
        &subscription,
        rest_raw("rollover-window", fetched_at),
        &rows[..=2],
        true,
    );
    cache.confirm_rest_snapshot(&subscription, &rows[..=2])?;

    let mut rolling = rows[1..=3].to_vec();
    cache.pin_rest_snapshot_with_provider_absence(
        &subscription,
        fetched_at,
        &mut rolling,
        &[],
        &[],
    )?;
    assert_eq!(
        rolling.last().map(|row| row.id.clone()),
        Some(rows[3].id.clone())
    );
    assert_eq!(cache.cached_prefix(&subscription, 8).1.len(), 3);

    let mut unexplained_future_hole = vec![rows[2].clone(), rows[4].clone()];
    assert!(matches!(
        cache.pin_rest_snapshot_with_provider_absence(
            &subscription,
            fetched_at,
            &mut unexplained_future_hole,
            &[],
            &[],
        ),
        Err(LabError::DataGap(_))
    ));
    Ok(())
}

#[tokio::test]
async fn confirmed_feed_refresh_uses_the_current_window_after_bootstrap_pages_age_out()
-> Result<(), TestError> {
    let root = TestRoot::new("completed-recovery-window")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let (latest, latest_rows) = owner
        .handle()
        .call("seed_aged_bootstrap_pages", |store| {
            let market = MarketId::parse_upbit("KRW-BTC")?;
            let interval = CandleInterval::H1;
            let at = time("2026-02-01T00:00:01Z")?;
            let base = time("2026-01-01T00:00:00Z")?;
            let page = |store: &mut crate::storage::Store, start: i64, at: UtcTimestamp| {
                let opens = (start..start + 200)
                    .rev()
                    .map(|hour| {
                        (base.0 + chrono::Duration::hours(hour))
                            .format("%Y-%m-%dT%H:%M:%S")
                            .to_string()
                    })
                    .collect::<Vec<_>>();
                recovery_fixture_page(
                    store,
                    &opens.iter().map(String::as_str).collect::<Vec<_>>(),
                    at,
                )
            };
            let (raw, initial) = page(store, 0, at)?;
            store.record_provider_feed_recovery_page(
                crate::storage::PersistedProviderRecoveryPage {
                    raw,
                    market: market.clone(),
                    interval,
                    requested_to: None,
                    seed_anchor: None,
                    target_count: 199,
                    observations: initial.clone(),
                    pending_absence_confirmation: false,
                    as_of_utc: at,
                },
            )?;
            store.confirm_provider_feed_recovery_window(&market, interval, &initial, at)?;
            // Normal reads can advance the confirmed tip repeatedly while the
            // original bootstrap page remains archived as durable evidence.
            let (_, advanced) = page(
                store,
                200,
                UtcTimestamp(at.0 + chrono::Duration::seconds(1)),
            )?;
            store.confirm_provider_feed_recovery_window(
                &market,
                interval,
                &advanced,
                UtcTimestamp(at.0 + chrono::Duration::seconds(1)),
            )?;
            page(
                store,
                201,
                UtcTimestamp(at.0 + chrono::Duration::seconds(2)),
            )
        })
        .await?;
    let (feed, runtime) = PublicMarketFeed::start(
        UpbitClient::new()?,
        owner.handle(),
        PublicMarketFeedConfig::loopback_fixture("ws://127.0.0.1:1/websocket/v1")?,
    )?;
    let subscription =
        PublicMarketSubscription::new(MarketId::parse_upbit("KRW-BTC")?, CandleInterval::H1);
    let cursor = owner
        .handle()
        .call("read_aged_bootstrap_cursor", |store| {
            store.provider_feed_recovery_cursor(
                &MarketId::parse_upbit("KRW-BTC")?,
                CandleInterval::H1,
            )
        })
        .await?
        .ok_or("missing completed cursor")?;
    let (rows, spans, _) = feed
        .prepare_provider_observations(ProviderRecoveryPage {
            subscription: &subscription,
            raw: &latest,
            observations: &latest_rows,
            seed_anchor: cursor_backfill_anchor(&cursor),
            cursor_before: Some(cursor),
            requested_to: None,
            target_count: 199,
        })
        .await?;
    validate_rest_snapshot(&rows, ProbeCount::try_from(199_u32)?, &spans)?;
    assert_eq!(rows.len(), latest_rows.len());
    assert_eq!(rows[0].id, latest_rows[0].id);
    runtime.shutdown().await?;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn cold_query_two_reuses_the_full_admitted_recovery_window() -> Result<(), TestError> {
    let root = TestRoot::new("query-two-full-source-window")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let interval = CandleInterval::H1;
    let seed_market = market.clone();
    let (current_raw, current_rows) = owner
        .handle()
        .call("seed_legacy_tip_only_source_window", move |store| {
            let (full_raw, full_rows) = recovery_fixture_page(
                store,
                &[
                    "2026-01-01T03:00:00",
                    "2026-01-01T02:00:00",
                    "2026-01-01T01:00:00",
                    "2026-01-01T00:00:00",
                ],
                time("2026-01-01T04:00:01Z")?,
            )?;
            let accepted = crate::contracts::ProviderAbsenceAnchor {
                observation_id: full_rows[0].id.clone(),
                open_time_utc: full_rows[0].candle.open_time_utc,
                content_digest: full_rows[0].content_digest.clone(),
            };
            let cursor = store.record_provider_feed_recovery_page(
                crate::storage::PersistedProviderRecoveryPage {
                    raw: full_raw,
                    market: seed_market.clone(),
                    interval,
                    requested_to: None,
                    seed_anchor: Some(accepted),
                    target_count: 4,
                    observations: full_rows.clone(),
                    pending_absence_confirmation: false,
                    as_of_utc: time("2026-01-01T04:00:02Z")?,
                },
            )?;
            assert!(cursor.is_complete());
            store.confirm_provider_feed_recovery_window(
                &seed_market,
                interval,
                &full_rows[3..],
                time("2026-01-01T04:00:02Z")?,
            )?;
            recovery_fixture_page(
                store,
                &["2026-01-01T03:00:00", "2026-01-01T02:00:00"],
                time("2026-01-01T04:00:03Z")?,
            )
        })
        .await?;
    let cursor_market = market.clone();
    let cursor = owner
        .handle()
        .call("read_query_two_complete_cursor", move |store| {
            store.provider_feed_recovery_cursor(&cursor_market, interval)
        })
        .await?
        .ok_or("missing complete legacy source cursor")?;
    let (feed, runtime) = PublicMarketFeed::start(
        UpbitClient::new()?,
        owner.handle(),
        PublicMarketFeedConfig::loopback_fixture("ws://127.0.0.1:1/websocket/v1")?,
    )?;
    let subscription = PublicMarketSubscription::new(market.clone(), interval);
    let (recovered, spans, _) = feed
        .prepare_provider_observations(ProviderRecoveryPage {
            subscription: &subscription,
            raw: &current_raw,
            observations: &current_rows,
            cursor_before: Some(cursor.clone()),
            seed_anchor: cursor_backfill_anchor(&cursor),
            requested_to: None,
            target_count: 2,
        })
        .await?;
    validate_rest_snapshot(&recovered, ProbeCount::try_from(2_u32)?, &spans)?;
    assert_eq!(recovered.len(), 4);
    assert_eq!(
        recovered[0].candle.open_time_utc,
        time("2026-01-01T00:00:00Z")?
    );
    assert_eq!(
        recovered[3].candle.open_time_utc,
        time("2026-01-01T03:00:00Z")?
    );
    let stored = owner
        .handle()
        .call("read_repaired_query_two_source_rows", move |store| {
            store.provider_feed_recovery_observations(&market, interval, cursor.started_at_utc)
        })
        .await?;
    assert_eq!(stored.len(), 4);
    runtime.shutdown().await?;
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cold_advanced_watermark_backfills_from_the_original_consumer_not_the_newer_source_seed()
-> Result<(), TestError> {
    let root = TestRoot::new("advanced-source-missing-consumer-history")?;
    let owner = crate::database::DatabaseOwner::open(root.0.clone())?;
    let market = MarketId::parse_upbit("KRW-BTC")?;
    let interval = CandleInterval::H1;
    let (raw, rows, accepted, old_epoch) = owner
        .handle()
        .call("seed_advanced_legacy_source", move |store| {
            let market = MarketId::parse_upbit("KRW-BTC")?;
            let at = time("2026-01-01T06:00:01Z")?;
            let (archived_raw, archived_rows) = recovery_fixture_page(
                store,
                &[
                    "2026-01-01T03:00:00",
                    "2026-01-01T02:00:00",
                    "2026-01-01T01:00:00",
                    "2026-01-01T00:00:00",
                ],
                at,
            )?;
            let (raw, rows) = recovery_fixture_page(
                store,
                &[
                    "2026-01-01T05:00:00",
                    "2026-01-01T04:00:00",
                    "2026-01-01T03:00:00",
                    "2026-01-01T02:00:00",
                    "2026-01-01T01:00:00",
                    "2026-01-01T00:00:00",
                ],
                time("2026-01-01T06:00:03Z")?,
            )?;
            let anchor = |row: &CandleObservation| crate::contracts::ProviderAbsenceAnchor {
                observation_id: row.id.clone(),
                open_time_utc: row.candle.open_time_utc,
                content_digest: row.content_digest.clone(),
            };
            store.record_provider_feed_recovery_page(
                crate::storage::PersistedProviderRecoveryPage {
                    raw: archived_raw,
                    market: market.clone(),
                    interval,
                    requested_to: None,
                    seed_anchor: Some(anchor(&rows[0])),
                    target_count: 2,
                    observations: archived_rows.clone(),
                    pending_absence_confirmation: false,
                    as_of_utc: at,
                },
            )?;
            store.confirm_provider_feed_recovery_window(
                &market,
                interval,
                &archived_rows[..1],
                at,
            )?;
            store.confirm_provider_feed_recovery_window(
                &market,
                interval,
                &archived_rows[3..4],
                at,
            )?;
            // A later archived epoch starts from the advanced source boundary, while
            // the protected paper consumer still accepts the original 00:00 candle.
            let (newer_raw, newer_rows) = recovery_fixture_page(
                store,
                &["2026-01-01T03:00:00"],
                time("2026-01-01T06:00:02Z")?,
            )?;
            let cursor = store.record_provider_feed_recovery_page(
                crate::storage::PersistedProviderRecoveryPage {
                    raw: newer_raw,
                    market: market.clone(),
                    interval,
                    requested_to: None,
                    seed_anchor: Some(anchor(&rows[3])),
                    target_count: 2,
                    observations: newer_rows,
                    pending_absence_confirmation: false,
                    as_of_utc: time("2026-01-01T06:00:02Z")?,
                },
            )?;
            store.confirm_provider_feed_recovery_window(
                &market,
                interval,
                &rows[5..],
                time("2026-01-01T06:00:03Z")?,
            )?;
            assert!(store.provider_feed_history_needs_backfill(
                &market,
                interval,
                &anchor(&rows[0]),
                &anchor(&rows[5]),
            )?);
            assert!(
                store
                    .repair_provider_feed_recovery_window(
                        &market,
                        interval,
                        cursor.started_at_utc,
                        UtcTimestamp::now(),
                    )?
                    .is_none(),
                "an old archive cannot falsely bridge the advanced watermark"
            );
            Ok((raw, rows.clone(), anchor(&rows[0]), cursor.started_at_utc))
        })
        .await?;
    let (feed, runtime) = PublicMarketFeed::start(
        UpbitClient::new()?,
        owner.handle(),
        PublicMarketFeedConfig::loopback_fixture("ws://127.0.0.1:1/websocket/v1")?,
    )?;
    let subscription = PublicMarketSubscription::new(market.clone(), interval);
    let cursor_market = market.clone();
    let cursor = owner
        .handle()
        .call("read_advanced_source_cursor", move |store| {
            store.provider_feed_recovery_cursor(&cursor_market, interval)
        })
        .await?
        .ok_or("missing advanced source cursor")?;
    assert_ne!(cursor.progress.accepted_anchor, Some(accepted.clone()));
    let (recovered, spans, progress) = feed
        .prepare_provider_observations(ProviderRecoveryPage {
            subscription: &subscription,
            raw: &raw,
            observations: &rows,
            cursor_before: Some(cursor),
            seed_anchor: Some(accepted.clone()),
            requested_to: None,
            target_count: 2,
        })
        .await?;
    assert_eq!(progress.accepted_anchor, Some(accepted.clone()));
    assert_eq!(
        recovered.len(),
        6,
        "query-two cannot truncate actual recovery history"
    );
    assert!(spans.is_empty());
    let recovered_copy = recovered.clone();
    owner
        .handle()
        .call("publish_full_advanced_source_history", move |store| {
            let market = MarketId::parse_upbit("KRW-BTC")?;
            store.confirm_provider_feed_recovery_window(
                &market,
                interval,
                &recovered_copy,
                UtcTimestamp::now(),
            )?;
            let cursor = store
                .provider_feed_recovery_cursor(&market, interval)?
                .ok_or_else(|| LabError::DataCorrupt("missing recovered cursor".into()))?;
            assert_ne!(cursor.started_at_utc, old_epoch);
            let watermark = cursor
                .last_recovered_tip
                .as_ref()
                .ok_or_else(|| LabError::DataCorrupt("missing recovered watermark".into()))?;
            assert_eq!(watermark.observation_id, recovered_copy[5].id);
            assert!(
                !store.provider_feed_history_needs_backfill(
                    &market, interval, &accepted, watermark
                )?
            );
            assert_eq!(
                store
                    .canonical_provider_feed_observations(&market, interval, &recovered_copy)?
                    .len(),
                6
            );
            assert_eq!(
                store
                    .provider_feed_recovery_observations(&market, interval, old_epoch)?
                    .len(),
                1,
                "original archived epoch remains unchanged"
            );
            Ok(())
        })
        .await?;
    runtime.shutdown().await?;
    owner.shutdown()?;
    Ok(())
}

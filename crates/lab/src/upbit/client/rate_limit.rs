use super::{ClientError, Duration, Instant};
use reqwest::header::HeaderMap;

/// One conservative budget shared by every clone of the private client.
/// HTTP effects are never retried by this transport.
pub(super) struct RateLimitState {
    next_request_at: Instant,
    blocked_until: Option<Instant>,
    suspended: bool,
    limit_failures: u32,
}

impl RateLimitState {
    pub(super) fn new() -> Self {
        Self {
            next_request_at: Instant::now(),
            blocked_until: None,
            suspended: false,
            limit_failures: 0,
        }
    }

    pub(super) fn reserve(&mut self, now: Instant) -> Result<Instant, ClientError> {
        self.check_block(now)?;
        let next = self.next_request_at.max(now);
        // This is below the strictest supported group (order-test: 8/second).
        self.next_request_at = next + Duration::from_millis(150);
        Ok(next)
    }

    pub(super) fn check_block(&self, now: Instant) -> Result<(), ClientError> {
        if self.suspended {
            return Err(ClientError::ProviderBlocked);
        }
        if let Some(until) = self.blocked_until.filter(|until| *until > now) {
            return Err(ClientError::RateLimited {
                retry_after_seconds: until.duration_since(now).as_secs().saturating_add(1),
            });
        }
        Ok(())
    }

    pub(super) fn observe(&mut self, status: u16, headers: &HeaderMap, now: Instant) {
        let retry = headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|seconds| (1..=86_400).contains(seconds));
        if status == 418 {
            // No documented block duration means no automatic retry permission.
            if let Some(seconds) = retry {
                self.extend_block(now + Duration::from_secs(seconds));
            } else {
                self.suspended = true;
            }
        } else if status == 429 {
            self.limit_failures = self.limit_failures.saturating_add(1).min(6);
            let seconds = retry.unwrap_or_else(|| 1_u64 << (self.limit_failures - 1));
            self.extend_block(now + Duration::from_secs(seconds));
        } else if (200..300).contains(&status) {
            self.limit_failures = 0;
        }
        if let Some(value) = headers.get("remaining-req") {
            let remaining = value.to_str().ok().and_then(remaining_requests);
            // A present malformed budget is not interpreted as unlimited.
            if remaining.is_none_or(|remaining| remaining == 0) {
                self.extend_block(now + Duration::from_secs(1));
            }
        }
    }

    fn extend_block(&mut self, until: Instant) {
        self.blocked_until = Some(self.blocked_until.map_or(until, |old| old.max(until)));
    }
}

fn remaining_requests(value: &str) -> Option<u32> {
    let mut seconds = None;
    for part in value.split(';') {
        let (key, value) = part.trim().split_once('=')?;
        if key.trim() == "sec" {
            if seconds.is_some() {
                return None;
            }
            seconds = Some(value.trim().parse().ok()?);
        }
    }
    seconds
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_budget_and_provider_block_stop_followup_requests() {
        let now = Instant::now();
        let mut state = RateLimitState::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            "remaining-req",
            "group=default; min=1800; sec=0".parse().unwrap(),
        );
        state.observe(200, &headers, now);
        assert!(matches!(
            state.reserve(now),
            Err(ClientError::RateLimited { .. })
        ));
        assert!(state.reserve(now + Duration::from_secs(2)).is_ok());
        state.observe(418, &HeaderMap::new(), now);
        assert!(matches!(
            state.reserve(now + Duration::from_secs(100)),
            Err(ClientError::ProviderBlocked)
        ));
    }

    #[test]
    fn rate_limit_cooldown_honors_retry_after_without_retrying_effects() {
        let now = Instant::now();
        let mut state = RateLimitState::new();
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "10".parse().unwrap());
        state.observe(429, &headers, now);
        assert!(state.reserve(now + Duration::from_secs(9)).is_err());
        assert!(state.reserve(now + Duration::from_secs(11)).is_ok());
        assert_eq!(remaining_requests("group=order; min=0; sec=3"), Some(3));
        assert_eq!(remaining_requests("group=order; sec=3; sec=4"), None);
    }
}

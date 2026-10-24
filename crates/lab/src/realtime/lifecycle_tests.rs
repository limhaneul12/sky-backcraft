use std::time::Duration;

use crate::contracts::LabError;
use crate::realtime::PublicFeedErrorCategory;

use super::{next_backoff, parser_failure, reconnect_delay_after_connection};

#[test]
fn short_successful_handshakes_preserve_exponential_reconnect_delay() {
    let minimum = Duration::from_millis(100);
    let maximum = Duration::from_millis(800);
    let stable_connection = Duration::from_secs(20);
    let mut delay = minimum;
    let mut attempts = Vec::new();

    for _ in 0..4 {
        attempts.push(delay);
        delay = reconnect_delay_after_connection(
            delay,
            Duration::from_millis(500),
            stable_connection,
            minimum,
        );
        delay = next_backoff(delay, maximum);
    }

    assert_eq!(
        attempts,
        [
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
        ]
    );
}

#[test]
fn stable_connection_resets_reconnect_delay_to_minimum() {
    assert_eq!(
        reconnect_delay_after_connection(
            Duration::from_secs(8),
            Duration::from_secs(20),
            Duration::from_secs(20),
            Duration::from_millis(250),
        ),
        Duration::from_millis(250)
    );
}

#[test]
fn parser_diagnostics_keep_error_text_out_of_connection_failure() {
    let parser = parser_failure(&LabError::ContractParse("sensitive provider input".into()));
    assert_eq!(parser.category, PublicFeedErrorCategory::Protocol);
    assert_eq!(parser.lab_error_category, Some("CONTRACT_PARSE"));
    assert_eq!(parser.detail, "invalid_candle_frame");
    assert!(!format!("{parser:?}").contains("sensitive provider input"));
}

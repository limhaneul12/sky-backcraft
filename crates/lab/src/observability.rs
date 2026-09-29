//! Process-wide structured observability initialization and lifecycle events.

use crate::transport::TransportState;
use tracing_subscriber::util::{SubscriberInitExt, TryInitError};

/// Initialize synchronous newline-delimited JSON tracing on stderr.
///
/// # Errors
///
/// Returns [`TryInitError`] when another global subscriber is already installed.
pub fn init_json_stderr() -> Result<(), TryInitError> {
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(std::io::stderr)
        .finish()
        .try_init()
}

/// Fixed low-cardinality lifecycle phases used by the binary shutdown owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecyclePhase {
    Startup,
    StopAdmission,
    Cancel,
    Drain,
    StoreClose,
    Join,
    Complete,
    Timeout,
}

impl LifecyclePhase {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::StopAdmission => "stop_admission",
            Self::Cancel => "cancel",
            Self::Drain => "drain",
            Self::StoreClose => "store_close",
            Self::Join => "join",
            Self::Complete => "complete",
            Self::Timeout => "timeout",
        }
    }
}

pub fn record_lifecycle_phase(phase: LifecyclePhase, state: &TransportState) {
    let snapshot = state.snapshot();
    tracing::info!(
        target: "spot_lab::lifecycle",
        event = "lifecycle_phase",
        phase = phase.as_str(),
        active_connections = snapshot.active_connections,
        active_requests = snapshot.active_requests,
        rejected_connections = snapshot.rejected_connections,
        rejected_requests = snapshot.rejected_requests,
        timed_out_connections = snapshot.timed_out_connections,
        timed_out_requests = snapshot.timed_out_requests,
        response_limit_rejections = snapshot.response_limit_rejections,
    );
}

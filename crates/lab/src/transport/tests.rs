use super::*;
use axum::{body::Body, response::IntoResponse, serve::Listener as _};
use std::{
    io::Write,
    sync::{Arc, Mutex, atomic::Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing_subscriber::fmt::MakeWriter;

fn limits_with(
    max_open_connections: usize,
    max_inflight_requests: usize,
    max_requests_per_second: u64,
) -> TransportLimits {
    TransportLimits {
        max_open_connections,
        max_inflight_requests,
        max_requests_per_second,
        ..TransportLimits::default()
    }
}

#[tokio::test(start_paused = true)]
async fn request_admission_rejects_saturation_rate_and_stopped_state() {
    let inflight_state = Arc::new(TransportState::new(limits_with(16, 2, 100)));
    let first = inflight_state.try_admit_request();
    let second = inflight_state.try_admit_request();
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert!(matches!(
        inflight_state.try_admit_request(),
        Err(AdmissionRejection::Inflight)
    ));
    drop(first);
    assert!(inflight_state.try_admit_request().is_ok());

    let rate_state = Arc::new(TransportState::new(limits_with(16, 8, 2)));
    let first = rate_state.try_admit_request();
    let second = rate_state.try_admit_request();
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert!(matches!(
        rate_state.try_admit_request(),
        Err(AdmissionRejection::Rate)
    ));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(rate_state.try_admit_request().is_ok());

    rate_state.stop_admission();
    assert!(matches!(
        rate_state.try_admit_request(),
        Err(AdmissionRejection::Stopped)
    ));
}

#[tokio::test]
async fn listener_drops_excess_connections_and_recovers_capacity() -> io::Result<()> {
    let tcp = TcpListener::bind("127.0.0.1:0").await?;
    let mut listener = BoundedListener::new(tcp, limits_with(2, 8, 20));
    let state = listener.state();
    let address = listener.local_addr()?;

    let first_client = TcpStream::connect(address).await?;
    let (first_server, _) = listener.accept().await;
    let second_client = TcpStream::connect(address).await?;
    let (second_server, _) = listener.accept().await;
    assert_eq!(state.snapshot().active_connections, 2);

    let accept_task = tokio::spawn(async move { listener.accept().await });
    let mut rejected_client = TcpStream::connect(address).await?;
    let mut byte = [0_u8; 1];
    let rejected_read =
        tokio::time::timeout(Duration::from_secs(1), rejected_client.read(&mut byte))
            .await
            .map_err(io::Error::other)?;
    match rejected_read {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ) => {}
        Ok(read) => {
            return Err(io::Error::other(format!(
                "rejected connection returned {read} bytes"
            )));
        }
        Err(error) => return Err(error),
    }
    assert!(!accept_task.is_finished());

    drop(first_server);
    let replacement_client = TcpStream::connect(address).await?;
    let (replacement_server, _) = accept_task.await.map_err(io::Error::other)?;
    assert_eq!(state.snapshot().active_connections, 2);
    assert_eq!(state.snapshot().rejected_connections, 1);

    drop(replacement_server);
    drop(second_server);
    drop(first_client);
    drop(second_client);
    drop(rejected_client);
    drop(replacement_client);
    assert_eq!(state.snapshot().active_connections, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn connection_timers_enforce_idle_and_absolute_boundaries() -> io::Result<()> {
    let limits = TransportLimits {
        connection_idle: Duration::from_secs(10),
        absolute_connection_lifetime: Duration::from_secs(25),
        ..TransportLimits::default()
    };
    let idle_state = Arc::new(TransportState::new(limits));
    let (idle_io, _idle_peer) = tokio::io::duplex(64);
    let mut idle_io = BoundedIo::new(idle_io, &idle_state, None);
    let idle_read = tokio::spawn(async move { idle_io.read_u8().await });
    tokio::time::advance(Duration::from_secs(10)).await;
    let idle_result = idle_read.await.map_err(io::Error::other)?;
    let Err(idle_error) = idle_result else {
        return Err(io::Error::other("idle connection did not time out"));
    };
    assert_eq!(idle_error.kind(), io::ErrorKind::TimedOut);

    let absolute_state = Arc::new(TransportState::new(limits));
    let (absolute_io, mut peer) = tokio::io::duplex(64);
    let mut absolute_io = BoundedIo::new(absolute_io, &absolute_state, None);
    for value in [1_u8, 2, 3] {
        tokio::time::advance(Duration::from_secs(8)).await;
        peer.write_u8(value).await?;
        assert_eq!(absolute_io.read_u8().await?, value);
    }
    let absolute_read = tokio::spawn(async move { absolute_io.read_u8().await });
    tokio::time::advance(Duration::from_secs(1)).await;
    let absolute_result = absolute_read.await.map_err(io::Error::other)?;
    let Err(absolute_error) = absolute_result else {
        return Err(io::Error::other(
            "absolute connection lifetime did not expire",
        ));
    };
    assert_eq!(absolute_error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(absolute_state.snapshot().timed_out_connections, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_closes_connections_and_reports_request_drain_boundary() -> io::Result<()> {
    let state = Arc::new(TransportState::new(TransportLimits::default()));
    let permit = state.try_admit_request();
    assert!(permit.is_ok());
    let connection_permit = state
        .connection_slots
        .clone()
        .try_acquire_owned()
        .map_err(io::Error::other)?;
    state.active_connections.fetch_add(1, Ordering::Relaxed);
    let (connection, _peer) = tokio::io::duplex(64);
    let mut connection = BoundedIo::new(connection, &state, Some(connection_permit));
    let connection_read = tokio::spawn(async move { connection.read_u8().await });
    state.stop_admission();
    let connection_result = connection_read.await.map_err(io::Error::other)?;
    let Err(connection_error) = connection_result else {
        return Err(io::Error::other("shutdown did not close active connection"));
    };
    assert_eq!(connection_error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(state.snapshot().active_connections, 0);

    assert!(matches!(
        state.wait_for_drain(SHUTDOWN_DEADLINE).await,
        DrainOutcome::TimedOut(snapshot) if snapshot.active_requests == 1
    ));
    drop(permit);
    assert_eq!(
        state.wait_for_drain(SHUTDOWN_DEADLINE).await,
        DrainOutcome::Drained
    );
    Ok(())
}

#[tokio::test]
async fn response_limit_fails_closed_without_truncation() {
    let response = Body::from(vec![b'x'; 9]).into_response();
    assert!(matches!(
        collect_bounded_response(response, 8).await,
        Err(ResponseFailure::UnavailableOrTooLarge)
    ));
    let response = Body::from(vec![b'x'; 8]).into_response();
    let bounded = collect_bounded_response(response, 8).await;
    assert!(bounded.is_ok());
}

#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

struct CaptureGuard(Arc<Mutex<Vec<u8>>>);

impl<'writer> MakeWriter<'writer> for CaptureWriter {
    type Writer = CaptureGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        CaptureGuard(Arc::clone(&self.0))
    }
}

impl Write for CaptureGuard {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        lock_unpoisoned(&self.0).extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn scoped_ndjson_is_correlated_and_excludes_request_secrets() -> Result<(), serde_json::Error> {
    let writer = CaptureWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(writer.clone())
        .finish();
    let state = Arc::new(TransportState::new(TransportLimits::default()));
    let correlation = RequestCorrelationId(Arc::from("req-0000000000000042"));
    tracing::subscriber::with_default(subscriber, || {
        record_request(
            &state,
            RequestObservation {
                event: "request_complete",
                correlation_id: &correlation,
                route: "/mcp",
                method: "POST",
                status: StatusCode::OK,
                elapsed: Duration::from_millis(7),
                error_category: "none",
            },
        );
    });

    let bytes = lock_unpoisoned(&writer.0).clone();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("req-0000000000000042"));
    assert!(text.contains("request_complete"));
    assert!(!text.contains("secret-header-sentinel"));
    assert!(!text.contains("secret-body-sentinel"));
    let _: serde_json::Value = serde_json::from_slice(bytes.trim_ascii())?;
    Ok(())
}

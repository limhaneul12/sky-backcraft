use super::*;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-database-{label}-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create database test root");
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn process_lock_is_exclusive_and_released_after_joined_shutdown() {
    let root = TempRoot::new("owner-lock");
    let owner = DatabaseOwner::open(root.0.clone()).expect("open first database owner");
    assert!(matches!(
        DatabaseOwner::open(root.0.clone()),
        Err(LabError::Conflict(_))
    ));
    owner.shutdown().expect("shutdown first owner");

    let reopened = DatabaseOwner::open(root.0.clone()).expect("lock released after shutdown");
    reopened.shutdown().expect("shutdown reopened owner");
}

#[test]
fn shutdown_stops_admission_and_drains_an_already_accepted_command() {
    let root = TempRoot::new("drain");
    let owner = DatabaseOwner::open(root.0.clone()).expect("open database owner");
    let caller_handle = owner.handle();
    let rejected_handle = caller_handle.clone();
    let admission_state = caller_handle.sender.clone();
    let completed = Arc::new(AtomicBool::new(false));
    let completed_by_work = completed.clone();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);

    let caller = std::thread::spawn(move || {
        caller_handle.call_blocking("drain_fixture", move |_store| {
            started_tx
                .send(())
                .map_err(|error| LabError::Internal(format!("signal accepted command: {error}")))?;
            release_rx.recv().map_err(|error| {
                LabError::Internal(format!("release accepted command: {error}"))
            })?;
            completed_by_work.store(true, Ordering::Release);
            Ok(42_u32)
        })
    });
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("accepted command started");

    let shutdown = std::thread::spawn(move || owner.shutdown());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let admission_closed = admission_state
            .lock()
            .expect("database admission lock")
            .is_none();
        if admission_closed {
            break;
        }
        assert!(Instant::now() < deadline, "shutdown did not stop admission");
        std::thread::yield_now();
    }
    assert!(matches!(
        rejected_handle.call_blocking("rejected_after_shutdown", |_store| Ok(())),
        Err(LabError::ResourceLimit(_))
    ));

    release_tx.send(()).expect("release accepted command");
    assert_eq!(
        caller
            .join()
            .expect("join command caller")
            .expect("command result"),
        42
    );
    shutdown
        .join()
        .expect("join shutdown caller")
        .expect("joined database shutdown");
    assert!(completed.load(Ordering::Acquire));
}

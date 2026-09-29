//! One storage-owner thread with bounded commands and explicit joined shutdown.
//!
//! Construct and shut down the owner at the synchronous application boundary.
//! After admission, dropping an async caller does not roll back an operation:
//! reconcile durable request IDs before retrying an unknown outcome.

use crate::contracts::LabError;
use crate::storage::Store;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use tokio::sync::{mpsc, oneshot};

const COMMAND_CAPACITY: usize = 32;
type Work = Box<dyn FnOnce(&mut Store) + Send + 'static>;
enum Command {
    Run(Work),
}

#[derive(Clone)]
pub struct DatabaseHandle {
    // Admission and closure are serialized without ever holding a guard over await.
    sender: Arc<Mutex<Option<mpsc::Sender<Command>>>>,
}

pub struct DatabaseOwner {
    handle: DatabaseHandle,
    thread: Option<JoinHandle<Result<(), LabError>>>,
    // Advisory OS ownership prevents a second application recovering live jobs.
    _root_lock: std::fs::File,
}

impl DatabaseOwner {
    /// Start the sole connection owner. Call outside an async executor.
    /// # Errors
    /// Reports filesystem/schema/SQLite/thread startup failures.
    pub fn open(root: PathBuf) -> Result<Self, LabError> {
        Self::start(root, None)
    }

    /// Restore into a new data root while retaining the process ownership lock.
    /// # Errors
    /// Rejects a busy/nonempty root, corrupt backup or failed owner startup.
    pub fn restore(backup: PathBuf, root: PathBuf) -> Result<Self, LabError> {
        Self::start(root, Some(backup))
    }

    fn start(root: PathBuf, backup: Option<PathBuf>) -> Result<Self, LabError> {
        let startup_time = crate::contracts::UtcTimestamp::now();
        let builtin_policies = crate::contracts::builtin_policy_definitions()?;
        std::fs::create_dir_all(&root)
            .map_err(|error| LabError::Internal(format!("create data root: {error}")))?;
        if std::fs::symlink_metadata(&root)
            .map_err(|error| LabError::Internal(format!("inspect data root: {error}")))?
            .file_type()
            .is_symlink()
        {
            return Err(LabError::InvalidConfig(
                "data root cannot be a symlink".into(),
            ));
        }
        let lock_path = root.join(".lab-owner.lock");
        match std::fs::symlink_metadata(&lock_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(LabError::InvalidConfig(
                    "data-root lock cannot be a symlink".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(LabError::Internal(format!(
                    "inspect data-root lock: {error}"
                )));
            }
        }
        let root_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|error| LabError::Internal(format!("open data-root lock: {error}")))?;
        root_lock.try_lock().map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => {
                LabError::Conflict("another application owns this data root".into())
            }
            std::fs::TryLockError::Error(error) => {
                LabError::Internal(format!("lock data root: {error}"))
            }
        })?;
        let (sender, mut receiver) = mpsc::channel::<Command>(COMMAND_CAPACITY);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("spot-lab-storage".into())
            .spawn(move || {
                let opened = match backup {
                    Some(backup) => Store::restore_locked(backup, root),
                    None => Store::open(root),
                }
                .and_then(|mut store| {
                    store.seed_builtin_policies_once(&builtin_policies, startup_time)?;
                    Ok(store)
                });
                let mut store = match opened {
                    Ok(store) => store,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return Err(error);
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    return store.close();
                }
                tracing::info!(event = "storage_started", queue_capacity = COMMAND_CAPACITY);
                while let Some(Command::Run(work)) = receiver.blocking_recv() {
                    work(&mut store);
                }
                let result = store.close();
                tracing::info!(event = "storage_closed", success = result.is_ok());
                result
            })
            .map_err(|error| LabError::Internal(format!("start storage owner: {error}")))?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                handle: DatabaseHandle {
                    sender: Arc::new(Mutex::new(Some(sender))),
                },
                thread: Some(thread),
                _root_lock: root_lock,
            }),
            other => {
                drop(sender);
                let _joined = thread.join();
                Err(LabError::Internal(format!(
                    "storage startup failed: {other:?}"
                )))
            }
        }
    }

    #[must_use]
    pub fn handle(&self) -> DatabaseHandle {
        self.handle.clone()
    }

    /// Stop admission, drain accepted commands and join the owner outside Tokio.
    /// # Errors
    /// Reports owner panic, poisoned admission state or database close failure.
    pub fn shutdown(mut self) -> Result<(), LabError> {
        self.close_and_join()
    }

    fn close_and_join(&mut self) -> Result<(), LabError> {
        let (sender, poisoned) = match self.handle.sender.lock() {
            Ok(mut guard) => (guard.take(), false),
            Err(error) => (error.into_inner().take(), true),
        };
        drop(sender);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| LabError::Internal("storage owner panicked".into()))??;
        }
        if poisoned {
            Err(LabError::Internal(
                "storage admission lock was poisoned; owner joined".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for DatabaseOwner {
    fn drop(&mut self) {
        // Normal paths call shutdown; this owner only lives outside Tokio in the CLI.
        if self.thread.is_some() && self.close_and_join().is_err() {
            tracing::error!(event = "storage_shutdown_failed", error_class = "INTERNAL");
        }
    }
}

impl DatabaseHandle {
    /// Execute a typed storage operation without blocking an async worker.
    /// # Errors
    /// Rejects before admission on full/closed queues; after admission loss is unknown.
    pub async fn call<T, F>(&self, operation: &'static str, work: F) -> Result<T, LabError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, LabError> + Send + 'static,
    {
        let receiver = self.enqueue(operation, work)?;
        receiver.await.map_err(|_| {
            LabError::OutcomeUnknown(format!(
                "storage {operation} was admitted but reply was lost; read back durable request ID"
            ))
        })?
    }

    /// Blocking bridge for the single owned computation/export worker only.
    /// # Errors
    /// Has the same admission/read-back semantics as `call`.
    pub fn call_blocking<T, F>(&self, operation: &'static str, work: F) -> Result<T, LabError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, LabError> + Send + 'static,
    {
        self.enqueue(operation, work)?.blocking_recv().map_err(|_| LabError::OutcomeUnknown(format!(
            "storage {operation} was admitted but reply was lost; read back durable request ID"
        )))?
    }

    fn enqueue<T, F>(
        &self,
        operation: &'static str,
        work: F,
    ) -> Result<oneshot::Receiver<Result<T, LabError>>, LabError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, LabError> + Send + 'static,
    {
        let (reply, receiver) = oneshot::channel();
        let span = tracing::Span::current();
        let command = Command::Run(Box::new(move |store| {
            let _entered = span.enter();
            let result = work(store);
            tracing::debug!(
                event = "storage_operation",
                operation,
                success = result.is_ok()
            );
            if reply.send(result).is_err() {
                tracing::warn!(
                    event = "storage_reply_dropped",
                    operation,
                    outcome = "READBACK_REQUIRED"
                );
            }
        }));
        let guard = self
            .sender
            .lock()
            .map_err(|_| LabError::Internal("storage admission lock poisoned".into()))?;
        let sender = guard
            .as_ref()
            .ok_or_else(|| LabError::ResourceLimit("storage owner is shutting down".into()))?;
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                LabError::ResourceLimit("storage command queue is full".into())
            }
            mpsc::error::TrySendError::Closed(_) => {
                LabError::Internal("storage owner unavailable before admission".into())
            }
        })?;
        Ok(receiver)
    }
}

#[cfg(test)]
mod tests;

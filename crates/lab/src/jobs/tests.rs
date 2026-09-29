use super::*;
use crate::database::DatabaseOwner;

#[test]
fn failed_execution_owner_closes_admission_and_is_joined() -> Result<(), Box<dyn std::error::Error>>
{
    let root = std::env::temp_dir().join(format!("spot-lab-runner-failure-{}", std::process::id()));
    std::fs::create_dir(&root)?;
    let database = DatabaseOwner::open(root.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let jobs = runtime.block_on(JobRuntime::start(
        database.handle(),
        UpbitClient::new()?,
        root.clone(),
        None,
    ))?;
    let service = jobs.service();
    // The owner is deliberately removed while the owned runner remains alive.
    // Wake then forces a real failed claim, rather than a mocked boolean state.
    database.shutdown()?;
    service.inner.wake.notify_one();
    let result = runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while service.inner.runner_available.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(!service.inner.admitting.load(Ordering::Acquire));
        assert!(
            service
                .control(JobControl::Retry {
                    job_id: JobId::new("not-admitted")?
                })
                .await
                .is_err()
        );
        assert!(jobs.shutdown().await.is_err());
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    std::fs::remove_dir_all(root)?;
    result
}

#[test]
fn durable_queue_is_polled_when_submission_notification_is_lost()
-> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("spot-lab-lost-wake-{}", std::process::id()));
    std::fs::create_dir(&root)?;
    let owner = DatabaseOwner::open(root.clone())?;
    let database = owner.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let jobs =
            JobRuntime::start(database.clone(), UpbitClient::new()?, root.clone(), None).await?;
        let service = jobs.service();
        // Let the initial empty claim and interval tick settle. The committed
        // mutation below deliberately bypasses the caller's Notify continuation.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let submission = JobSubmission {
            request_id: crate::contracts::RequestId::new("lost-notification")?,
            payload: JobPayload::Export {
                run_id: RunId::new("missing-run")?,
                market: None,
            },
        };
        let admitted = database
            .call("submit_without_wake", move |store| {
                store.submit_job(&submission, UtcTimestamp::now())
            })
            .await?;
        let observed = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let job = service.get(admitted.id.clone()).await?;
                if job
                    .attempts
                    .last()
                    .is_some_and(|attempt| attempt.state.is_terminal())
                {
                    return Ok::<_, LabError>(job);
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        let shutdown = jobs.shutdown().await;
        let job = observed??;
        assert!(matches!(job.attempts[0].state, AttemptState::Failed { .. }));
        shutdown?;
        Ok::<(), Box<dyn std::error::Error>>(())
    });
    drop(runtime);
    let closed = owner.shutdown();
    std::fs::remove_dir_all(root)?;
    result?;
    closed?;
    Ok(())
}

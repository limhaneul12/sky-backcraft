use super::job_kind;
use crate::contracts::{
    ArbitrationPolicy, JobPayload, JobStatus, JobSubmission, LabError, PortfolioAssetSpec,
    PortfolioRiskPolicy, PortfolioRunRequest, PortfolioSpec, RequestId, ResolvedPlan, Weight,
};
use crate::storage::Store;
use rust_decimal::Decimal;
use std::path::PathBuf;

struct HistoryTestRoot(PathBuf);

impl HistoryTestRoot {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("spot-lab-history-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).expect("create history fixture root");
        Self(path)
    }
}

impl Drop for HistoryTestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn portfolio_submission(seed: &str, plan: &ResolvedPlan) -> Result<JobSubmission, LabError> {
    let request_id = RequestId::new(format!("history-{seed}"))?;
    Ok(JobSubmission {
        request_id: request_id.clone(),
        payload: JobPayload::PortfolioBacktest {
            request: Box::new(PortfolioRunRequest {
                request_id,
                plan_id: plan.id.clone(),
                input_digest: plan.input_digest.clone(),
                portfolio: PortfolioSpec {
                    initial_cash: plan.spec.initial_cash,
                    assets: vec![PortfolioAssetSpec {
                        market: crate::contracts::MarketId::parse_upbit("KRW-BTC")?,
                        max_weight: Weight::new(Decimal::new(8, 1))?,
                    }],
                    risk: PortfolioRiskPolicy {
                        max_gross_exposure: Weight::new(Decimal::new(8, 1))?,
                        min_cash_weight: Weight::new(Decimal::new(2, 1))?,
                        drawdown_stop: Weight::new(Decimal::new(1, 1))?,
                    },
                    arbitration: ArbitrationPolicy::Priority,
                },
                regime: None,
            }),
        },
    })
}

fn time(value: &str) -> crate::contracts::UtcTimestamp {
    crate::contracts::UtcTimestamp::parse_rfc3339(value).expect("fixture timestamp")
}

#[test]
fn job_history_projects_portfolio_jobs_and_preserves_cursor_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let root = HistoryTestRoot::new();
    let mut store = Store::open(&root.0)?;
    let plan = crate::storage::tests::save_portfolio_plan_fixture(&mut store)?;
    let older = store.submit_job(
        &portfolio_submission("older", &plan)?,
        time("2024-01-01T00:00:00Z"),
    )?;
    let newest = store.submit_job(
        &portfolio_submission("newest", &plan)?,
        time("2024-01-01T00:00:01Z"),
    )?;

    let first = store.job_history(None, 1)?;
    assert_eq!(first.total_count, 2);
    assert_eq!(first.returned_count, 1);
    let item = &first.items[0];
    assert_eq!(item.job_id, newest.id);
    assert_eq!(item.status, JobStatus::Queued);
    assert_eq!(item.attempt_count, 1);
    assert_eq!(item.current_attempt_id, newest.attempts[0].id);
    assert_eq!(
        serde_json::to_value(&item.kind)?,
        serde_json::json!("PORTFOLIO_BACKTEST")
    );

    let cursor = first.next_cursor.as_ref().expect("older page cursor");
    let second = store.job_history(Some(cursor), 1)?;
    assert_eq!(second.total_count, 2);
    assert_eq!(second.returned_count, 1);
    assert_eq!(second.items[0].job_id, older.id);
    assert!(second.next_cursor.is_none());
    assert!(matches!(
        store.job_history(None, 0),
        Err(LabError::ResourceLimit(_))
    ));

    for (stored, expected) in [
        ("COLLECT", "COLLECT"),
        ("BACKTEST", "BACKTEST"),
        ("PORTFOLIO_BACKTEST", "PORTFOLIO_BACKTEST"),
        ("EXPORT", "EXPORT"),
        ("VERIFY", "VERIFY"),
    ] {
        assert_eq!(
            serde_json::to_value(job_kind(stored)?)?,
            serde_json::json!(expected)
        );
    }
    assert!(matches!(
        job_kind("FUTURE_KIND"),
        Err(LabError::DataCorrupt(_))
    ));
    Ok(())
}

use super::*;
use crate::contracts::{
    AutomationSessionMode, AutomationSessionWrite, CandleInterval, MarketId, PolicyWrite, RequestId,
};

#[test]
fn automation_policy_history_blocks_delete_even_after_stop()
-> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!(
        "automation-delete-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut store = Store::open(&root)?;
        let now = UtcTimestamp::parse_rfc3339("2026-10-14T00:00:00Z")?;
        let (_, _, definition) = crate::contracts::builtin_policy_definitions()?.remove(0);
        let revision = store.write_policy(
            &PolicyWrite::Create {
                request_id: RequestId::new("automation-delete-policy")?,
                definition,
            },
            now,
        )?;
        let reference = revision.snapshot.reference.clone();
        let start = AutomationSessionWrite::Start {
            request_id: RequestId::new("automation-delete-start")?,
            policy_ref: reference.clone(),
            market: MarketId::parse_upbit("KRW-BTC")?,
            decision_interval: CandleInterval::M1,
            mode: AutomationSessionMode::Mock,
            risk: serde_json::from_value(serde_json::json!({
                "max_order_notional_krw":"100000", "max_daily_notional_krw":"1000000",
                "max_asset_exposure_krw":"500000", "max_daily_loss_krw":"50000",
                "max_open_orders":2, "max_orders_per_interval":1,
                "max_balance_age_seconds":30, "max_feed_age_seconds":90
            }))?,
        };
        let session = store
            .automation_write(
                &start,
                Some(revision.snapshot),
                Some("mock:upbit-default".into()),
                now,
            )?
            .session;
        for stopped in [false, true] {
            if stopped {
                store.automation_write(
                    &AutomationSessionWrite::Stop {
                        request_id: RequestId::new("automation-delete-stop")?,
                        session_id: session.session_id.clone(),
                        expected_sequence: session.sequence,
                    },
                    None,
                    None,
                    now,
                )?;
            }
            let preview = store.delete_preview(
                &DeleteResource::Policy {
                    policy_id: reference.policy_id.clone(),
                },
                now,
            )?;
            assert!(preview.blockers.iter().any(|blocker| {
                blocker.class == DeleteBlockerClass::ProtectedReference
                    && blocker.reference == format!("automation:{}", session.session_id)
            }));
            let outcome = store.execute_hard_delete(
                &HardDeleteRequest {
                    preview,
                    cascade: true,
                },
                now,
            );
            assert!(
                matches!(outcome, Err(LabError::Conflict(reason)) if reason.contains("automation:"))
            );
            assert!(store.load_policy_revision(&reference)?.is_some());
        }
        Ok(())
    })();
    std::fs::remove_dir_all(&root)?;
    result
}

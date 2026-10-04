//! Compact per-model comparison rows derived from the canonical review projection.

use crate::contracts::{ContentHash, LabError, RunBundle, RunModelComparison};
use crate::reporting::build_review;

/// Build bounded comparison rows without reinterpreting ledger accounting.
///
/// The causal digest is computed by the v3 execution boundary from the exact causal input slice;
/// this projection commits it beside the canonical semantic digest.
/// # Errors
/// Propagates canonical review/accounting failures or model/review shape mismatches.
pub fn build_comparisons(
    bundle: &RunBundle,
    causal_input_digest: &ContentHash,
) -> Result<Vec<RunModelComparison>, LabError> {
    let review = build_review(bundle)?;
    if review.models.len() != bundle.models.len() {
        return Err(LabError::AccountingInvariant(
            "review model count differs from run ledger".into(),
        ));
    }
    bundle
        .models
        .iter()
        .zip(review.models)
        .map(|(model, reviewed)| {
            if model.model_id != reviewed.model_id || model.market != reviewed.market {
                return Err(LabError::AccountingInvariant(
                    "review model order or identity differs from run ledger".into(),
                ));
            }
            let completed = reviewed.status == crate::contracts::ModelStatus::Completed;
            Ok(RunModelComparison {
                run_id: bundle.manifest.run_id.clone(),
                model_id: model.model_id.clone(),
                market: model.market.clone(),
                policy_ref: model.strategy.policy_ref().cloned(),
                status: reviewed.status,
                initial_equity: reviewed.equity.initial_equity,
                final_equity: reviewed.equity.final_equity,
                net_pnl: completed.then_some(reviewed.costs.net_pnl),
                net_return: reviewed.equity.total_return,
                max_drawdown: reviewed.equity.max_drawdown,
                turnover: reviewed.equity.turnover,
                cumulative_fees: completed.then_some(reviewed.costs.cumulative_fees),
                closed_episodes: completed.then_some(reviewed.episodes.closed_count),
                semantic_digest: bundle.semantic_digest.clone(),
                causal_input_digest: causal_input_digest.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_reuses_canonical_exact_financials_and_lineage() {
        let bundle = crate::reporting::tests::policy_fixture();
        let causal = ContentHash::of_bytes(b"causal-slice");
        let rows = build_comparisons(&bundle, &causal).expect("comparison projection");
        assert_eq!(rows.len(), bundle.models.len());
        assert_eq!(rows[0].semantic_digest, bundle.semantic_digest);
        assert_eq!(rows[0].causal_input_digest, causal);
        assert_eq!(
            rows[0].policy_ref,
            bundle.plan.spec.policy_selections.first().cloned()
        );
        assert!(rows[0].initial_equity.is_some());
        assert!(rows[0].final_equity.is_some());
        assert!(rows[0].net_pnl.is_some());
        assert!(rows[0].net_return.value.is_some());
    }

    #[test]
    fn unavailable_model_financials_remain_absent_instead_of_zero() {
        let mut bundle = crate::reporting::tests::policy_fixture();
        let model = &mut bundle.models[0];
        model.status = crate::contracts::ModelStatus::BlockedEvidence;
        model.status_reason = Some("evidence unavailable".into());
        model.signals.clear();
        model.orders.clear();
        model.order_events.clear();
        model.fills.clear();
        model.episodes.clear();
        model.account_marks.clear();
        let rows = build_comparisons(&bundle, &ContentHash::of_bytes(b"blocked-causal"))
            .expect("blocked comparison");
        assert!(rows[0].initial_equity.is_none());
        assert!(rows[0].final_equity.is_none());
        assert!(rows[0].net_pnl.is_none());
        assert!(rows[0].cumulative_fees.is_none());
        assert!(rows[0].closed_episodes.is_none());
        assert!(rows[0].net_return.value.is_none());
        assert!(rows[0].net_return.null_reason.is_some());
    }
}

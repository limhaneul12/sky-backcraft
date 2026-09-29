//! Canonical editable built-in policy definitions.

use super::{
    LabError, PolicyDefinition, PolicyId, PolicyProgram, StateParameters, StrategyKind,
    StrategySpec, Weight,
};
use rust_decimal::Decimal;

/// Return the explicit built-in policy seed catalog.
///
/// Existing persisted policies retain their edited heads; storage decides whether a seed is new.
///
/// # Errors
/// Rejects an invalid built-in identity, exact decimal parameter or policy definition.
pub fn builtin_policy_definitions()
-> Result<Vec<(PolicyId, StrategyKind, PolicyDefinition)>, LabError> {
    let state = StateParameters {
        ema_length: 100,
        vol_length: 20,
        k: 0.5,
    };
    let definitions = vec![
        builtin(
            "builtin-s1",
            StrategyKind::S1,
            "S1 EMA volatility band",
            "Editable S1 research seed: EMA 100, sample volatility 20, band coefficient 0.5.",
            StrategySpec::S1 {
                state: state.clone(),
            },
        )?,
        builtin(
            "builtin-s2",
            StrategyKind::S2,
            "S2 prior-range breakout",
            "Editable S2 research seed: prior-high entry window 55 and prior-low exit window 20.",
            StrategySpec::S2 {
                entry_length: 55,
                exit_length: 20,
            },
        )?,
        builtin(
            "builtin-s3",
            StrategyKind::S3,
            "S3 trend pullback",
            "Editable S3 research seed: trend 200, RSI 14, entry 35, exit 55, maximum holding 20 bars and one-bar signal expiry.",
            StrategySpec::S3 {
                trend_length: 200,
                rsi_length: 14,
                entry_threshold: 35.0,
                exit_threshold: 55.0,
                max_holding_bars: 20,
                signal_expiry_bars: 1,
            },
        )?,
        builtin(
            "builtin-s4",
            StrategyKind::S4,
            "S4 volatility target",
            "Editable S4 research seed: EMA 100, sample volatility 20, band coefficient 0.5, annual volatility target 0.4, floor 0.01 and rebalance band 0.01.",
            StrategySpec::S4 {
                state: state.clone(),
                target_annual_vol: 0.4,
                vol_floor: 0.01,
                rebalance_band: Weight::new(Decimal::new(1, 2))?,
            },
        )?,
        builtin(
            "builtin-s5",
            StrategyKind::S5,
            "S5 evidence-adjusted band",
            "Editable S5 research seed: EMA 100, sample volatility 20 and band coefficient 0.5 with the evidence overlay enabled.",
            StrategySpec::S5 {
                state: state.clone(),
            },
        )?,
        builtin(
            "builtin-buy-and-hold",
            StrategyKind::BuyAndHold,
            "Buy and hold",
            "Editable buy-and-hold research seed with no strategy parameters.",
            StrategySpec::BuyAndHold,
        )?,
        builtin(
            "builtin-s1-coverage-control",
            StrategyKind::S1CoverageControl,
            "S1 matched coverage control",
            "Editable S1 matched-control research seed: EMA 100, sample volatility 20 and band coefficient 0.5 with evidence coverage control.",
            StrategySpec::S1CoverageControl { state },
        )?,
    ];
    for (_, _, definition) in &definitions {
        definition.validate()?;
    }
    Ok(definitions)
}

fn builtin(
    id: &str,
    family: StrategyKind,
    name: &str,
    description: &str,
    strategy: StrategySpec,
) -> Result<(PolicyId, StrategyKind, PolicyDefinition), LabError> {
    Ok((
        PolicyId::new(id)?,
        family,
        PolicyDefinition {
            schema_version: "1.0".into(),
            name: name.into(),
            description: description.into(),
            program: PolicyProgram::Builtin { strategy },
        },
    ))
}

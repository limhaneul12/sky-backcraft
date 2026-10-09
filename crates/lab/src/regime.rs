//! Pure deterministic regime classification over completed OHLCV bars.
//!
//! The classifier is a streaming evaluator: every decision time observes only
//! the bars that closed at or before it, so labels are point-in-time by
//! construction and identical inputs always produce identical labels.

use crate::contracts::{
    CandleInterval, CandleObservation, ContentHash, LabError, MarketId, RegimeClassifierSpec,
    RegimeFeatures, RegimeGateSpec, RegimeLabel, RegimeObservation, VolState,
};
use rust_decimal::Decimal;
use std::collections::VecDeque;

/// Bars-per-year factor for annualizing realized volatility on one interval.
#[must_use]
pub fn annualization_factor(interval: CandleInterval) -> f64 {
    let seconds = interval.duration().num_seconds().max(1);
    // 24/7 crypto market convention, matching the report clock.
    let bars_per_year = 365.0 * 24.0 * 3600.0 / f64::from(u32::try_from(seconds).unwrap_or(1));
    bars_per_year.sqrt()
}

/// Streaming rule classifier for one market.
#[derive(Debug)]
pub struct RegimeClassifier {
    spec: RegimeClassifierSpec,
    market: MarketId,
    dataset_digest: ContentHash,
    closes: VecDeque<f64>,
    highs: VecDeque<f64>,
    lows: VecDeque<f64>,
    volumes: VecDeque<f64>,
    decision_bars: u64,
}

impl RegimeClassifier {
    /// Bind one classifier to one market/dataset identity.
    #[must_use]
    pub fn new(spec: RegimeClassifierSpec, market: MarketId, dataset_digest: ContentHash) -> Self {
        Self {
            spec,
            market,
            dataset_digest,
            closes: VecDeque::new(),
            highs: VecDeque::new(),
            lows: VecDeque::new(),
            volumes: VecDeque::new(),
            decision_bars: 0,
        }
    }

    /// Completed bars observed so far.
    #[must_use]
    pub fn observed_bars(&self) -> u64 {
        self.decision_bars
    }

    fn sma(values: &VecDeque<f64>, length: usize) -> Option<f64> {
        if values.len() < length {
            return None;
        }
        let window = values.iter().rev().take(length);
        Some(window.sum::<f64>() / f64::from(u32::try_from(length).unwrap_or(1)))
    }

    fn sma_slope(values: &VecDeque<f64>, length: usize, lookback: usize) -> Option<f64> {
        let now = Self::sma(values, length)?;
        let past_len = values.len().checked_sub(lookback)?;
        let past: Vec<f64> = values.iter().take(past_len).copied().collect();
        let past = Self::sma(&past.into(), length)?;
        Some((now - past) / f64::from(u32::try_from(lookback).unwrap_or(1)))
    }

    /// Observe one completed decision bar. Warmup is an explicit UNKNOWN
    /// observation with absent features; it is never silently omitted.
    ///
    /// # Errors
    /// Rejects nonfinite derived statistics (a corrupted bar can only fail
    /// closed; it never produces a guessed label).
    pub fn observe(
        &mut self,
        bar: &CandleObservation,
        interval: CandleInterval,
    ) -> Result<Option<RegimeObservation>, LabError> {
        let close = decimal_to_f64(bar.candle.close.get())?;
        let high = decimal_to_f64(bar.candle.high.get())?;
        let low = decimal_to_f64(bar.candle.low.get())?;
        let volume = decimal_to_f64(bar.candle.volume.get())?;
        if !close.is_finite() || !high.is_finite() || !low.is_finite() || !volume.is_finite() {
            return Err(LabError::InvalidConfig(
                "candle fields must be finite for regime features".into(),
            ));
        }
        self.push(close, high, low, volume);
        self.decision_bars = self.decision_bars.saturating_add(1);
        let causal_input_digest = self.observation_digest(bar, interval)?;
        if self.closes.len() < self.spec.warmup_bars() {
            return Ok(Some(RegimeObservation {
                market: self.market.clone(),
                decision_time: bar.candle.close_time_utc,
                features: None,
                regime: RegimeLabel::Unknown,
                vol_state: VolState::NormalVol,
                classifier_revision: self.spec.revision.clone(),
                causal_input_digest,
            }));
        }
        self.classified_observation(bar, interval, close, volume, causal_input_digest)
            .map(Some)
    }

    fn observation_digest(
        &self,
        bar: &CandleObservation,
        interval: CandleInterval,
    ) -> Result<ContentHash, LabError> {
        ContentHash::of_value(&(
            "regime-observation-v2",
            &self.spec,
            &self.dataset_digest,
            self.market.code(),
            interval,
            bar.candle.close_time_utc,
            &bar.content_digest,
        ))
    }

    fn classified_observation(
        &self,
        bar: &CandleObservation,
        interval: CandleInterval,
        close: f64,
        volume: f64,
        causal_input_digest: ContentHash,
    ) -> Result<RegimeObservation, LabError> {
        let sma_long = Self::sma(&self.closes, self.spec.sma_long).ok_or_else(|| {
            LabError::Internal("regime warmup verified but long SMA unavailable".into())
        })?;
        let sma_mid = Self::sma(&self.closes, self.spec.sma_mid).ok_or_else(|| {
            LabError::Internal("regime warmup verified but mid SMA unavailable".into())
        })?;
        let sma_short = Self::sma(&self.closes, self.spec.sma_short).ok_or_else(|| {
            LabError::Internal("regime warmup verified but short SMA unavailable".into())
        })?;
        let slope = Self::sma_slope(&self.closes, self.spec.sma_short, self.spec.slope_lookback)
            .ok_or_else(|| {
                LabError::Internal("regime warmup verified but slope unavailable".into())
            })?;
        let atr = self.true_range_average(self.spec.atr_lookback);
        let realized = self.realized_vol(self.spec.vol_lookback);
        let volume_avg = Self::sma(&self.volumes, self.spec.vol_lookback).unwrap_or(volume);
        let volume_expansion = if volume_avg > 0.0 {
            volume / volume_avg
        } else {
            0.0
        };
        let up_count = self
            .closes
            .iter()
            .rev()
            .take(self.spec.slope_lookback + 1)
            .collect::<Vec<_>>()
            .windows(2)
            .filter(|pair| pair[1] > pair[0])
            .count();
        let persistence_window = self.spec.slope_lookback;
        let directional_persistence = if persistence_window == 0 {
            0.0
        } else {
            f64::from(u32::try_from(up_count).unwrap_or(u32::MAX))
                / f64::from(u32::try_from(persistence_window).unwrap_or(u32::MAX))
        };
        let regime = if close > sma_long && sma_mid > sma_short && slope > 0.0 {
            RegimeLabel::TrendUp
        } else if close < sma_long && sma_mid < sma_short {
            RegimeLabel::TrendDown
        } else {
            RegimeLabel::Chop
        };
        let vol_state = match (self.spec.high_vol_annualized, self.spec.low_vol_annualized) {
            (Some(high), Some(low)) => {
                let annualized = realized * annualization_factor(interval);
                if annualized > high {
                    VolState::HighVol
                } else if annualized < low {
                    VolState::LowVol
                } else {
                    VolState::NormalVol
                }
            }
            _ => VolState::NormalVol,
        };
        // SMA values of positive candle closes are positive; the guards only
        // protect the division, never guess a direction.
        let features = RegimeFeatures {
            close_vs_sma_long: ratio_or_zero(close, sma_long)? - 1.0,
            sma_mid_vs_short: ratio_or_zero(sma_mid, sma_short)? - 1.0,
            sma_short_slope: slope,
            atr_ratio: ratio_or_zero(atr, close)?,
            realized_vol_annualized: realized * annualization_factor(interval),
            volume_expansion,
            directional_persistence,
        };
        // The digest binds the complete frozen classifier plus this causal
        // completed bar. Parameter or source-content changes cannot alias.
        Ok(RegimeObservation {
            market: self.market.clone(),
            decision_time: bar.candle.close_time_utc,
            features: Some(features),
            regime,
            vol_state,
            classifier_revision: self.spec.revision.clone(),
            causal_input_digest,
        })
    }

    fn push(&mut self, close: f64, high: f64, low: f64, volume: f64) {
        let bound = self
            .spec
            .warmup_bars()
            .max(self.spec.vol_lookback)
            .max(self.spec.sma_long + self.spec.slope_lookback + 1);
        push_bounded(&mut self.closes, close, bound);
        push_bounded(&mut self.highs, high, bound);
        push_bounded(&mut self.lows, low, bound);
        push_bounded(&mut self.volumes, volume, bound);
    }

    /// Simple mean true range over the last `length` completed bars.
    fn true_range_average(&self, length: usize) -> f64 {
        let n = self.closes.len();
        if n < length + 1 {
            return 0.0;
        }
        let closes: Vec<f64> = self.closes.iter().copied().collect();
        let highs: Vec<f64> = self.highs.iter().copied().collect();
        let lows: Vec<f64> = self.lows.iter().copied().collect();
        let mut total = 0.0;
        for index in n - length..n {
            let previous_close = closes[index - 1];
            let true_range = (highs[index] - lows[index])
                .max((highs[index] - previous_close).abs())
                .max((lows[index] - previous_close).abs());
            total += true_range;
        }
        total / f64::from(u32::try_from(length).unwrap_or(1))
    }

    /// Sample standard deviation of per-bar returns over the window.
    fn realized_vol(&self, length: usize) -> f64 {
        let n = self.closes.len();
        if n < length + 1 {
            return 0.0;
        }
        let closes: Vec<f64> = self.closes.iter().copied().collect();
        let mut returns = Vec::with_capacity(length);
        for index in n - length..n {
            if closes[index - 1] > 0.0 {
                returns.push(closes[index] / closes[index - 1] - 1.0);
            }
        }
        if returns.is_empty() {
            return 0.0;
        }
        let mean =
            returns.iter().sum::<f64>() / f64::from(u32::try_from(returns.len()).unwrap_or(1));
        let variance = returns
            .iter()
            .map(|value| (value - mean) * (value - mean))
            .sum::<f64>()
            / f64::from(u32::try_from(returns.len().saturating_sub(1)).unwrap_or(1)).max(1.0);
        variance.sqrt()
    }
}

fn push_bounded(queue: &mut VecDeque<f64>, value: f64, bound: usize) {
    queue.push_back(value);
    while queue.len() > bound {
        queue.pop_front();
    }
}

/// Deterministic gate decision for one label.
#[must_use]
pub fn gate_action(
    spec: &RegimeGateSpec,
    regime: RegimeLabel,
) -> crate::contracts::RegimeGateAction {
    spec.rules.action(regime).clone()
}

/// `numerator / denominator` with a zero-denominator guard that fails only on
/// nonfinite output, never on a zero denominator (which yields zero).
fn ratio_or_zero(numerator: f64, denominator: f64) -> Result<f64, LabError> {
    if denominator == 0.0 {
        return Ok(0.0);
    }
    let ratio = numerator / denominator;
    if ratio.is_finite() {
        Ok(ratio)
    } else {
        Err(LabError::InvalidConfig(
            "regime feature ratio is not finite".into(),
        ))
    }
}

/// Decimal helper shared with the portfolio runner.
pub(crate) fn decimal_to_f64(value: Decimal) -> Result<f64, LabError> {
    use rust_decimal::prelude::ToPrimitive;
    value
        .to_f64()
        .filter(|converted| converted.is_finite())
        .ok_or_else(|| LabError::InvalidConfig("decimal is not representable as f64".into()))
}

#[cfg(test)]
mod tests;

use crate::contracts::LabError;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Sma {
    length: usize,
    values: VecDeque<f64>,
    sum: f64,
}

impl Sma {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            values: VecDeque::with_capacity(length),
            sum: 0.0,
        }
    }

    pub(crate) fn update(&mut self, value: f64) -> Result<Option<f64>, LabError> {
        finite(value, "SMA input")?;
        self.values.push_back(value);
        self.sum += value;
        if self.values.len() > self.length {
            let removed = self
                .values
                .pop_front()
                .ok_or_else(|| LabError::Internal("rolling SMA queue invariant failed".into()))?;
            self.sum -= removed;
        }
        finite(self.sum, "SMA sum")?;
        if self.values.len() < self.length {
            Ok(None)
        } else {
            let average = self.sum / length_f64(self.length)?;
            finite(average, "SMA")?;
            Ok(Some(average))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Ema {
    length: usize,
    seed_sum: f64,
    seed_count: usize,
    value: Option<f64>,
}

impl Ema {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            seed_sum: 0.0,
            seed_count: 0,
            value: None,
        }
    }

    pub(crate) fn update(&mut self, value: f64) -> Result<Option<f64>, LabError> {
        finite(value, "EMA input")?;
        if let Some(previous) = self.value {
            let alpha = 2.0 / (length_f64(self.length)? + 1.0);
            let next = alpha.mul_add(value - previous, previous);
            finite(next, "EMA")?;
            self.value = Some(next);
        } else {
            self.seed_sum += value;
            self.seed_count += 1;
            if self.seed_count == self.length {
                let seeded = self.seed_sum / length_f64(self.length)?;
                finite(seeded, "EMA seed")?;
                self.value = Some(seeded);
            }
        }
        Ok(self.value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RollingVol {
    length: usize,
    returns: VecDeque<f64>,
    sum: f64,
    sum_sq: f64,
    previous_close: Option<f64>,
}

impl RollingVol {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            returns: VecDeque::with_capacity(length),
            sum: 0.0,
            sum_sq: 0.0,
            previous_close: None,
        }
    }

    pub(crate) fn update(&mut self, close: f64) -> Result<Option<f64>, LabError> {
        finite_positive(close, "volatility close")?;
        let Some(previous) = self.previous_close.replace(close) else {
            return Ok(None);
        };
        let value = (close / previous).ln();
        finite(value, "log return")?;
        self.returns.push_back(value);
        self.sum += value;
        self.sum_sq = value.mul_add(value, self.sum_sq);
        if self.returns.len() > self.length {
            let removed = self.returns.pop_front().ok_or_else(|| {
                LabError::Internal("rolling volatility queue invariant failed".into())
            })?;
            self.sum -= removed;
            self.sum_sq -= removed * removed;
        }
        if self.returns.len() < self.length {
            return Ok(None);
        }
        let count = length_f64(self.length)?;
        let numerator = self.sum_sq - self.sum * self.sum / count;
        let tolerance = f64::EPSILON * (self.sum_sq.abs() + 1.0) * count * 16.0;
        let nonnegative = if numerator >= 0.0 {
            numerator
        } else if numerator >= -tolerance {
            0.0
        } else {
            return Err(LabError::DataCorrupt(
                "rolling variance became materially negative".into(),
            ));
        };
        let sigma = (nonnegative / (count - 1.0)).sqrt();
        finite(sigma, "sample volatility")?;
        Ok(Some(sigma))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Rsi {
    length: usize,
    previous_close: Option<f64>,
    seed_count: usize,
    gain_sum: f64,
    loss_sum: f64,
    averages: Option<(f64, f64)>,
}

impl Rsi {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            previous_close: None,
            seed_count: 0,
            gain_sum: 0.0,
            loss_sum: 0.0,
            averages: None,
        }
    }

    pub(crate) fn update(&mut self, close: f64) -> Result<Option<f64>, LabError> {
        finite_positive(close, "RSI close")?;
        let Some(previous) = self.previous_close.replace(close) else {
            return Ok(None);
        };
        let change = close - previous;
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        if let Some((average_gain, average_loss)) = self.averages {
            let length = length_f64(self.length)?;
            let next_gain = ((length - 1.0) * average_gain + gain) / length;
            let next_loss = ((length - 1.0) * average_loss + loss) / length;
            finite(next_gain, "Wilder average gain")?;
            finite(next_loss, "Wilder average loss")?;
            self.averages = Some((next_gain, next_loss));
        } else {
            self.gain_sum += gain;
            self.loss_sum += loss;
            self.seed_count += 1;
            if self.seed_count == self.length {
                self.averages = Some((
                    self.gain_sum / length_f64(self.length)?,
                    self.loss_sum / length_f64(self.length)?,
                ));
            }
        }
        self.averages.map(rsi_value).transpose()
    }
}

fn rsi_value((gain, loss): (f64, f64)) -> Result<f64, LabError> {
    let rsi = if gain == 0.0 && loss == 0.0 {
        50.0
    } else if loss == 0.0 {
        100.0
    } else if gain == 0.0 {
        0.0
    } else {
        100.0 - 100.0 / (1.0 + gain / loss)
    };
    finite(rsi, "RSI")?;
    Ok(rsi)
}

fn length_f64(length: usize) -> Result<f64, LabError> {
    u32::try_from(length).map(f64::from).map_err(|_| {
        LabError::InvalidConfig("indicator lookback exceeds supported u32 range".to_owned())
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RollingExtreme {
    length: usize,
    index: u64,
    maximum: VecDeque<(u64, f64)>,
    minimum: VecDeque<(u64, f64)>,
}

impl RollingExtreme {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            index: 0,
            maximum: VecDeque::new(),
            minimum: VecDeque::new(),
        }
    }

    /// Return thresholds from prior bars, then admit the current bar.
    pub(crate) fn prior_then_update(
        &mut self,
        high: f64,
        low: f64,
    ) -> Result<(Option<f64>, Option<f64>), LabError> {
        finite_positive(high, "Donchian high")?;
        finite_positive(low, "Donchian low")?;
        let complete = self.index >= self.length as u64;
        let prior = if complete {
            (
                self.maximum.front().map(|entry| entry.1),
                self.minimum.front().map(|entry| entry.1),
            )
        } else {
            (None, None)
        };
        while self.maximum.back().is_some_and(|entry| entry.1 <= high) {
            self.maximum.pop_back();
        }
        while self.minimum.back().is_some_and(|entry| entry.1 >= low) {
            self.minimum.pop_back();
        }
        self.maximum.push_back((self.index, high));
        self.minimum.push_back((self.index, low));
        self.index += 1;
        let first_kept = self.index.saturating_sub(self.length as u64);
        while self
            .maximum
            .front()
            .is_some_and(|entry| entry.0 < first_kept)
        {
            self.maximum.pop_front();
        }
        while self
            .minimum
            .front()
            .is_some_and(|entry| entry.0 < first_kept)
        {
            self.minimum.pop_front();
        }
        Ok(prior)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Atr {
    length: usize,
    previous_close: Option<f64>,
    values: VecDeque<f64>,
    sum: f64,
}

impl Atr {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            previous_close: None,
            values: VecDeque::with_capacity(length),
            sum: 0.0,
        }
    }

    pub(crate) fn update(
        &mut self,
        high: f64,
        low: f64,
        close: f64,
    ) -> Result<Option<f64>, LabError> {
        finite_positive(high, "ATR high")?;
        finite_positive(low, "ATR low")?;
        finite_positive(close, "ATR close")?;
        let Some(previous) = self.previous_close.replace(close) else {
            return Ok(None);
        };
        let tr = (high - low)
            .max((high - previous).abs())
            .max((low - previous).abs());
        finite(tr, "ATR true range")?;
        self.values.push_back(tr);
        self.sum += tr;
        if self.values.len() > self.length {
            let removed = self
                .values
                .pop_front()
                .ok_or_else(|| LabError::Internal("rolling ATR queue invariant failed".into()))?;
            self.sum -= removed;
        }
        if self.values.len() < self.length {
            return Ok(None);
        }
        let atr = self.sum.max(0.0) / length_f64(self.length)?;
        finite(atr, "ATR")?;
        Ok(Some(atr))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum BollingerOutput {
    Upper,
    Lower,
    Bandwidth,
    PercentB,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Bollinger {
    length: usize,
    multiplier: f64,
    output: BollingerOutput,
    values: VecDeque<f64>,
}

impl Bollinger {
    pub(crate) fn new(length: usize, multiplier: f64, output: BollingerOutput) -> Self {
        Self {
            length,
            multiplier,
            output,
            values: VecDeque::with_capacity(length),
        }
    }

    pub(crate) fn update(&mut self, close: f64) -> Result<Option<f64>, LabError> {
        finite_positive(close, "Bollinger close")?;
        finite_positive(self.multiplier, "Bollinger multiplier")?;
        self.values.push_back(close);
        if self.values.len() > self.length {
            self.values.pop_front();
        }
        if self.values.len() < self.length {
            return Ok(None);
        }
        let count = length_f64(self.length)?;
        let mean = self.values.iter().sum::<f64>() / count;
        let variance = self
            .values
            .iter()
            .map(|value| (value - mean) * (value - mean))
            .sum::<f64>()
            / count;
        let std_dev = variance.max(0.0).sqrt();
        let offset = self.multiplier * std_dev;
        let upper = mean + offset;
        let lower = mean - offset;
        let result = match self.output {
            BollingerOutput::Upper => upper,
            BollingerOutput::Lower => lower,
            BollingerOutput::Bandwidth => (upper - lower) / mean,
            BollingerOutput::PercentB => {
                let span = upper - lower;
                if span.abs() <= f64::EPSILON {
                    0.5
                } else {
                    (close - lower) / span
                }
            }
        };
        finite(result, "Bollinger")?;
        Ok(Some(result))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum MacdOutput {
    Line,
    Signal,
    Histogram,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Macd {
    fast: Ema,
    slow: Ema,
    signal: Option<Ema>,
    output: MacdOutput,
}

impl Macd {
    pub(crate) fn new(
        fast_window: usize,
        slow_window: usize,
        signal_window: Option<usize>,
        output: MacdOutput,
    ) -> Self {
        Self {
            fast: Ema::new(fast_window),
            slow: Ema::new(slow_window),
            signal: signal_window.map(Ema::new),
            output,
        }
    }

    pub(crate) fn update(&mut self, close: f64) -> Result<Option<f64>, LabError> {
        let fast_val = self.fast.update(close)?;
        let slow_val = self.slow.update(close)?;
        let (Some(fast_ema), Some(slow_ema)) = (fast_val, slow_val) else {
            return Ok(None);
        };
        let line = fast_ema - slow_ema;
        finite(line, "MACD line")?;
        match (&mut self.signal, self.output) {
            (_, MacdOutput::Line) => Ok(Some(line)),
            (Some(signal_ema), MacdOutput::Signal) => signal_ema.update(line),
            (Some(signal_ema), MacdOutput::Histogram) => {
                let Some(sig) = signal_ema.update(line)? else {
                    return Ok(None);
                };
                let hist = line - sig;
                finite(hist, "MACD histogram")?;
                Ok(Some(hist))
            }
            (None, MacdOutput::Signal | MacdOutput::Histogram) => Err(LabError::Internal(
                "MACD signal EMA missing for signal/histogram output".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ZScore {
    length: usize,
    values: VecDeque<f64>,
}

impl ZScore {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            values: VecDeque::with_capacity(length),
        }
    }

    pub(crate) fn update(&mut self, close: f64) -> Result<Option<f64>, LabError> {
        finite_positive(close, "ZScore close")?;
        if self.length < 2 {
            return Err(LabError::InvalidConfig(
                "ZScore window must be at least 2".into(),
            ));
        }
        self.values.push_back(close);
        if self.values.len() > self.length {
            self.values.pop_front();
        }
        if self.values.len() < self.length {
            return Ok(None);
        }
        let count = length_f64(self.length)?;
        let mean = self.values.iter().sum::<f64>() / count;
        let sample_variance = self
            .values
            .iter()
            .map(|value| (value - mean) * (value - mean))
            .sum::<f64>()
            / (count - 1.0);
        let std_dev = sample_variance.max(0.0).sqrt();
        let z_score = if std_dev == 0.0 {
            0.0
        } else {
            (close - mean) / std_dev
        };
        finite(z_score, "ZScore")?;
        Ok(Some(z_score))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Roc {
    length: usize,
    values: VecDeque<f64>,
}

impl Roc {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            values: VecDeque::with_capacity(length.saturating_add(1)),
        }
    }

    pub(crate) fn update(&mut self, close: f64) -> Result<Option<f64>, LabError> {
        finite_positive(close, "ROC close")?;
        let required = self
            .length
            .checked_add(1)
            .ok_or_else(|| LabError::InvalidConfig("ROC window overflow".into()))?;
        self.values.push_back(close);
        if self.values.len() > required {
            self.values.pop_front();
        }
        if self.values.len() < required {
            return Ok(None);
        }
        let prior = *self
            .values
            .front()
            .ok_or_else(|| LabError::Internal("ROC queue invariant failed".into()))?;
        let roc = (close - prior) / prior;
        finite(roc, "ROC")?;
        Ok(Some(roc))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DrawdownFromHigh {
    length: usize,
    values: VecDeque<f64>,
}

impl DrawdownFromHigh {
    pub(crate) fn new(length: usize) -> Self {
        Self {
            length,
            values: VecDeque::with_capacity(length),
        }
    }

    pub(crate) fn update(&mut self, close: f64) -> Result<Option<f64>, LabError> {
        finite_positive(close, "DrawdownFromHigh close")?;
        self.values.push_back(close);
        if self.values.len() > self.length {
            self.values.pop_front();
        }
        if self.values.len() < self.length {
            return Ok(None);
        }
        let peak = self.values.iter().copied().fold(close, f64::max);
        let drawdown = (peak - close) / peak;
        finite(drawdown, "DrawdownFromHigh")?;
        Ok(Some(drawdown))
    }
}

pub(crate) fn finite(value: f64, label: &str) -> Result<(), LabError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(LabError::DataCorrupt(format!("nonfinite {label}")))
    }
}

pub(crate) fn finite_positive(value: f64, label: &str) -> Result<(), LabError> {
    finite(value, label)?;
    if value > 0.0 {
        Ok(())
    } else {
        Err(LabError::DataCorrupt(format!("nonpositive {label}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hand_traced_incremental_indicator_contracts() {
        let mut ema = Ema::new(2);
        assert_eq!(ema.update(2.0).unwrap(), None);
        assert_eq!(ema.update(4.0).unwrap(), Some(3.0));
        assert!((ema.update(8.0).unwrap().unwrap() - 19.0 / 3.0).abs() < 1e-12);

        let mut volatility = RollingVol::new(2);
        assert_eq!(volatility.update(100.0).unwrap(), None);
        assert_eq!(volatility.update(100.0 * 1.0_f64.exp()).unwrap(), None);
        let sigma = volatility.update(100.0 * 4.0_f64.exp()).unwrap().unwrap();
        assert!((sigma - 2.0_f64.sqrt()).abs() < 1e-12);

        for (closes, expected) in [
            ([100.0, 100.0, 100.0], 50.0),
            ([100.0, 101.0, 102.0], 100.0),
            ([100.0, 99.0, 98.0], 0.0),
        ] {
            let mut rsi = Rsi::new(2);
            assert_eq!(rsi.update(closes[0]).unwrap(), None);
            assert_eq!(rsi.update(closes[1]).unwrap(), None);
            assert_eq!(rsi.update(closes[2]).unwrap(), Some(expected));
        }

        let mut donchian = RollingExtreme::new(2);
        assert_eq!(donchian.prior_then_update(10.0, 9.0).unwrap(), (None, None));
        assert_eq!(donchian.prior_then_update(12.0, 8.0).unwrap(), (None, None));
        assert_eq!(
            donchian.prior_then_update(100.0, 1.0).unwrap(),
            (Some(12.0), Some(8.0))
        );

        let mut atr = Atr::new(2);
        assert_eq!(atr.update(110.0, 90.0, 100.0).unwrap(), None);
        assert_eq!(atr.update(115.0, 95.0, 105.0).unwrap(), None);
        assert!((atr.update(125.0, 100.0, 120.0).unwrap().unwrap() - 22.5).abs() < 1e-12);

        let mut bb_upper = Bollinger::new(2, 2.0, BollingerOutput::Upper);
        assert_eq!(bb_upper.update(100.0).unwrap(), None);
        assert!((bb_upper.update(120.0).unwrap().unwrap() - 130.0).abs() < 1e-12);

        let mut z = ZScore::new(2);
        assert_eq!(z.update(100.0).unwrap(), None);
        assert!((z.update(120.0).unwrap().unwrap() - 0.5_f64.sqrt()).abs() < 1e-12);

        let mut roc = Roc::new(2);
        assert_eq!(roc.update(100.0).unwrap(), None);
        assert_eq!(roc.update(110.0).unwrap(), None);
        assert!((roc.update(125.0).unwrap().unwrap() - 0.25).abs() < 1e-12);

        let mut dd = DrawdownFromHigh::new(2);
        assert_eq!(dd.update(100.0).unwrap(), None);
        assert!((dd.update(80.0).unwrap().unwrap() - 0.20).abs() < 1e-12);
    }
}

use crate::contracts::LabError;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    }
}

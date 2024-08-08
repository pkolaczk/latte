use crate::autocorrelation::EffectiveSampleSizeEstimator;
use crate::histogram::SerializableHistogram;
use crate::percentiles::Percentiles;
use crate::stats::Mean;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Captures latency mean and percentiles, with uncertainty estimates.
#[derive(Serialize, Deserialize, Debug)]
pub struct LatencyDistribution {
    pub mean: Mean,
    pub percentiles: Percentiles,
    pub histogram: SerializableHistogram,
}

/// Builds TimeDistribution from a stream of durations.
#[derive(Clone, Debug)]
pub struct LatencyDistributionRecorder {
    histogram_ns: Histogram<u64>,
    ess_estimator: EffectiveSampleSizeEstimator,
}

impl LatencyDistributionRecorder {
    pub fn record(&mut self, time: Duration) {
        self.histogram_ns
            .record(time.as_nanos().clamp(1, u64::MAX as u128) as u64)
            .unwrap();
        self.ess_estimator.record(time.as_secs_f64());
    }

    pub fn add(&mut self, other: &LatencyDistributionRecorder) {
        self.histogram_ns.add(&other.histogram_ns).unwrap();
        self.ess_estimator.add(&other.ess_estimator);
    }

    pub fn clear(&mut self) {
        self.histogram_ns.clear();
        self.ess_estimator.clear();
    }

    pub fn distribution(&self) -> LatencyDistribution {
        LatencyDistribution {
            mean: Mean::from(&self.histogram_ns, 1e-6, 1),
            percentiles: Percentiles::compute(&self.histogram_ns, 1e-6),
            histogram: SerializableHistogram(self.histogram_ns.clone()),
        }
    }
    pub fn distribution_with_errors(&self) -> LatencyDistribution {
        let ess = self.ess_estimator.effective_sample_size();
        LatencyDistribution {
            mean: Mean::from(&self.histogram_ns, 1e-6, ess),
            percentiles: Percentiles::compute_with_errors(&self.histogram_ns, 1e-6, ess),
            histogram: SerializableHistogram(self.histogram_ns.clone()),
        }
    }
}

impl Default for LatencyDistributionRecorder {
    fn default() -> Self {
        Self {
            histogram_ns: Histogram::new(3).unwrap(),
            ess_estimator: Default::default(),
        }
    }
}

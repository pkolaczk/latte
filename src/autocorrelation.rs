use more_asserts::assert_le;
use rand_distr::num_traits::Pow;

/// Estimates the effective size of the sample, by taking account for
/// autocorrelation between measurements.
///
/// In most statistical operations we assume measurements to be independent of each other.
/// However, in reality time series data are often autocorrelated, which means measurements
/// in near proximity affect each other. A single performance fluctuation of the measured system
/// can affect multiple measurements one after another. This effect decreases the effective
/// sample size and increases measurement uncertainty.
///
/// The algorithm used for computing autocorrelation matrix is quite inaccurate as it doesn't compute
/// the full covariance matrix, but approximates it by pre-merging data points.
/// However, it is fairly fast (O(n log log n) and works in O(log n) memory incrementally.
#[derive(Clone, Debug, Default)]
pub struct EffectiveSampleSizeEstimator {
    n: u64,
    levels: Vec<Level>,
}

#[derive(Clone, Debug)]
struct Level {
    level: usize,
    buf: Vec<f64>,
    variance: VarianceEstimator,
}

/// Estimates the effective sample size by using batch means method.
impl EffectiveSampleSizeEstimator {
    /// Adds a single data point
    pub fn record(&mut self, x: f64) {
        self.n += 1;
        self.insert(x, 0);
    }

    /// Merges another estimator data into this one
    pub fn add(&mut self, other: &EffectiveSampleSizeEstimator) {
        self.n += other.n;
        for level in &other.levels {
            self.add_level(level);
        }
    }

    pub fn clear(&mut self) {
        self.n = 0;
        self.levels.clear();
    }

    fn insert(&mut self, x: f64, level: usize) {
        if self.levels.len() == level {
            self.levels.push(Level::new(level));
        }
        if let Some(carry) = self.levels[level].record(x) {
            self.insert(carry, level + 1);
        }
    }

    fn add_level(&mut self, level: &Level) {
        if self.levels.len() == level.level {
            self.levels.push(level.clone());
        } else if let Some(carry) = self.levels[level.level].add(level) {
            self.insert(carry, level.level + 1);
        }
    }

    pub fn effective_sample_size(&self) -> u64 {
        if self.n <= 1 {
            return self.n;
        }

        assert!(!self.levels.is_empty());

        // Autocorrelation time can be estimated as:
        // size_of_the_batch * variance_of_the_batch_mean / variance_of_the_whole_sample
        // We can technically compute it from any level but there are some constraints:
        // - the batch size must be greater than the autocorrelation time
        // - the number of batches should be also large enough for the variance
        //   of the mean be accurate
        let sample_variance = self.levels[0].variance.value();
        let autocorrelation_time = self
            .levels
            .iter()
            .map(|l| {
                (
                    l.batch_len(),
                    l.batch_len() as f64 * l.variance.value() / sample_variance,
                )
            })
            .take_while(|(batch_len, time)| *time > 0.2 * *batch_len as f64)
            .map(|(_, time)| time)
            .reduce(f64::max)
            .unwrap();

        (self.n as f64 / autocorrelation_time) as u64
    }
}

impl Level {
    fn new(level: usize) -> Self {
        Level {
            level,
            buf: Vec::with_capacity(2),
            variance: Default::default(),
        }
    }

    fn batch_len(&self) -> usize {
        1 << self.level
    }

    fn record(&mut self, value: f64) -> Option<f64> {
        self.variance.record(value);
        self.buf.push(value);
        self.merge()
    }

    fn add(&mut self, other: &Level) -> Option<f64> {
        assert_eq!(self.level, other.level);
        self.variance.add(&other.variance);
        // If there was more than 1 item recorded by the other level, then we must
        // drop our queued item, because it is not a neighbour of the other item
        if other.variance.n > 1 {
            self.buf.clear();
        }
        self.buf.extend(&other.buf);
        assert_le!(self.buf.len(), 2);
        self.merge()
    }

    fn merge(&mut self) -> Option<f64> {
        if self.buf.len() == 2 {
            let merged = (self.buf[0] + self.buf[1]) / 2.0;
            self.buf.clear();
            Some(merged)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Default)]
struct VarianceEstimator {
    mean: f64,
    var: f64,
    n: u64,
}

/// Incrementally estimates covariance of two random variables X and Y.
/// Uses Welford's online algorithm.
impl VarianceEstimator {
    pub fn record(&mut self, x: f64) {
        self.n += 1;
        let delta1 = x - self.mean;
        self.mean += delta1 / self.n as f64;
        let delta2 = x - self.mean;
        self.var += delta1 * delta2;
    }

    pub fn add(&mut self, other: &VarianceEstimator) {
        let n1 = self.n as f64;
        let n2 = other.n as f64;
        let m1 = self.mean;
        let m2 = other.mean;
        let new_mean = (m1 * n1 + m2 * n2) / (n1 + n2);

        self.n += other.n;
        self.mean = new_mean;
        self.var = self.var + other.var + (m1 - new_mean).pow(2) * n1 + (m2 - new_mean).pow(2) * n2;
    }

    pub fn value(&self) -> f64 {
        if self.n <= 1 {
            f64::NAN
        } else {
            self.var / (self.n - 1) as f64
        }
    }
}

#[cfg(test)]
mod test {
    use crate::autocorrelation::{EffectiveSampleSizeEstimator, VarianceEstimator};
    use assert_approx_eq::assert_approx_eq;
    use more_asserts::{assert_gt, assert_le};
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;

    #[test]
    fn test_random() {
        let mut rng = SmallRng::seed_from_u64(4);
        let mut estimator = EffectiveSampleSizeEstimator::default();
        const N: u64 = 1000;
        for _ in 0..N {
            estimator.record(rng.gen());
        }
        assert_gt!(estimator.effective_sample_size(), N / 2);
        assert_le!(estimator.effective_sample_size(), N);
    }

    #[rstest]
    #[case(100, 2)]
    #[case(100, 4)]
    #[case(100, 10)]
    #[case(100, 16)]
    #[case(100, 50)]
    #[case(100, 100)]
    #[case(100, 256)]
    #[case(10, 1000)]
    #[case(10, 10000)]
    fn test_correlated(#[case] n: u64, #[case] cluster_size: usize) {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut estimator = EffectiveSampleSizeEstimator::default();
        for _ in 0..n {
            let v = rng.gen();
            for _ in 0..cluster_size {
                estimator.record(v);
            }
        }
        assert_gt!(estimator.effective_sample_size(), n / 2);
        assert_le!(estimator.effective_sample_size(), n * 2);
    }

    #[test]
    fn test_merge_variances() {
        const COUNT: usize = 1000;
        let mut rng = SmallRng::seed_from_u64(1);
        let data: Vec<_> = (0..COUNT)
            .map(|i| 0.001 * i as f64 + rng.gen::<f64>())
            .collect();
        let mut est = VarianceEstimator::default();
        data.iter().for_each(|x| est.record(*x));

        let (sub1, sub2) = data.split_at(COUNT / 3);
        let mut sub_est1 = VarianceEstimator::default();
        let mut sub_est2 = VarianceEstimator::default();
        sub1.iter().for_each(|x| sub_est1.record(*x));
        sub2.iter().for_each(|x| sub_est2.record(*x));

        let mut est2 = VarianceEstimator::default();
        est2.add(&sub_est1);
        est2.add(&sub_est2);

        assert_approx_eq!(est.value(), est2.value(), 0.00001);
    }

    #[test]
    fn test_merge_ess() {
        const COUNT: usize = 10000;
        let mut rng = SmallRng::seed_from_u64(1);
        let mut data = Vec::new();
        data.extend((0..COUNT / 2).map(|_| rng.gen::<f64>()));
        data.extend((0..COUNT / 2).map(|_| rng.gen::<f64>() + 0.2));

        let mut est = EffectiveSampleSizeEstimator::default();
        data.iter().for_each(|x| est.record(*x));

        let (sub1, sub2) = data.split_at(COUNT / 3);
        let mut sub_est1 = EffectiveSampleSizeEstimator::default();
        let mut sub_est2 = EffectiveSampleSizeEstimator::default();
        sub1.iter().for_each(|x| sub_est1.record(*x));
        sub2.iter().for_each(|x| sub_est2.record(*x));

        let mut est2 = EffectiveSampleSizeEstimator::default();
        est2.add(&sub_est1);
        est2.add(&sub_est2);

        assert_approx_eq!(
            est.effective_sample_size() as f64,
            est2.effective_sample_size() as f64,
            est.effective_sample_size() as f64 * 0.1
        );
    }
}

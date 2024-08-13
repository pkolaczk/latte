use crate::stats::Mean;
use more_asserts::assert_le;
use rand_distr::num_traits::Pow;

/// Estimates the mean and effective size of the sample, by taking account for
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
pub struct TimeSeriesStats {
    n: u64,
    levels: Vec<Level>,
}

#[derive(Clone, Debug)]
struct Level {
    level: usize,
    buf: Vec<(f64, f64)>,
    stats: Stats,
}

/// Estimates the effective sample size by using batch means method.
impl TimeSeriesStats {
    /// Adds a single data point
    pub fn record(&mut self, x: f64, weight: f64) {
        self.n += 1;
        self.insert(x, weight, 0);
    }

    /// Merges another estimator data into this one
    pub fn add(&mut self, other: &TimeSeriesStats) {
        self.n += other.n;
        for level in &other.levels {
            self.add_level(level);
        }
    }

    pub fn clear(&mut self) {
        self.n = 0;
        self.levels.clear();
    }

    fn insert(&mut self, x: f64, weight: f64, level: usize) {
        if self.levels.len() == level {
            self.levels.push(Level::new(level));
        }
        if let Some((x, w)) = self.levels[level].record(x, weight) {
            self.insert(x, w, level + 1);
        }
    }

    fn add_level(&mut self, level: &Level) {
        if self.levels.len() == level.level {
            self.levels.push(level.clone());
        } else if let Some((x, w)) = self.levels[level.level].add(level) {
            self.insert(x, w, level.level + 1);
        }
    }

    pub fn mean(&self) -> Mean {
        let n = self.effective_sample_size();
        Mean {
            n,
            value: if n == 0 {
                f64::NAN
            } else {
                self.levels[0].stats.mean()
            },
            std_err: if self.n <= 1 {
                None
            } else {
                Some(self.levels[0].stats.variance().sqrt() / (n as f64).sqrt())
            },
        }
    }

    pub fn effective_sample_size(&self) -> u64 {
        // variances and means must be defined and level-0 must exist
        if self.n < 2 {
            return self.n;
        }

        assert!(!self.levels.is_empty());

        // Autocorrelation time can be estimated as:
        // size_of_the_batch * variance_of_the_batch_mean / variance_of_the_whole_sample
        // We can technically compute it from any level but there are some constraints:
        // - the batch size must be greater than the autocorrelation time
        // - the number of batches should be also large enough for the variance
        //   of the mean be accurate
        let sample_variance = self.levels[0].stats.variance();
        assert!(!sample_variance.is_nan());

        let autocorrelation_time = self
            .levels
            .iter()
            .map(|l| {
                (
                    l.batch_len(),
                    l.batch_len() as f64 * l.stats.variance() / sample_variance,
                )
            })
            .take_while(|(batch_len, time)| *time > 0.2 * *batch_len as f64)
            .map(|(_, time)| time)
            .fold(1.0, f64::max);

        (self.n as f64 / autocorrelation_time) as u64
    }
}

impl Level {
    fn new(level: usize) -> Self {
        Level {
            level,
            buf: Vec::with_capacity(2),
            stats: Default::default(),
        }
    }

    fn batch_len(&self) -> usize {
        1 << self.level
    }

    fn record(&mut self, value: f64, weight: f64) -> Option<(f64, f64)> {
        self.stats.record(value, weight);
        self.buf.push((value, weight));
        self.merge()
    }

    fn add(&mut self, other: &Level) -> Option<(f64, f64)> {
        assert_eq!(self.level, other.level);
        self.stats.add(&other.stats);
        // If there was more than 1 item recorded by the other level, then we must
        // drop our queued item, because it is not a neighbour of the other item
        if other.stats.n > 1 {
            self.buf.clear();
        }
        self.buf.extend(&other.buf);
        assert_le!(self.buf.len(), 2);
        self.merge()
    }

    fn merge(&mut self) -> Option<(f64, f64)> {
        if self.buf.len() == 2 {
            let (x1, w1) = self.buf[0];
            let (x2, w2) = self.buf[1];
            let merged_w = w1 + w2;
            let merged_x = (x1 * w1 + x2 * w2) / merged_w;
            self.buf.clear();
            Some((merged_x, merged_w))
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Stats {
    mean: f64,
    var: f64,
    total_weight: f64,
    n: u64,
}

/// Incrementally estimates basic statistics such as mean and variance over a weighted set of data.
/// Uses Welford's online algorithm.
impl Stats {
    pub fn record(&mut self, x: f64, weight: f64) {
        assert!(weight > 0.0, "weight must be greater than 0.0");
        self.n += 1;
        self.total_weight += weight;
        let delta1 = x - self.mean;
        self.mean += weight * delta1 / self.total_weight;
        let delta2 = x - self.mean;
        self.var += weight * delta1 * delta2;
    }

    pub fn add(&mut self, other: &Stats) {
        let w1 = self.total_weight;
        let w2 = other.total_weight;
        let m1 = self.mean;
        let m2 = other.mean;
        let new_mean = (m1 * w1 + m2 * w2) / (w1 + w2);

        self.n += other.n;
        self.mean = new_mean;
        self.var = self.var + other.var + (m1 - new_mean).pow(2) * w1 + (m2 - new_mean).pow(2) * w2;
        self.total_weight = w1 + w2;
    }

    pub fn mean(&self) -> f64 {
        if self.total_weight == 0.0 {
            f64::NAN
        } else {
            self.mean
        }
    }

    pub fn variance(&self) -> f64 {
        if self.n <= 1 {
            f64::NAN
        } else {
            let n = self.n as f64;
            self.var / self.total_weight * n / (n - 1.0)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::stats::timeseries::{Stats, TimeSeriesStats};
    use assert_approx_eq::assert_approx_eq;
    use more_asserts::{assert_gt, assert_le};
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;

    #[test]
    fn test_random() {
        let mut rng = SmallRng::seed_from_u64(4);
        let mut estimator = TimeSeriesStats::default();
        const N: u64 = 1000;
        for _ in 0..N {
            estimator.record(rng.gen(), 1.0);
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
        let mut estimator = TimeSeriesStats::default();
        for _ in 0..n {
            let v = rng.gen();
            for _ in 0..cluster_size {
                estimator.record(v, 1.0);
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
        let mut est = Stats::default();
        data.iter().for_each(|x| est.record(*x, 1.0));

        let (sub1, sub2) = data.split_at(COUNT / 3);
        let mut sub_est1 = Stats::default();
        let mut sub_est2 = Stats::default();
        sub1.iter().for_each(|x| sub_est1.record(*x, 1.0));
        sub2.iter().for_each(|x| sub_est2.record(*x, 1.0));

        let mut est2 = Stats::default();
        est2.add(&sub_est1);
        est2.add(&sub_est2);

        assert_approx_eq!(est.variance(), est2.variance(), 0.00001);
    }

    #[test]
    fn test_merge_ess() {
        const COUNT: usize = 10000;
        let mut rng = SmallRng::seed_from_u64(1);
        let mut data = Vec::new();
        data.extend((0..COUNT / 2).map(|_| rng.gen::<f64>()));
        data.extend((0..COUNT / 2).map(|_| rng.gen::<f64>() + 0.2));

        let mut est = TimeSeriesStats::default();
        data.iter().for_each(|x| est.record(*x, 1.0));

        let (sub1, sub2) = data.split_at(COUNT / 3);
        let mut sub_est1 = TimeSeriesStats::default();
        let mut sub_est2 = TimeSeriesStats::default();
        sub1.iter().for_each(|x| sub_est1.record(*x, 1.0));
        sub2.iter().for_each(|x| sub_est2.record(*x, 1.0));

        let mut est2 = TimeSeriesStats::default();
        est2.add(&sub_est1);
        est2.add(&sub_est2);

        assert_approx_eq!(
            est.effective_sample_size() as f64,
            est2.effective_sample_size() as f64,
            est.effective_sample_size() as f64 * 0.1
        );
    }
}

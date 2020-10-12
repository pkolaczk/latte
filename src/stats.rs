use std::cmp::min;
use std::f64::consts;

use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use statrs::distribution::{StudentsT, Univariate};
use strum::EnumCount;
use strum::IntoEnumIterator;
use strum_macros::{EnumCount as EnumCountM, EnumIter};
use tokio::time::{Duration, Instant};

/// Controls the maximum order of autocovariance taken into
/// account when estimating the long run mean error. Higher values make the estimator
/// capture more autocorrelation from the signal, but also make the results
/// more random. Lower values increase the bias (underestimation) of error, but offer smoother
/// results for small N and better performance for large N.
/// The value has been established empirically.
/// Probably anything between 0.2 and 0.8 is good enough.
/// Valid range is 0.0 to 1.0.
const BANDWIDTH_COEFF: f64 = 0.66;

/// Arithmetic mean of values in the vector
pub fn mean(v: &[f32]) -> f64 {
    v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64
}

/// Estimates the variance of the mean of a time-series.
/// Takes into account the fact that the observations can be dependent on each other
/// (i.e. there is a non-zero amount of auto-correlation in the signal).
///
/// Contrary to the classic variance estimator, the order of the
/// data points does matter here. If the observations are totally independent from each other,
/// the expected return value of this function is close to the expected sample variance.
pub fn long_run_variance(mean: f64, v: &[f32]) -> f64 {
    if v.len() <= 1 {
        return f64::NAN;
    }
    let len = v.len() as f64;

    // Compute the variance:
    let mut var = 0.0;
    for x in v.iter() {
        let diff = *x as f64 - mean;
        var += diff * diff;
    }
    var /= len;

    // Compute a sum of autocovariances of orders 1 to (cutoff - 1).
    // Cutoff (bandwidth) and diminishing weights are needed to reduce random error
    // introduced by higher order autocovariance estimates.
    let bandwidth = len.powf(BANDWIDTH_COEFF);
    let max_lag = min(v.len(), bandwidth.ceil() as usize);
    let mut cov = 0.0;
    for lag in 1..max_lag {
        let rel_lag = lag as f64 / bandwidth;
        let weight = 0.5 * (1.0 + (consts::PI * rel_lag).cos());
        //let weight = 1.0 - rel_lag;
        for i in lag..v.len() {
            let diff_1 = v[i] as f64 - mean;
            let diff_2 = v[i - lag] as f64 - mean;
            cov += 2.0 * diff_1 * diff_2 * weight;
        }
    }
    cov /= len;

    // It is possible that we end up with a negative sum of autocovariances here.
    // But we don't want that because we're trying to estimate
    // the worst-case error and for small N this situation is likely a random coincidence.
    // Additionally, `var + cov` must be at least 0.0.
    cov = cov.max(0.0);

    // Correct bias for small n:
    let inflation = 1.0 + cov / (var + f64::MIN_POSITIVE);
    let bias_correction = (inflation / len).exp();
    bias_correction * (var + cov)
}

/// Estimates the error of the mean of a time-series.
/// See `long_run_variance`.
pub fn long_run_err(mean: f64, v: &[f32]) -> f64 {
    (long_run_variance(mean, &v) / v.len() as f64).sqrt()
}

/// Holds a mean and its error together.
/// Makes it more convenient to compare means and it also reduces the number
/// of fields, because we don't have to keep the values and the errors in separate fields.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Mean {
    pub n: u64,
    pub value: f64,
    pub std_err: f64,
}

impl From<&[f32]> for Mean {
    fn from(v: &[f32]) -> Self {
        let m = mean(&v);
        Mean {
            n: v.len() as u64,
            value: m,
            std_err: long_run_err(m, &v),
        }
    }
}

/// Returns the probability that the difference between two means is due to a chance
/// See https://en.wikipedia.org/wiki/Student%27s_t-test
///
/// Assumes data are i.i.d and distributed normally, but it can be used
/// for autocorrelated data as well, if the errors are properly corrected for autocorrelation
/// using Wilk's method. This is what `Mean` struct is doing automatically
/// when constructed from a vector.
pub fn t_test(mean1: &Mean, mean2: &Mean) -> f64 {
    let n1 = mean1.n as f64;
    let n2 = mean2.n as f64;
    let e1 = mean1.std_err;
    let e2 = mean2.std_err;
    let m1 = mean1.value;
    let m2 = mean2.value;
    let e1_sq = e1 * e1;
    let e2_sq = e2 * e2;
    let se_sq = e1_sq + e2_sq;
    let se = se_sq.sqrt();
    let t = (m1 - m2) / se;
    let freedom = se_sq * se_sq / (e1_sq * e1_sq / (n1 - 1.0) + e2_sq * e2_sq / (n2 - 1.0));
    let distrib = StudentsT::new(0.0, 1.0, freedom).unwrap();
    2.0 * (1.0 - distrib.cdf(t.abs()))
}

#[derive(Debug)]
pub struct QueryStats {
    pub duration: Duration,
    pub row_count: u64,
    pub partition_count: u64,
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, EnumIter, EnumCountM)]
pub enum Percentile {
    Min = 0,
    P1,
    P2,
    P5,
    P10,
    P25,
    P50,
    P75,
    P90,
    P95,
    P98,
    P99,
    P99_9,
    P99_99,
    Max,
}

impl Percentile {
    pub fn value(&self) -> f64 {
        match self {
            Percentile::Min => 0.0,
            Percentile::P1 => 1.0,
            Percentile::P2 => 2.0,
            Percentile::P5 => 5.0,
            Percentile::P10 => 10.0,
            Percentile::P25 => 25.0,
            Percentile::P50 => 50.0,
            Percentile::P75 => 75.0,
            Percentile::P90 => 90.0,
            Percentile::P95 => 95.0,
            Percentile::P98 => 98.0,
            Percentile::P99 => 99.0,
            Percentile::P99_9 => 99.9,
            Percentile::P99_99 => 99.99,
            Percentile::Max => 100.0,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Percentile::Min => "  Min",
            Percentile::P1 => "    1",
            Percentile::P2 => "    2",
            Percentile::P5 => "    5",
            Percentile::P10 => "   10",
            Percentile::P25 => "   25",
            Percentile::P50 => "   50",
            Percentile::P75 => "   75",
            Percentile::P90 => "   90",
            Percentile::P95 => "   95",
            Percentile::P98 => "   98",
            Percentile::P99 => "   99",
            Percentile::P99_9 => " 99.9",
            Percentile::P99_99 => "99.99",
            Percentile::Max => "  Max",
        }
    }
}

/// Records basic statistics for a sample of requests
#[derive(Serialize, Deserialize)]
pub struct Sample {
    pub count: u64,
    pub time_s: f32,
    pub throughput: f32,
    pub mean_resp_time: f32,
    pub resp_time_percentiles: [f32; Percentile::COUNT],
}

struct Log {
    start: Instant,
    last_sample_time: Instant,
    samples: Vec<Sample>,
    curr_histogram: Histogram<u64>,
}

impl Log {
    fn new(start_time: Instant) -> Log {
        Log {
            start: start_time,
            last_sample_time: start_time,
            samples: Vec::new(),
            curr_histogram: Histogram::new(3).unwrap(),
        }
    }

    fn record(&mut self, duration: Duration) {
        self.curr_histogram
            .record(duration.as_micros() as u64)
            .unwrap();
    }

    fn next(&mut self, time: Instant) -> &Sample {
        let histogram = &self.curr_histogram;
        let mut percentiles = [0.0; Percentile::COUNT];
        for (i, p) in Percentile::iter().enumerate() {
            percentiles[i] = histogram.value_at_percentile(p.value()) as f32 / 1000.0;
        }
        let result = Sample {
            count: histogram.len(),
            time_s: (time - self.start).as_secs_f32(),
            throughput: 1000000.0 * histogram.len() as f32
                / (time - self.last_sample_time).as_micros() as f32,
            mean_resp_time: histogram.mean() as f32 / 1000.0,
            resp_time_percentiles: percentiles,
        };
        self.curr_histogram.clear();
        self.last_sample_time = time;
        self.samples.push(result);
        self.samples.last().unwrap()
    }

    fn throughput(&self) -> Mean {
        let t: Vec<f32> = self.samples.iter().map(|s| s.throughput).collect();
        Mean::from(t.as_slice())
    }

    fn throughput_histogram(&self) -> Histogram<u64> {
        let mut histogram = Histogram::new(5).unwrap();
        for s in &self.samples {
            histogram.record(s.throughput as u64).unwrap();
        }
        histogram
    }

    fn throughput_percentiles(&self) -> [f64; Percentile::COUNT] {
        let histogram = self.throughput_histogram();
        let mut result = [0.0; Percentile::COUNT];
        for p in Percentile::iter() {
            result[p as usize] = histogram.value_at_percentile(p.value()) as f64;
        }
        result
    }

    fn resp_time(&self) -> Mean {
        let t: Vec<f32> = self.samples.iter().map(|s| s.mean_resp_time).collect();
        Mean::from(t.as_slice())
    }

    fn resp_time_percentile(&self, p: Percentile) -> Mean {
        let t: Vec<f32> = self
            .samples
            .iter()
            .map(|s| s.resp_time_percentiles[p as usize])
            .collect();
        Mean::from(t.as_slice())
    }
}

#[derive(Serialize, Deserialize)]
pub struct RespTimeCount {
    pub percentile: f64,
    pub resp_time_ms: f64,
    pub count: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BenchmarkStats {
    pub elapsed_time_s: f64,
    pub cpu_time_s: f64,
    pub cpu_util: f64,
    pub completed_requests: u64,
    pub completed_ratio: f64,
    pub errors: u64,
    pub errors_ratio: f64,
    pub rows: u64,
    pub partitions: u64,
    pub throughput: Mean,
    pub throughput_ratio: Option<f64>,
    pub throughput_percentiles: Vec<f64>,
    pub resp_time_ms: Mean,
    pub resp_time_percentiles: Vec<Mean>,
    pub resp_time_distribution: Vec<RespTimeCount>,
    pub parallelism: f64,
    pub parallelism_ratio: f64,
    pub samples: Vec<Sample>,
}

pub struct BenchmarkCmp<'a> {
    pub v1: &'a BenchmarkStats,
    pub v2: Option<&'a BenchmarkStats>,
}

/// Significance level denoting strength of hypothesis
#[derive(Clone, Copy)]
pub struct Significance(pub f64);

impl BenchmarkCmp<'_> {
    /// Compares samples collected in both runs for statistically significant difference.
    /// `f` a function applied to each sample
    fn cmp<F>(&self, f: F) -> Option<Significance>
    where
        F: Fn(&BenchmarkStats) -> Mean + Copy,
    {
        self.v2.map(|v2| {
            let m1 = f(self.v1);
            let m2 = f(v2);
            Significance(t_test(&m1, &m2))
        })
    }

    /// Checks if throughput means of two benchmark runs are significantly different.
    /// Returns None if the second benchmark is unset.
    pub fn cmp_throughput(&self) -> Option<Significance> {
        self.cmp(|s| s.throughput)
    }

    // Checks if mean response time of two benchmark runs are significantly different.
    // Returns None if the second benchmark is unset.
    pub fn cmp_mean_resp_time(&self) -> Option<Significance> {
        self.cmp(|s| s.resp_time_ms)
    }

    // Checks corresponding response time percentiles of two benchmark runs
    // are statistically different. Returns None if the second benchmark is unset.
    pub fn cmp_resp_time_percentile(&self, p: Percentile) -> Option<Significance> {
        self.cmp(|s| s.resp_time_percentiles[p as usize])
    }
}

pub struct Recorder {
    pub start_time: Instant,
    pub end_time: Instant,
    rate_limit: Option<f64>,
    parallelism_limit: usize,
    resp_times: Histogram<u64>,
    resp_time_sum: f64,
    log: Log,
    completed: u64,
    errors: u64,
    rows: u64,
    partitions: u64,
    start_cpu_time: ProcessTime,
    end_cpu_time: ProcessTime,
    queue_len_sum: u64,
}

impl Recorder {
    pub fn start(rate_limit: Option<f64>, parallelism_limit: usize) -> Recorder {
        let start_time = Instant::now();
        Recorder {
            resp_times: Histogram::<u64>::new(4).unwrap(),
            log: Log::new(start_time),
            rate_limit,
            parallelism_limit,
            start_time,
            end_time: start_time,
            completed: 0,
            errors: 0,
            rows: 0,
            partitions: 0,
            start_cpu_time: ProcessTime::now(),
            end_cpu_time: ProcessTime::now(),
            queue_len_sum: 0,
            resp_time_sum: 0.0,
        }
    }

    pub fn record<E>(&mut self, item: Result<QueryStats, E>) {
        match item {
            Ok(s) => {
                self.completed += 1;
                self.rows += s.row_count;
                self.partitions += s.partition_count;
                self.log.record(s.duration);
                self.resp_time_sum += s.duration.as_micros() as f64 / 1000.0;
                self.resp_times
                    .record(s.duration.as_micros() as u64)
                    .unwrap();
            }
            Err(_) => {
                self.errors += 1;
            }
        };
    }

    pub fn sample(&mut self, time: Instant) -> &Sample {
        self.log.next(time)
    }

    pub fn last_sample_time(&self) -> Instant {
        self.log.last_sample_time
    }

    pub fn enqueued(&mut self, queue_length: usize) {
        self.queue_len_sum += queue_length as u64;
    }

    pub fn finish(mut self) -> BenchmarkStats {
        self.end_time = Instant::now();
        self.end_cpu_time = ProcessTime::now();
        if self.log.samples.is_empty() {
            self.sample(self.end_time);
        }
        self.stats()
    }

    /// Computes the final statistics based on collected data
    /// and turn them into report that can be serialized
    pub fn stats(self) -> BenchmarkStats {
        let elapsed_time_s = (self.end_time - self.start_time).as_secs_f64();
        let cpu_time_s = self
            .end_cpu_time
            .duration_since(self.start_cpu_time)
            .as_secs_f64();
        let cpu_util = 100.0 * cpu_time_s / elapsed_time_s / num_cpus::get() as f64;
        let count = self.completed + self.errors;

        let throughput = self.log.throughput();
        let parallelism = self.queue_len_sum as f64 / count as f64;
        let parallelism_ratio = 100.0 * parallelism / self.parallelism_limit as f64;

        let resp_time_percentiles: Vec<Mean> = Percentile::iter()
            .map(|p| self.log.resp_time_percentile(p))
            .collect();

        let mut resp_time_distribution = Vec::new();
        for x in self.resp_times.iter_log(self.resp_times.min(), 1.25) {
            resp_time_distribution.push(RespTimeCount {
                percentile: x.percentile(),
                resp_time_ms: x.value_iterated_to() as f64 / 1000.0,
                count: x.count_since_last_iteration(),
            });
        }

        BenchmarkStats {
            elapsed_time_s,
            cpu_time_s,
            cpu_util,
            completed_requests: self.completed,
            completed_ratio: 100.0 * self.completed as f64 / count as f64,
            errors: self.errors,
            errors_ratio: 100.0 * self.errors as f64 / count as f64,
            rows: self.rows,
            partitions: self.partitions,
            throughput,
            throughput_ratio: self.rate_limit.map(|r| 100.0 * throughput.value / r),
            throughput_percentiles: Vec::from(self.log.throughput_percentiles()),
            resp_time_ms: self.log.resp_time(),
            resp_time_percentiles,
            resp_time_distribution,
            parallelism,
            parallelism_ratio,
            samples: self.log.samples,
        }
    }
}

#[cfg(test)]
mod test {
    use hdrhistogram::Histogram;
    use rand::distributions::Distribution;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use statrs::distribution::{Normal, Poisson};
    use statrs::statistics::Statistics;

    use crate::stats::{t_test, Mean};

    /// Returns a random sample of size `len`.
    /// All data points i.i.d with N(`mean`, `std_dev`).
    fn random_vector(seed: usize, len: usize, mean: f64, std_dev: f64) -> Vec<f32> {
        let mut rng = StdRng::seed_from_u64(seed as u64);
        let distrib = Normal::new(mean, std_dev).unwrap();
        (0..len)
            .into_iter()
            .map(|_| distrib.sample(&mut rng) as f32)
            .collect()
    }

    fn random_vector_distr(seed: usize, len: usize, distrib: &impl Distribution<f64>) -> Vec<f32> {
        let mut rng = StdRng::seed_from_u64(seed as u64);
        (0..len)
            .into_iter()
            .map(|_| distrib.sample(&mut rng) as f32)
            .collect()
    }

    /// Introduces a strong dependency between the observations,
    /// making it an AR(1) process
    fn make_autocorrelated(v: &mut Vec<f32>) {
        for i in 1..v.len() {
            v[i] = 0.01 * v[i] + 0.99 * v[i - 1];
        }
    }

    /// Traditional standard error assuming i.i.d variables
    fn reference_err(v: &Vec<f32>) -> f64 {
        v.iter().map(|x| *x as f64).std_dev() / (v.len() as f64).sqrt()
    }

    #[test]
    fn mean_err_no_auto_correlation() {
        let run_len = 1000;
        let mean = 1.0;
        let std_dev = 1.0;
        for i in 0..10 {
            let v = random_vector(i, run_len, mean, std_dev);
            let err = super::long_run_err(mean, &v);
            let ref_err = reference_err(&v);
            assert!(err > 0.99 * ref_err);
            assert!(err < 1.33 * ref_err);
        }
    }

    #[test]
    fn mean_err_with_auto_correlation() {
        let run_len = 1000;
        let mean = 1.0;
        let std_dev = 1.0;
        for i in 0..10 {
            let mut v = random_vector(i, run_len, mean, std_dev);
            make_autocorrelated(&mut v);
            let mean_err = super::long_run_err(mean, &v);
            let ref_err = reference_err(&v);
            assert!(mean_err > 6.0 * ref_err);
        }
    }

    #[test]
    fn t_test_same() {
        let mean1 = Mean {
            n: 100,
            value: 1.0,
            std_err: 0.1,
        };
        let mean2 = Mean {
            n: 100,
            value: 1.0,
            std_err: 0.2,
        };
        assert!(t_test(&mean1, &mean2) > 0.9999);
    }

    #[test]
    fn t_test_different() {
        let mean1 = Mean {
            n: 100,
            value: 1.0,
            std_err: 0.1,
        };
        let mean2 = Mean {
            n: 100,
            value: 1.3,
            std_err: 0.1,
        };
        assert!(t_test(&mean1, &mean2) < 0.05);
        assert!(t_test(&mean2, &mean1) < 0.05);

        let mean1 = Mean {
            n: 10000,
            value: 1.0,
            std_err: 0.0,
        };
        let mean2 = Mean {
            n: 10000,
            value: 1.329,
            std_err: 0.1,
        };
        assert!(t_test(&mean1, &mean2) < 0.0011);
        assert!(t_test(&mean2, &mean1) < 0.0011);
    }

    fn std_dev_of_percentile(len: usize, percentile: f64) -> f64 {
        let count = 1000;
        let mut results = Vec::new();
        //let distr = Uniform::new(0.0, 10000.0).unwrap();
        let distr = Poisson::new(10.0).unwrap();
        for i in 0..count {
            let v = random_vector_distr(i, len, &distr);
            let mut h = Histogram::<u64>::new(5).unwrap();
            for x in v.iter() {
                h.record((x.abs() * 100000.0) as u64).unwrap();
            }
            results.push(h.value_at_percentile(percentile) as f64);
        }
        println!("mean: {}", results.iter().mean());
        results.std_dev()
    }

    #[test]
    fn test_p99() {
        println!("Std dev 1 of p99: {}", std_dev_of_percentile(100, 50.00));
        println!("Std dev 2 of p99: {}", std_dev_of_percentile(10000, 50.00));
    }
}

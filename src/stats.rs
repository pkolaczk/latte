use std::cmp::{max, min};
use std::f64::consts;
use std::time::{Duration, Instant};

use crate::workload::WorkloadStats;
use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use statrs::distribution::{StudentsT, Univariate};
use strum::EnumCount;
use strum::IntoEnumIterator;
use strum_macros::{EnumCount as EnumCountM, EnumIter};

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

/// Returns the probability that the difference between two means is due to a chance.
/// Uses Welch's t-test allowing samples to have different variances.
/// See https://en.wikipedia.org/wiki/Welch%27s_t-test.
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
pub struct RequestStats {
    pub duration: Duration,
    pub error_count: u64,
    pub row_count: u64,
    pub partition_count: u64,
}

impl RequestStats {
    pub fn from_result<E>(s: Result<WorkloadStats, E>, duration: Duration) -> RequestStats {
        match s {
            Ok(s) => RequestStats {
                duration,
                error_count: 0,
                row_count: s.row_count,
                partition_count: s.partition_count,
            },
            Err(_) => RequestStats {
                duration,
                error_count: 1,
                row_count: 0,
                partition_count: 0,
            },
        }
    }
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

/// Stores information about sample of requests detailed enough that we
/// can compute all the required statistics later
pub struct Sample {
    pub start_time: Instant,
    pub end_time: Instant,
    pub partition_count: u64,
    pub row_count: u64,
    pub error_count: u64,
    pub histogram: Histogram<u64>,
}

impl Sample {
    pub fn new(start_time: Instant) -> Sample {
        Sample {
            start_time,
            end_time: start_time,
            partition_count: 0,
            row_count: 0,
            error_count: 0,
            histogram: Histogram::new(3).unwrap(),
        }
    }

    pub fn record(&mut self, stats: RequestStats) {
        self.error_count += stats.error_count;
        self.partition_count += stats.partition_count;
        self.row_count += stats.row_count;
        self.histogram
            .record(max(1, stats.duration.as_micros() as u64))
            .unwrap();
    }

    pub fn is_empty(&self) -> bool {
        self.histogram.is_empty()
    }

    pub fn finish(&mut self, end_time: Instant) {
        self.end_time = end_time;
    }
}

/// Records basic statistics for a sample (a group) of requests
#[derive(Serialize, Deserialize)]
pub struct SampleStats {
    pub time_s: f32,
    pub duration_s: f32,
    pub request_count: u64,
    pub partition_count: u64,
    pub row_count: u64,
    pub error_count: u64,
    pub throughput: f32,
    pub mean_resp_time_ms: f32,
    pub resp_time_percentiles: [f32; Percentile::COUNT],
}

impl SampleStats {
    pub fn aggregate(base_start_time: Instant, samples: &[Sample]) -> SampleStats {
        assert!(!samples.is_empty());
        let mut request_count = 0;
        let mut partition_count = 0;
        let mut row_count = 0;
        let mut error_count = 0;
        let mut duration_s = 0.0;
        let mut histogram = Histogram::new(3).unwrap();

        for s in samples {
            request_count += s.histogram.len() as u64;
            partition_count += s.partition_count;
            row_count += s.row_count;
            error_count += s.error_count;
            duration_s += (s.end_time - s.start_time).as_secs_f32() / samples.len() as f32;
            histogram.add(&s.histogram).unwrap();
        }

        let mut percentiles = [0.0; Percentile::COUNT];
        for (i, p) in Percentile::iter().enumerate() {
            percentiles[i] = histogram.value_at_percentile(p.value()) as f32 / 1000.0;
        }

        SampleStats {
            time_s: (samples[0].start_time - base_start_time).as_secs_f32(),
            duration_s,
            request_count,
            partition_count,
            row_count,
            error_count,
            throughput: request_count as f32 / duration_s,
            mean_resp_time_ms: histogram.mean() as f32 / 1000.0,
            resp_time_percentiles: percentiles,
        }
    }
}

/// Collects the samples and computes aggregate statistics
struct Log {
    samples: Vec<SampleStats>,
}

impl Log {
    fn new() -> Log {
        Log {
            samples: Vec::new(),
        }
    }

    fn append(&mut self, sample: SampleStats) -> &SampleStats {
        self.samples.push(sample);
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

    fn resp_time_ms(&self) -> Mean {
        let t: Vec<f32> = self.samples.iter().map(|s| s.mean_resp_time_ms).collect();
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

/// Stores the final statistics of the test run.
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
    pub samples: Vec<SampleStats>,
}

/// Stores the statistics of one or two test runs.
/// If the second run is given, enables comparisons between the runs.
pub struct BenchmarkCmp<'a> {
    pub v1: &'a BenchmarkStats,
    pub v2: Option<&'a BenchmarkStats>,
}

/// Significance level denoting strength of hypothesis.
/// The wrapped value denotes the probability of observing given outcome assuming
/// null-hypothesis is true (see: https://en.wikipedia.org/wiki/P-value).
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

/// Observes requests and computes their statistics such as mean throughput, mean response time,
/// throughput and response time distributions. Computes confidence intervals.
/// Can be also used to split the time-series into smaller sub-samples and to
/// compute statistics for each sub-sample separately.
pub struct Recorder {
    pub start_time: Instant,
    pub end_time: Instant,
    pub start_cpu_time: ProcessTime,
    pub end_cpu_time: ProcessTime,
    pub request_count: u64,
    pub error_count: u64,
    pub row_count: u64,
    pub partition_count: u64,
    pub resp_times: Histogram<u64>,
    pub queue_len_sum: u64,
    log: Log,
    rate_limit: Option<f64>,
    parallelism_limit: usize,
}

impl Recorder {
    /// Creates a new recorder.
    /// The `rate_limit` and `parallelism_limit` parameters are used only as the
    /// reference levels for relative throughput and relative parallelism.
    pub fn start(rate_limit: Option<f64>, parallelism_limit: usize) -> Recorder {
        let start_time = Instant::now();
        Recorder {
            start_time,
            end_time: start_time,
            start_cpu_time: ProcessTime::now(),
            end_cpu_time: ProcessTime::now(),
            log: Log::new(),
            rate_limit,
            parallelism_limit,
            request_count: 0,
            row_count: 0,
            partition_count: 0,
            error_count: 0,
            resp_times: Histogram::new(3).unwrap(),
            queue_len_sum: 0,
        }
    }

    /// Adds the statistics of the completed request to the already collected statistics.
    /// Called on completion of each sample.
    pub fn record(&mut self, samples: &[Sample]) -> &SampleStats {
        for s in samples.iter() {
            self.resp_times.add(&s.histogram).unwrap();
        }
        let stats = SampleStats::aggregate(self.start_time, samples);
        self.request_count += stats.request_count;
        self.row_count += stats.row_count;
        self.partition_count += stats.partition_count;
        self.error_count += stats.error_count;
        self.log.append(stats)
    }

    /// Stops the recording, computes the statistics and returns them as the new object.
    pub fn finish(mut self) -> BenchmarkStats {
        self.end_time = Instant::now();
        self.end_cpu_time = ProcessTime::now();
        self.stats()
    }

    /// Computes the final statistics based on collected data
    /// and turn them into report that can be serialized
    fn stats(self) -> BenchmarkStats {
        let elapsed_time_s = (self.end_time - self.start_time).as_secs_f64();
        let cpu_time_s = self
            .end_cpu_time
            .duration_since(self.start_cpu_time)
            .as_secs_f64();
        let cpu_util = 100.0 * cpu_time_s / elapsed_time_s / num_cpus::get() as f64;
        let count = self.request_count + self.error_count;

        let throughput = self.log.throughput();
        let parallelism = self.queue_len_sum as f64 / count as f64;
        let parallelism_ratio = 100.0 * parallelism / self.parallelism_limit as f64;

        let resp_time_percentiles: Vec<Mean> = Percentile::iter()
            .map(|p| self.log.resp_time_percentile(p))
            .collect();

        let mut resp_time_distribution = Vec::new();
        if !self.resp_times.is_empty() {
            for x in self.resp_times.iter_log(self.resp_times.min(), 1.25) {
                resp_time_distribution.push(RespTimeCount {
                    percentile: x.percentile(),
                    resp_time_ms: x.value_iterated_to() as f64 / 1000.0,
                    count: x.count_since_last_iteration(),
                });
            }
        }

        BenchmarkStats {
            elapsed_time_s,
            cpu_time_s,
            cpu_util,
            completed_requests: self.request_count,
            completed_ratio: 100.0 * self.request_count as f64 / count as f64,
            errors: self.error_count,
            errors_ratio: 100.0 * self.error_count as f64 / count as f64,
            rows: self.row_count,
            partitions: self.partition_count,
            throughput,
            throughput_ratio: self.rate_limit.map(|r| 100.0 * throughput.value / r),
            throughput_percentiles: Vec::from(self.log.throughput_percentiles()),
            resp_time_ms: self.log.resp_time_ms(),
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
    use rand::distributions::Distribution;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use statrs::distribution::Normal;
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
}

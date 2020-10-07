use std::cmp::min;
use std::f64::consts;

use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use rustats::hypothesis_testings::MannWhitneyU;
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, Instant};

use crate::stats::Significance::{Medium, Strong, Weak};

/// A standard error is multiplied by this factor to get the error margin.
/// For a normally distributed random variable,
/// this should give us 0.999 confidence the expected value is within the (result +- error) range.
const ERR_MARGIN: f64 = 3.29;

/// Controls the maximum order of autocovariance taken into
/// account when estimating the long run mean error. Higher values make the estimator
/// capture more autocorrelation from the signal, but also make the results
/// more random. Lower values increase the bias (underestimation) of error, but offer smoother
/// results for small N and better performance for large N.
/// The value has been established empirically.
/// Probably anything between 0.2 and 0.8 is good enough.
/// Valid range is 0.0 to 1.0, but you probably don't want to set it over 0.9.
const BANDWIDTH_COEFF: f64 = 0.66;

/// Estimates the expected standard error of the mean of a time-series.
/// Takes into account the fact that the observations can be dependent on each other
/// (i.e. there is a non-zero amount of auto-correlation in the signal).
///
/// Contrary to the classic standard error or standard deviation, the order of the
/// data points does matter here. If the observations are totally independent from each other,
/// the expected return value of this function is close to the expected standard error
/// of the sample.
pub fn long_run_mean_err(v: &[f32]) -> f64 {
    if v.len() <= 1 {
        return f64::NAN;
    }

    let len = v.len() as f64;
    let mean = v.iter().map(|x| *x as f64).sum::<f64>() / len;

    // Compute the variance:
    let mut var = 0.0;
    for x in v.iter() {
        let diff = *x as f64 - mean;
        var += diff * diff;
    }

    // Compute a sum of autocovariances of orders 1 to (cutoff - 1).
    // Cutoff (bandwidth) and diminishing weights are needed to reduce random error
    // introduced by higher order autocovariance estimates.
    let bandwidth = len.powf(BANDWIDTH_COEFF);
    let cutoff = min(v.len(), bandwidth.ceil() as usize);
    let mut cov = 0.0;
    for j in 1..cutoff {
        let x = j as f64 / bandwidth;
        let weight = 0.5 * (1.0 + (consts::PI * x).cos());
        for i in 0..(v.len() - j) {
            let diff_1 = v[i] as f64 - mean;
            let diff_2 = v[i + j] as f64 - mean;
            cov += 2.0 * diff_1 * diff_2 * weight;
        }
    }
    // It is possible that we end up with a negative sum of autocovariances here.
    // But we don't want that because we're trying to estimate
    // the worst-case error and for small N this situation is likely a random coincidence.
    // Additionally, `var + cov` must be at least 0.0.
    cov = cov.max(0.0);
    ((var + cov) / (len * (len - 1.0))).sqrt()
}

#[derive(Debug)]
pub struct QueryStats {
    pub duration: Duration,
    pub row_count: u64,
    pub partition_count: u64,
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
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

pub const PERCENTILES: [Percentile; 15] = [
    Percentile::Min,
    Percentile::P1,
    Percentile::P2,
    Percentile::P5,
    Percentile::P10,
    Percentile::P25,
    Percentile::P50,
    Percentile::P75,
    Percentile::P90,
    Percentile::P95,
    Percentile::P98,
    Percentile::P99,
    Percentile::P99_9,
    Percentile::P99_99,
    Percentile::Max,
];

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
    pub time_s: f32,
    pub throughput: f32,
    pub mean_resp_time: f32,
    pub resp_time_percentiles: [f32; PERCENTILES.len()],
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
        let mut percentiles: [f32; PERCENTILES.len()] = [0.0; PERCENTILES.len()];
        for (i, p) in PERCENTILES.iter().enumerate() {
            percentiles[i] = histogram.value_at_percentile(p.value()) as f32 / 1000.0;
        }
        let result = Sample {
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

    fn throughput_err(&self) -> f64 {
        let t: Vec<f32> = self.samples.iter().map(|s| s.throughput).collect();
        long_run_mean_err(t.as_slice()) as f64 * ERR_MARGIN
    }

    fn throughput_histogram(&self) -> Histogram<u64> {
        let mut histogram = Histogram::new(5).unwrap();
        for s in &self.samples {
            histogram.record(s.throughput as u64).unwrap();
        }
        histogram
    }

    fn throughput_percentiles(&self) -> [f32; PERCENTILES.len()] {
        let histogram = self.throughput_histogram();
        let mut result = [0.0; PERCENTILES.len()];
        for p in PERCENTILES.iter() {
            result[*p as usize] = histogram.value_at_percentile(p.value()) as f32;
        }
        result
    }

    fn mean_resp_time_err(&self) -> f64 {
        let t: Vec<f32> = self.samples.iter().map(|s| s.mean_resp_time).collect();
        long_run_mean_err(t.as_slice()) as f64 * ERR_MARGIN
    }

    fn resp_time_percentile_err(&self, p: Percentile) -> f64 {
        let t: Vec<f32> = self
            .samples
            .iter()
            .map(|s| s.resp_time_percentiles[p as usize])
            .collect();
        long_run_mean_err(t.as_slice()) as f64 * ERR_MARGIN
    }
}

#[derive(Serialize, Deserialize)]
pub struct RespTimeCount {
    pub percentile: f32,
    pub resp_time_ms: f32,
    pub count: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BenchmarkStats {
    pub elapsed_time_s: f32,
    pub cpu_time_s: f32,
    pub cpu_util: f32,
    pub completed_requests: u64,
    pub completed_ratio: f32,
    pub errors: u64,
    pub errors_ratio: f32,
    pub rows: u64,
    pub partitions: u64,
    pub throughput: f32,
    pub throughput_ratio: Option<f32>,
    pub throughput_err: f32,
    pub throughput_percentiles: [f32; PERCENTILES.len()],
    pub resp_time_ms: f32,
    pub resp_time_err: f32,
    pub resp_time_percentiles: [f32; PERCENTILES.len()],
    pub resp_time_percentiles_err: [f32; PERCENTILES.len()],
    pub resp_time_distribution: Vec<RespTimeCount>,
    pub parallelism: f32,
    pub parallelism_ratio: f32,
    pub samples: Vec<Sample>,
}

pub struct BenchmarkCmp<'a> {
    pub v1: &'a BenchmarkStats,
    pub v2: Option<&'a BenchmarkStats>,
}

/// Significance level denoting strength of hypothesis
#[derive(Clone, Copy)]
pub enum Significance {
    None,   // P >= 0.05
    Weak,   // P < 0.05
    Medium, // P < 0.001
    Strong, // P < 0.0001
}

impl Significance {
    pub fn p_level(&self) -> f64 {
        match self {
            Significance::None => f64::MAX,
            Significance::Weak => 0.01,
            Significance::Medium => 0.001,
            Significance::Strong => 0.0001,
        }
    }
}

impl BenchmarkCmp<'_> {
    /// Compares samples collected in both runs for statistically significant difference.
    /// `f` a function applied to each sample
    fn cmp<F, T>(&self, f: F) -> Option<Significance>
    where
        F: Fn(&Sample) -> T + Copy,
        T: PartialOrd,
    {
        self.v2.map(|v2| {
            let t1 = self.v1.samples.iter().map(f);
            let t2 = v2.samples.iter().map(f);
            let mw = MannWhitneyU::new(t1, t2);
            *[Strong, Medium, Weak]
                .iter()
                .find(|s| mw.test(s.p_level()))
                .unwrap_or(&Significance::None)
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
        self.cmp(|s| s.mean_resp_time)
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
    rate_limit: Option<f32>,
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
    pub fn start(rate_limit: Option<f32>, parallelism_limit: usize) -> Recorder {
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
        let elapsed_time_s = (self.end_time - self.start_time).as_secs_f32();
        let cpu_time_s = self
            .end_cpu_time
            .duration_since(self.start_cpu_time)
            .as_secs_f32();
        let cpu_util = 100.0 * cpu_time_s / elapsed_time_s / num_cpus::get() as f32;
        let count = self.completed + self.errors;

        let throughput = self.completed as f32 / elapsed_time_s;
        let parallelism = self.queue_len_sum as f32 / count as f32;
        let parallelism_ratio = 100.0 * parallelism / self.parallelism_limit as f32;

        let mut resp_time_percentiles = [0.0; PERCENTILES.len()];
        let mut resp_time_percentiles_err = [0.0; PERCENTILES.len()];
        for p in PERCENTILES.iter() {
            resp_time_percentiles[*p as usize] =
                self.resp_times.value_at_percentile(p.value()) as f32 / 1000.0;
            resp_time_percentiles_err[*p as usize] = self.log.resp_time_percentile_err(*p) as f32;
        }

        let mut resp_time_distribution = Vec::new();
        for x in self.resp_times.iter_log(self.resp_times.min(), 1.25) {
            resp_time_distribution.push(RespTimeCount {
                percentile: x.percentile() as f32,
                resp_time_ms: x.value_iterated_to() as f32 / 1000.0,
                count: x.count_since_last_iteration(),
            });
        }

        BenchmarkStats {
            elapsed_time_s,
            cpu_time_s,
            cpu_util,
            completed_requests: self.completed,
            completed_ratio: 100.0 * self.completed as f32 / count as f32,
            errors: self.errors,
            errors_ratio: 100.0 * self.errors as f32 / count as f32,
            rows: self.rows,
            partitions: self.partitions,
            throughput,
            throughput_err: self.log.throughput_err() as f32,
            throughput_ratio: self.rate_limit.map(|r| 100.0 * throughput / r),
            throughput_percentiles: self.log.throughput_percentiles(),
            resp_time_ms: (self.resp_time_sum / self.completed as f64) as f32,
            resp_time_err: self.log.mean_resp_time_err() as f32,
            resp_time_percentiles,
            resp_time_percentiles_err,
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

    fn random_vector(i: usize, count: usize, mean: f64, std_dev: f64) -> Vec<f32> {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let distrib = Normal::new(mean, std_dev).unwrap();
        let mut v = Vec::with_capacity(count);
        for _ in 0..count {
            v.push(distrib.sample(&mut rng) as f32)
        }
        v
    }

    fn make_autocorrelated(v: &mut Vec<f32>) {
        for i in 1..v.len() {
            v[i] = 0.01 * v[i] + 0.99 * v[i - 1];
        }
    }

    fn reference_mean_err(v: &Vec<f32>) -> f64 {
        v.iter().map(|x| *x as f64).std_dev() / (v.len() as f64).sqrt()
    }

    #[test]
    fn mean_err_no_auto_correlation() {
        let run_len = 100;
        let std_dev = 1.0;
        for i in 0..10 {
            let v = random_vector(i, run_len, 1.0, std_dev);
            let err = super::long_run_mean_err(&v);
            let ref_err = reference_mean_err(&v);
            assert!(err > 0.99 * ref_err);
            assert!(err < 1.5 * ref_err);
        }
    }

    #[test]
    fn mean_err_with_auto_correlation() {
        let run_len = 100;
        let std_dev = 1.0;
        for i in 0..10 {
            let mut v = random_vector(i, run_len, 1.0, std_dev);
            make_autocorrelated(&mut v);
            let mean_err = super::long_run_mean_err(&v);
            let ref_err = reference_mean_err(&v);
            assert!(mean_err > 3.0 * ref_err);
        }
    }
}

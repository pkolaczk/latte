use chrono::{DateTime, Local};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::Mul;
use std::time::{Instant, SystemTime};

use crate::latency::{LatencyDistribution, LatencyDistributionRecorder};
use crate::percentiles::Percentile;
use crate::throughput::ThroughputMeter;
use crate::timeseries::TimeSeriesStats;
use crate::workload::WorkloadStats;
use cpu_time::ProcessTime;
use serde::{Deserialize, Serialize};
use statrs::distribution::{ContinuousCDF, StudentsT};

/// Holds a mean and its error together.
/// Makes it more convenient to compare means, and it also reduces the number
/// of fields, because we don't have to keep the values and the errors in separate fields.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Mean {
    pub n: u64,
    pub value: f64,
    pub std_err: Option<f64>,
}

impl Mul<f64> for Mean {
    type Output = Mean;

    fn mul(self, rhs: f64) -> Self::Output {
        Mean {
            n: self.n,
            value: self.value * rhs,
            std_err: self.std_err.map(|e| e * rhs),
        }
    }
}

/// Returns the probability that the difference between two means is due to a chance.
/// Uses Welch's t-test allowing samples to have different variances.
/// See https://en.wikipedia.org/wiki/Welch%27s_t-test.
///
/// If any of the means is given without the error, or if the number of observations is too low,
/// returns 1.0.
///
/// Assumes data are i.i.d and distributed normally, but it can be used
/// for autocorrelated data as well, if the errors are properly corrected for autocorrelation
/// using Wilk's method. This is what `Mean` struct is doing automatically
/// when constructed from a vector.
pub fn t_test(mean1: &Mean, mean2: &Mean) -> f64 {
    if mean1.std_err.is_none() || mean2.std_err.is_none() {
        return 1.0;
    }
    let n1 = mean1.n as f64;
    let n2 = mean2.n as f64;
    let e1 = mean1.std_err.unwrap();
    let e2 = mean2.std_err.unwrap();
    let m1 = mean1.value;
    let m2 = mean2.value;
    let e1_sq = e1 * e1;
    let e2_sq = e2 * e2;
    let se_sq = e1_sq + e2_sq;
    let se = se_sq.sqrt();
    let t = (m1 - m2) / se;
    let freedom = se_sq * se_sq / (e1_sq * e1_sq / (n1 - 1.0) + e2_sq * e2_sq / (n2 - 1.0));
    if let Ok(distrib) = StudentsT::new(0.0, 1.0, freedom) {
        2.0 * (1.0 - distrib.cdf(t.abs()))
    } else {
        1.0
    }
}

/// Converts NaN to None.
fn not_nan(x: f64) -> Option<f64> {
    if x.is_nan() {
        None
    } else {
        Some(x)
    }
}

/// Converts NaN to None.
fn not_nan_f32(x: f32) -> Option<f32> {
    if x.is_nan() {
        None
    } else {
        Some(x)
    }
}

const MAX_KEPT_ERRORS: usize = 10;

/// Records basic statistics for a sample (a group) of requests
#[derive(Serialize, Deserialize, Debug)]
pub struct Sample {
    pub time_s: f32,
    pub duration_s: f32,
    pub cycle_count: u64,
    pub cycle_error_count: u64,
    pub request_count: u64,
    pub req_retry_count: u64,
    pub req_errors: HashSet<String>,
    pub req_error_count: u64,
    pub row_count: u64,
    pub mean_queue_len: f32,
    pub cycle_throughput: f32,
    pub req_throughput: f32,
    pub row_throughput: f32,

    pub cycle_latency: LatencyDistribution,
    pub cycle_latency_by_fn: HashMap<String, LatencyDistribution>,
    pub request_latency: LatencyDistribution,
}

impl Sample {
    pub fn new(base_start_time: Instant, stats: &[WorkloadStats]) -> Sample {
        assert!(!stats.is_empty());

        let mut cycle_count = 0;
        let mut cycle_error_count = 0;
        let mut request_count = 0;
        let mut req_retry_count = 0;
        let mut row_count = 0;
        let mut errors = HashSet::new();
        let mut req_error_count = 0;
        let mut mean_queue_len = 0.0;
        let mut duration_s = 0.0;

        let mut request_latency = LatencyDistributionRecorder::default();
        let mut cycle_latency = LatencyDistributionRecorder::default();
        let mut cycle_latency_per_fn = HashMap::<String, LatencyDistributionRecorder>::new();

        for s in stats {
            let ss = &s.session_stats;
            request_count += ss.req_count;
            row_count += ss.row_count;
            if errors.len() < MAX_KEPT_ERRORS {
                errors.extend(ss.req_errors.iter().cloned());
            }
            req_error_count += ss.req_error_count;
            req_retry_count += ss.req_retry_count;
            mean_queue_len += ss.mean_queue_length / stats.len() as f32;
            duration_s += (s.end_time - s.start_time).as_secs_f32() / stats.len() as f32;
            request_latency.add(&ss.resp_times_ns);

            for fs in &s.function_stats {
                cycle_count += fs.call_count;
                cycle_error_count = fs.error_count;
                cycle_latency.add(&fs.call_latency);
                cycle_latency_per_fn
                    .entry(fs.function.name.clone())
                    .or_default()
                    .add(&fs.call_latency);
            }
        }

        Sample {
            time_s: (stats[0].start_time - base_start_time).as_secs_f32(),
            duration_s,
            cycle_count,
            cycle_error_count,
            request_count,
            req_retry_count,
            req_errors: errors,
            req_error_count,
            row_count,
            mean_queue_len: not_nan_f32(mean_queue_len).unwrap_or(0.0),

            cycle_throughput: cycle_count as f32 / duration_s,
            req_throughput: request_count as f32 / duration_s,
            row_throughput: row_count as f32 / duration_s,

            cycle_latency: cycle_latency.distribution(),
            cycle_latency_by_fn: cycle_latency_per_fn
                .into_iter()
                .map(|(k, v)| (k, v.distribution()))
                .collect(),

            request_latency: request_latency.distribution(),
        }
    }
}

/// Stores the final statistics of the test run.
#[derive(Serialize, Deserialize, Debug)]
pub struct BenchmarkStats {
    pub start_time: DateTime<Local>,
    pub end_time: DateTime<Local>,
    pub elapsed_time_s: f64,
    pub cpu_time_s: f64,
    pub cpu_util: f64,
    pub cycle_count: u64,
    pub request_count: u64,
    pub requests_per_cycle: f64,
    pub request_retry_count: u64,
    pub request_retry_per_request: Option<f64>,
    pub errors: Vec<String>,
    pub error_count: u64,
    pub errors_ratio: Option<f64>,
    pub row_count: u64,
    pub row_count_per_req: Option<f64>,
    pub cycle_throughput: Mean,
    pub cycle_throughput_ratio: Option<f64>,
    pub req_throughput: Mean,
    pub row_throughput: Mean,
    pub cycle_latency: LatencyDistribution,
    pub cycle_latency_by_fn: HashMap<String, LatencyDistribution>,
    pub request_latency: Option<LatencyDistribution>,
    pub concurrency: Mean,
    pub concurrency_ratio: f64,
    pub log: Vec<Sample>,
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
        F: Fn(&BenchmarkStats) -> Option<Mean> + Copy,
    {
        self.v2.and_then(|v2| {
            let m1 = f(self.v1);
            let m2 = f(v2);
            m1.and_then(|m1| m2.map(|m2| Significance(t_test(&m1, &m2))))
        })
    }

    /// Checks if call throughput means of two benchmark runs are significantly different.
    /// Returns None if the second benchmark is unset.
    pub fn cmp_cycle_throughput(&self) -> Option<Significance> {
        self.cmp(|s| Some(s.cycle_throughput))
    }

    /// Checks if request throughput means of two benchmark runs are significantly different.
    /// Returns None if the second benchmark is unset.
    pub fn cmp_req_throughput(&self) -> Option<Significance> {
        self.cmp(|s| Some(s.req_throughput))
    }

    /// Checks if row throughput means of two benchmark runs are significantly different.
    /// Returns None if the second benchmark is unset.
    pub fn cmp_row_throughput(&self) -> Option<Significance> {
        self.cmp(|s| Some(s.row_throughput))
    }

    // Checks if mean response time of two benchmark runs are significantly different.
    // Returns None if the second benchmark is unset.
    pub fn cmp_mean_resp_time(&self) -> Option<Significance> {
        self.cmp(|s| s.request_latency.as_ref().map(|r| r.mean))
    }

    // Checks corresponding response time percentiles of two benchmark runs
    // are statistically different. Returns None if the second benchmark is unset.
    pub fn cmp_resp_time_percentile(&self, p: Percentile) -> Option<Significance> {
        self.cmp(|s| s.request_latency.as_ref().map(|r| r.percentiles.get(p)))
    }
}

/// Observes requests and computes their statistics such as mean throughput, mean response time,
/// throughput and response time distributions. Computes confidence intervals.
/// Can be also used to split the time-series into smaller sub-samples and to
/// compute statistics for each sub-sample separately.
pub struct Recorder {
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub start_instant: Instant,
    pub end_instant: Instant,
    pub start_cpu_time: ProcessTime,
    pub end_cpu_time: ProcessTime,
    pub cycle_count: u64,
    pub request_count: u64,
    pub request_retry_count: u64,
    pub request_error_count: u64,
    pub throughput_meter: ThroughputMeter,
    pub errors: HashSet<String>,
    pub cycle_error_count: u64,
    pub row_count: u64,
    pub cycle_latency: LatencyDistributionRecorder,
    pub cycle_latency_by_fn: HashMap<String, LatencyDistributionRecorder>,
    pub request_latency: LatencyDistributionRecorder,
    pub concurrency_meter: TimeSeriesStats,
    log: Vec<Sample>,
    rate_limit: Option<f64>,
    concurrency_limit: NonZeroUsize,
    keep_log: bool,
}

impl Recorder {
    /// Creates a new recorder.
    /// The `rate_limit` and `concurrency_limit` parameters are used only as the
    /// reference levels for relative throughput and relative parallelism.
    pub fn start(
        rate_limit: Option<f64>,
        concurrency_limit: NonZeroUsize,
        keep_log: bool,
    ) -> Recorder {
        let start_time = SystemTime::now();
        let start_instant = Instant::now();
        Recorder {
            start_time,
            end_time: start_time,
            start_instant,
            end_instant: start_instant,
            start_cpu_time: ProcessTime::now(),
            end_cpu_time: ProcessTime::now(),
            log: Vec::new(),
            rate_limit,
            concurrency_limit,
            cycle_count: 0,
            request_count: 0,
            request_retry_count: 0,
            request_error_count: 0,
            row_count: 0,
            errors: HashSet::new(),
            cycle_error_count: 0,
            cycle_latency: LatencyDistributionRecorder::default(),
            cycle_latency_by_fn: HashMap::new(),
            request_latency: LatencyDistributionRecorder::default(),
            throughput_meter: ThroughputMeter::default(),
            concurrency_meter: TimeSeriesStats::default(),
            keep_log,
        }
    }

    /// Adds the statistics of the completed request to the already collected statistics.
    /// Called on completion of each sample.
    pub fn record(&mut self, samples: &[WorkloadStats]) -> &Sample {
        assert!(!samples.is_empty());
        for s in samples.iter() {
            self.request_latency.add(&s.session_stats.resp_times_ns);

            for fs in &s.function_stats {
                self.cycle_latency.add(&fs.call_latency);
                self.cycle_latency_by_fn
                    .entry(fs.function.name.clone())
                    .or_default()
                    .add(&fs.call_latency);
            }
        }
        let sample = Sample::new(self.start_instant, samples);
        self.cycle_count += sample.cycle_count;
        self.cycle_error_count += sample.cycle_error_count;
        self.request_count += sample.request_count;
        self.request_retry_count += sample.req_retry_count;
        self.request_error_count += sample.req_error_count;
        self.row_count += sample.row_count;
        self.throughput_meter.record(sample.cycle_count);
        self.concurrency_meter
            .record(sample.mean_queue_len as f64, sample.duration_s as f64);
        if self.errors.len() < MAX_KEPT_ERRORS {
            self.errors.extend(sample.req_errors.iter().cloned());
        }
        if !self.keep_log {
            self.log.clear();
        }
        self.log.push(sample);
        self.log.last().unwrap()
    }

    /// Stops the recording, computes the statistics and returns them as the new object.
    pub fn finish(mut self) -> BenchmarkStats {
        self.end_time = SystemTime::now();
        self.end_instant = Instant::now();
        self.end_cpu_time = ProcessTime::now();

        let elapsed_time_s = (self.end_instant - self.start_instant).as_secs_f64();
        let cpu_time_s = self
            .end_cpu_time
            .duration_since(self.start_cpu_time)
            .as_secs_f64();
        let cpu_util = 100.0 * cpu_time_s / elapsed_time_s / num_cpus::get() as f64;

        let cycle_throughput = self.throughput_meter.throughput();
        let cycle_throughput_ratio = self.rate_limit.map(|r| 100.0 * cycle_throughput.value / r);
        let req_throughput =
            cycle_throughput * (self.request_count as f64 / self.cycle_count as f64);
        let row_throughput = cycle_throughput * (self.row_count as f64 / self.cycle_count as f64);
        let concurrency = self.concurrency_meter.mean();
        let concurrency_ratio = 100.0 * concurrency.value / self.concurrency_limit.get() as f64;

        if !self.keep_log {
            self.log.clear();
        }

        BenchmarkStats {
            start_time: self.start_time.into(),
            end_time: self.end_time.into(),
            elapsed_time_s,
            cpu_time_s,
            cpu_util,
            cycle_count: self.cycle_count,
            errors: self.errors.into_iter().collect(),
            error_count: self.cycle_error_count,
            errors_ratio: not_nan(100.0 * self.cycle_error_count as f64 / self.cycle_count as f64),
            request_count: self.request_count,
            request_retry_count: self.request_retry_count,
            request_retry_per_request: not_nan(
                self.request_retry_count as f64 / self.request_count as f64,
            ),
            requests_per_cycle: self.request_count as f64 / self.cycle_count as f64,
            row_count: self.row_count,
            row_count_per_req: not_nan(self.row_count as f64 / self.request_count as f64),
            cycle_throughput,
            cycle_throughput_ratio,
            req_throughput,
            row_throughput,
            cycle_latency: self.cycle_latency.distribution_with_errors(),
            cycle_latency_by_fn: self
                .cycle_latency_by_fn
                .into_iter()
                .map(|(k, v)| (k, v.distribution_with_errors()))
                .collect(),
            request_latency: if self.request_count > 0 {
                Some(self.request_latency.distribution_with_errors())
            } else {
                None
            },
            concurrency,
            concurrency_ratio,
            log: self.log,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::stats::{t_test, Mean};

    #[test]
    fn t_test_same() {
        let mean1 = Mean {
            n: 100,
            value: 1.0,
            std_err: Some(0.1),
        };
        let mean2 = Mean {
            n: 100,
            value: 1.0,
            std_err: Some(0.2),
        };
        assert!(t_test(&mean1, &mean2) > 0.9999);
    }

    #[test]
    fn t_test_different() {
        let mean1 = Mean {
            n: 100,
            value: 1.0,
            std_err: Some(0.1),
        };
        let mean2 = Mean {
            n: 100,
            value: 1.3,
            std_err: Some(0.1),
        };
        assert!(t_test(&mean1, &mean2) < 0.05);
        assert!(t_test(&mean2, &mean1) < 0.05);

        let mean1 = Mean {
            n: 10000,
            value: 1.0,
            std_err: Some(0.0),
        };
        let mean2 = Mean {
            n: 10000,
            value: 1.329,
            std_err: Some(0.1),
        };
        assert!(t_test(&mean1, &mean2) < 0.0011);
        assert!(t_test(&mean2, &mean1) < 0.0011);
    }
}

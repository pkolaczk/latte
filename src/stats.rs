use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use statrs::statistics::Statistics;
use tokio::time::{Duration, Instant};

const ERR_MARGIN: f64 = 3.29; // 0.999 confidence, 2-sided

#[derive(Debug)]
pub struct ActionStats {
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

impl Sample {
    pub fn print_log_header() {
        println!(
            "LOG =================================================================================================");
        println!(
            "                         ----------------------- Response times [ms]-------------------------"
        );
        println!(
            "Time [s]  Throughput          Min        25        50        75        90        99       Max"
        )
    }

    pub fn to_string(&self) -> String {
        format!(
            "{:8.3} {:11.0}    {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
            self.time_s,
            self.throughput,
            self.resp_time_percentiles[Percentile::Min as usize],
            self.resp_time_percentiles[Percentile::P25 as usize],
            self.resp_time_percentiles[Percentile::P50 as usize],
            self.resp_time_percentiles[Percentile::P75 as usize],
            self.resp_time_percentiles[Percentile::P90 as usize],
            self.resp_time_percentiles[Percentile::P99 as usize],
            self.resp_time_percentiles[Percentile::Max as usize]
        )
    }
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
            curr_histogram: Histogram::new(4).unwrap(),
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
            throughput: 1000000.0 * histogram.len() as f32 / (time - self.last_sample_time).as_micros() as f32,
            mean_resp_time: histogram.mean() as f32 / 1000.0,
            resp_time_percentiles: percentiles,
        };
        self.curr_histogram.clear();
        self.last_sample_time = time;
        self.samples.push(result);
        self.samples.last().unwrap()
    }

    fn throughput_err(&self) -> f64 {
        let std_dev = self.samples.iter().map(|s| s.throughput as f64).std_dev();
        std_dev / (self.samples.len() as f64).sqrt() * ERR_MARGIN
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
        let std_dev = self
            .samples
            .iter()
            .map(|s| s.mean_resp_time as f64)
            .std_dev();
        std_dev / (self.samples.len() as f64).sqrt() * ERR_MARGIN
    }

    fn resp_time_percentile_err(&self, p: Percentile) -> f64 {
        let std_dev = self
            .samples
            .iter()
            .map(|s| s.resp_time_percentiles[p as usize] as f64)
            .std_dev();
        std_dev / (self.samples.len() as f64).sqrt() * ERR_MARGIN
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
            start_time: start_time,
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

    pub fn record<E>(&mut self, item: Result<ActionStats, E>) {
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

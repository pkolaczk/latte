use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use statrs::statistics::Statistics;
use tokio::time::{Duration, Instant};

use crate::config::Config;

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

const PERCENTILES: [Percentile; 11] = [
    Percentile::Min,
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
    fn value(&self) -> f64 {
        match self {
            Percentile::Min => 0.0,
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

    fn name(&self) -> &'static str {
        match self {
            Percentile::Min => "  Min",
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
pub struct Sample {
    pub start_time: Instant,
    pub end_time: Instant,
    pub throughput: f32,
    pub mean_resp_time: f32,
    pub resp_time_percentiles: [f32; PERCENTILES.len()],
}

impl Sample {
    pub fn print_log_header() {
        println!(
            "LOG ======================================================================================");
        println!(
            "                      ----------------------- Response times [ms]-------------------------"
        );
        println!(
            "Time [s]  Throughput       Min        25        50        75        90        99       Max"
        )
    }

    pub fn to_string(&self, benchmark_start: Instant) -> String {
        format!(
            "{:8.3} {:11.0} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
            (self.end_time - benchmark_start).as_secs_f32(),
            self.throughput,
            self.resp_time_percentiles[0] / 1000.0,
            self.resp_time_percentiles[1] / 1000.0,
            self.resp_time_percentiles[2] / 1000.0,
            self.resp_time_percentiles[3] / 1000.0,
            self.resp_time_percentiles[4] / 1000.0,
            self.resp_time_percentiles[6] / 1000.0,
            self.resp_time_percentiles[10] / 1000.0
        )
    }
}

struct Log {
    start: Instant,
    samples: Vec<Sample>,
    curr_histogram: Histogram<u64>,
}

impl Log {
    fn new(start_time: Instant) -> Log {
        Log {
            start: start_time,
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
            percentiles[i] = histogram.value_at_percentile(p.value()) as f32;
        }
        let result = Sample {
            start_time: self.start,
            end_time: time,
            throughput: 1000000.0 * histogram.len() as f32 / (time - self.start).as_micros() as f32,
            mean_resp_time: histogram.mean() as f32,
            resp_time_percentiles: percentiles,
        };
        self.curr_histogram.clear();
        self.start = time;
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

    fn mean_resp_time_err(&self) -> f64 {
        let std_dev = self
            .samples
            .iter()
            .map(|s| s.mean_resp_time as f64)
            .std_dev();
        std_dev / (self.samples.len() as f64).sqrt() * ERR_MARGIN
    }

    fn percentile_err(&self, p: Percentile) -> f64 {
        let std_dev = self
            .samples
            .iter()
            .map(|s| s.resp_time_percentiles[p as usize] as f64)
            .std_dev();
        std_dev / (self.samples.len() as f64).sqrt() * ERR_MARGIN
    }
}

pub struct BenchmarkStats {
    pub start_time: Instant,
    end_time: Instant,
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

impl BenchmarkStats {
    pub fn start() -> BenchmarkStats {
        let start_time = Instant::now();
        BenchmarkStats {
            resp_times: Histogram::<u64>::new(4).unwrap(),
            log: Log::new(start_time),
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
                self.resp_time_sum += s.duration.as_micros() as f64;
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
        self.log.start
    }

    pub fn enqueued(&mut self, queue_length: usize) {
        self.queue_len_sum += queue_length as u64;
    }

    pub fn finish(&mut self) {
        self.end_time = Instant::now();
        self.end_cpu_time = ProcessTime::now();
        if self.log.samples.is_empty() {
            self.sample(self.end_time);
        }
    }

    pub fn print(&self, conf: &Config) {
        let wall_clock_time = (self.end_time - self.start_time).as_secs_f64();
        let cpu_time = self
            .end_cpu_time
            .duration_since(self.start_cpu_time)
            .as_secs_f64();
        let count = self.completed + self.errors;
        let cpu_util = 100.0 * cpu_time / wall_clock_time / num_cpus::get() as f64;
        let completed_rate = 100.0 * self.completed as f64 / count as f64;
        let error_rate = 100.0 * self.errors as f64 / count as f64;
        let mean_resp_time = self.resp_time_sum / self.completed as f64 / 1000.0;
        let mean_resp_time_err = self.log.mean_resp_time_err() / 1000.0;

        let throughput = self.completed as f64 / wall_clock_time;
        let throughput_err = self.log.throughput_err();
        let throughput_ratio = 100.0 * throughput / conf.rate.unwrap_or(f64::MAX);
        let partitions_per_sec = self.partitions as f64 / wall_clock_time;
        let rows_per_sec = self.rows as f64 / wall_clock_time;

        let concurrency = self.queue_len_sum as f64 / count as f64;
        let concurrency_ratio = 100.0 * concurrency / conf.concurrency as f64;

        println!("SUMMARY STATS ============================================================================");
        println!("            Elapsed: {:11.3}          s", wall_clock_time);
        println!(
            "           CPU time: {:11.3}          s       {:6.1}%",
            cpu_time, cpu_util
        );
        println!(
            "          Completed: {:11.0}          req     {:6.1}%",
            self.completed, completed_rate
        );
        println!(
            "             Errors: {:11.0}          req     {:6.1}%",
            self.errors, error_rate
        );

        println!("         Partitions: {:11}", self.partitions);
        println!("               Rows: {:11}", self.rows);

        println!(
            "    Mean throughput: {:11.0} ± {:<6.0} req/s   {:6.1}%",
            throughput, throughput_err, throughput_ratio,
        );

        println!(
            "    Mean resp. time: {:11.2} ± {:<6.2} ms",
            mean_resp_time, mean_resp_time_err
        );

        println!(
            "   Mean concurrency: {:11.2}          req     {:6.1}%",
            concurrency, concurrency_ratio
        );

        println!();
        let histogram = self.log.throughput_histogram();
        println!("THROUGHPUT ===============================================================================");
        println!("                Min: {:11}          req/s", histogram.min());
        for p in &[1.0, 5.0, 25.0, 50.0, 75.0, 95.0] {
            println!(
                "              {:5}: {:11}          req/s",
                *p,
                histogram.value_at_percentile(*p)
            );
        }
        println!("                Max: {:11}          req/s", histogram.max());

        let histogram = &self.resp_times;
        println!();
        println!("RESPONSE TIMES ===========================================================================");
        for p in PERCENTILES.iter() {
            println!(
                "              {:5}: {:11.2} ± {:<6.2} ms",
                p.name(),
                self.resp_times.value_at_percentile(p.value()) as f64 / 1000.0,
                self.log.percentile_err(*p) / 1000.0,
            );
        }

        println!();
        println!("RESPONSE TIME DISTRIBUTION ==============================================================");
        println!("Percentile   Resp. time      Count");
        for x in self.resp_times.iter_log(histogram.min(), 1.25) {
            println!(
                " {:9.5} {:9.2} ms  {:9}   {}",
                x.percentile(),
                x.value_iterated_to() as f64 / 1000.0,
                x.count_since_last_iteration(),
                "*".repeat((100 * x.count_since_last_iteration() / histogram.len()) as usize)
            )
        }
    }
}

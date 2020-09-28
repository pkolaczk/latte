use crate::config::Config;
use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct ActionStats {
    pub duration: Duration,
    pub row_count: u64,
    pub partition_count: u64,
}

/// Measures average throughput - the number of recorded actions in a unit of time.
/// In order to learn the variance of the throughput, we take multiple short measurements during
/// the run of the whole benchmark. Each new measurement is started with a call to `reset`.
struct ThroughputMeter {
    start: Instant,
    count: u64,
}

impl ThroughputMeter {
    fn new() -> ThroughputMeter {
        ThroughputMeter {
            start: Instant::now(),
            count: 0,
        }
    }

    fn record(&mut self) {
        self.count += 1
    }

    fn value(&self) -> f64 {
        self.count as f64 / (Instant::now() - self.start).as_micros() as f64 * 1000000.0
    }

    fn reset(&mut self) {
        self.start = Instant::now();
        self.count = 0;
    }
}

pub struct BenchmarkStats {
    response_times: Histogram<u64>,
    throughput: Histogram<u64>,
    throughput_meter: ThroughputMeter,
    start: Instant,
    end: Instant,
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
        BenchmarkStats {
            response_times: Histogram::<u64>::new(3).unwrap(),
            throughput: Histogram::<u64>::new(3).unwrap(),
            start: Instant::now(),
            end: Instant::now(),
            completed: 0,
            errors: 0,
            rows: 0,
            partitions: 0,
            start_cpu_time: ProcessTime::now(),
            end_cpu_time: ProcessTime::now(),
            queue_len_sum: 0,
            throughput_meter: ThroughputMeter::new(),
        }
    }

    pub fn record<E>(&mut self, item: Result<ActionStats, E>) {
        match item {
            Ok(s) => {
                self.completed += 1;
                self.rows += s.row_count;
                self.partitions += s.partition_count;
                self.response_times
                    .record(s.duration.as_micros() as u64)
                    .unwrap();
                self.throughput_meter.record();
            }
            Err(_) => {
                self.errors += 1;
            }
        };
    }

    pub fn sample(&mut self) {
        self.throughput
            .record(self.throughput_meter.value() as u64)
            .unwrap();
        self.throughput_meter.reset();
    }

    pub fn last_sample_time(&self) -> Instant {
        self.throughput_meter.start
    }

    pub fn enqueued(&mut self, queue_length: usize) {
        self.queue_len_sum += queue_length as u64;
    }

    pub fn finish(&mut self) {
        if self.throughput.is_empty() {
            self.sample();
        }
        self.end = Instant::now();
        self.end_cpu_time = ProcessTime::now();
    }

    pub fn print(&self, conf: &Config) {
        let wall_clock_time = (self.end - self.start).as_secs_f64();
        let cpu_time = self
            .end_cpu_time
            .duration_since(self.start_cpu_time)
            .as_secs_f64();
        let count = self.completed + self.errors;
        let cpu_util = 100.0 * cpu_time / wall_clock_time / num_cpus::get() as f64;
        let completed_rate = 100.0 * self.completed as f64 / count as f64;
        let error_rate = 100.0 * self.errors as f64 / count as f64;
        let mean_resp_time = self.response_times.mean() / 1000.0;
        let throughput = self.completed as f64 / wall_clock_time;
        let throughput_ratio = 100.0 * throughput / conf.rate.unwrap_or(f64::MAX);
        let partitions_per_sec = self.partitions as f64 / wall_clock_time;
        let rows_per_sec = self.rows as f64 / wall_clock_time;

        let concurrency = self.queue_len_sum as f64 / count as f64;
        let concurrency_ratio = 100.0 * concurrency / conf.concurrency as f64;

        println!("SUMMARY -----------------------------------------------------------------");
        println!("            Elapsed: {:11.3} s", wall_clock_time);
        println!(
            "           CPU time: {:11.3} s       {:6.1}%",
            cpu_time, cpu_util
        );
        println!(
            "          Completed: {:11} req     {:6.1}%",
            self.completed, completed_rate
        );
        println!(
            "             Errors: {:11} req     {:6.1}%",
            self.errors, error_rate
        );

        println!("         Partitions: {:11}", self.partitions);
        println!("               Rows: {:11}", self.rows);

        println!(
            "    Mean throughput: {:11.0} req/s   {:6.1}%\n\
          \x20                    {:11.0} par/s\n\
          \x20                    {:11.0} row/s ",
            throughput, throughput_ratio, partitions_per_sec, rows_per_sec
        );

        println!("    Mean resp. time: {:11.2} ms", mean_resp_time);

        println!(
            "   Mean concurrency: {:11.2} req     {:6.1}%",
            concurrency, concurrency_ratio
        );

        println!();
        println!("THROUGHPUT --------------------------------------------------------------");
        println!("            Samples: {:11}", self.throughput.len());
        println!("                Min: {:11} req/s", self.throughput.min());
        for p in &[5.0, 25.0, 50.0, 75.0, 95.0] {
            println!(
                "              {:5}: {:11} req/s",
                *p,
                self.throughput.value_at_percentile(*p)
            );
        }
        println!("                Max: {:11} req/s", self.throughput.max());
        println!("          Std. dev.: {:11.0} req/s", self.throughput.stdev());
        println!(
            "         Std. error: {:11.0} req/s",
            self.throughput.stdev() / (self.throughput.len() as f64).sqrt()
        );


        let histogram = &self.response_times;
        println!();
        println!("RESPONSE TIMES ----------------------------------------------------------");
        println!(
            "                Min: {:11.2} ms",
            histogram.min() as f64 / 1000.0
        );
        for p in &[25.0, 50.0, 75.0, 90.0, 95.0, 98.0, 99.0, 99.9, 99.99] {
            println!(
                "              {:5}: {:11.2} ms",
                *p,
                self.response_times.value_at_percentile(*p) as f64 / 1000.0
            );
        }
        println!(
            "                Max: {:11.2} ms",
            histogram.max() as f64 / 1000.0
        );

        println!();
        println!("RESPONSE TIME DISTRIBUTION ----------------------------------------------");
        println!("Percentile   Resp. time      Count");
        for x in self.response_times.iter_log(histogram.min(), 1.25) {
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

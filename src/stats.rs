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

pub struct BenchmarkStats {
    histogram: Histogram<u64>,
    start: Instant,
    end: Instant,
    completed: u64,
    errors: u64,
    rows: u64,
    partitions: u64,
    start_cpu_time: ProcessTime,
    end_cpu_time: ProcessTime,
    enqueued: u64,
    queue_len_sum: u64,
}

impl BenchmarkStats {
    pub fn start() -> BenchmarkStats {
        BenchmarkStats {
            histogram: Histogram::<u64>::new(3).unwrap(),
            start: Instant::now(),
            end: Instant::now(),
            completed: 0,
            errors: 0,
            rows: 0,
            partitions: 0,
            start_cpu_time: ProcessTime::now(),
            end_cpu_time: ProcessTime::now(),
            enqueued: 0,
            queue_len_sum: 0,
        }
    }

    pub fn record<E>(&mut self, item: Result<ActionStats, E>) {
        match item {
            Ok(s) => {
                self.completed += 1;
                self.rows += s.row_count;
                self.partitions += s.partition_count;
                self.histogram
                    .record(s.duration.as_micros() as u64)
                    .unwrap();
            }
            Err(_) => {
                self.errors += 1;
            }
        };
    }

    pub fn enqueued(&mut self, queue_length: usize) {
        self.enqueued += 1;
        self.queue_len_sum += queue_length as u64;
    }

    pub fn finish(&mut self) {
        self.end = Instant::now();
        self.end_cpu_time = ProcessTime::now();
    }

    fn print_percentile(&self, percentile: f64) {
        println!(
            "             {:5}: {:11.3} ms",
            percentile,
            self.histogram.value_at_percentile(percentile) as f64 / 1000.0
        );
    }

    pub fn print(&self, conf: &Config) {
        let wall_clock_time = (self.end - self.start).as_secs_f64();
        let cpu_time = self
            .end_cpu_time
            .duration_since(self.start_cpu_time)
            .as_secs_f64();
        let cpu_util = 100.0 * cpu_time / wall_clock_time / num_cpus::get() as f64;
        let completed_rate = 100.0 * self.completed as f64 / self.enqueued as f64;
        let error_rate = 100.0 * self.errors as f64 / self.enqueued as f64;
        let throughput = self.completed as f64 / wall_clock_time;
        let throughput_ratio = 100.0 * throughput / conf.rate.unwrap_or(f64::MAX);
        let partitions_per_sec = self.partitions as f64 / wall_clock_time;
        let rows_per_sec = self.rows as f64 / wall_clock_time;

        let concurrency = self.queue_len_sum as f64 / self.enqueued as f64;
        let concurrency_ratio = 100.0 * concurrency / conf.concurrency as f64;

        println!("SUMMARY ----------------------------------------");
        println!("           Elapsed: {:11.3} s", wall_clock_time);
        println!(
            "          CPU time: {:11.3} s       {:6.1}%",
            cpu_time, cpu_util
        );
        println!(
            "         Completed: {:11} req     {:6.1}%",
            self.completed, completed_rate
        );
        println!(
            "            Errors: {:11} req     {:6.1}%",
            self.errors, error_rate
        );

        println!("        Partitions: {:11}", self.partitions);
        println!("              Rows: {:11}", self.rows);

        println!(
            "        Throughput: {:11.1} req/s   {:6.1}%\n\
          \x20                   {:11.1} par/s\n\
          \x20                   {:11.1} row/s ",
            throughput, throughput_ratio, partitions_per_sec, rows_per_sec
        );

        println!(
            "  Avg. concurrency: {:11.1} req     {:6.1}%",
            concurrency, concurrency_ratio
        );

        let histogram = &self.histogram;
        println!();
        println!("RESPONSE TIMES ---------------------------------");
        println!(
            "               Min: {:11.3} ms",
            histogram.min() as f64 / 1000.0
        );

        for p in &[25.0, 50.0, 75.0, 90.0, 95.0, 98.0, 99.0, 99.9, 99.99] {
            self.print_percentile(*p);
        }
        println!(
            "               Max: {:11.3} ms",
            histogram.max() as f64 / 1000.0
        );

        println!();
        println!("DETAILED HISTOGRAM -----------------------------");
        println!("Percentile   Resp. time      Count");
        for x in self.histogram.iter_log(histogram.min(), 1.3) {
            println!(
                "{:6.2}     {:9.3} ms  {:9}   |{}",
                x.percentile(),
                x.value_iterated_to() as f64 / 1000.0,
                x.count_since_last_iteration(),
                "*".repeat((100 * x.count_since_last_iteration() / histogram.len()) as usize)
            )
        }
    }
}

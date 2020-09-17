use crate::config::Config;
use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use tokio::time::{Duration, Instant};

pub struct Stats {
    histogram: Histogram<u64>,
    start: Instant,
    end: Instant,
    completed: u64,
    errors: u64,
    start_cpu_time: ProcessTime,
    end_cpu_time: ProcessTime,
    enqueued: u64,
    queue_len_sum: u64,
}

impl Stats {
    pub fn start() -> Stats {
        Stats {
            histogram: Histogram::<u64>::new(3).unwrap(),
            start: Instant::now(),
            end: Instant::now(),
            completed: 0,
            errors: 0,
            start_cpu_time: ProcessTime::now(),
            end_cpu_time: ProcessTime::now(),
            enqueued: 0,
            queue_len_sum: 0,
        }
    }

    pub fn record<E>(&mut self, item: Result<Duration, E>) {
        match item {
            Ok(d) => {
                self.completed += 1;
                self.histogram.record(d.as_micros() as u64).unwrap();
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
        let cpu_util = 100.0 * cpu_time / wall_clock_time;
        let completed_rate = 100.0 * self.completed as f64 / self.enqueued as f64;
        let error_rate = 100.0 * self.errors as f64 / self.enqueued as f64;
        let throughput = self.completed as f64 / wall_clock_time;
        let throughput_ratio = 100.0 * throughput / conf.rate as f64;
        let concurrency = self.queue_len_sum as f64 / self.enqueued as f64;
        let concurrency_ratio = 100.0 * concurrency / conf.parallelism as f64;

        println!("STATS -----------------------------------------");
        println!("           Elapsed: {:11.3} s", wall_clock_time);
        println!(
            "          CPU time: {:11.3} s       {:6.1}%",
            cpu_time, cpu_util
        );
        println!(
            "         Completed: {:11} reqs    {:6.1}%",
            self.completed, completed_rate
        );
        println!(
            "            Errors: {:11} reqs    {:6.1}%",
            self.errors, error_rate
        );
        println!(
            "        Throughput: {:11.1} req/s   {:6.1}%",
            throughput, throughput_ratio
        );
        println!(
            "       Concurrency: {:11.1} reqs    {:6.1}%",
            concurrency, concurrency_ratio
        );

        let histogram = &self.histogram;
        println!();
        println!("LATENCY ---------------------------------------");
        println!(
            "               Min: {:11.3} ms",
            histogram.min() as f64 / 1000.0
        );

        for p in &[25.0, 50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99] {
            self.print_percentile(*p);
        }
        println!(
            "               Max: {:11.3} ms",
            histogram.max() as f64 / 1000.0
        );

        println!();
        println!("HISTOGRAM ------------------------------------");
        println!("Percentile   Latency     Count");
        for x in self.histogram.iter_log(histogram.min(), 1.3) {
            println!(
                "{:6.2}   {:8.3} ms  {:8}   |{}",
                x.percentile(),
                x.value_iterated_to() as f64 / 1000.0,
                x.count_since_last_iteration(),
                "*".repeat((100 * x.count_since_last_iteration() / histogram.len()) as usize)
            )
        }
    }
}

use hdrhistogram::Histogram;
use tokio::time::{Duration, Instant};

pub struct Stats {
    histogram: Histogram<u64>,
    start: Instant,
    end: Instant,
    completed: u64,
    errors: u64,
}

impl Stats {
    pub fn start() -> Stats {
        Stats {
            histogram: Histogram::<u64>::new(3).unwrap(),
            start: Instant::now(),
            end: Instant::now(),
            completed: 0,
            errors: 0,
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

    pub fn finish(&mut self) {
        self.end = Instant::now();
    }

    fn print_percentile(&self, percentile: f64) {
        println!(
            "{:6.2}%   {:7} µs",
            percentile,
            self.histogram.value_at_percentile(percentile)
        );
    }

    pub fn print(&self) {
        let duration = (self.end - self.start).as_secs_f64();
        println!(" Completed: {} reqs", self.completed);
        println!("    Errors: {} reqs", self.errors);
        println!("   Elapsed: {:.3} s", duration);
        println!("Throughput: {:.2} req/s", (self.completed as f64 / duration));

        let histogram = &self.histogram;
        if !histogram.is_empty() {
            println!();
            println!("Latency summary:");
            println!("percentile   latency");
            println!("    min   {:7} µs", histogram.min());
            for p in &[50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99] {
                self.print_percentile(*p);
            }
            println!("    max   {:7} µs", histogram.max());

            println!();
            println!("Histogram:");
            println!("percentile   latency     count");
            for x in self.histogram.iter_log(histogram.min(), 1.3) {
                println!(
                    "{:6.2}%   {:7} µs  {:8}   |{}",
                    x.percentile(),
                    x.value_iterated_to(),
                    x.count_since_last_iteration(),
                    "*".repeat((100 * x.count_since_last_iteration() / histogram.len()) as usize)
                )
            }
        }
    }
}

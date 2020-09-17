use hdrhistogram::Histogram;
use tokio::time::{Duration, Instant};
use cpu_time::{ThreadTime, ProcessTime};

pub struct Stats {
    histogram: Histogram<u64>,
    start: Instant,
    end: Instant,
    completed: u64,
    errors: u64,
    process_cpu_time: ProcessTime,
    benchmark_thread_cpu_time: ThreadTime,
    enqueued: u64,
    queue_len_sum: u64
}

impl Stats {
    pub fn start() -> Stats {
        Stats {
            histogram: Histogram::<u64>::new(3).unwrap(),
            start: Instant::now(),
            end: Instant::now(),
            completed: 0,
            errors: 0,
            process_cpu_time: ProcessTime::now(),
            benchmark_thread_cpu_time: ThreadTime::now(),
            enqueued: 0,
            queue_len_sum: 0
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
        self.process_cpu_time = ProcessTime::now();
        self.benchmark_thread_cpu_time = ThreadTime::now();
    }

    fn print_percentile(&self, percentile: f64) {
        println!(
            "  p{:<5} {:8.3} ms",
            percentile,
            self.histogram.value_at_percentile(percentile) as f64 / 1000.0
        );
    }

    pub fn print(&self) {
        let wall_clock_time = (self.end - self.start).as_secs_f64();
        let process_cpu_time = self.process_cpu_time.as_duration().as_secs_f64();
        println!("------------------------------------");
        println!("    Elapsed:  {:9.3} s", wall_clock_time);
        println!("   CPU time:  {:9.3} s ({:6.1}%)", process_cpu_time, 100.0 * process_cpu_time / wall_clock_time);
        println!("  Completed: {:9} reqs", self.completed);
        println!("     Errors: {:9} reqs", self.errors);
        println!(" Throughput: {:9.1} req/s", (self.completed as f64 / wall_clock_time));
        println!("Concurrency: {:9.1} reqs", self.queue_len_sum as f64 / self.enqueued as f64);
        println!("------------------------------------");

        let histogram = &self.histogram;
        if !histogram.is_empty() {
            println!();
            println!("Latency:");
            println!("  min    {:8.3} ms", histogram.min() as f64 / 1000.0);

            for p in &[50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99] {
                self.print_percentile(*p);
            }
            println!("  max    {:8.3} ms", histogram.max() as f64 / 1000.0);

            println!();
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
}

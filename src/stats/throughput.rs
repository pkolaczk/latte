use crate::stats::timeseries::TimeSeriesStats;
use crate::stats::Mean;
use std::time::Instant;

pub struct ThroughputMeter {
    last_record_time: Instant,
    count: u64,
    stats: TimeSeriesStats,
}

impl Default for ThroughputMeter {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            last_record_time: now,
            count: 0,
            stats: TimeSeriesStats::default(),
        }
    }
}

impl ThroughputMeter {
    pub fn record(&mut self, count: u64) {
        let now = Instant::now();
        let duration = now.duration_since(self.last_record_time).as_secs_f64();
        let throughput = count as f64 / duration;
        self.count += count;
        self.stats.record(throughput, duration);
        self.last_record_time = now;
    }

    /// Returns mean throughput in events per second
    pub fn throughput(&self) -> Mean {
        self.stats.mean()
    }
}

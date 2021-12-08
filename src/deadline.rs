use std::cmp;
use std::ops::Range;
use crate::config;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::Instant;



/// Decides when to stop benchmark execution.
pub enum Deadline {
    FixedCount {
        current: AtomicU64,
        count: u64,
    },
    FixedTime {
        current: AtomicU64,
        deadline: Instant,
    },
}

const BATCH_SIZE: u64 = 64;

impl Deadline {
    /// Creates a new deadline based on configured benchmark duration.
    /// For time-based deadline, the clock starts ticking when the `Deadline` is created.
    pub fn new(duration: config::Duration) -> Deadline {
        let current = AtomicU64::new(0);
        match duration {
            config::Duration::Count(count) => Deadline::FixedCount { current, count },
            config::Duration::Time(d) => Deadline::FixedTime {
                current,
                deadline: Instant::now() + d,
            },
        }
    }

    /// Returns the next iteration number or `None` if deadline or iteration count was exceeded.
    pub fn next(&self) -> Range<u64> {
        match self {
            Deadline::FixedCount { current, count } => {
                let start = current.fetch_add(BATCH_SIZE, Ordering::Relaxed);
                let end = cmp::min(*count, start + BATCH_SIZE);
                Range { start, end }
            }
            Deadline::FixedTime { current, deadline } => {
                if Instant::now() < *deadline {
                    let start = current.fetch_add(BATCH_SIZE, Ordering::Relaxed);
                    Range { start, end: start + BATCH_SIZE }
                } else {
                    Range::default()
                }
            }
        }
    }
}

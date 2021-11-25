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
    pub fn next(&self) -> Option<u64> {
        match self {
            Deadline::FixedCount { current, count } => {
                let next = current.fetch_add(1, Ordering::Relaxed);
                if next < *count {
                    Some(next)
                } else {
                    None
                }
            }
            Deadline::FixedTime { current, deadline } => {
                if Instant::now() < *deadline {
                    Some(current.fetch_add(1, Ordering::Relaxed))
                } else {
                    None
                }
            }
        }
    }
}

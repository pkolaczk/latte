use crate::config;
use crate::config::Duration;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const BATCH_SIZE: u64 = 64;

/// Provides distinct benchmark iteration numbers to multiple threads of execution.
pub struct IterationCounter {
    shared: Arc<AtomicU64>,
    local: u64,
    local_max: u64,
}

impl IterationCounter {
    /// Creates a new iteration counter, starting at `start`.
    /// The counter is logically positioned at one item before `start`, so the first call
    /// to `next` will return `start`.
    pub fn new(start: u64) -> Self {
        IterationCounter {
            shared: Arc::new(AtomicU64::new(start)),
            local: 0, // the value does not matter as long as it is not lower than local_max
            local_max: 0, // force getting the shared count in the first call to `next`
        }
    }

    /// Gets the next iteration number and advances the counter by one
    pub fn next(&mut self) -> u64 {
        if self.local >= self.local_max {
            self.next_batch();
        }
        let result = self.local;
        self.local += 1;
        result
    }

    /// Reserves the next batch of iterations
    fn next_batch(&mut self) {
        self.local = self.shared.fetch_add(BATCH_SIZE, Ordering::Relaxed);
        self.local_max = self.local + BATCH_SIZE;
    }

    /// Creates a new counter sharing the list of iterations with this one.
    /// The new counter will never return the same iteration number as this one.
    pub fn share(&self) -> IterationCounter {
        IterationCounter {
            shared: self.shared.clone(),
            local: 0,
            local_max: 0,
        }
    }
}

/// Provides distinct benchmark iteration numbers to multiple threads of execution.
/// Decides when to stop the benchmark execution.
pub struct BoundedIterationCounter {
    pub duration: config::Duration,
    start_time: Instant,
    iteration_counter: IterationCounter,
}

impl BoundedIterationCounter {
    /// Creates a new iteration counter based on configured benchmark duration.
    /// For time-based deadline, the clock starts ticking when this object is created.
    pub fn new(duration: config::Duration) -> Self {
        BoundedIterationCounter {
            duration,
            start_time: Instant::now(),
            iteration_counter: IterationCounter::new(0),
        }
    }

    /// Returns the next iteration number or `None` if deadline or iteration count was exceeded.
    pub fn next(&mut self) -> Option<u64> {
        match self.duration {
            Duration::Count(count) => {
                let result = self.iteration_counter.next();
                if result < count {
                    Some(result)
                } else {
                    None
                }
            }
            Duration::Time(duration) => {
                if Instant::now() < self.start_time + duration {
                    Some(self.iteration_counter.next())
                } else {
                    None
                }
            }
        }
    }

    /// Shares this counter e.g. with another thread.
    pub fn share(&self) -> Self {
        BoundedIterationCounter {
            start_time: self.start_time,
            duration: self.duration,
            iteration_counter: self.iteration_counter.share(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::iteration::{IterationCounter, BATCH_SIZE};
    use itertools::Itertools;
    use std::collections::BTreeSet;

    #[test]
    pub fn iteration_counter_must_return_all_numbers() {
        let mut counter = IterationCounter::new(10);
        for i in 10..(10 + 2 * BATCH_SIZE) {
            let iter = counter.next();
            assert_eq!(i, iter)
        }
    }

    #[test]
    pub fn shared_iteration_counter_must_return_distinct_numbers() {
        let mut counter1 = IterationCounter::new(10);
        let mut counter2 = counter1.share();
        let mut set1 = BTreeSet::new();
        let mut set2 = BTreeSet::new();
        for _ in 10..(10 + 2 * BATCH_SIZE) {
            set1.insert(counter1.next());
            set2.insert(counter2.next());
        }
        assert_eq!(
            set1.intersection(&set2).cloned().collect_vec(),
            Vec::<u64>::new()
        )
    }
}

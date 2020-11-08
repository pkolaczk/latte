use std::cmp::min;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// An atomic counter that can count only backwards until it reaches zero.
/// It is useful for distributing work between parallel tasks without paying
/// the price of an SPMC queue.
///
/// Imagine there are N tasks to be processed by M threads.
/// The threads share a single `CountDown`
/// instance initiated to N. Before doing work, each thread decreases
/// the counter by the number of items it wishes to process and receives
/// the allowed number of items to process.
/// All work is done when the counter cannot be further decreased.
///
pub struct CountDown {
    value: AtomicU64,
}

impl CountDown {
    pub fn new(initial_value: u64) -> Self {
        CountDown {
            value: AtomicU64::new(initial_value),
        }
    }

    /// Decreases the counter by `delta` but doesn't allow the counter
    /// to drop below zero. Returns the actual amount the counter was decreased by.
    pub fn dec(&self, delta: u64) -> u64 {
        let mut prev = self.value.load(Ordering::Relaxed);
        loop {
            let delta = min(delta, prev);
            match self.value.compare_exchange_weak(
                prev,
                prev - delta,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return delta,
                Err(x) => prev = x,
            }
        }
    }
}

pub struct BatchedCountDown {
    batch_size: u64,
    local: u64,
    shared: Arc<CountDown>
}

impl BatchedCountDown {

    pub fn new(shared: Arc<CountDown>, batch_size: u64) -> Self {
        BatchedCountDown { batch_size, local: 0, shared }
    }

    /// Decreases the counter by 1 until the counter drops to 0.
    /// Returns true if the counter was decreased.
    /// Returns false when the counter was already 0 and couldn't be decreased any more.
    pub fn dec(&mut self) -> bool {
        if self.local == 0 {
            self.local = self.shared.dec(self.batch_size);
        }
        if self.local == 0 {
            false
        } else {
            self.local -= 1;
            true
        }
    }

}
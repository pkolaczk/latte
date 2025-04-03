use crate::adapters::Adapters;
use crate::error::LatteError;
use crate::stats::session::SessionStats;
use rand::prelude::ThreadRng;
use rune::runtime::{Object, Shared};
use rune::{Any, Value};
use tokio::time::Instant;
use try_lock::TryLock;

/// This is the main object that a workload script uses to interface with the outside world.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Context {
    adapter: Adapters,
    start_time: TryLock<Instant>,
    #[rune(get, set, add_assign, copy)]
    pub load_cycle_count: u64,
    #[rune(get)]
    pub data: Value,
    pub rng: ThreadRng,
}

// Needed, because Rune `Value` is !Send, as it may contain some internal pointers.
// Therefore, it is not safe to pass a `Value` to another thread by cloning it, because
// both objects could accidentally share some unprotected, `!Sync` data.
// To make it safe, the same `Context` is never used by more than one thread at once, and
// we make sure in `clone` to make a deep copy of the `data` field by serializing
// and deserializing it, so no pointers could get through.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(adapters: Adapters) -> Context {
        Context {
            start_time: TryLock::new(Instant::now()),
            adapter: adapters,
            load_cycle_count: 0,
            data: Value::Object(Shared::new(Object::new()).unwrap()),
            rng: rand::thread_rng(),
        }
    }

    /// Clones the context for use by another thread.
    /// The new clone gets fresh statistics.
    /// The user data gets passed through serialization and deserialization to avoid
    /// accidental data sharing.
    pub fn clone(&self) -> Result<Self, LatteError> {
        let serialized = rmp_serde::to_vec(&self.data)?;
        let deserialized: Value = rmp_serde::from_slice(&serialized)?;
        Ok(Context {
            adapter: self.adapter.clone(),
            data: deserialized,
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
            rng: rand::thread_rng(),
            ..*self
        })
    }

    pub fn adapter(&self) -> &Adapters {
        &self.adapter
    }

    pub fn adapter_mut(&mut self) -> &mut Adapters {
        &mut self.adapter
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start_time.try_lock().unwrap().elapsed().as_secs_f64()
    }

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        self.adapter.take_session_stats()
    }

    /// Resets query and request counters
    pub fn reset(&self) {
        self.adapter.reset();
        *self.start_time.try_lock().unwrap() = Instant::now();
    }
}

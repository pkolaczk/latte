use crate::adapters::aerospike::AerospikeAdapter;
use crate::adapters::postgres::PostgresAdapter;
use crate::adapters::scylla::ScyllaAdapter;
use crate::stats::session::SessionStats;

pub mod aerospike;
pub mod postgres;
pub mod scylla;

#[derive(Clone)]
pub enum Adapters {
    Aerospike(AerospikeAdapter),
    Scylla(ScyllaAdapter),
    Postgres(PostgresAdapter),
}

impl Adapters {
    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        match self {
            Adapters::Aerospike(a) => a.take_session_stats(),
            Adapters::Scylla(s) => s.take_session_stats(),
            Adapters::Postgres(s) => s.take_session_stats(),
        }
    }

    /// Resets query and request counters
    pub fn reset(&self) {
        match self {
            Adapters::Aerospike(a) => a.reset(),
            Adapters::Scylla(s) => s.reset(),
            Adapters::Postgres(s) => s.reset(),
        }
    }
}

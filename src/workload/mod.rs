use std::sync::Arc;

use async_trait::async_trait;
use cassandra_cpp::Result;

pub mod read_none;
pub mod read_same;
pub mod write;

pub struct WorkloadStats {
    pub partition_count: u64,
    pub row_count: u64,
}

/// Allows us to easily extend latte with new workload types.
#[async_trait]
pub trait Workload
where
    Self: Sync + Send,
{
    /// Controls how many times `populate` should be called
    fn population_size(&self) -> u64;

    /// Inserts a chunk of information into the test table and returns the
    /// number of inserted partitions and rows
    async fn populate(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>;

    /// Executes queries to be benchmarked and
    /// returns the number of processed partitions and rows
    async fn run(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>;
}

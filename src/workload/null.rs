use std::sync::Arc;

use async_trait::async_trait;

use crate::workload::{Workload, WorkloadStats};

use super::Result;

/// A dummy workload that doesn't really send any queries.
/// Its purpose is testing the benchmarking machinery.
pub struct Null {}

#[async_trait]
impl Workload for Null {
    fn populate_count(&self) -> u64 {
        0
    }

    async fn populate(self: Arc<Self>, _iteration: u64) -> Result<WorkloadStats> {
        Ok(WorkloadStats {
            row_count: 0,
            partition_count: 0,
        })
    }

    async fn run(self: Arc<Self>, _iteration: u64) -> Result<WorkloadStats> {
        Ok(WorkloadStats {
            row_count: 1,
            partition_count: 1,
        })
    }
}

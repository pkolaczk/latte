use std::sync::Arc;

use async_trait::async_trait;
use cassandra_cpp::{stmt, PreparedStatement, Result, Session};

use crate::workload::{Workload, WorkloadStats};

/// A workload that reads an read_none result set by looking up a non-existing
/// key in an read_none table
///
/// Preparation:
/// ```
/// CREATE TABLE read_none(pk BIGINT PRIMARY KEY)
/// ```
///
/// Benchmarked query:
/// ```
/// SELECT * FROM read_none WHERE pk = 1
/// ```
pub struct ReadNone<S>
where
    S: AsRef<Session> + Sync + Send,
{
    session: S,
    read_statement: PreparedStatement,
}

impl<S> ReadNone<S>
where
    S: AsRef<Session> + Sync + Send,
{
    pub async fn new(session: S) -> Result<Self> {
        let s = session.as_ref();
        let result = s.execute(&stmt!(
            "CREATE TABLE IF NOT EXISTS read_none (pk BIGINT PRIMARY KEY)"
        ));
        result.await?;

        let read_statement = s.prepare("SELECT * FROM read_none WHERE pk = 1")?.await?;
        Ok(ReadNone {
            session,
            read_statement,
        })
    }
}

#[async_trait]
impl<S> Workload for ReadNone<S>
where
    S: AsRef<Session> + Sync + Send,
{
    fn population_size(&self) -> u64 {
        0
    }

    async fn populate(self: Arc<Self>, _iteration: u64) -> Result<WorkloadStats>
    where
        S: 'async_trait,
    {
        Ok(WorkloadStats {
            partition_count: 0,
            row_count: 0,
        })
    }

    async fn run(self: Arc<Self>, _iteration: u64) -> Result<WorkloadStats>
    where
        S: 'async_trait,
    {
        let s = self.session.as_ref();
        let result = s.execute(&self.read_statement.bind());
        let result = result.await?;
        let row_count = result.row_count();
        Ok(WorkloadStats {
            partition_count: row_count,
            row_count,
        })
    }
}

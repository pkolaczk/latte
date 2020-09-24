use std::sync::Arc;

use async_trait::async_trait;
use cassandra_cpp::{stmt, PreparedStatement, Result, Session};

use crate::workload::{Workload, WorkloadStats};

/// A workload that reads the same row from a 6 column table.
/// Preparation:
/// ```
/// CREATE TABLE read_same (pk BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT)
/// INSERT INTO read_same(pk, c1, c2, c3, c4, c5) VALUES (1, 1, 2, 3, 4, 5)
/// ```
///
/// Benchmarked query:
/// ```
/// SELECT * FROM read_same WHERE pk = 1
/// ```
pub struct ReadSame<S>
where
    S: AsRef<Session> + Sync + Send,
{
    session: S,
    read_statement: PreparedStatement,
}

impl<S> ReadSame<S>
where
    S: AsRef<Session> + Sync + Send,
{
    pub async fn new(session: S) -> Result<Self> {
        let s = session.as_ref();
        let result = s.execute(&stmt!(
            "CREATE TABLE IF NOT EXISTS read_same\
            (pk BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT)"
        ));
        result.await?;

        let read_statement = s.prepare("SELECT * FROM read_same WHERE pk = 1")?.await?;
        Ok(ReadSame {
            session,
            read_statement,
        })
    }
}

#[async_trait]
impl<S> Workload for ReadSame<S>
where
    S: AsRef<Session> + Sync + Send,
{
    fn population_size(&self) -> u64 {
        1
    }

    async fn populate(self: Arc<Self>, _iteration: u64) -> Result<WorkloadStats>
    where
        S: 'async_trait,
    {
        let s = self.session.as_ref();
        let result = s.execute(&stmt!(
            "INSERT INTO read_same(pk, c1, c2, c3, c4, c5) VALUES (1, 1, 2, 3, 4, 5)"
        ));
        result.await?;
        Ok(WorkloadStats {
            partition_count: 1,
            row_count: 1,
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

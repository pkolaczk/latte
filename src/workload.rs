use async_trait::async_trait;
use cassandra_cpp::{stmt, PreparedStatement, Result, Session};
use std::sync::Arc;

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

/// A workload that reads the same tiny row from a 6 column table.
/// Preparation:
/// ```
/// CREATE TABLE tiny (pk BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT)
/// INSERT INTO tiny(pk, c1, c2, c3, c4, c5) VALUES (1, 1, 2, 3, 4, 5)
/// ```
///
/// Benchmarked query:
/// ```
/// SELECT * FROM tiny WHERE pk = 1
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
            "CREATE TABLE IF NOT EXISTS tiny\
            (pk BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT)"
        ));
        result.await?;

        let read_statement = s.prepare("SELECT * FROM tiny WHERE pk = 1")?.await?;
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
            "INSERT INTO tiny(pk, c1, c2, c3, c4, c5) VALUES (1, 1, 2, 3, 4, 5)"
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

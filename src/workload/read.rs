use std::sync::Arc;

use async_trait::async_trait;
use cassandra_cpp::{stmt, BindRustType, PreparedStatement, Result, Session};

use crate::workload::{Workload, WorkloadStats};

pub struct Read<S>
where
    S: AsRef<Session> + Sync + Send,
{
    session: S,
    row_count: u64,
    write_statement: PreparedStatement,
    read_statement: PreparedStatement,
}

impl<S> Read<S>
where
    S: AsRef<Session> + Sync + Send,
{
    pub async fn new(session: S, row_count: u64) -> Result<Self> {
        let s = session.as_ref();
        s.execute(&stmt!(
            "CREATE TABLE IF NOT EXISTS read (pk BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT)"
        ))
        .await?;

        let read = s.prepare("SELECT * FROM read WHERE pk = ?")?.await?;
        let write = s
            .prepare("INSERT INTO read (pk, c1, c2, c3, c4, c5) VALUES (?, 1, 2, 3, 4, 5)")?
            .await?;

        Ok(Read {
            session,
            row_count,
            write_statement: write,
            read_statement: read,
        })
    }
}

#[async_trait]
impl<S> Workload for Read<S>
where
    S: AsRef<Session> + Sync + Send,
{
    fn populate_count(&self) -> u64 {
        self.row_count
    }

    async fn populate(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>
    where
        S: 'async_trait,
    {
        let s = self.session.as_ref();

        let mut statement = self.write_statement.bind();
        statement.bind(0, (iteration % self.row_count) as i64)?;
        let result = s.execute(&statement);
        result.await?;

        Ok(WorkloadStats {
            partition_count: 1,
            row_count: 1,
        })
    }

    async fn run(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>
    where
        S: 'async_trait,
    {
        let s = self.session.as_ref();
        let mut statement = self.read_statement.bind();
        let key = if self.row_count == 0 {
            -1
        } else {
            (iteration % self.row_count) as i64
        };

        statement.bind(0, key)?;
        let result = s.execute(&statement);
        let result = result.await?;
        let row_count = result.row_count();
        Ok(WorkloadStats {
            partition_count: row_count,
            row_count,
        })
    }
}

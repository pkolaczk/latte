use std::sync::Arc;

use async_trait::async_trait;
use cassandra_cpp::{BindRustType, PreparedStatement, Session};

use crate::workload::{
    gen_random_blob, Result, Workload, WorkloadConfig, WorkloadError, WorkloadStats,
};

/// A workload that writes rows to the table
pub struct Write<S>
where
    S: AsRef<Session> + Sync + Send,
{
    session: S,
    column_count: usize,
    column_size: usize,
    row_count: u64,
    write_statement: PreparedStatement,
}

impl<S> Write<S>
where
    S: AsRef<Session> + Sync + Send,
{
    pub async fn new(session: S, conf: &WorkloadConfig) -> Result<Self> {
        if conf.partitions == 0 {
            return Err(WorkloadError::Other(
                "Number of partitions cannot be 0 for a write workload".to_owned(),
            ));
        }

        let schema = super::Schema {
            table_name: "write".to_owned() + "_" + &conf.schema_params_str(),
            column_count: conf.columns,
            compaction: conf.compaction,
        };
        let s = session.as_ref();
        s.execute(&schema.drop_table_stmt()).await?;
        s.execute(&schema.create_table_stmt()).await?;

        let insert_cql = schema.insert_cql();
        let write_statement = s.prepare(insert_cql.as_str())?.await?;
        Ok(Write {
            session,
            column_count: conf.columns,
            column_size: conf.column_size,
            row_count: conf.partitions,
            write_statement,
        })
    }
}

#[async_trait]
impl<S> Workload for Write<S>
where
    S: AsRef<Session> + Sync + Send,
{
    fn populate_count(&self) -> u64 {
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

    async fn run(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>
    where
        S: 'async_trait,
    {
        let s = self.session.as_ref();
        let mut statement = self.write_statement.bind();
        statement.bind(0, (iteration % self.row_count) as i64)?;
        for i in 0..self.column_count {
            statement.bind(i + 1, gen_random_blob(self.column_size))?;
        }
        let result = s.execute(&statement);
        result.await?;
        Ok(WorkloadStats {
            partition_count: 1,
            row_count: 1,
        })
    }
}

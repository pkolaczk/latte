use std::sync::Arc;

use async_trait::async_trait;

use crate::workload::{
    gen_random_blob, Result, Workload, WorkloadConfig, WorkloadError, WorkloadStats,
};
use scylla::frame::value::SerializedValues;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session;

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
        s.query(schema.drop_table_cql(), &[]).await?;
        s.query(schema.create_table_cql(), &[]).await?;

        let insert_cql = schema.insert_cql();
        let write_statement = s.prepare(insert_cql.as_str()).await?;
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

        let mut params = SerializedValues::new();

        params
            .add_value(&((iteration % self.row_count) as i64))
            .unwrap();
        for _ in 0..self.column_count {
            params
                .add_value(&gen_random_blob(self.column_size))
                .unwrap();
        }

        let result = s.execute(&self.write_statement, params);
        result.await?;
        Ok(WorkloadStats {
            partition_count: 1,
            row_count: 1,
        })
    }
}

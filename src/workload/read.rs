use std::sync::Arc;

use async_trait::async_trait;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session;

use crate::workload::{gen_random_blob, Result, Workload, WorkloadConfig, WorkloadStats};
use scylla::frame::value::SerializedValues;

pub struct Read<S>
    where
        S: AsRef<Session> + Sync + Send,
{
    session: S,
    row_count: u64,
    column_count: usize,
    column_size: usize,
    write_statement: PreparedStatement,
    read_statement: PreparedStatement,
}

impl<S> Read<S>
where
    S: AsRef<Session> + Sync + Send,
{
    pub async fn new(session: S, conf: &WorkloadConfig) -> Result<Self> {
        let schema = super::Schema {
            table_name: "read".to_owned() + "_" + &conf.schema_params_str(),
            column_count: conf.columns,
            compaction: conf.compaction,
        };
        let s = session.as_ref();
        s.query(schema.create_table_cql(), &[]).await?;
        let read_cql = format!("SELECT * FROM {} WHERE pk = ?", schema.table_name);
        let read = s.prepare(read_cql.as_str()).await?;
        let write = s.prepare(schema.insert_cql().as_str()).await?;

        Ok(Read {
            session,
            row_count: conf.partitions,
            column_count: conf.columns,
            column_size: conf.column_size,
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

        let mut params = SerializedValues::new();
        params.add_value(&(iteration as i64)).unwrap();
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

    async fn run(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>
    where
        S: 'async_trait,
    {
        let s = self.session.as_ref();
        let key = if self.row_count == 0 {
            -1
        } else {
            (iteration % self.row_count) as i64
        };

        let result = s.execute(&self.read_statement, vec![key]);
        let result = result.await?;
        let row_count = result.rows.unwrap().len() as u64;
        Ok(WorkloadStats {
            partition_count: row_count,
            row_count,
        })
    }
}

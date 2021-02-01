use std::sync::Arc;

use async_trait::async_trait;
use cassandra_cpp::Statement;
use clap::Clap;
use err_derive::*;
use itertools::Itertools;
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use strum::*;

pub mod null;
pub mod read;
pub mod write;

#[derive(Debug, Error)]
pub enum WorkloadError {
    #[error(display = "Cassandra error: {}", _0)]
    Cassandra(#[source] cassandra_cpp::Error),
    #[error(display = "{}", _0)]
    Other(String),
}

pub type Result<T> = std::result::Result<T, WorkloadError>;

#[derive(Serialize, Deserialize, Debug, AsStaticStr, Clap, Copy, Clone)]
pub enum Compaction {
    None,
    LCS,
    STCS,
}

/// Parameters of a workload.
/// Applies to read and write and possibly other workloads.
pub struct WorkloadConfig {
    pub partitions: u64,
    pub columns: usize,
    pub column_size: usize,
    pub compaction: Compaction,
}

impl WorkloadConfig {
    pub fn schema_params_str(&self) -> String {
        format!(
            "{}_{}_{}",
            self.columns,
            self.column_size,
            self.compaction.as_static()
        )
    }
}

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
    /// Controls how many rows should be inserted into the test table before the test.
    /// Should return None if the number of rows should be determined from the iteration
    /// count or the value given by the user.
    fn populate_count(&self) -> u64;

    /// Inserts a row into the test table and returns the
    /// number of inserted partitions and rows
    async fn populate(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>;

    /// Executes queries to be benchmarked and
    /// returns the number of processed partitions and rows
    async fn run(self: Arc<Self>, iteration: u64) -> Result<WorkloadStats>;
}

/// Common schema for both read and write workloads
struct Schema {
    table_name: String,
    compaction: Compaction,
    column_count: usize,
}

impl Schema {
    pub fn data_columns(&self) -> Vec<String> {
        (0..self.column_count)
            .into_iter()
            .map(|i| format!("data_{}", i))
            .collect()
    }

    /// Returns the CREATE TABLE statement common to the read and write workloads
    pub fn create_table_cql(&self) -> String {
        let mut columns = Vec::with_capacity(1 + self.column_count);
        columns.push("pk BIGINT PRIMARY KEY".to_owned());
        columns.extend(self.data_columns().into_iter().map(|c| c + " BLOB"));

        let compaction_str = match self.compaction {
            Compaction::None => "{'class':'SizeTieredCompactionStrategy', 'enabled':'false'}",
            Compaction::STCS => "{'class':'SizeTieredCompactionStrategy'}",
            Compaction::LCS => "{'class':'LeveledCompactionStrategy'}",
        };

        format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) WITH compaction = {}",
            self.table_name,
            columns.join(", "),
            compaction_str
        )
    }

    pub fn create_table_stmt(&self) -> Statement {
        Statement::new(self.create_table_cql().as_str(), 0)
    }

    /// Returns the statement that drops the table
    pub fn drop_table_cql(&self) -> String {
        format!("DROP TABLE IF EXISTS {}", self.table_name)
    }

    pub fn drop_table_stmt(&self) -> Statement {
        Statement::new(self.drop_table_cql().as_str(), 0)
    }

    /// Returns the INSERT INTO statement common to the read and write workloads
    /// used for populating the table
    pub fn insert_cql(&self) -> String {
        let mut columns = Vec::with_capacity(1 + self.column_count);
        columns.push("pk".to_owned());
        columns.extend(self.data_columns());
        let placeholders = columns.iter().map(|_| "?").join(", ");
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table_name,
            columns.join(", "),
            placeholders
        )
    }
}

/// Generates random blob of data of size `column_size`
pub fn gen_random_blob(len: usize) -> Vec<u8> {
    let mut rng = thread_rng();
    let mut result = Vec::with_capacity(len);
    for _ in 0..len {
        result.push(rng.next_u32() as u8)
    }
    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_table_cql() {
        let s = Schema {
            table_name: "foo".to_owned(),
            column_count: 2,
            compaction: Compaction::STCS,
        };
        assert_eq!(
            s.create_table_cql(),
            "CREATE TABLE IF NOT EXISTS foo (pk BIGINT PRIMARY KEY, data_0 BLOB, data_1 BLOB) \
             WITH compaction = {'class':'SizeTieredCompactionStrategy'}"
        )
    }

    #[test]
    fn insert_cql() {
        let s = Schema {
            table_name: "foo".to_owned(),
            column_count: 2,
            compaction: Compaction::STCS,
        };
        assert_eq!(
            s.insert_cql(),
            "INSERT INTO foo (pk, data_0, data_1) VALUES (?, ?, ?)"
        )
    }
}

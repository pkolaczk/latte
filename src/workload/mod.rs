use std::sync::Arc;

use async_trait::async_trait;
use cassandra_cpp::Statement;
use err_derive::*;
use itertools::Itertools;

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

/// Parameters of a workload.
/// Applies to read and write and possibly other workloads.
pub struct WorkloadConfig {
    pub partitions: u64,
    pub columns: usize,
    pub column_size: usize,
}

impl WorkloadConfig {
    pub fn schema_params_str(&self) -> String {
        format!("{}_{}", self.columns, self.column_size)
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
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            self.table_name,
            columns.join(", ")
        )
    }

    pub fn create_table_stmt(&self) -> Statement {
        Statement::new(self.create_table_cql().as_str(), 0)
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_table_cql() {
        let s = Schema {
            table_name: "foo".to_owned(),
            column_count: 2,
        };
        assert_eq!(
            s.create_table_cql(),
            "CREATE TABLE IF NOT EXISTS foo (pk BIGINT PRIMARY KEY, data_0 BLOB, data_1 BLOB)"
        )
    }

    #[test]
    fn insert_cql() {
        let s = Schema {
            table_name: "foo".to_owned(),
            column_count: 2,
        };
        assert_eq!(
            s.insert_cql(),
            "INSERT INTO foo (pk, data_0, data_1) VALUES (?, ?, ?)"
        )
    }
}

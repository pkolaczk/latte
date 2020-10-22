use core::fmt;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use crate::workload::{Compaction, WorkloadConfig};
use chrono::Utc;
use clap::{AppSettings, Clap};
use serde::{Deserialize, Serialize};

#[derive(Clap, Debug, Serialize, Deserialize)]
pub enum Workload {
    Read,
    Write,
    Null,
}

impl Display for Workload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Workload::Read => write!(f, "read")?,
            Workload::Write => write!(f, "write")?,
            Workload::Null => write!(f, "null")?,
        };
        Ok(())
    }
}

#[derive(Clap, Debug, Serialize, Deserialize)]
#[clap(
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::NextLineHelp),
    setting(AppSettings::DeriveDisplayOrder)
)]
pub struct RunCommand {
    /// Name of the keyspace
    #[clap(short('k'), long, default_value = "latte")]
    pub keyspace: String,

    /// Number of requests per second to send.
    /// If not given the requests will be sent as fast as possible within the parallelism limit
    #[clap(short('r'), long)]
    pub rate: Option<f64>,

    /// Number of non-measured, warmup requests
    #[clap(short('w'), long("warmup"), default_value = "1")]
    pub warmup_count: u64,

    /// Number of measured requests
    #[clap(short('n'), long, default_value = "1000000")]
    pub count: u64,

    /// Number of I/O threads used by the driver
    #[clap(short('t'), long, default_value = "1")]
    pub threads: usize,

    /// Number of connections per io_thread
    #[clap(short('c'), long, default_value = "1")]
    pub connections: usize,

    /// Max number of concurrent async requests
    #[clap(short('p'), long, default_value = "1024")]
    pub parallelism: usize,

    /// Throughput sampling period, in seconds
    #[clap(short('s'), long, default_value = "1.0")]
    pub sampling_period: f64,

    /// Label that will be added to the report to help identifying the test
    #[clap(long)]
    pub tag: Option<String>,

    /// Path to an output file where the JSON report should be written to
    #[clap(short('o'), long)]
    #[serde(skip)]
    pub output: Option<PathBuf>,

    /// Path to a report from another run that should be compared to side-by-side
    #[clap(short('x'), long)]
    pub compare: Option<PathBuf>,

    /// Workload type
    #[clap(arg_enum, name = "workload", required = true)]
    pub workload: Workload,

    /// Total number of partitions in the data-set.
    /// Applies to read and write workloads. Defaults to count.
    #[clap(short('P'), long)]
    pub partitions: Option<u64>,

    /// Number of data cells in a row
    #[clap(short('C'), long, default_value("1"))]
    pub columns: usize,

    /// Size of a single cell's data in bytes
    #[clap(short('S'), long, default_value("16"))]
    pub column_size: usize,

    /// Cassandra compaction strategy to use for the data table
    #[clap(arg_enum, long, default_value("stcs"))]
    pub compaction: Compaction,

    /// List of Cassandra addresses to connect to
    #[clap(name = "addresses", required = true, default_value = "localhost")]
    pub addresses: Vec<String>,

    /// Seconds since 1970-01-01T00:00:00Z
    #[clap(hidden(true), long)]
    pub timestamp: Option<i64>,
}

impl RunCommand {
    pub fn set_timestamp_if_empty(mut self) -> Self {
        if self.timestamp.is_none() {
            self.timestamp = Some(Utc::now().timestamp())
        }
        self
    }

    pub fn workload_config(&self) -> WorkloadConfig {
        WorkloadConfig {
            partitions: self.partitions.unwrap_or(self.count),
            columns: self.columns,
            column_size: self.column_size,
            compaction: self.compaction,
        }
    }
}

#[derive(Clap, Debug)]
pub struct ShowCommand {
    /// Path to the JSON report file
    pub report1: String,
    /// Optional path to another JSON report file
    pub report2: Option<String>,
}

#[derive(Clap, Debug)]
pub enum Command {
    /// Runs the benchmark
    Run(RunCommand),
    /// Displays the report(s) of previously executed benchmark(s)
    Show(ShowCommand),
}

#[derive(Clap, Debug)]
#[clap(
    name = "Cassandra Latency and Throughput Tester",
    author = "Piotr Kołaczkowski <pkolaczk@datastax.com>",
    version = clap::crate_version!(),
    setting(AppSettings::ColoredHelp),
)]
pub struct AppConfig {
    #[clap(subcommand)]
    pub command: Command,
}

use core::fmt;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use chrono::Utc;
use clap::{Clap, AppSettings};
use serde::{Deserialize, Serialize};

#[derive(Clap, Debug, Serialize, Deserialize)]
pub enum Workload {
    Read,
    Write,
}

impl Display for Workload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Workload::Read => write!(f, "read")?,
            Workload::Write => write!(f, "write")?,
        };
        Ok(())
    }
}

/// Latency Tester for Apache Cassandra
#[derive(Clap, Debug, Serialize, Deserialize)]
#[clap(setting(AppSettings::ColoredHelp), setting(AppSettings::DeriveDisplayOrder))]
pub struct Config {
    /// Name of the keyspace
    #[clap(short('k'), long, default_value = "latte")]
    pub keyspace: String,

    /// Number of requests per second to send.
    /// If not given the requests will be sent as fast as possible within the parallelism limit
    #[clap(short('r'), long)]
    pub rate: Option<f32>,

    /// Number of non-measured, warmup requests
    #[clap(short('w'), long("warmup"), default_value = "1")]
    pub warmup_count: u64,

    /// Number of measured requests
    #[clap(short('n'), long, default_value = "1000000")]
    pub count: u64,

    /// Total number of distinct rows in the data-set.
    /// Applies to read and write workloads. Defaults to count.
    #[clap(short('d'), long)]
    pub rows: Option<u64>,

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
    pub sampling_period: f32,

    /// Label that will be added to the report to help identifying the test
    #[clap(short, long)]
    pub label: Option<String>,

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

    /// List of Cassandra addresses to connect to
    #[clap(name = "addresses", required = true, default_value = "localhost")]
    pub addresses: Vec<String>,

    /// Seconds since 1970-01-01T00:00:00Z
    #[clap(hidden(true), long)]
    pub timestamp: Option<i64>,
}

impl Config {
    pub fn set_timestamp_if_empty(mut self) -> Self {
        if self.timestamp.is_none() {
            self.timestamp = Some(Utc::now().timestamp())
        }
        self
    }
}

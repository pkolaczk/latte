use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::anyhow;
use chrono::Utc;
use clap::{AppSettings, Parser};
use serde::{Deserialize, Serialize};

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), anyhow::Error>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow!("invalid KEY=value: no `=` found in `{}`", s))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

/// Controls how long the benchmark should run.
/// We can specify either a time-based duration or a number of calls to perform.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Duration {
    Count(u64),
    Time(tokio::time::Duration),
}

impl Duration {
    pub fn is_not_zero(&self) -> bool {
        match self {
            Duration::Count(cnt) => *cnt > 0,
            Duration::Time(d) => !d.is_zero(),
        }
    }

    pub fn calls(&self) -> Option<u64> {
        if let Duration::Count(c) = self {
            Some(*c)
        } else {
            None
        }
    }

    pub fn seconds(&self) -> Option<f32> {
        if let Duration::Time(d) = self {
            Some(d.as_secs_f32())
        } else {
            None
        }
    }
}

/// If the string is a valid integer, it is assumed to be the number of iterations.
/// If the string additionally contains a time unit, e.g. "s" or "secs", it is parsed
/// as time duration.
impl FromStr for Duration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(i) = s.parse() {
            Ok(Duration::Count(i))
        } else if let Ok(d) = parse_duration::parse(s) {
            Ok(Duration::Time(d))
        } else {
            Err("Required integer number of iterations or time duration".to_string())
        }
    }
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(
setting(AppSettings::NextLineHelp),
setting(AppSettings::DeriveDisplayOrder)
)]
pub struct RunCommand {
    /// Number of requests per second to send.
    /// If not given the requests will be sent as fast as possible within the parallelism limit
    #[clap(short('r'), long, value_name = "COUNT")]
    pub rate: Option<f64>,

    /// Number of iterations or time duration of the warmup phase
    #[clap(short('w'), long("warmup"), default_value = "1", value_name = "TIME | COUNT")]
    pub warmup_duration: Duration,

    /// Number of iterations or time duration of the main benchmark phase
    #[clap(short('d'), long("duration"), default_value = "60s", value_name = "TIME | COUNT")]
    pub run_duration: Duration,

    /// Number of I/O threads used by the driver
    #[clap(short('t'), long, default_value = "1", value_name = "COUNT")]
    pub threads: usize,

    /// Number of connections per IO thread
    #[clap(short('c'), long, default_value = "1", value_name = "COUNT")]
    pub connections: usize,

    /// Max number of concurrent async requests per IO thread
    #[clap(short('p'), long, default_value = "384", value_name = "COUNT")]
    pub concurrency: usize,

    /// Throughput sampling period, in seconds
    #[clap(short('s'), long("sampling"), default_value = "1s", value_name = "TIME | COUNT")]
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

    /// Skips erasing and loading data before running the benchmark.
    #[clap(long)]
    pub no_load: bool,

    /// Path to the workload definition file
    #[clap(name = "workload", required = true)]
    pub workload: PathBuf,

    #[clap(short('P'), parse(try_from_str = parse_key_val),
    number_of_values = 1, multiple_occurrences = true)]
    pub params: Vec<(String, String)>,

    /// List of Cassandra addresses to connect to
    #[clap(name = "addresses", default_value = "localhost")]
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
}

#[derive(Parser, Debug)]
pub struct ShowCommand {
    /// Path to the JSON report file
    pub report1: String,
    /// Optional path to another JSON report file
    pub report2: Option<String>,
}

#[derive(Parser, Debug)]
pub enum Command {
    /// Runs the benchmark
    Run(RunCommand),
    /// Displays the report(s) of previously executed benchmark(s)
    Show(ShowCommand),
}

#[derive(Parser, Debug)]
#[clap(
name = "Cassandra Latency and Throughput Tester",
author = "Piotr Kołaczkowski <pkolaczk@datastax.com>",
version = clap::crate_version ! (),
)]
pub struct AppConfig {
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Deserialize, Default)]
pub struct SchemaConfig {
    #[serde(default)]
    pub script: Vec<String>,
    #[serde(default)]
    pub cql: String,
}

#[derive(Debug, Deserialize)]
pub struct LoadConfig {
    pub count: u64,
    #[serde(default)]
    pub script: Vec<String>,
    #[serde(default)]
    pub cql: String,
}

mod defaults {
    pub fn ratio() -> f64 {
        1.0
    }
}

#[derive(Debug, Deserialize)]
pub struct RunConfig {
    #[serde(default = "defaults::ratio")]
    pub ratio: f64,
    #[serde(default)]
    pub script: Vec<String>,
    #[serde(default)]
    pub cql: String,
}

#[derive(Debug, Deserialize)]
pub struct WorkloadConfig {
    #[serde(default)]
    pub schema: SchemaConfig,
    #[serde(default)]
    pub load: HashMap<String, LoadConfig>,
    pub run: HashMap<String, RunConfig>,
    #[serde(default)]
    pub bindings: HashMap<String, String>,
}

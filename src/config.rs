use anyhow::anyhow;
use chrono::Utc;
use clap::builder::PossibleValue;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

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
/// It is also used for controlling sampling.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Interval {
    Count(u64),
    Time(tokio::time::Duration),
    Unbounded,
}

impl Interval {
    pub fn is_not_zero(&self) -> bool {
        match self {
            Interval::Count(cnt) => *cnt > 0,
            Interval::Time(d) => !d.is_zero(),
            Interval::Unbounded => false,
        }
    }

    pub fn is_bounded(&self) -> bool {
        !matches!(self, Interval::Unbounded)
    }

    pub fn count(&self) -> Option<u64> {
        if let Interval::Count(c) = self {
            Some(*c)
        } else {
            None
        }
    }

    pub fn period(&self) -> Option<tokio::time::Duration> {
        if let Interval::Time(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    pub fn period_secs(&self) -> Option<f32> {
        if let Interval::Time(d) = self {
            Some(d.as_secs_f32())
        } else {
            None
        }
    }
}

/// If the string is a valid integer, it is assumed to be the number of cycles.
/// If the string additionally contains a time unit, e.g. "s" or "secs", it is parsed
/// as time duration.
impl FromStr for Interval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(i) = s.parse() {
            Ok(Interval::Count(i))
        } else if let Ok(d) = parse_duration::parse(s) {
            Ok(Interval::Time(d))
        } else {
            Err("Required integer number of cycles or time duration".to_string())
        }
    }
}

/// Controls the min and max retry interval for retry mechanism
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryDelay {
    pub min: Duration,
    pub max: Duration,
}

impl RetryDelay {
    pub fn new(time: &str) -> Option<Self> {
        let values: Vec<&str> = time.split(',').collect();
        if values.len() > 2 {
            return None;
        }
        let min = parse_duration::parse(values.first().unwrap_or(&"")).ok()?;
        let max = parse_duration::parse(values.get(1).unwrap_or(&"")).unwrap_or(min);
        if min > max {
            None
        } else {
            Some(RetryDelay { min, max })
        }
    }
}

impl FromStr for RetryDelay {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(interval) = RetryDelay::new(s) {
            Ok(interval)
        } else {
            Err(concat!(
                "Expected 1 or 2 parts separated by comma such as '500ms' or '200ms,5s' or '1s'.",
                " First value cannot be bigger than second one.",
            )
            .to_string())
        }
    }
}

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct ConnectionConf {
    /// Number of connections per Cassandra node / Scylla shard.
    #[clap(
        short('c'),
        long("connections"),
        default_value = "1",
        value_name = "COUNT"
    )]
    pub count: NonZeroUsize,

    /// List of Cassandra addresses to connect to.
    #[clap(name = "addresses", default_value = "localhost")]
    pub addresses: Vec<String>,

    /// Cassandra user name
    #[clap(long, env("CASSANDRA_USER"), default_value = "")]
    pub user: String,

    /// Password to use if password authentication is required by the server
    #[clap(long, env("CASSANDRA_PASSWORD"), default_value = "")]
    pub password: String,

    /// Enable SSL
    #[clap(long("ssl"))]
    pub ssl: bool,

    /// Path to the CA certificate file in PEM format
    #[clap(long("ssl-ca"), value_name = "PATH")]
    pub ssl_ca_cert_file: Option<PathBuf>,

    /// Path to the client SSL certificate file in PEM format
    #[clap(long("ssl-cert"), value_name = "PATH")]
    pub ssl_cert_file: Option<PathBuf>,

    /// Path to the client SSL private key file in PEM format
    #[clap(long("ssl-key"), value_name = "PATH")]
    pub ssl_key_file: Option<PathBuf>,

    /// Datacenter name
    #[clap(long("datacenter"), required = false)]
    pub datacenter: Option<String>,

    /// Default CQL query consistency level
    #[clap(long("consistency"), required = false, default_value = "LOCAL_QUORUM")]
    pub consistency: Consistency,

    #[clap(long("request-timeout"), default_value = "5s", value_name = "DURATION", value_parser = parse_duration::parse)]
    pub request_timeout: Duration,

    #[clap(long("retries"), default_value = "3", value_name = "COUNT")]
    pub retries: u64,

    #[clap(
        long("retry-delay"),
        default_value = "100ms,5s",
        value_name = "MIN[,MAX]"
    )]
    pub retry_interval: RetryDelay,
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Consistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalOne,
    #[default]
    LocalQuorum,
    EachQuorum,
}

impl Consistency {
    pub fn scylla_consistency(&self) -> scylla::frame::types::Consistency {
        match self {
            Self::Any => scylla::frame::types::Consistency::Any,
            Self::One => scylla::frame::types::Consistency::One,
            Self::Two => scylla::frame::types::Consistency::Two,
            Self::Three => scylla::frame::types::Consistency::Three,
            Self::Quorum => scylla::frame::types::Consistency::Quorum,
            Self::All => scylla::frame::types::Consistency::All,
            Self::LocalOne => scylla::frame::types::Consistency::LocalOne,
            Self::LocalQuorum => scylla::frame::types::Consistency::LocalQuorum,
            Self::EachQuorum => scylla::frame::types::Consistency::EachQuorum,
        }
    }
}

impl ValueEnum for Consistency {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            Self::Any,
            Self::One,
            Self::Two,
            Self::Three,
            Self::Quorum,
            Self::All,
            Self::LocalOne,
            Self::LocalQuorum,
            Self::EachQuorum,
        ]
    }

    fn from_str(s: &str, _ignore_case: bool) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "any" => Ok(Self::Any),
            "one" | "1" => Ok(Self::One),
            "two" | "2" => Ok(Self::Two),
            "three" | "3" => Ok(Self::Three),
            "quorum" | "q" => Ok(Self::Quorum),
            "all" => Ok(Self::All),
            "local_one" | "localone" | "l1" => Ok(Self::LocalOne),
            "local_quorum" | "localquorum" | "lq" => Ok(Self::LocalQuorum),
            "each_quorum" | "eachquorum" | "eq" => Ok(Self::EachQuorum),
            s => Err(format!("Unknown consistency level {s}")),
        }
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::Any => Some(PossibleValue::new("ANY")),
            Self::One => Some(PossibleValue::new("ONE")),
            Self::Two => Some(PossibleValue::new("TWO")),
            Self::Three => Some(PossibleValue::new("THREE")),
            Self::Quorum => Some(PossibleValue::new("QUORUM")),
            Self::All => Some(PossibleValue::new("ALL")),
            Self::LocalOne => Some(PossibleValue::new("LOCAL_ONE")),
            Self::LocalQuorum => Some(PossibleValue::new("LOCAL_QUORUM")),
            Self::EachQuorum => Some(PossibleValue::new("EACH_QUORUM")),
        }
    }
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(next_line_help = true)]
pub struct EditCommand {
    /// Path to the workload definition file.
    #[clap(name = "workload", required = true, value_name = "PATH")]
    pub workload: PathBuf,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(next_line_help = true)]
pub struct SchemaCommand {
    /// Parameter values passed to the workload, accessible through param! macro.
    #[clap(short('P'), value_parser = parse_key_val::<String, String>, number_of_values = 1)]
    pub params: Vec<(String, String)>,

    /// Path to the workload definition file.
    #[clap(name = "workload", required = true, value_name = "PATH")]
    pub workload: PathBuf,

    // Cassandra connection settings.
    #[clap(flatten)]
    pub connection: ConnectionConf,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(next_line_help = true)]
pub struct LoadCommand {
    /// Number of cycles per second to execute.
    /// If not given, the load cycles will be executed as fast as possible.
    #[clap(short('r'), long, value_name = "COUNT")]
    pub rate: Option<f64>,

    /// Number of worker threads used by the driver.
    #[clap(short('t'), long, default_value = "1", value_name = "COUNT")]
    pub threads: NonZeroUsize,

    /// Max number of concurrent async requests per thread during data loading phase.
    #[clap(long, default_value = "512", value_name = "COUNT")]
    pub concurrency: NonZeroUsize,

    /// Parameter values passed to the workload, accessible through param! macro.
    #[clap(short('P'), value_parser = parse_key_val::<String, String>, number_of_values = 1)]
    pub params: Vec<(String, String)>,

    /// Don't display the progress bar.
    #[clap(short, long)]
    pub quiet: bool,

    /// Path to the workload definition file.
    #[clap(name = "workload", required = true, value_name = "PATH")]
    pub workload: PathBuf,

    // Cassandra connection settings.
    #[clap(flatten)]
    pub connection: ConnectionConf,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(next_line_help = true)]
pub struct RunCommand {
    /// Number of cycles per second to execute.
    /// If not given, the benchmark cycles will be executed as fast as possible.
    #[clap(short('r'), long, value_name = "COUNT")]
    pub rate: Option<f64>,

    /// Number of cycles or duration of the warmup phase.
    #[clap(
        short('w'),
        long("warmup"),
        default_value = "1",
        value_name = "TIME | COUNT"
    )]
    pub warmup_duration: Interval,

    /// Number of cycles or duration of the main benchmark phase.
    #[clap(
        short('d'),
        long("duration"),
        default_value = "60s",
        value_name = "TIME | COUNT"
    )]
    pub run_duration: Interval,

    /// Number of worker threads used by the driver.
    #[clap(short('t'), long, default_value = "1", value_name = "COUNT")]
    pub threads: NonZeroUsize,

    /// Max number of concurrent async requests per thread during the main benchmark phase.
    #[clap(short('p'), long, default_value = "128", value_name = "COUNT")]
    pub concurrency: NonZeroUsize,

    /// Throughput sampling period, in seconds.
    #[clap(
        short('s'),
        long("sampling"),
        default_value = "1s",
        value_name = "TIME | COUNT"
    )]
    pub sampling_interval: Interval,

    /// Label that will be added to the report to help identifying the test
    #[clap(long("tag"), number_of_values = 1)]
    pub tags: Vec<String>,

    /// Path to an output file or directory where the JSON report should be written to.
    #[clap(short('o'), long)]
    #[serde(skip)]
    pub output: Option<PathBuf>,

    /// Path to a report from another earlier run that should be compared to side-by-side
    #[clap(short('b'), long, value_name = "PATH")]
    pub baseline: Option<PathBuf>,

    /// Path to the workload definition file.
    #[clap(name = "workload", required = true, value_name = "PATH")]
    pub workload: PathBuf,

    /// Function of the workload to invoke.
    #[clap(long, short('f'), required = false, default_value = "run")]
    pub function: String,

    /// Parameter values passed to the workload, accessible through param! macro.
    #[clap(short('P'), value_parser = parse_key_val::<String, String>, number_of_values = 1)]
    pub params: Vec<(String, String)>,

    /// Don't display the progress bar.
    #[clap(short, long)]
    pub quiet: bool,

    // Cassandra connection settings.
    #[clap(flatten)]
    pub connection: ConnectionConf,

    /// Seconds since 1970-01-01T00:00:00Z
    #[clap(hide = true, long)]
    pub timestamp: Option<i64>,

    #[clap(skip)]
    pub cluster_name: Option<String>,

    #[clap(skip)]
    pub cass_version: Option<String>,

    #[clap(skip)]
    pub id: Option<String>,
}

impl RunCommand {
    pub fn set_timestamp_if_empty(mut self) -> Self {
        if self.timestamp.is_none() {
            self.timestamp = Some(Utc::now().timestamp())
        }
        self
    }

    /// Returns the value of parameter under given key.
    /// If key doesn't exist, or parameter is not a number, returns `None`.
    pub fn get_param(&self, key: &str) -> Option<f64> {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|v| v.1.parse().ok())
    }
}

#[derive(Parser, Debug)]
pub struct ListCommand {
    /// Lists only the runs of specified workload.
    #[clap()]
    pub workload: Option<String>,

    /// Lists only the runs of given function.
    #[clap(long, short('f'))]
    pub function: Option<String>,

    /// Lists only the runs with specified tags.
    #[clap(long("tag"), number_of_values = 1)]
    pub tags: Vec<String>,

    /// Path to JSON reports directory where the JSON reports were written to.
    #[clap(long, short('o'), long, default_value = ".", number_of_values = 1)]
    pub output: Vec<PathBuf>,

    /// Descends into subdirectories recursively.
    #[clap(short('r'), long)]
    pub recursive: bool,
}

#[derive(Parser, Debug)]
pub struct ShowCommand {
    /// Path to the JSON report file
    #[clap(value_name = "PATH")]
    pub report: PathBuf,

    /// Optional path to another JSON report file
    #[clap(short('b'), long, value_name = "PATH")]
    pub baseline: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct HdrCommand {
    /// Path to the input JSON report file
    #[clap(value_name = "PATH")]
    pub report: PathBuf,

    /// Output file; if not given, the hdr log gets printed to stdout
    #[clap(short('o'), long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Optional tag prefix to add to each histogram
    #[clap(long, value_name = "STRING")]
    pub tag: Option<String>,
}

#[derive(Parser, Debug)]
pub struct PlotCommand {
    /// Path to the input JSON report file(s)
    #[clap(value_name = "PATH", required = true)]
    pub reports: Vec<PathBuf>,

    /// Plot given response time percentiles. Can be used multiple times.
    #[clap(short, long("percentile"), number_of_values = 1)]
    pub percentiles: Vec<f64>,

    /// Plot throughput.
    #[clap(short, long("throughput"))]
    pub throughput: bool,

    /// Write output to the given file.
    #[clap(short('o'), long, value_name = "PATH")]
    pub output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Opens the specified workload script file for editing.
    ///
    /// Searches for the script on the workload search path. Workload files
    /// are first searched in the current working directory, next in the paths
    /// specified by LATTE_WORKLOAD_PATH environment variable. If the variable
    /// is not defined, `/usr/share/latte/workloads` and `.local/share/latte/workloads`
    /// are searched.
    ///
    /// Opens the editor pointed by LATTE_EDITOR or EDITOR environment variable.
    /// If no variable is set, tries to launch vi.
    ///
    Edit(EditCommand),

    /// Creates the database schema by invoking the `schema` function of the workload script.
    ///
    /// The function should remove the old schema if present.
    /// Calling this is likely to remove data from the database.
    Schema(SchemaCommand),

    /// Erases and generates fresh data needed for the benchmark by invoking the `erase` and `load`
    /// functions of the workload script.
    ///
    /// Running this command is typically needed by read benchmarks.
    /// You need to create the schema before.
    Load(LoadCommand),

    /// Runs the benchmark.
    ///
    /// Prints nicely formatted statistics to the standard output.
    /// Additionally dumps all data into a JSON report file.
    Run(RunCommand),

    /// Lists benchmark reports saved in the current or specified directory
    /// with summaries of their results.
    List(ListCommand),

    /// Displays the report(s) of previously executed benchmark(s).
    ///
    /// Can compare two runs.
    Show(ShowCommand),

    /// Exports histograms as a compressed HDR interval log.
    ///
    /// To be used with HdrHistogram (https://github.com/HdrHistogram/HdrHistogram).
    /// Timestamps are given in seconds since Unix epoch.
    /// Response times are recorded in nanoseconds.
    Hdr(HdrCommand),

    /// Plots recorded samples. Saves output in SVG format.
    Plot(PlotCommand),
}

#[derive(Parser, Debug)]
#[command(
name = "Cassandra Latency and Throughput Tester",
author = "Piotr Kołaczkowski <pkolaczk@datastax.com>",
version = clap::crate_version ! (),
)]
pub struct AppConfig {
    /// Name of the log file.
    ///
    /// If not given, the log file name will be created automatically based on the current timestamp.
    /// If relative path given, the file will be placed in the directory determined by `log-dir`.
    /// The log file will store detailed information about e.g. query errors.
    #[clap(long("log-file"))]
    pub log_file: Option<PathBuf>,

    /// Directory where log files are stored.
    #[clap(long("log-dir"), env("LATTE_LOG_DIR"), default_value = ".")]
    pub log_dir: PathBuf,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Deserialize, Default)]
#[allow(unused)]
pub struct SchemaConfig {
    #[serde(default)]
    pub script: Vec<String>,
    #[serde(default)]
    pub cql: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
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
#[allow(unused)]
pub struct RunConfig {
    #[serde(default = "defaults::ratio")]
    pub ratio: f64,
    #[serde(default)]
    pub script: Vec<String>,
    #[serde(default)]
    pub cql: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct WorkloadConfig {
    #[serde(default)]
    pub schema: SchemaConfig,
    #[serde(default)]
    pub load: HashMap<String, LoadConfig>,
    pub run: HashMap<String, RunConfig>,
    #[serde(default)]
    pub bindings: HashMap<String, String>,
}

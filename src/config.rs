use aerospike::{CommitLevel, ConsistencyLevel, Priority, RecordExistsAction};
use anyhow::anyhow;
use chrono::Utc;
use clap::builder::PossibleValue;
use clap::{Args, Parser, ValueEnum};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tokio_postgres::config::{ChannelBinding, LoadBalanceHosts, SslMode, SslNegotiation};

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
pub struct ScyllaConnectionConf {
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
}

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct PostgresConnectionConf {
    #[clap(long("dbname"), default_value = "postgres")]
    pub dbname: String,

    #[clap(long("port"), default_value = "5432")]
    pub port: u16,

    #[clap(long("ssl-mode"), default_value = "disable")]
    pub ssl_mode: CliSslMode,

    #[clap(long("ssl-negotiation"), default_value = "direct")]
    pub ssl_negotiation: CliSslNegotiation,

    #[clap(long("channel-binding"), default_value = "prefer")]
    pub channel_binding: CliChannelBinding,

    #[clap(long("load-balance-hosts"), default_value = "disable")]
    pub load_balance_hosts: CliLoadBalanceHosts,
}

// CLI Enum for SslMode
#[derive(Debug, Clone, ValueEnum, Serialize, Deserialize)]
pub enum CliSslMode {
    Disable,
    Prefer,
    Require,
}

// CLI Enum for SslNegotiation
#[derive(Debug, Clone, ValueEnum, Serialize, Deserialize)]
pub enum CliSslNegotiation {
    Postgres,
    Direct,
}

// CLI Enum for ChannelBinding
#[derive(Debug, Clone, ValueEnum, Serialize, Deserialize)]
pub enum CliChannelBinding {
    Disable,
    Prefer,
    Require,
}

// CLI Enum for LoadBalanceHosts
#[derive(Debug, Clone, ValueEnum, Serialize, Deserialize)]
pub enum CliLoadBalanceHosts {
    Disable,
    Random,
}

// Mappers to Postgres Enums
impl CliSslMode {
    pub fn to_postgres_enum(&self) -> SslMode {
        match self {
            CliSslMode::Disable => SslMode::Disable,
            CliSslMode::Prefer => SslMode::Prefer,
            CliSslMode::Require => SslMode::Require,
        }
    }
}

impl CliSslNegotiation {
    pub fn to_postgres_enum(&self) -> SslNegotiation {
        match self {
            CliSslNegotiation::Postgres => SslNegotiation::Postgres,
            CliSslNegotiation::Direct => SslNegotiation::Direct,
        }
    }
}

impl CliChannelBinding {
    pub fn to_postgres_enum(&self) -> ChannelBinding {
        match self {
            CliChannelBinding::Disable => ChannelBinding::Disable,
            CliChannelBinding::Prefer => ChannelBinding::Prefer,
            CliChannelBinding::Require => ChannelBinding::Require,
        }
    }
}

impl CliLoadBalanceHosts {
    pub fn to_postgres_enum(&self) -> LoadBalanceHosts {
        match self {
            CliLoadBalanceHosts::Disable => LoadBalanceHosts::Disable,
            CliLoadBalanceHosts::Random => LoadBalanceHosts::Random,
        }
    }
}

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct AerospikeConnectionConf {
    #[clap(long("namespace"), default_value = "default")]
    pub namespace: String,

    #[clap(long("set"), default_value = "default")]
    pub set: String,

    #[clap(long("max-retries"), default_value = "2")]
    pub max_retries: usize,

    #[clap(long("sleep-between-retries"), default_value = "1ms", value_name = "DURATION", value_parser = parse_duration::parse
    )]
    pub sleep_between_retries: Duration,

    #[clap(long("expiration-seconds"), default_value = "7200")]
    pub expiration_seconds: u32,

    /// Default CQL query consistency level
    #[clap(long("read-priority"), required = false, default_value = "high")]
    pub read_priority: CliPriority,

    /// Default CQL query consistency level
    #[clap(long("write-priority"), required = false, default_value = "medium")]
    pub write_priority: CliPriority,

    /// Default CQL query consistency level
    #[clap(long("consistency-level"), required = false, default_value = "one")]
    pub consistency_level: CliConsistencyLevel,

    /// Default CQL query consistency level
    #[clap(
        long("record-exists-action"),
        required = false,
        default_value = "replace"
    )]
    pub record_exists_action: CliRecordExistsAction,

    /// Default CQL query consistency level
    #[clap(long("commit-level"), required = false, default_value = "all")]
    pub commit_level: CliCommitLevel,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct ConnectionConf {
    /// Number of connections per Cassandra node / Scylla shard.
    #[clap(long("db"))]
    pub db: DBEngine,

    /// Number of connections per node.
    #[clap(
        short('c'),
        long("connections"),
        default_value = "1",
        value_name = "COUNT"
    )]
    pub count: NonZeroUsize,

    #[clap(long("request-timeout"), default_value = "50ms", value_name = "DURATION", value_parser = parse_duration::parse
    )]
    pub request_timeout: Duration,

    /// List of Cassandra addresses to connect to.
    #[clap(name = "addresses", default_value = "localhost")]
    pub addresses: Vec<String>,

    /// Cassandra user name
    #[clap(long, env("CASSANDRA_USER"), default_value = "")]
    pub user: String,

    /// Password to use if password authentication is required by the server
    #[clap(long, env("CASSANDRA_PASSWORD"), default_value = "")]
    pub password: String,

    #[clap(flatten)]
    pub scylla_connection_conf: ScyllaConnectionConf,

    #[clap(flatten)]
    pub aerospike_connection_conf: AerospikeConnectionConf,

    #[clap(flatten)]
    pub postgres_connection_conf: PostgresConnectionConf,

    #[clap(flatten)]
    pub retry_strategy: RetryStrategy,
}

#[derive(Args, Copy, Clone, Debug, Serialize, Deserialize)]
pub struct RetryStrategy {
    /// Maximum number of times to retry a failed query
    ///
    /// Unless `retry-on-all-errors` flag is set, retries happens only for
    /// timeout / overload errors.
    #[clap(long("retries"), default_value = "3", value_name = "COUNT")]
    pub retries: u64,

    /// Controls the delay to apply before each retry
    ///
    /// If the maximum delay is unspecified, the same delay is used for each retry.
    /// If the maximum delay is not specified, the retries use exponential back-off delay strategy.
    /// The first retry uses the minimum delay, and each subsequent retry doubles the delay until
    /// it reaches the max delay specified.
    #[clap(long, default_value = "100ms,5s", value_name = "MIN[,MAX]")]
    pub retry_delay: RetryDelay,

    /// Retries queries after all errors, even it the error is considered non-recoverable.   
    #[clap(long)]
    pub retry_on_all_errors: bool,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum, Serialize, Deserialize)]
pub enum DBEngine {
    Scylla,
    Aerospike,
    Foundation,
    PostgreSQL,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeightedFunction {
    pub name: String,
    pub weight: f64,
}

impl FromStr for WeightedFunction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.contains(':') {
            Ok(Self {
                name: s.to_string(),
                weight: 1.0,
            })
        } else if let Some((name, weight)) = s.split(':').collect_tuple() {
            let weight: f64 = weight
                .parse()
                .map_err(|e| format!("Invalid weight value: {e}"))?;
            if weight < 0.0 {
                return Err("Weight must be greater or equal 0.0".to_string());
            }
            Ok(Self {
                name: name.to_string(),
                weight,
            })
        } else {
            Err("Failed to parse function specification. Expected <NAME>[:WEIGHT]".to_string())
        }
    }
}

impl Display for WeightedFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.weight)
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

    /// The initial value of the cycle counter.
    ///
    /// Normally the cycle counter starts from 0, but you can start from a different value.
    /// This is particularly useful for splitting the workload into
    /// parts executed from different client nodes.
    #[clap(long, default_value = "0")]
    pub start_cycle: i64,

    /// The maximum value of the cycle counter at which the cycle counter wraps-around back
    /// to the start value.
    #[clap(long, default_value = "9223372036854775807")]
    pub end_cycle: i64,

    /// Number of worker threads used by the driver.
    #[clap(short('t'), long, default_value = "1", value_name = "COUNT")]
    pub threads: NonZeroUsize,

    /// Max number of concurrent async requests per thread during the main benchmark phase.
    #[clap(short('p'), long, default_value = "128", value_name = "COUNT")]
    pub concurrency: NonZeroUsize,

    /// Sampling period, in seconds.
    ///
    /// While running the workload, periodically takes a snapshot of the statistics
    /// and records it as a separate data point in the log. At the end, the log gets saved to
    /// the final report. The sampling log can be used later for generating plots
    /// or HDR histogram logs for further detailed data analysis.
    ///
    /// Sampling period does not affect the value of the final statistics computed
    /// for the whole run. You'll not get more accurate measurements by sampling more frequently
    /// (assuming the same total length of the run).   
    ///
    /// The sampling log is used for analyzing throughput fluctuations and the number of samples
    /// does affect the accuracy of estimating the throughput error to some degree.
    /// The throughput error estimate may be inaccurate if you collect less than 10 samples.  
    #[clap(
        short('s'),
        long("sampling"),
        default_value = "1s",
        value_name = "TIME | COUNT"
    )]
    pub sampling_interval: Interval,

    /// Doesn't keep the sampling log in the report
    ///
    /// Use this option when you want to run the workload for a very long time, to keep memory
    /// usage low and steady. The sample log will still be printed while running, but its data won't
    /// be kept in memory nor saved to the final report.
    ///
    /// Caution: you will not be able to later generate plots nor HDR histogram logs from the
    /// report if you enable this option.   
    #[clap(long)]
    pub drop_sampling_log: bool,

    /// Label that will be added to the report to help identifying the test
    #[clap(long("tag"), value_delimiter = ',')]
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
    #[clap(
        long,
        short('f'),
        required = false,
        default_value = "run",
        value_delimiter = ','
    )]
    pub functions: Vec<WeightedFunction>,

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
    #[clap(long("tag"), value_delimiter = ',')]
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
    version = clap::crate_version!(),
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

/// CLI Priority of operations on database server.
#[derive(Debug, Clone, clap::ValueEnum, Serialize, Deserialize)]
pub enum CliPriority {
    /// Default determines that the server defines the priority.
    Default,

    /// Low determines that the server should run the operation in a background thread.
    Low,

    /// Medium determines that the server should run the operation at medium priority.
    Medium,

    /// High determines that the server should run the operation at the highest priority.
    High,
}

impl CliPriority {
    /// Maps `CliPriority` to the SDK `Priority` enum.
    pub fn to_sdk_priority(&self) -> Priority {
        match self {
            CliPriority::Default => Priority::Default,
            CliPriority::Low => Priority::Low,
            CliPriority::Medium => Priority::Medium,
            CliPriority::High => Priority::High,
        }
    }
}

/// CLI consistency level for read operations.
#[derive(Debug, Clone, clap::ValueEnum, Serialize, Deserialize)]
pub enum CliConsistencyLevel {
    /// ConsistencyOne indicates only a single replica should be consulted in the read operation.
    One,

    /// ConsistencyAll indicates that all replicas should be consulted in the read operation.
    All,
}

impl CliConsistencyLevel {
    /// Maps `CliConsistencyLevel` to the SDK `ConsistencyLevel` enum.
    pub fn to_sdk_consistency_level(&self) -> ConsistencyLevel {
        match self {
            CliConsistencyLevel::One => ConsistencyLevel::ConsistencyOne,
            CliConsistencyLevel::All => ConsistencyLevel::ConsistencyAll,
        }
    }
}

/// CLI action to perform when a record exists.
#[derive(Debug, Clone, clap::ValueEnum, Serialize, Deserialize)]
pub enum CliRecordExistsAction {
    /// Update means: Create or update record.
    /// Merge write command bins with existing bins.
    Update,

    /// UpdateOnly means: Update record only. Fail if record does not exist.
    UpdateOnly,

    /// Replace means: Create or replace record.
    /// Delete existing bins not referenced by write command bins.
    Replace,

    /// ReplaceOnly means: Replace record only. Fail if record does not exist.
    ReplaceOnly,

    /// CreateOnly means: Create only. Fail if record exists.
    CreateOnly,
}

impl CliRecordExistsAction {
    /// Maps `CliRecordExistsAction` to the SDK `RecordExistsAction` enum.
    pub fn to_sdk_record_exists_action(&self) -> RecordExistsAction {
        match self {
            CliRecordExistsAction::Update => RecordExistsAction::Update,
            CliRecordExistsAction::UpdateOnly => RecordExistsAction::UpdateOnly,
            CliRecordExistsAction::Replace => RecordExistsAction::Replace,
            CliRecordExistsAction::ReplaceOnly => RecordExistsAction::ReplaceOnly,
            CliRecordExistsAction::CreateOnly => RecordExistsAction::CreateOnly,
        }
    }
}

/// CLI commit level for record writes.
#[derive(Debug, Clone, clap::ValueEnum, Serialize, Deserialize)]
pub enum CliCommitLevel {
    /// CommitAll indicates the server should wait until successfully committing master and all replicas.
    All,

    /// CommitMaster indicates the server should wait until successfully committing master only.
    Master,
}

impl CliCommitLevel {
    /// Maps `CliCommitLevel` to the SDK `CommitLevel` enum.
    pub fn to_sdk_commit_level(&self) -> CommitLevel {
        match self {
            CliCommitLevel::All => CommitLevel::CommitAll,
            CliCommitLevel::Master => CommitLevel::CommitMaster,
        }
    }
}

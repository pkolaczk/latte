use crate::config::{DBEngine, RunCommand, WeightedFunction};
use crate::stats::percentiles::Percentile;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Mean, Sample, Significance};
use chrono::{DateTime, Local, TimeZone};
use console::{pad_str, style, Alignment};
use core::fmt;
use err_derive::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::io::{BufReader, BufWriter};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fs, io};
use strum::IntoEnumIterator;
use table::Row;

pub mod plot;
pub mod table;

/// A standard error is multiplied by this factor to get the error margin.
/// For a normally distributed random variable,
/// this should give us 0.999 confidence the expected value is within the (result +- error) range.
const ERR_MARGIN: f64 = 3.29;

#[derive(Debug, Error)]
pub enum ReportLoadError {
    #[error(display = "{}", _0)]
    IO(#[source] io::Error),
    #[error(display = "{}", _0)]
    Deserialize(#[source] serde_json::Error),
}

/// Keeps all data we want to save in a report:
/// run metadata, configuration and results
#[derive(Serialize, Deserialize)]
pub struct Report {
    pub conf: RunCommand,
    pub percentiles: Vec<f32>,
    pub result: BenchmarkStats,
}

impl Report {
    /// Creates a new report from given configuration and results
    pub fn new(conf: RunCommand, result: BenchmarkStats) -> Report {
        let percentiles: Vec<f32> = Percentile::iter().map(|p| p.value() as f32).collect();
        Report {
            conf,
            percentiles,
            result,
        }
    }
    /// Loads benchmark results from a JSON file
    pub fn load(path: &Path) -> Result<Report, ReportLoadError> {
        let file = fs::File::open(path)?;
        let reader = BufReader::new(file);
        let report = serde_json::from_reader(reader)?;
        Ok(report)
    }

    /// Saves benchmark results to a JSON file
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let f = fs::File::create(path)?;
        let writer = BufWriter::new(f);
        serde_json::to_writer_pretty(writer, &self)?;
        Ok(())
    }

    pub fn summary(&self) -> Summary {
        Summary {
            workload: self.conf.workload.clone(),
            functions: self
                .conf
                .functions
                .iter()
                .map(WeightedFunction::to_string)
                .join(", "),
            timestamp: self
                .conf
                .timestamp
                .and_then(|ts| Local.timestamp_opt(ts, 0).latest()),
            tags: self.conf.tags.clone(),
            params: self.conf.params.clone(),
            rate: self.conf.rate,
            throughput: self.result.cycle_throughput.value,
            latency_p50: self
                .result
                .request_latency
                .as_ref()
                .map(|t| t.percentiles.get(Percentile::P50).value),
            latency_p99: self
                .result
                .request_latency
                .as_ref()
                .map(|t| t.percentiles.get(Percentile::P99).value),
        }
    }
}

/// A displayable, optional value with an optional error.
/// Controls formatting options such as precision.
/// Thanks to this wrapper we can format all numeric values in a consistent way.
pub struct Quantity<T> {
    pub value: Option<T>,
    pub error: Option<T>,
    pub precision: Option<usize>,
}

impl<T> Quantity<T> {
    pub fn new(value: Option<T>) -> Quantity<T> {
        Quantity {
            value,
            error: None,
            precision: None,
        }
    }

    pub fn with_precision(mut self, precision: usize) -> Self {
        self.precision = Some(precision);
        self
    }

    pub fn with_error(mut self, e: Option<T>) -> Self {
        self.error = e;
        self
    }
}

impl<T: Display> Quantity<T> {
    fn format_error(&self) -> String {
        let prec = self.precision.unwrap_or_default();
        match &self.error {
            None => "".to_owned(),
            Some(e) => format!("± {:<6.prec$}", e, prec = prec),
        }
    }
}

impl<T: Display> From<T> for Quantity<T> {
    fn from(value: T) -> Self {
        Quantity::new(Some(value))
    }
}

impl<T: Display> From<Option<T>> for Quantity<T> {
    fn from(value: Option<T>) -> Self {
        Quantity::new(value)
    }
}

impl From<Mean> for Quantity<f64> {
    fn from(m: Mean) -> Self {
        Quantity::new(Some(m.value)).with_error(m.std_err.map(|e| e * ERR_MARGIN))
    }
}

impl From<Option<Mean>> for Quantity<f64> {
    fn from(m: Option<Mean>) -> Self {
        Quantity::new(m.map(|mean| mean.value))
            .with_error(m.and_then(|mean| mean.std_err.map(|e| e * ERR_MARGIN)))
    }
}

impl<T: Display> Display for Quantity<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match (&self.value, self.precision) {
            (None, _) => write!(f, "{}", " ".repeat(18)),
            (Some(v), None) => write!(
                f,
                "{value:9} {error:8}",
                value = style(v).bright().for_stdout(),
                error = style(self.format_error()).dim().for_stdout(),
            ),
            (Some(v), Some(prec)) => write!(
                f,
                "{value:9.prec$} {error:8}",
                value = style(v).bright().for_stdout(),
                prec = prec,
                error = style(self.format_error()).dim().for_stdout(),
            ),
        }
    }
}

/// Wrapper for displaying an optional value.
/// If value is `Some`, displays the original value.
/// If value is `None`, displays nothing (empty string).
struct OptionDisplay<T>(Option<T>);

impl<T: Display> Display for OptionDisplay<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => write!(f, ""),
            Some(v) => write!(f, "{v}"),
        }
    }
}

trait Rational {
    fn ratio(a: Self, b: Self) -> Option<f32>;
}

impl Rational for f32 {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        Some(a / b)
    }
}

impl Rational for f64 {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        Some((a / b) as f32)
    }
}

impl Rational for u64 {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        Some(a as f32 / b as f32)
    }
}

impl Rational for i64 {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        Some(a as f32 / b as f32)
    }
}

impl Rational for usize {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        Some(a as f32 / b as f32)
    }
}

impl Rational for NonZeroUsize {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        Some(a.get() as f32 / b.get() as f32)
    }
}

impl<T: Rational> Rational for OptionDisplay<T> {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        a.0.and_then(|a| b.0.and_then(|b| Rational::ratio(a, b)))
    }
}

impl<T: Rational + Display> Rational for Quantity<T> {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        a.value
            .and_then(|a| b.value.and_then(|b| Rational::ratio(a, b)))
    }
}

impl Rational for String {
    fn ratio(_a: Self, _b: Self) -> Option<f32> {
        None
    }
}

impl Rational for &str {
    fn ratio(_a: Self, _b: Self) -> Option<f32> {
        None
    }
}

impl Display for Significance {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let levels = [0.000001, 0.00001, 0.0001, 0.001, 0.01];
        let stars = "*".repeat(levels.iter().filter(|&&l| l > self.0).count());
        let s = format!("{:7.5}  {:5}", self.0, stars);
        if self.0 <= 0.01 {
            write!(f, "{}", style(s).cyan().bright())
        } else {
            write!(f, "{}", style(s).dim())
        }
    }
}
/// A single line of text report
struct Line<M, V, F>
where
    M: Display + Rational,
    F: Fn(V) -> M,
{
    /// Text label
    pub label: String,
    /// Unit of measurement
    pub unit: String,
    /// 1 means the more of the quantity the better, -1 means the more of it the worse, 0 is neutral
    pub orientation: i8,
    /// First object to measure
    pub v1: V,
    /// Second object to measure
    pub v2: Option<V>,
    /// Statistical significance level
    pub significance: Option<Significance>,
    /// Measurement function
    pub f: F,
}

impl<M, V, F> Line<M, V, F>
where
    M: Display + Rational,
    V: Copy,
    F: Fn(V) -> M,
{
    fn new(label: String, unit: String, orientation: i8, v1: V, v2: Option<V>, f: F) -> Self {
        Line {
            label,
            unit,
            orientation,
            v1,
            v2,
            significance: None,
            f,
        }
    }

    fn into_box(self) -> Box<Self> {
        Box::new(self)
    }

    fn with_orientation(mut self, orientation: i8) -> Self {
        self.orientation = orientation;
        self
    }

    fn with_significance(mut self, s: Option<Significance>) -> Self {
        self.significance = s;
        self
    }

    /// Measures the object `v` by applying `f` to it and formats the measurement result.
    /// If the object is None, returns an empty string.
    fn fmt_measurement(&self, v: Option<V>) -> String {
        v.map(|v| format!("{}", (self.f)(v)))
            .unwrap_or_else(|| "".to_owned())
    }

    /// Computes the relative difference between v2 and v1 as: 100.0 * f(v2) / f(v1) - 100.0.
    /// Then formats the difference as percentage.
    /// If any of the values are missing, returns an empty String
    fn fmt_relative_change(&self, direction: i8, significant: bool) -> String {
        self.v2
            .and_then(|v2| {
                let m1 = (self.f)(self.v1);
                let m2 = (self.f)(v2);
                let ratio = Rational::ratio(m1, m2);
                ratio.map(|r| {
                    let mut diff = 100.0 * (r - 1.0);
                    if diff.is_nan() {
                        diff = 0.0;
                    }
                    let good = diff * direction as f32;
                    let diff = format!("{diff:+7.1}%");
                    let styled = if good == 0.0 || !significant {
                        style(diff).dim()
                    } else if good > 0.0 {
                        style(diff).bright().green()
                    } else {
                        style(diff).bright().red()
                    };
                    format!("{styled}")
                })
            })
            .unwrap_or_default()
    }

    fn fmt_unit(&self) -> String {
        match self.unit.as_str() {
            "" => "".to_string(),
            u => format!("[{u}]"),
        }
    }
}

impl<M, V, F> Display for Line<M, V, F>
where
    M: Display + Rational,
    F: Fn(V) -> M,
    V: Copy,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // if v2 defined, put v2 on left
        let m1 = self.fmt_measurement(self.v2.or(Some(self.v1)));
        let m2 = self.fmt_measurement(self.v2.map(|_| self.v1));
        let is_significant = match self.significance {
            None => false,
            Some(s) => s.0 <= 0.01,
        };
        write!(
            f,
            "{label:>16} {unit:>9}  {m1} {m2}  {cmp:6}     {signif}",
            label = style(&self.label).yellow().bold().for_stdout(),
            unit = style(self.fmt_unit()).yellow(),
            m1 = pad_str(m1.as_str(), 30, Alignment::Left, None),
            m2 = pad_str(m2.as_str(), 30, Alignment::Left, None),
            cmp = self.fmt_relative_change(self.orientation, is_significant),
            signif = match &self.significance {
                Some(s) => format!("{s}"),
                None => "".to_owned(),
            }
        )
    }
}

const REPORT_WIDTH: usize = 124;

fn fmt_section_header(name: &str) -> String {
    format!(
        "{} {}",
        style(name).yellow().bold().bright().for_stdout(),
        style("═".repeat(REPORT_WIDTH - name.len() - 1))
            .yellow()
            .bold()
            .bright()
            .for_stdout()
    )
}

fn fmt_horizontal_line() -> String {
    format!("{}", style("─".repeat(REPORT_WIDTH)).yellow().dim())
}

fn fmt_cmp_header(display_significance: bool) -> String {
    let header = format!(
        "{} {} {}",
        " ".repeat(27),
        "───────────── A ─────────────  ────────────── B ────────────     Change    ",
        if display_significance {
            "P-value  Signif."
        } else {
            ""
        }
    );
    format!("{}", style(header).yellow().bold().for_stdout())
}

pub struct RunConfigCmp<'a> {
    pub v1: &'a RunCommand,
    pub v2: Option<&'a RunCommand>,
}

impl RunConfigCmp<'_> {
    fn line<S, M, F>(&self, label: S, unit: &str, f: F) -> Box<Line<M, &RunCommand, F>>
    where
        S: ToString,
        M: Display + Rational,
        F: Fn(&RunCommand) -> M,
    {
        Box::new(Line::new(
            label.to_string(),
            unit.to_string(),
            0,
            self.v1,
            self.v2,
            f,
        ))
    }

    fn format_time(&self, conf: &RunCommand, format: &str) -> String {
        format_time(conf.timestamp, format)
    }

    /// Returns the set union of custom user parameters in both configurations.
    fn param_names(&self) -> BTreeSet<&String> {
        let mut keys = BTreeSet::new();
        keys.extend(self.v1.params.iter().map(|x| &x.0));
        if let Some(v2) = self.v2 {
            keys.extend(v2.params.iter().map(|x| &x.0));
        }
        keys
    }
}

impl Display for RunConfigCmp<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", fmt_section_header("CONFIG"))?;
        if self.v2.is_some() {
            writeln!(f, "{}", fmt_cmp_header(false))?;
        }

        let lines: Vec<Box<dyn Display>> = vec![
            self.line("Date", "", |conf| self.format_time(conf, "%a, %d %b %Y")),
            self.line("Time", "", |conf| self.format_time(conf, "%H:%M:%S %z")),
            self.line("Cluster", "", |conf| {
                OptionDisplay(conf.cluster_name.clone())
            }),
            self.line("Datacenter", "", |conf| {
                conf.connection
                    .scylla_connection_conf
                    .datacenter
                    .clone()
                    .unwrap_or_default()
            }),
            self.line("Cass. version", "", |conf| {
                OptionDisplay(conf.cass_version.clone())
            }),
            self.line("Workload", "", |conf| {
                conf.workload
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default()
            }),
            self.line("Function(s)", "", |conf| {
                conf.functions
                    .iter()
                    .map(WeightedFunction::to_string)
                    .join(", ")
            }),
            self.line("Consistency", "", |conf| {
                conf.connection
                    .scylla_connection_conf
                    .consistency
                    .scylla_consistency()
                    .to_string()
            }),
            self.line("Tags", "", |conf| conf.tags.iter().join(", ")),
        ];

        for l in lines {
            writeln!(f, "{l}")?;
        }

        writeln!(f, "{}", fmt_horizontal_line()).unwrap();

        let param_names = self.param_names();
        if !param_names.is_empty() {
            for k in param_names {
                let label = format!("-P {k}");
                let line = self.line(label.as_str(), "", |conf| {
                    match Quantity::from(conf.get_param(k)).value {
                        Some(quantity) => quantity.to_string(),
                        None => {
                            let str_value = conf
                                .params
                                .iter()
                                .find(|(key, _)| key == k)
                                .map(|(_, value)| value.clone())
                                .unwrap_or_else(|| "".to_string());
                            str_value
                        }
                    }
                });
                writeln!(f, "{line}").unwrap();
            }
            writeln!(f, "{}", fmt_horizontal_line()).unwrap();
        }

        let lines: Vec<Box<dyn Display>> = vec![
            self.line("Threads", "", |conf| Quantity::from(conf.threads)),
            self.line("Connections", "", |conf| {
                Quantity::from(get_connection_count(conf))
            }),
            self.line("Concurrency", "req", |conf| {
                Quantity::from(conf.concurrency)
            }),
            self.line("Max rate", "op/s", |conf| Quantity::from(conf.rate)),
            self.line("Warmup", "s", |conf| {
                Quantity::from(conf.warmup_duration.period_secs())
            }),
            self.line("└─", "op", |conf| {
                Quantity::from(conf.warmup_duration.count())
            }),
            self.line("Run time", "s", |conf| {
                Quantity::from(conf.run_duration.period_secs()).with_precision(1)
            }),
            self.line("└─", "op", |conf| {
                Quantity::from(conf.run_duration.count())
            }),
            self.line("Sampling", "s", |conf| {
                Quantity::from(conf.sampling_interval.period_secs()).with_precision(1)
            }),
            self.line("└─", "op", |conf| {
                Quantity::from(conf.sampling_interval.count())
            }),
            self.line("Request timeout", "s", |conf| {
                Quantity::from(get_request_timeout(conf).as_secs_f64())
            }),
            self.line("Retries", "", |conf| {
                Quantity::from(conf.connection.retry_strategy.retries)
            }),
            self.line("├─ min delay", "ms", |conf| {
                Quantity::from(
                    conf.connection.retry_strategy.retry_delay.min.as_secs_f64() * 1000.0,
                )
            }),
            self.line("└─ max delay", "ms", |conf| {
                Quantity::from(
                    conf.connection.retry_strategy.retry_delay.max.as_secs_f64() * 1000.0,
                )
            }),
        ];

        for l in lines {
            writeln!(f, "{l}")?;
        }
        Ok(())
    }
}

pub fn print_log_header() {
    println!("{}", fmt_section_header("LOG"));
    println!("{}", style("    Time    Cycles    Errors    Thrpt.     ────────────────────────────────── Latency [ms/op] ──────────────────────────────").yellow().bold().for_stdout());
    println!("{}", style("     [s]      [op]      [op]    [op/s]             Min        25        50        75        90        99      99.9       Max").yellow().for_stdout());
}

impl Display for Sample {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:8.3} {:9.0} {:9.0} {:9.0}       {:9.1} {:9.1} {:9.1} {:9.1} {:9.1} {:9.1} {:9.1} {:9.1}",
            self.time_s + self.duration_s,
            self.cycle_count,
            self.cycle_error_count,
            self.cycle_throughput,
            self.cycle_latency.percentiles.get(Percentile::Min).value,
            self.cycle_latency.percentiles.get(Percentile::P25).value,
            self.cycle_latency.percentiles.get(Percentile::P50).value,
            self.cycle_latency.percentiles.get(Percentile::P75).value,
            self.cycle_latency.percentiles.get(Percentile::P90).value,
            self.cycle_latency.percentiles.get(Percentile::P99).value,
            self.cycle_latency.percentiles.get(Percentile::P99_9).value,
            self.cycle_latency.percentiles.get(Percentile::Max).value
        )
    }
}

impl BenchmarkCmp<'_> {
    fn line<S, M, F>(&self, label: S, unit: &str, f: F) -> Box<Line<M, &BenchmarkStats, F>>
    where
        S: ToString,
        M: Display + Rational,
        F: Fn(&BenchmarkStats) -> M,
    {
        Box::new(Line::new(
            label.to_string(),
            unit.to_string(),
            0,
            self.v1,
            self.v2,
            f,
        ))
    }
}

pub fn get_request_timeout(conf: &RunCommand) -> Duration {
    match conf.connection.db {
        DBEngine::Scylla => conf.connection.request_timeout,
        DBEngine::Aerospike => conf.connection.request_timeout,
        DBEngine::Foundation => Duration::from_millis(0),
        DBEngine::PostgreSQL => Duration::from_millis(0),
    }
}

pub fn get_connection_count(conf: &RunCommand) -> NonZeroUsize {
    match conf.connection.db {
        DBEngine::Scylla => conf.connection.count,
        DBEngine::Aerospike => conf.connection.count,
        DBEngine::Foundation => NonZeroUsize::new(1).unwrap(),
        DBEngine::PostgreSQL => NonZeroUsize::new(1).unwrap(),
    }
}

/// Formats all benchmark stats
impl Display for BenchmarkCmp<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", fmt_section_header("SUMMARY STATS"))?;
        if self.v2.is_some() {
            writeln!(f, "{}", fmt_cmp_header(true))?;
        }

        let summary: Vec<Box<dyn Display>> = vec![
            self.line("Elapsed time", "s", |s| {
                Quantity::from(s.elapsed_time_s).with_precision(3)
            }),
            self.line("CPU time", "s", |s| {
                Quantity::from(s.cpu_time_s).with_precision(3)
            }),
            self.line("CPU utilisation", "%", |s| {
                Quantity::from(s.cpu_util).with_precision(1)
            }),
            self.line("Cycles", "op", |s| Quantity::from(s.cycle_count)),
            self.line("Errors", "op", |s| Quantity::from(s.error_count)),
            self.line("└─", "%", |s| {
                Quantity::from(s.errors_ratio).with_precision(1)
            }),
            self.line("Requests", "req", |s| Quantity::from(s.request_count)),
            self.line("└─", "req/op", |s| {
                Quantity::from(s.requests_per_cycle).with_precision(1)
            }),
            self.line("Retries", "ret", |s| Quantity::from(s.request_retry_count)),
            self.line("└─", "ret/req", |s| {
                Quantity::from(s.request_retry_per_request).with_precision(1)
            }),
            self.line("Rows", "row", |s| Quantity::from(s.row_count)),
            self.line("└─", "row/req", |s| {
                Quantity::from(s.row_count_per_req).with_precision(1)
            }),
            self.line("Concurrency", "req", |s| {
                Quantity::from(s.concurrency).with_precision(0)
            }),
            self.line("└─", "%", |s| {
                Quantity::from(s.concurrency_ratio).with_precision(0)
            }),
            self.line("Throughput", "op/s", |s| {
                Quantity::from(s.cycle_throughput).with_precision(0)
            })
            .with_significance(self.cmp_cycle_throughput())
            .with_orientation(1)
            .into_box(),
            self.line("├─", "req/s", |s| {
                Quantity::from(s.req_throughput).with_precision(0)
            })
            .with_significance(self.cmp_req_throughput())
            .with_orientation(1)
            .into_box(),
            self.line("└─", "row/s", |s| {
                Quantity::from(s.row_throughput).with_precision(0)
            })
            .with_significance(self.cmp_row_throughput())
            .with_orientation(1)
            .into_box(),
            self.line("Cycle latency", "ms", |s| {
                Quantity::from(s.cycle_latency.mean).with_precision(3)
            })
            .with_significance(self.cmp_mean_resp_time())
            .with_orientation(-1)
            .into_box(),
            self.line("Request latency", "ms", |s| {
                Quantity::from(s.request_latency.as_ref().map(|rt| rt.mean)).with_precision(3)
            })
            .with_significance(self.cmp_mean_resp_time())
            .with_orientation(-1)
            .into_box(),
        ];

        for l in summary {
            writeln!(f, "{l}")?;
        }
        writeln!(f)?;

        let resp_time_percentiles = [
            Percentile::Min,
            Percentile::P25,
            Percentile::P50,
            Percentile::P75,
            Percentile::P90,
            Percentile::P95,
            Percentile::P98,
            Percentile::P99,
            Percentile::P99_9,
            Percentile::P99_99,
            Percentile::Max,
        ];

        for fn_name in self.v1.cycle_latency_by_fn.keys() {
            writeln!(f)?;
            writeln!(
                f,
                "{}",
                fmt_section_header(format!("CYCLE LATENCY for {fn_name} [ms] ").as_str())
            )?;
            if self.v2.is_some() {
                writeln!(f, "{}", fmt_cmp_header(true))?;
            }

            for p in resp_time_percentiles.iter() {
                let l = self
                    .line(p.name(), "", |s| {
                        let rt = s
                            .cycle_latency_by_fn
                            .get(fn_name)
                            .map(|l| l.percentiles.get(*p));
                        Quantity::from(rt).with_precision(3)
                    })
                    .with_orientation(-1)
                    .with_significance(self.cmp_resp_time_percentile(*p));
                writeln!(f, "{l}")?;
            }
        }

        writeln!(f)?;
        writeln!(f, "{}", fmt_section_header("RESPONSE LATENCY [ms]"))?;
        if self.v2.is_some() {
            writeln!(f, "{}", fmt_cmp_header(true))?;
        }

        for p in resp_time_percentiles.iter() {
            let l = self
                .line(p.name(), "", |s| {
                    let rt = s.request_latency.as_ref().map(|l| l.percentiles.get(*p));
                    Quantity::from(rt).with_precision(3)
                })
                .with_orientation(-1)
                .with_significance(self.cmp_resp_time_percentile(*p));
            writeln!(f, "{l}")?;
        }

        if self.v1.error_count > 0 {
            writeln!(f)?;
            writeln!(f, "{}", fmt_section_header("ERRORS"))?;
            for e in self.v1.errors.iter() {
                writeln!(f, "{e}")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PathAndSummary(pub PathBuf, pub Summary);

#[derive(Debug)]
pub struct Summary {
    pub workload: PathBuf,
    pub functions: String,
    pub timestamp: Option<DateTime<Local>>,
    pub tags: Vec<String>,
    pub params: Vec<(String, String)>,
    pub rate: Option<f64>,
    pub throughput: f64,
    pub latency_p50: Option<f64>,
    pub latency_p99: Option<f64>,
}

impl PathAndSummary {
    pub const COLUMNS: &'static [&'static str] = &[
        "File",
        "Timestamp",
        "Workload",
        "Function(s)",
        "Params",
        "Tags",
        "Rate",
        "Thrpt. [req/s]",
        "P50 [ms]",
        "P99 [ms]",
    ];
}

impl Row for PathAndSummary {
    fn cell_value(&self, column: &str) -> Option<String> {
        match column {
            "File" => Some(self.0.display().to_string()),
            "Workload" => Some(
                self.1
                    .workload
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
            ),
            "Function(s)" => Some(self.1.functions.clone()),
            "Timestamp" => self
                .1
                .timestamp
                .map(|ts| ts.format("%Y-%m-%d %H:%M:%S").to_string()),
            "Tags" => Some(self.1.tags.join(", ")),
            "Params" => Some(
                self.1
                    .params
                    .iter()
                    .map(|(k, v)| format!("{k} = {v}"))
                    .join(", "),
            ),
            "Rate" => self.1.rate.map(|r| r.to_string()),
            "Thrpt. [req/s]" => Some(format!("{:.0}", self.1.throughput)),
            "P50 [ms]" => self.1.latency_p50.map(|l| format!("{:.1}", l)),
            "P99 [ms]" => self.1.latency_p99.map(|l| format!("{:.1}", l)),
            _ => None,
        }
    }
}

fn format_time(timestamp: Option<i64>, format: &str) -> String {
    timestamp
        .and_then(|ts| {
            Local
                .timestamp_opt(ts, 0)
                .latest()
                .map(|l| l.format(format).to_string())
        })
        .unwrap_or_default()
}

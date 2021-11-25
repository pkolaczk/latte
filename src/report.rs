use core::fmt;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::{fs, io};

use chrono::{Local, NaiveDateTime, TimeZone};
use console::{pad_str, style, Alignment};
use err_derive::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use statrs::statistics::Statistics;
use strum::IntoEnumIterator;

use crate::config::RunCommand;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Bucket, Percentile, Sample, Significance};

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
        let report = serde_json::from_reader(file)?;
        Ok(report)
    }

    /// Saves benchmark results to a JSON file
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let f = fs::File::create(path)?;
        serde_json::to_writer_pretty(f, &self)?;
        Ok(())
    }
}

/// This is similar as the builtin `Option`, but we need it, because the
/// builtin `Option` doesn't implement
/// `Display` and there is no way to do it in this crate.
/// Maybe formats None as an empty string.
pub enum Maybe<T> {
    None,
    Some(T),
}

impl<T> From<Option<T>> for Maybe<T> {
    fn from(opt: Option<T>) -> Maybe<T> {
        match opt {
            Some(t) => Maybe::Some(t),
            None => Maybe::None,
        }
    }
}

impl<T: Display> Display for Maybe<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            Maybe::Some(value) => value.fmt(f),
            Maybe::None => Ok(()),
        }
    }
}

pub struct Quantity<T: Display> {
    pub value: T,
    pub error: Option<T>,
    pub precision: usize,
}

impl<T: Display> Quantity<T> {
    pub fn new(value: T, precision: usize) -> Quantity<T> {
        Quantity {
            value,
            error: None,
            precision,
        }
    }

    pub fn with_error(mut self, e: T) -> Self {
        self.error = Some(e);
        self
    }

    fn format_error(&self) -> String {
        match &self.error {
            None => "".to_owned(),
            Some(e) => format!("± {:<6.prec$}", e, prec = self.precision),
        }
    }
}

impl<T: Display> Display for Quantity<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{value:9.prec$} {error:8}",
            value = style(&self.value).bright().for_stdout(),
            prec = self.precision,
            error = style(self.format_error()).dim().for_stdout(),
        )
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

impl<T: Rational> Rational for Maybe<T> {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        match (a, b) {
            (Maybe::Some(a), Maybe::Some(b)) => Rational::ratio(a, b),
            _ => None,
        }
    }
}

impl<T: Rational + Display> Rational for Quantity<T> {
    fn ratio(a: Self, b: Self) -> Option<f32> {
        Rational::ratio(a.value, b.value)
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
    pub goodness: i8,
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
    fn new(label: String, unit: String, goodness: i8, v1: V, v2: Option<V>, f: F) -> Self {
        Line {
            label,
            unit,
            goodness,
            v1,
            v2,
            significance: None,
            f,
        }
    }

    fn into_box(self) -> Box<Self> {
        Box::new(self)
    }

    fn with_goodness(mut self, goodness: i8) -> Self {
        self.goodness = goodness;
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
                    let diff = format!("{:+7.1}%", diff);
                    let styled = if good == 0.0 || !significant {
                        style(diff).dim()
                    } else if good > 0.0 {
                        style(diff).bright().green()
                    } else {
                        style(diff).bright().red()
                    };
                    format!("{}", styled)
                })
            })
            .unwrap_or_else(|| "".to_string())
    }

    fn fmt_unit(&self) -> String {
        match self.unit.as_str() {
            "" => "".to_string(),
            u => format!("[{}]", u),
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
            cmp = self.fmt_relative_change(self.goodness, is_significant),
            signif = format!("{}", Maybe::from(self.significance))
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
        conf.timestamp
            .map(|ts| {
                let utc = NaiveDateTime::from_timestamp(ts, 0);
                Local.from_utc_datetime(&utc).format(format).to_string()
            })
            .unwrap_or_else(|| "".to_string())
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

impl<'a> Display for RunConfigCmp<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", fmt_section_header("CONFIG"))?;
        if self.v2.is_some() {
            writeln!(f, "{}", fmt_cmp_header(false))?;
        }

        let lines: Vec<Box<dyn Display>> = vec![
            self.line("Date", "", |conf| self.format_time(conf, "%a, %d %b %Y")),
            self.line("Time", "", |conf| self.format_time(conf, "%H:%M:%S %z")),
            self.line("Cluster", "", |conf| Maybe::from(conf.cluster_name.clone())),
            self.line("C* version", "", |conf| {
                Maybe::from(conf.cass_version.clone())
            }),
            self.line("Tags", "", |conf| conf.tags.iter().join(", ")),
            self.line("Workload", "", |conf| {
                conf.workload
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "".to_string())
            }),
        ];

        for l in lines {
            writeln!(f, "{}", l)?;
        }

        writeln!(f, "{}", fmt_horizontal_line()).unwrap();
        for k in self.param_names() {
            let label = format!("-P {}", k);
            writeln!(
                f,
                "{}",
                self.line(label.as_str(), "", |conf| {
                    Quantity::new(Maybe::from(conf.get_param(k)), 0)
                })
            )
                .unwrap();
        }

        writeln!(f, "{}", fmt_horizontal_line()).unwrap();

        let lines: Vec<Box<dyn Display>> = vec![
            self.line("Threads", "", |conf| Quantity::new(conf.threads, 0)),
            self.line("Connections", "", |conf| Quantity::new(conf.connections, 0)),
            self.line("Concurrency", "req", |conf| {
                Quantity::new(conf.concurrency, 0)
            }),
            self.line("Max rate", "op/s", |conf| match conf.rate {
                Some(r) => Quantity::new(Maybe::Some(r), 0),
                None => Quantity::new(Maybe::None, 0),
            }),
            self.line("Warmup", "s", |conf| {
                Quantity::new(Maybe::from(conf.warmup_duration.seconds()), 0)
            }),
            self.line("└─", "op", |conf| {
                Quantity::new(Maybe::from(conf.warmup_duration.calls()), 0)
            }),
            self.line("Run time", "s", |conf| {
                Quantity::new(Maybe::from(conf.run_duration.seconds()), 1)
            }),
            self.line("└─", "op", |conf| {
                Quantity::new(Maybe::from(conf.run_duration.calls()), 0)
            }),
            self.line("Sampling", "s", |conf| {
                Quantity::new(conf.sampling_period, 1)
            }),
        ];

        for l in lines {
            writeln!(f, "{}", l)?;
        }
        Ok(())
    }
}

pub fn print_log_header() {
    println!("{}", fmt_section_header("LOG"));
    println!("{}", style("    Time  ───── Throughput ─────  ────────────────────────────────── Response times [ms] ───────────────────────────────────").yellow().bold().for_stdout());
    println!("{}", style("     [s]      [op/s]     [req/s]         Min        25        50        75        90        95        99      99.9       Max").yellow().for_stdout());
}

impl Display for Sample {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:8.3} {:11.0} {:11.0}   {:9.3} {:9.3} {:9.3} {:9.3} {:9.3} {:9.3} {:9.3} {:9.3} {:9.3}",
            self.time_s,
            self.call_throughput,
            self.req_throughput,
            self.resp_time_percentiles[Percentile::Min as usize],
            self.resp_time_percentiles[Percentile::P25 as usize],
            self.resp_time_percentiles[Percentile::P50 as usize],
            self.resp_time_percentiles[Percentile::P75 as usize],
            self.resp_time_percentiles[Percentile::P90 as usize],
            self.resp_time_percentiles[Percentile::P95 as usize],
            self.resp_time_percentiles[Percentile::P99 as usize],
            self.resp_time_percentiles[Percentile::P99_9 as usize],
            self.resp_time_percentiles[Percentile::Max as usize]
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

/// Formats all benchmark stats
impl<'a> Display for BenchmarkCmp<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", fmt_section_header("SUMMARY STATS"))?;
        if self.v2.is_some() {
            writeln!(f, "{}", fmt_cmp_header(true))?;
        }

        let summary: Vec<Box<dyn Display>> = vec![
            self.line("Elapsed time", "s", |s| Quantity::new(s.elapsed_time_s, 3)),
            self.line("CPU time", "s", |s| Quantity::new(s.cpu_time_s, 3)),
            self.line("CPU utilisation", "%", |s| Quantity::new(s.cpu_util, 1)),
            self.line("Calls", "op", |s| Quantity::new(s.call_count, 0)),
            self.line("Errors", "op", |s| Quantity::new(s.error_count, 0)),
            self.line("└─", "%", |s| Quantity::new(s.errors_ratio, 1)),
            self.line("Requests", "req", |s| Quantity::new(s.request_count, 0)),
            self.line("└─", "req/op", |s| {
                Quantity::new(s.requests_per_call, 1)
            }),
            self.line("Rows", "row", |s| Quantity::new(s.row_count, 0)),
            self.line("└─", "row/req", |s| {
                Quantity::new(s.row_count_per_req, 1)
            }),
            self.line("Samples", "", |s| Quantity::new(s.samples.len(), 0)),
            self.line("Mean sample size", "op", |s| {
                Quantity::new(s.samples.iter().map(|s| s.call_count as f64).mean(), 0)
            }),
            self.line("└─", "req", |s| {
                Quantity::new(s.samples.iter().map(|s| s.request_count as f64).mean(), 0)
            }),
            self.line("Concurrency", "req", |s| {
                Quantity::new(s.concurrency.value, 1)
            }),
            self.line("└─", "%", |s| Quantity::new(s.concurrency_ratio, 1)),
            self.line("Throughput", "op/s", |s| {
                Quantity::new(s.call_throughput.value, 0)
                    .with_error(s.call_throughput.std_err * ERR_MARGIN)
            })
                .with_significance(self.cmp_call_throughput())
                .with_goodness(1)
                .into_box(),
            self.line("├─", "req/s", |s| {
                Quantity::new(s.req_throughput.value, 0)
                    .with_error(s.req_throughput.std_err * ERR_MARGIN)
            })
                .with_significance(self.cmp_req_throughput())
                .with_goodness(1)
                .into_box(),
            self.line("└─", "row/s", |s| {
                Quantity::new(s.row_throughput.value, 0)
                    .with_error(s.row_throughput.std_err * ERR_MARGIN)
            })
                .with_significance(self.cmp_row_throughput())
                .with_goodness(1)
                .into_box(),
            self.line("Mean call time", "ms", |s| {
                Quantity::new(s.call_time_ms.mean.value, 3)
                    .with_error(s.call_time_ms.mean.std_err * ERR_MARGIN)
            })
                .with_significance(self.cmp_mean_resp_time())
                .with_goodness(-1)
                .into_box(),
            self.line("Mean resp. time", "ms", |s| {
                Quantity::new(s.resp_time_ms.mean.value, 3)
                    .with_error(s.resp_time_ms.mean.std_err * ERR_MARGIN)
            })
                .with_significance(self.cmp_mean_resp_time())
                .with_goodness(-1)
                .into_box(),
        ];

        for l in summary {
            writeln!(f, "{}", l)?;
        }
        writeln!(f)?;

        if self.v1.request_count > 0 {
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
            writeln!(f)?;
            writeln!(f, "{}", fmt_section_header("RESPONSE TIMES [ms]"))?;
            if self.v2.is_some() {
                writeln!(f, "{}", fmt_cmp_header(true))?;
            }

            for p in resp_time_percentiles.iter() {
                let l = self
                    .line(p.name(), "", |s| {
                        let rt = s.resp_time_ms.percentiles[*p as usize];
                        Quantity::new(rt.value, 3).with_error(rt.std_err * ERR_MARGIN)
                    })
                    .with_goodness(-1)
                    .with_significance(self.cmp_resp_time_percentile(*p));
                writeln!(f, "{}", l)?;
            }

            writeln!(f)?;
            writeln!(f, "{}", fmt_section_header("RESPONSE TIME DISTRIBUTION"))?;
            writeln!(f, "{}", style("── Resp. time [ms] ──  ────────────────────────────────────────────── Count ────────────────────────────────────────────────").yellow().bold().for_stdout())?;
            let zero = Bucket {
                percentile: 0.0,
                duration_ms: 0.0,
                count: 0,
                cumulative_count: 0,
            };
            let dist = &self.v1.resp_time_ms.distribution;
            let max_count = dist.iter().map(|b| b.count).max().unwrap_or(1);
            for (low, high) in ([zero].iter().chain(dist)).tuple_windows() {
                writeln!(
                    f,
                    "{:8.1} {} {:8.1}  {:9} {:6.2}%  {}",
                    style(low.duration_ms).yellow().for_stdout(),
                    style("...").yellow().for_stdout(),
                    style(high.duration_ms).yellow().for_stdout(),
                    high.count,
                    high.percentile - low.percentile,
                    style("▪".repeat((82 * high.count / max_count) as usize))
                        .dim()
                        .for_stdout()
                )?;
                if high.cumulative_count == self.v1.request_count {
                    break;
                }
            }
        }

        if self.v1.error_count > 0 {
            writeln!(f)?;
            writeln!(f, "{}", fmt_section_header("ERRORS"))?;
            for e in self.v1.errors.iter() {
                writeln!(f, "{}", e)?;
            }
        }
        Ok(())
    }
}

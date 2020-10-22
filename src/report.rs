use core::fmt;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::{fs, io};

use chrono::{Local, NaiveDateTime, TimeZone};
use err_derive::*;
use serde::{Deserialize, Serialize};
use strum::{AsStaticRef, IntoEnumIterator};

use crate::config::RunCommand;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Percentile, SampleStats, Significance};
use statrs::statistics::Statistics;

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
    pub fn load(path: &PathBuf) -> Result<Report, ReportLoadError> {
        let file = fs::File::open(path)?;
        let report = serde_json::from_reader(file)?;
        Ok(report)
    }

    /// Saves benchmark results to a JSON file
    pub fn save(&self, path: &PathBuf) -> io::Result<()> {
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
    pub ratio: Option<f64>,
}

impl<T: Display> Quantity<T> {
    pub fn new(value: T, precision: usize) -> Quantity<T> {
        Quantity {
            value,
            error: None,
            precision,
            ratio: None,
        }
    }

    pub fn with_error(mut self, e: T) -> Self {
        self.error = Some(e);
        self
    }

    pub fn with_ratio(mut self, ratio: f64) -> Self {
        self.ratio = Some(ratio);
        self
    }

    pub fn with_opt_ratio(mut self, ratio: Option<f64>) -> Self {
        self.ratio = ratio;
        self
    }

    fn format_error(&self) -> String {
        match &self.error {
            None => "".to_owned(),
            Some(e) => format!("± {:<6.prec$}", e, prec = self.precision),
        }
    }

    fn format_ratio(&self) -> String {
        match &self.ratio {
            None => "".to_owned(),
            Some(r) => format!("({:5.1}%)", r),
        }
    }
}

impl<T: Display> Display for Quantity<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{value:9.prec$} {error:8} {ratio:8}",
            value = self.value,
            prec = self.precision,
            error = self.format_error(),
            ratio = self.format_ratio()
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
        let levels = [0.0001, 0.001, 0.01];
        let stars = "*".repeat(levels.iter().filter(|&&l| l >= self.0).count());
        write!(f, "({:3}) {:6.4}", stars, self.0)
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
    fn new(label: String, unit: String, v1: V, v2: Option<V>, f: F) -> Self {
        Line {
            label,
            unit,
            v1,
            v2,
            significance: None,
            f,
        }
    }

    fn into_box(self) -> Box<Self> {
        Box::new(self)
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
    fn fmt_relative_diff(&self) -> String {
        self.v2
            .and_then(|v2| {
                let m1 = (self.f)(self.v1);
                let m2 = (self.f)(v2);
                let ratio = Rational::ratio(m1, m2);
                ratio.map(|r| {
                    let diff = 100.0 * (r - 1.0);
                    if !diff.is_nan() {
                        format!("{:+7.1}%", diff)
                    } else {
                        "".to_string()
                    }
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
        write!(
            f,
            "{label:>16} {unit:>7}: {m1:26} {m2:26} {cmp:6}  {signif:>11}",
            label = self.label,
            unit = self.fmt_unit(),
            m1 = self.fmt_measurement(Some(self.v1)),
            m2 = self.fmt_measurement(self.v2),
            cmp = self.fmt_relative_diff(),
            signif = format!("{}", Maybe::from(self.significance))
        )
    }
}

fn fmt_section_header(name: &str) -> String {
    format!("{} {}", name, "=".repeat(104 - name.len() - 1))
}

fn fmt_cmp_header(display_significance: bool) -> String {
    let mut str = " ".repeat(26);
    str += "---------- This ---------- ---------- Other ----------    Change       ";
    if display_significance {
        str += "P-value"
    }
    str
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
            self.line("Tag", "", |conf| {
                conf.tag.clone().unwrap_or_else(|| "".to_string())
            }),
            self.line("Workload", "", |conf| conf.workload.to_string()),
            self.line("Compaction", "", |conf| conf.compaction.as_static()),
            self.line("Partitions", "", |conf| {
                Quantity::new(conf.workload_config().partitions, 0)
            }),
            self.line("Columns", "", |conf| {
                Quantity::new(conf.workload_config().columns, 0)
            }),
            self.line("Column size", "B", |conf| {
                Quantity::new(conf.workload_config().column_size, 0)
            }),
            self.line("Threads", "", |conf| Quantity::new(conf.threads, 0)),
            self.line("Connections", "", |conf| Quantity::new(conf.connections, 0)),
            self.line("Max parallelism", "req", |conf| {
                Quantity::new(conf.parallelism, 0)
            }),
            self.line("Max rate", "req/s", |conf| match conf.rate {
                Some(r) => Quantity::new(Maybe::Some(r), 0),
                None => Quantity::new(Maybe::None, 0),
            }),
            self.line("Warmup", "req", |conf| Quantity::new(conf.warmup_count, 0)),
            self.line("Iterations", "req", |conf| Quantity::new(conf.count, 0)),
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
    println!("\
      \x20   Time  Throughput        ----------------------- Response times [ms]---------------------------------\n\
      \x20    [s]     [req/s]           Min        25        50        75        90        95        99       Max");
}

impl Display for SampleStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:8.3} {:11.0}     {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
            self.time_s,
            self.throughput,
            self.resp_time_percentiles[Percentile::Min as usize],
            self.resp_time_percentiles[Percentile::P25 as usize],
            self.resp_time_percentiles[Percentile::P50 as usize],
            self.resp_time_percentiles[Percentile::P75 as usize],
            self.resp_time_percentiles[Percentile::P90 as usize],
            self.resp_time_percentiles[Percentile::P95 as usize],
            self.resp_time_percentiles[Percentile::P99 as usize],
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
            self.line("Elapsed", "s", |s| Quantity::new(s.elapsed_time_s, 3)),
            self.line("CPU time", "s", |s| {
                Quantity::new(s.cpu_time_s, 3).with_ratio(s.cpu_util)
            }),
            self.line("Completed", "req", |s| {
                Quantity::new(s.completed_requests, 0).with_ratio(s.completed_ratio)
            }),
            self.line("Errors", "req", |s| {
                Quantity::new(s.errors, 0).with_ratio(s.errors_ratio)
            }),
            self.line("Partitions", "", |s| Quantity::new(s.partitions, 0)),
            self.line("Rows", "", |s| Quantity::new(s.rows, 0)),
            self.line("Samples", "", |s| Quantity::new(s.samples.len(), 0)),
            self.line("Mean sample size", "req", |s| {
                Quantity::new(s.samples.iter().map(|s| s.request_count as f64).mean(), 0)
            }),
            self.line("Parallelism", "req", |s| {
                Quantity::new(s.parallelism, 1).with_ratio(s.parallelism_ratio)
            }),
            self.line("Throughput", "req/s", |s| {
                Quantity::new(s.throughput.value, 0)
                    .with_opt_ratio(s.throughput_ratio)
                    .with_error(s.throughput.std_err * ERR_MARGIN)
            })
            .with_significance(self.cmp_throughput())
            .into_box(),
            self.line("Mean resp. time", "ms", |s| {
                Quantity::new(s.resp_time_ms.value, 2)
                    .with_error(s.resp_time_ms.std_err * ERR_MARGIN)
            })
            .with_significance(self.cmp_mean_resp_time())
            .into_box(),
        ];

        for l in summary {
            writeln!(f, "{}", l)?;
        }

        writeln!(f)?;
        writeln!(
            f,
            "{}",
            fmt_section_header("THROUGHPUT DISTRIBUTION [req/s]")
        )?;
        if self.v2.is_some() {
            writeln!(f, "{}", fmt_cmp_header(false))?;
        }
        let throughput_percentiles = [
            Percentile::Min,
            Percentile::P1,
            Percentile::P2,
            Percentile::P5,
            Percentile::P10,
            Percentile::P25,
            Percentile::P50,
            Percentile::P75,
            Percentile::P90,
            Percentile::P95,
            Percentile::Max,
        ];
        for p in throughput_percentiles.iter() {
            let l = self.line(p.name(), "", |s| {
                Quantity::new(s.throughput_percentiles[*p as usize], 0)
            });
            writeln!(f, "{}", l)?;
        }

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
        writeln!(f, "{}", fmt_section_header("MEAN RESPONSE TIMES [ms]"))?;
        if self.v2.is_some() {
            writeln!(f, "{}", fmt_cmp_header(true))?;
        }

        for p in resp_time_percentiles.iter() {
            let l = self
                .line(p.name(), "", |s| {
                    let rt = s.resp_time_percentiles[*p as usize];
                    Quantity::new(rt.value, 2).with_error(rt.std_err * ERR_MARGIN)
                })
                .with_significance(self.cmp_resp_time_percentile(*p));
            writeln!(f, "{}", l)?;
        }

        writeln!(f)?;
        writeln!(f, "{}", fmt_section_header("RESPONSE TIME DISTRIBUTION"))?;
        writeln!(f, "Percentile    Resp. time [ms]  ------------------------------- Count -----------------------------------")?;
        for x in self.v1.resp_time_distribution.iter() {
            writeln!(
                f,
                " {:9.5}     {:9.2}       {:9}   {}",
                x.percentile,
                x.resp_time_ms,
                x.count,
                "*".repeat((100 * x.count / self.v1.completed_requests) as usize)
            )?;
        }
        Ok(())
    }
}

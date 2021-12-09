use std::cmp::max;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures::channel::mpsc;
use futures::channel::mpsc::{Receiver, Sender};
use futures::future::ready;
use futures::{SinkExt, Stream, StreamExt};
use itertools::Itertools;
use rune::Source;
use status_line::StatusLine;
use tokio::runtime::Builder;
use tokio_stream::wrappers::IntervalStream;

use config::RunCommand;

use crate::config::{AppConfig, Command, ShowCommand};
use crate::deadline::Deadline;
use crate::error::{LatteError, Result};
use crate::interrupt::InterruptHandler;
use crate::progress::Progress;
use crate::report::{Report, RunConfigCmp};
use crate::session::*;
use crate::session::{CassError, CassErrorKind, Session, SessionStats};
use crate::stats::{BenchmarkCmp, BenchmarkStats, Recorder};
use crate::workload::{FnRef, Workload, WorkloadStats, LOAD_FN, RUN_FN};

mod config;
mod deadline;
mod error;
mod interrupt;
mod progress;
mod report;
mod session;
mod stats;
mod workload;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn interval_stream(rate: f64) -> IntervalStream {
    let interval = Duration::from_nanos(max(1, (1000000000.0 / rate) as u64));
    IntervalStream::new(tokio::time::interval(interval))
}

/// Fetches session statistics and sends them to the channel.
async fn send_stats(workload: &Workload, tx: &mut Sender<Result<WorkloadStats>>) {
    let stats = workload.take_stats(Instant::now());
    tx.send(Ok(stats)).await.unwrap();
}

/// Responsible for periodically getting a snapshot of statistics from the `workload`
/// and sending them to the `output` channel. The sampling period is controlled by `sampling`.
struct Snapshotter<'a> {
    sampling: config::Duration,
    workload: &'a Workload,
    output: &'a mut Sender<Result<WorkloadStats>>,
    last_snapshot_time: Instant,
    last_snapshot_iter: u64,
    current_iter: u64,
}

impl<'a> Snapshotter<'a> {
    pub fn new(
        sampling: config::Duration,
        workload: &'a Workload,
        output: &'a mut Sender<Result<WorkloadStats>>,
    ) -> Snapshotter<'a> {
        Snapshotter {
            sampling,
            workload,
            output,
            last_snapshot_time: Instant::now(),
            last_snapshot_iter: 0,
            current_iter: 0,
        }
    }

    /// Should be called when a workload iteration finished.
    /// If there comes the time, it will send the stats to the output.
    pub async fn iteration_completed(&mut self, end_time: Instant) {
        self.current_iter += 1;
        match self.sampling {
            config::Duration::Time(d) => {
                if end_time - self.last_snapshot_time > d {
                    send_stats(self.workload, self.output).await;
                    self.last_snapshot_time += d;
                }
            }
            config::Duration::Count(cnt) => {
                if self.current_iter - self.last_snapshot_iter > cnt {
                    send_stats(self.workload, self.output).await;
                    self.last_snapshot_iter += cnt;
                }
            }
        }
    }
}

/// Runs a stream of workload iterations till completion in the context of the current task.
/// Periodically sends workload statistics to the `out` channel.
///
/// # Parameters
/// - stream: a stream of iteration numbers; None means the end of the stream
/// - workload: defines the function to call
/// - concurrency: the maximum number of pending workload calls
/// - sampling: controls when to output workload statistics
/// - progress: progress bar notified about each successful iteration
/// - interrupt: allows for terminating the stream early
/// - out: the channel to receive workload statistics
async fn run_stream(
    stream: impl Stream<Item = Range<u64>> + std::marker::Unpin,
    workload: Workload,
    concurrency: NonZeroUsize,
    sampling: Option<config::Duration>,
    interrupt: Arc<InterruptHandler>,
    progress: Arc<StatusLine<Progress>>,
    mut out: Sender<Result<WorkloadStats>>,
) {
    workload.reset(Instant::now());

    let mut result_stream = stream
        .take_while(|i| ready(!i.is_empty()))
        .flat_map(futures::stream::iter)
        // unconstrained to workaround quadratic complexity of buffer_unordered ()
        .map(|i| tokio::task::unconstrained(workload.run(i as i64)))
        .buffer_unordered(concurrency.get())
        .inspect(|_| progress.tick());

    let mut snapshotter = sampling.map(|s| Snapshotter::new(s, &workload, &mut out));
    while let Some(res) = result_stream.next().await {
        match res {
            Ok(end_time) => {
                if let Some(snapshotter) = &mut snapshotter {
                    snapshotter.iteration_completed(end_time).await
                }
            }
            Err(e) => {
                out.send(Err(e)).await.unwrap();
                return;
            }
        }

        if interrupt.is_interrupted() {
            break;
        }
    }
    // Send the statistics of remaining requests
    send_stats(&workload, &mut out).await;
}

/// Launches a new worker task that runs a series of invocations of the workload function.
///
/// The task will run as long as `deadline` produces new iteration numbers.
/// The task updates the `progress` bar after each successful iteration.
///
/// Returns a stream where workload statistics are published.
fn spawn_stream(
    concurrency: NonZeroUsize,
    rate: Option<f64>,
    sampling: Option<config::Duration>,
    workload: Workload,
    deadline: Arc<Deadline>,
    interrupt: Arc<InterruptHandler>,
    progress: Arc<StatusLine<Progress>>,
) -> Receiver<Result<WorkloadStats>> {
    let (tx, rx) = mpsc::channel(1);

    tokio::spawn(async move {
        match rate {
            Some(rate) => {
                let stream = interval_stream(rate).map(|_| deadline.next());
                run_stream(
                    stream,
                    workload,
                    concurrency,
                    sampling,
                    interrupt,
                    progress,
                    tx,
                )
                .await
            }
            None => {
                let stream = futures::stream::repeat_with(|| deadline.next());
                run_stream(
                    stream,
                    workload,
                    concurrency,
                    sampling,
                    interrupt,
                    progress,
                    tx,
                )
                .await
            }
        }
    });
    rx
}

/// Receives one item from each of the streams.
/// Streams that are closed are ignored.
async fn receive_one_of_each<T, S>(streams: &mut [S]) -> Vec<T>
where
    S: Stream<Item = T> + Unpin,
{
    let mut items = Vec::with_capacity(streams.len());
    for s in streams {
        if let Some(item) = s.next().await {
            items.push(item);
        }
    }
    items
}

/// Controls the intensity of requests sent to the server
struct ExecutionOptions {
    /// How long to execute
    duration: config::Duration,
    /// Maximum rate of requests in requests per second, `None` means no limit
    rate: Option<f64>,
    /// Number of parallel threads of execution
    threads: NonZeroUsize,
    /// Number of outstanding async requests per each thread
    concurrency: NonZeroUsize,
}

/// Executes the given function many times in parallel.
/// Draws a progress bar.
/// Returns the statistics such as throughput or duration histogram.
///
/// # Parameters
///   - `name`: text displayed next to the progress bar
///   - `count`: number of iterations
///   - `exec_options`: controls execution options such as parallelism level and rate
///   - `workload`: encapsulates a set of queries to execute
async fn par_execute(
    name: &str,
    exec_options: &ExecutionOptions,
    sampling_period: Option<config::Duration>,
    workload: Workload,
    signals: Arc<InterruptHandler>,
) -> Result<BenchmarkStats> {
    let thread_count = exec_options.threads.get();
    let concurrency = exec_options.concurrency;
    let rate = exec_options.rate;
    let progress = match exec_options.duration {
        config::Duration::Count(count) => Progress::with_count(name.to_string(), count),
        config::Duration::Time(duration) => Progress::with_duration(name.to_string(), duration),
    };
    let progress = Arc::new(StatusLine::new(progress));
    let deadline = Arc::new(Deadline::new(exec_options.duration));
    let sampling_period = sampling_period.map(|s| match s {
        config::Duration::Count(cnt) => config::Duration::Count(cnt / thread_count as u64),
        config::Duration::Time(d) => config::Duration::Time(d),
    });
    let mut streams = Vec::with_capacity(thread_count);
    let mut stats = Recorder::start(rate, concurrency);

    for _ in 0..thread_count {
        let s = spawn_stream(
            concurrency,
            rate.map(|r| r / (thread_count as f64)),
            sampling_period,
            workload.clone(),
            deadline.clone(),
            signals.clone(),
            progress.clone(),
        );
        streams.push(s);
    }

    loop {
        let partial_stats: Vec<_> = receive_one_of_each(&mut streams)
            .await
            .into_iter()
            .try_collect()?;

        if partial_stats.is_empty() {
            break;
        }

        let aggregate = stats.record(&partial_stats);
        if sampling_period.is_some() {
            progress.set_visible(false);
            println!("{}", aggregate);
            progress.set_visible(true);
        }
    }

    Ok(stats.finish())
}

fn load_report_or_abort(path: &Path) -> Report {
    match Report::load(path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "error: Failed to read report from {}: {}",
                path.display(),
                e
            );
            exit(1)
        }
    }
}

/// Constructs the output report file name from the parameters in the command.
/// Separates parameters with underscores.
fn get_default_output_name(conf: &RunCommand) -> PathBuf {
    let name = conf
        .workload
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let mut components = vec![name];
    components.extend(conf.cluster_name.iter().map(|x| x.replace(" ", "_")));
    components.extend(conf.cass_version.iter().cloned());
    components.extend(conf.tags.iter().cloned());
    if let Some(r) = conf.rate {
        components.push(format!("r{}", r))
    };
    components.push(format!("p{}", conf.concurrency));
    components.push(format!("t{}", conf.threads));
    components.push(format!("c{}", conf.connections));
    let params = conf.params.iter().map(|(k, v)| format!("{}{}", k, v));
    components.extend(params);
    components.push(chrono::Local::now().format("%Y%m%d.%H%M%S").to_string());
    PathBuf::from(format!("{}.json", components.join(".")))
}

async fn run(conf: RunCommand) -> Result<()> {
    let mut conf = conf.set_timestamp_if_empty();
    let compare = conf.baseline.as_ref().map(|p| load_report_or_abort(p));
    eprintln!(
        "info: Loading workload script {}...",
        conf.workload.display()
    );
    let script = Source::from_path(&conf.workload)
        .map_err(|e| LatteError::ScriptRead(conf.workload.clone(), e))?;

    let mut program = workload::Program::new(script, conf.params.iter().cloned().collect())?;

    if !program.has_run() {
        eprintln!("error: Function `run` not found in the workload script.");
        exit(255);
    }

    eprintln!("info: Connecting to {:?}... ", conf.addresses);
    let session = connect_or_abort(&conf).await;
    let cluster_info = session
        .query("SELECT cluster_name, release_version FROM system.local", ())
        .await;
    if let Ok(rs) = cluster_info {
        if let Some(rows) = rs.rows {
            if let Some(row) = rows.into_iter().next() {
                if let Ok((cluster_name, cass_version)) = row.into_typed() {
                    conf.cluster_name = Some(cluster_name);
                    conf.cass_version = Some(cass_version);
                }
            }
        }
    }

    eprintln!(
        "info: Connected to {} running Cassandra version {}",
        conf.cluster_name.as_deref().unwrap_or("unknown"),
        conf.cass_version.as_deref().unwrap_or("unknown")
    );

    let mut session = Session::new(session);

    if program.has_schema() {
        eprintln!("info: Creating schema...");
        if let Err(e) = program.schema(&mut session).await {
            eprintln!("error: Failed to create schema: {}", e);
            exit(255);
        }
    }

    if program.has_erase() && !conf.no_load {
        eprintln!("info: Erasing data...");
        if let Err(e) = program.erase(&mut session).await {
            eprintln!("error: Failed to erase: {}", e);
            exit(255);
        }
    }

    if program.has_prepare() {
        eprintln!("info: Preparing...");
        if let Err(e) = program.prepare(&mut session).await {
            eprintln!("error: Failed to prepare: {}", e);
            exit(255);
        }
    }

    let loader = Workload::new(session.clone(), program.clone(), FnRef::new(LOAD_FN));
    let runner = Workload::new(session.clone(), program.clone(), FnRef::new(RUN_FN));

    let interrupt = Arc::new(InterruptHandler::install());

    if !conf.no_load {
        let load_count = program.load_count();
        if load_count > 0 && program.has_load() {
            eprintln!("info: Loading data...");
            let load_options = ExecutionOptions {
                duration: config::Duration::Count(load_count),
                rate: None,
                threads: conf.threads,
                concurrency: conf.load_concurrency,
            };
            par_execute("Loading...", &load_options, None, loader, interrupt.clone()).await?;
        }
    }

    if interrupt.is_interrupted() {
        return Err(LatteError::Interrupted);
    }

    if conf.warmup_duration.is_not_zero() {
        eprintln!("info: Warming up...");
        let warmup_options = ExecutionOptions {
            duration: conf.warmup_duration,
            rate: None,
            threads: conf.threads,
            concurrency: conf.concurrency,
        };
        par_execute(
            "Warming up...",
            &warmup_options,
            None,
            runner.clone(),
            interrupt.clone(),
        )
        .await?;
    }

    if interrupt.is_interrupted() {
        return Err(LatteError::Interrupted);
    }

    eprintln!("info: Running benchmark...");

    println!(
        "{}",
        RunConfigCmp {
            v1: &conf,
            v2: compare.as_ref().map(|c| &c.conf),
        }
    );

    let exec_options = ExecutionOptions {
        duration: conf.run_duration,
        concurrency: conf.concurrency,
        rate: conf.rate,
        threads: conf.threads,
    };
    report::print_log_header();
    let stats = par_execute(
        "Running...",
        &exec_options,
        Some(conf.sampling_period),
        runner,
        interrupt.clone(),
    )
    .await?;

    let stats_cmp = BenchmarkCmp {
        v1: &stats,
        v2: compare.as_ref().map(|c| &c.result),
    };
    println!();
    println!("{}", &stats_cmp);

    let path = conf
        .output
        .clone()
        .unwrap_or_else(|| get_default_output_name(&conf));

    let report = Report::new(conf, stats);
    match report.save(&path) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("error: Failed to save report to {}: {}", path.display(), e);
            exit(1)
        }
    }
    Ok(())
}

async fn show(conf: ShowCommand) -> Result<()> {
    let report1 = load_report_or_abort(&conf.report);
    let report2 = conf.baseline.map(|p| load_report_or_abort(&p));

    let config_cmp = RunConfigCmp {
        v1: &report1.conf,
        v2: report2.as_ref().map(|r| &r.conf),
    };
    println!("{}", config_cmp);

    let results_cmp = BenchmarkCmp {
        v1: &report1.result,
        v2: report2.as_ref().map(|r| &r.result),
    };
    println!("{}", results_cmp);
    Ok(())
}

async fn async_main(command: Command) -> Result<()> {
    match command {
        Command::Run(config) => run(config).await?,
        Command::Show(config) => show(config).await?,
    }
    Ok(())
}

fn main() {
    console::set_colors_enabled(true);
    let command = AppConfig::parse().command;
    let runtime = match &command {
        Command::Run(cmd) if cmd.threads.get() >= 1 => Builder::new_multi_thread()
            .worker_threads(cmd.threads.get())
            .enable_all()
            .build(),
        _ => Builder::new_current_thread().enable_all().build(),
    };
    if let Err(e) = runtime.unwrap().block_on(async_main(command)) {
        eprintln!("error: {}", e);
        exit(128);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_take_one_of_each() {
        let s1 = futures::stream::iter(1..=3);
        let s2 = futures::stream::iter(1..=2);
        let s3 = futures::stream::iter(1..=2);
        let mut streams = vec![s1, s2, s3];
        assert_eq!(take_one_of_each(&mut streams).await, vec![1, 1, 1]);
        assert_eq!(take_one_of_each(&mut streams).await, vec![2, 2, 2]);
        assert_eq!(take_one_of_each(&mut streams).await, vec![3]);
        assert_eq!(take_one_of_each(&mut streams).await, Vec::<u32>::new());
        assert!(streams.is_empty());
    }
}

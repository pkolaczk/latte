use std::cmp::max;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cassandra_cpp::Session;
use clap::Clap;
use futures::{Future, SinkExt, Stream, StreamExt};
use tokio::runtime::Builder;
use tokio::time::Interval;

use config::RunCommand;

use crate::config::{AppConfig, Command, ShowCommand};
use crate::progress::FastProgressBar;
use crate::report::{Report, RunConfigCmp};
use crate::session::*;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Recorder, RequestStats, Sample};
use crate::workload::null::Null;
use crate::workload::read::Read;
use crate::workload::write::Write;
use crate::workload::{Workload, WorkloadStats};

mod config;
mod progress;
mod report;
mod session;
mod stats;
mod workload;

/// Reports an error and aborts the program if workload creation fails.
/// Returns unwrapped workload.
fn unwrap_workload<W: Workload>(w: workload::Result<W>) -> W {
    match w {
        Ok(w) => w,
        Err(e) => {
            eprintln!("error: Failed to initialize workload: {}", e);
            exit(1);
        }
    }
}

async fn workload(conf: &RunCommand, session: Session) -> Arc<dyn Workload> {
    let session = Box::new(session);
    let wc = conf.workload_config();
    match conf.workload {
        config::Workload::Read => Arc::new(unwrap_workload(Read::new(session, &wc).await)),
        config::Workload::Write => Arc::new(unwrap_workload(Write::new(session, &wc).await)),
        config::Workload::Null => Arc::new(Null {}),
    }
}

fn interval(rate: f64) -> Interval {
    let interval = Duration::from_nanos(max(1, (1000000000.0 / rate) as u64));
    tokio::time::interval(interval)
}

/// Rounds the duration down to the highest number of whole periods
fn round(duration: Duration, period: Duration) -> Duration {
    let mut duration = duration.as_micros();
    duration /= period.as_micros();
    duration *= period.as_micros();
    Duration::from_micros(duration as u64)
}

/// Runs a series of requests on a separate thread and
/// produces a stream of QueryStats
fn req_stream<F, C, R, E>(
    count: u64,
    parallelism: usize,
    rate: Option<f64>,
    sampling_period: Duration,
    context: Arc<C>,
    action: F,
) -> impl Stream<Item = Sample>
where
    F: Fn(Arc<C>, u64) -> R + Send + Sync + 'static,
    C: ?Sized + Send + Sync + 'static,
    R: Future<Output = Result<WorkloadStats, E>> + Send,
    E: Send + 'static,
{
    let (mut tx, rx) = futures::channel::mpsc::channel(1024);

    tokio::spawn(async move {
        let rate = rate.unwrap_or(f64::MAX);
        let action = &action;
        let mut req_stats = interval(rate)
            .take(count as usize)
            .enumerate()
            .map(|(i, _)| {
                let context = context.clone();
                async move {
                    let start = Instant::now();
                    let result = action(context, i as u64).await;
                    let end = Instant::now();
                    RequestStats::from_result(result, end - start)
                }
            })
            .buffer_unordered(parallelism);

        let mut sample = Sample::new(Instant::now());
        while let Some(req) = req_stats.next().await {
            sample.record(req);
            let now = Instant::now();
            if now - sample.start_time > sampling_period {
                let start_time = sample.start_time;
                let elapsed_rounded = round(now - start_time, sampling_period);
                let end_time = start_time + elapsed_rounded;
                sample.finish(end_time);
                tx.send(sample).await.unwrap();
                sample = Sample::new(end_time);
            }
        }
        if !sample.is_empty() {
            sample.finish(Instant::now());
            tx.send(sample).await.unwrap();
        }
    });

    rx
}

/// Waits until one item arrives in each of the streams
/// and collects them into a vector.
/// If all streams are finished, returns None.
async fn take_one_of_each<S, T>(v: &mut Vec<S>) -> Option<Vec<T>>
where
    S: Stream<Item = T> + std::marker::Unpin,
{
    let mut result = Vec::with_capacity(v.len());
    for s in v {
        if let Some(item) = s.next().await {
            result.push(item)
        }
    }
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Executes the given function many times in parallel.
/// Draws a progress bar.
/// Returns the statistics such as throughput or duration histogram.
///
/// # Parameters
///   - `name`: text displayed next to the progress bar
///   - `count`: number of iterations
///   - `parallelism`: maximum number of concurrent executions of `action`
///   - `rate`: optional rate limit given as number of calls to `action` per second
///   - `context`: a shared object to be passed to all invocations of `action`,
///      used to share e.g. a Cassandra session or Workload
///   - `action`: an async function to call; this function may fail and return an `Err`
async fn par_execute<F, C, R, RE>(
    name: &str,
    count: u64,
    threads: usize,
    parallelism: usize,
    rate: Option<f64>,
    sampling_period: Duration,
    context: Arc<C>,
    action: F,
) -> BenchmarkStats
where
    F: Fn(Arc<C>, u64) -> R + Send + Sync + Copy + 'static,
    C: ?Sized + Send + Sync + 'static,
    R: Future<Output = Result<WorkloadStats, RE>> + Send,
    RE: Send + 'static,
{
    let progress = FastProgressBar::new_progress_bar(name, count);
    let progress_ref_1 = Arc::new(progress);
    let progress_ref_2 = progress_ref_1.clone();
    let mut stats = Recorder::start(rate, parallelism);
    let action = move |c, i| {
        let result = action(c, i);
        progress_ref_1.tick();
        result
    };

    let mut streams = Vec::with_capacity(threads);
    let count = max(1, count / threads as u64);
    for _ in 0..threads {
        let s = req_stream(
            count,
            parallelism,
            rate,
            sampling_period,
            context.clone(),
            action.clone(),
        );
        streams.push(s)
    }

    while let Some(samples) = take_one_of_each(&mut streams).await {
        let aggregate = stats.record(&samples);
        if sampling_period.as_secs() < u64::MAX {
            progress_ref_2.println(format!("{}", aggregate));
        }
    }
    stats.finish()
}

fn load_report_or_abort(path: &PathBuf) -> Report {
    match Report::load(path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to load report from {}: {}", path.display(), e);
            exit(1)
        }
    }
}

async fn run(conf: RunCommand) {
    let conf = conf.set_timestamp_if_empty();
    let compare = conf.compare.as_ref().map(|p| load_report_or_abort(p));

    let mut cluster = cluster(&conf);
    let session = connect_or_abort(&mut cluster).await;
    setup_keyspace_or_abort(&conf, &session).await;
    let session = connect_keyspace_or_abort(&mut cluster, conf.keyspace.as_str()).await;
    let workload = workload(&conf, session).await;

    println!(
        "{}",
        RunConfigCmp {
            v1: &conf,
            v2: compare.as_ref().map(|c| &c.conf)
        }
    );

    par_execute(
        "Populating...",
        workload.populate_count(),
        conf.threads,
        conf.parallelism,
        None, // make it as fast as possible
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.populate(i),
    )
    .await;

    par_execute(
        "Warming up...",
        conf.warmup_count,
        conf.threads,
        conf.parallelism,
        None,
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    report::print_log_header();
    let stats = par_execute(
        "Running...",
        conf.count,
        conf.threads,
        conf.parallelism,
        conf.rate,
        Duration::from_secs_f64(conf.sampling_period),
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    let stats_cmp = BenchmarkCmp {
        v1: &stats,
        v2: compare.as_ref().map(|c| &c.result),
    };
    println!();
    println!("{}", &stats_cmp);

    let path = conf
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(".latte-report.json"));

    let report = Report::new(conf, stats);
    match report.save(&path) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Failed to save report to {}: {}", path.display(), e);
            exit(1)
        }
    }
}

async fn show(conf: ShowCommand) {
    let report1 = load_report_or_abort(&PathBuf::from(conf.report1));
    let report2 = conf
        .report2
        .map(|p| load_report_or_abort(&PathBuf::from(p)));

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
}

async fn async_main() {
    let command = AppConfig::parse().command;
    match command {
        Command::Run(config) => run(config).await,
        Command::Show(config) => show(config).await,
    }
}

fn main() {
    console::set_colors_enabled(true);
    Builder::new_multi_thread()
        .thread_name("tokio")
        .enable_time()
        .build()
        .unwrap()
        .block_on(async_main());
}

use std::cmp::max;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Clap;
use futures::future::ready;
use futures::{Future, SinkExt, Stream, StreamExt};
use scylla::Session;
use tokio::runtime::Builder;
use tokio::time::Interval;

use config::RunCommand;

use crate::config::{AppConfig, Command, ShowCommand};
use crate::count_down::{BatchedCountDown, CountDown};
use crate::progress::FastProgressBar;
use crate::report::{Report, RunConfigCmp};
use crate::session::*;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Recorder, RequestStats, Sample};
use crate::workload::null::Null;
use crate::workload::read::Read;
use crate::workload::write::Write;
use crate::workload::{Workload, WorkloadStats};
use tokio_stream::wrappers::IntervalStream;

mod config;
mod count_down;
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
    count: Arc<CountDown>,
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
    let (mut tx, rx) = futures::channel::mpsc::channel(16);

    tokio::spawn(async move {
        let rate = rate.unwrap_or(f64::MAX);
        let action = &action;
        let mut remaining_count = BatchedCountDown::new(count, 64);
        let pending_count = AtomicUsize::new(0);
        let mut req_stats = IntervalStream::new(interval(rate))
            .take_while(|_| ready(remaining_count.dec()))
            .enumerate()
            .map(|(i, _)| {
                let context = context.clone();
                let pending_count = &pending_count;
                async move {
                    let queue_len = pending_count.fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let result = action(context, i as u64).await;
                    let end = Instant::now();
                    pending_count.fetch_sub(1, Ordering::Relaxed);
                    RequestStats::from_result(result, end - start, queue_len)
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
/// Finished streams are removed from `streams`.
async fn take_one_of_each<S, T>(streams: &mut Vec<S>) -> Vec<T>
where
    S: Stream<Item=T> + std::marker::Unpin,
{
    let mut result = Vec::with_capacity(streams.len());
    for i in (0..streams.len()).rev() {
        match streams[i].next().await {
            Some(item) => {
                result.push(item);
            }
            None => {
                streams.swap_remove(i);
            }
        }
    }
    result
}

/// Controls the intensity of requests sent to the server
struct ExecutionOptions {
    /// Maximum rate of requests in requests per second, None means no limit
    rate: Option<f64>,
    /// Number of parallel threads of execution
    threads: usize,
    /// Number of outstanding async requests per each thread
    parallelism: usize,
}

/// Executes the given function many times in parallel.
/// Draws a progress bar.
/// Returns the statistics such as throughput or duration histogram.
///
/// # Parameters
///   - `name`: text displayed next to the progress bar
///   - `count`: number of iterations
///   - `threads`: number of threads to run in parallel
///   - `parallelism`: maximum number of concurrent executions of `action`
///   - `rate`: optional rate limit given as number of calls to `action` per second
///   - `context`: a shared object to be passed to all invocations of `action`,
///      used to share e.g. a Cassandra session or Workload
///   - `action`: an async function to call; this function may fail and return an `Err`
async fn par_execute<F, C, R, RE>(
    name: &str,
    count: u64,
    exec_options: &ExecutionOptions,
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
    let threads = exec_options.threads;
    let parallelism = exec_options.parallelism;
    let rate = exec_options.rate;

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
    let count = Arc::new(CountDown::new(count));
    for _ in 0..threads {
        let s = req_stream(
            count.clone(),
            parallelism,
            rate.map(|r| r / (threads as f64)),
            sampling_period,
            context.clone(),
            action.clone(),
        );
        streams.push(s)
    }

    while !streams.is_empty() {
        let samples = take_one_of_each(&mut streams).await;
        if !samples.is_empty() {
            let aggregate = stats.record(&samples);
            if sampling_period.as_secs() < u64::MAX {
                progress_ref_2.println(format!("{}", aggregate));
            }
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

    let session = connect_or_abort(&conf).await;
    setup_keyspace_or_abort(&conf, &session).await;
    use_keyspace_or_abort(&session, conf.keyspace.as_str()).await;
    let workload = workload(&conf, session).await;
    let exec_options = ExecutionOptions {
        parallelism: conf.parallelism,
        rate: conf.rate,
        threads: conf.threads,
    };

    println!(
        "{}",
        RunConfigCmp {
            v1: &conf,
            v2: compare.as_ref().map(|c| &c.conf),
        }
    );

    par_execute(
        "Populating...",
        workload.populate_count(),
        &exec_options,
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.populate(i),
    )
    .await;

    par_execute(
        "Warming up...",
        conf.warmup_count,
        &exec_options,
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    report::print_log_header();
    let stats = par_execute(
        "Running...",
        conf.count,
        &exec_options,
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
        .worker_threads(16)
        .thread_name("tokio")
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main());
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

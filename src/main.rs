use std::cmp::max;
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cassandra_cpp::Session;
use clap::Clap;
use tokio::macros::support::Future;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::time::Interval;

use config::Config;

use crate::progress::FastProgressBar;
use crate::session::*;
use crate::stats::BenchmarkStats;
use crate::workload::read_none::*;
use crate::workload::read_same::*;
use crate::workload::write::*;
use crate::workload::Workload;

mod config;
mod progress;
mod session;
mod stats;
mod workload;

/// Reports an error and aborts the program if workload creation fails.
/// Returns unwrapped workload.
fn unwrap_workload<W: Workload>(w: cassandra_cpp::Result<W>) -> W {
    match w {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Failed to initialize workload: {}", e);
            exit(1);
        }
    }
}

async fn workload(conf: &Config, session: Session) -> Arc<dyn Workload> {
    let session = Box::new(session);
    match conf.workload {
        config::Workload::ReadNone => Arc::new(unwrap_workload(ReadNone::new(session).await)),
        config::Workload::ReadSame => Arc::new(unwrap_workload(ReadSame::new(session).await)),
        config::Workload::Write => Arc::new(unwrap_workload(Write::new(session).await)),
    }
}

fn interval(rate: f64) -> Interval {
    let interval = Duration::from_nanos(max(1, (1000000000.0 / rate) as u64));
    tokio::time::interval(interval)
}

/// Executes the given function many times in parallel.
/// Draws a progress bar.
/// Returns the statistics such as throughput or duration histogram.
///
/// # Parameters
///   - `name`: text displayed next to the progress bar
///   - `count`: number of iterations
///   - `concurrency`: maximum number of concurrent executions of `action`
///   - `rate`: optional rate limit given as number of calls to `action` per second
///   - `context`: a shared object to be passed to all invocations of `action`,
///      used to share e.g. a Cassandra session or Workload
///   - `action`: an async function to call; this function may fail and return an `Err`
async fn par_execute<F, C, R, RR, RE>(
    name: &str,
    count: u64,
    concurrency: usize,
    rate: Option<f64>,
    context: Arc<C>,
    action: F,
) -> BenchmarkStats
where
    F: Fn(Arc<C>, u64) -> R + Send + Sync + Copy + 'static,
    C: ?Sized + Send + Sync + 'static,
    R: Future<Output = Result<RR, RE>> + Send,
    RR: Send,
    RE: Send,
{
    let progress = Arc::new(FastProgressBar::new_progress_bar(name, count));
    let mut stats = BenchmarkStats::start();
    let mut interval = interval(rate.unwrap_or(f64::MAX));
    let semaphore = Arc::new(Semaphore::new(concurrency));

    type Item = Result<Duration, ()>;
    let (tx, mut rx): (Sender<Item>, Receiver<Item>) = tokio::sync::mpsc::channel(concurrency);

    for i in 0..count {
        if rate.is_some() {
            interval.tick().await;
        }
        let permit = semaphore.clone().acquire_owned().await;
        let concurrent_count = concurrency - semaphore.available_permits();
        stats.enqueued(concurrent_count);
        while let Ok(d) = rx.try_recv() {
            stats.record(d)
        }
        let mut tx = tx.clone();
        let context = context.clone();
        let progress = progress.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            match action(context, i).await {
                Ok(_) => {
                    let end = Instant::now();
                    let duration = max(Duration::from_micros(1), end - start);
                    tx.send(Ok(duration)).await.unwrap();
                }
                Err(_) => tx.send(Err(())).await.unwrap(),
            }
            progress.tick();
            drop(permit);
        });
    }
    drop(tx);

    while let Some(d) = rx.next().await {
        stats.record(d)
    }
    stats.finish();
    stats
}

async fn async_main() {
    let conf: Config = Config::parse();
    let mut cluster = cluster(&conf);
    let session = connect_or_abort(&mut cluster).await;
    setup_keyspace_or_abort(&conf, &session).await;
    let session = connect_keyspace_or_abort(&mut cluster, conf.keyspace.as_str()).await;
    let workload = workload(&conf, session).await;

    par_execute(
        "Populating...",
        workload.population_size(),
        conf.concurrency,
        None, // make it as fast as possible
        workload.clone(),
        |w, i| w.populate(i),
    )
    .await;

    par_execute(
        "Warming up...",
        conf.warmup_count,
        conf.concurrency,
        None,
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    let stats = par_execute(
        "Running...",
        conf.count,
        conf.concurrency,
        conf.rate,
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    conf.print();
    stats.print(&conf);
}

fn main() {
    let mut runtime = tokio::runtime::Builder::new()
        .max_threads(1)
        .basic_scheduler()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async_main());
}

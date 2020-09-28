use std::cmp::max;
use std::process::exit;
use std::sync::Arc;

use cassandra_cpp::Session;
use clap::Clap;
use tokio::macros::support::Future;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::time::{Duration, Instant, Interval};

use config::Config;

use crate::progress::FastProgressBar;
use crate::session::*;
use crate::stats::{ActionStats, BenchmarkStats};
use crate::workload::read::Read;
use crate::workload::write::Write;
use crate::workload::{Workload, WorkloadStats};

mod config;
mod progress;
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

async fn workload(conf: &Config, session: Session) -> Arc<dyn Workload> {
    let session = Box::new(session);
    let row_count = conf.rows.unwrap_or(conf.count);
    match conf.workload {
        config::Workload::Read => Arc::new(unwrap_workload(Read::new(session, row_count).await)),
        config::Workload::Write => Arc::new(unwrap_workload(Write::new(session, row_count).await)),
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
async fn par_execute<F, C, R, RE>(
    name: &str,
    count: u64,
    concurrency: usize,
    rate: Option<f64>,
    sampling_period: Duration,
    context: Arc<C>,
    action: F,
) -> BenchmarkStats
where
    F: Fn(Arc<C>, u64) -> R + Send + Sync + Copy + 'static,
    C: ?Sized + Send + Sync + 'static,
    R: Future<Output = Result<WorkloadStats, RE>> + Send,
    RE: Send,
{
    let progress = Arc::new(FastProgressBar::new_progress_bar(name, count));
    let mut stats = BenchmarkStats::start();
    let mut interval = interval(rate.unwrap_or(f64::MAX));
    let semaphore = Arc::new(Semaphore::new(concurrency));

    type Item = Result<ActionStats, ()>;
    let (tx, mut rx): (Sender<Item>, Receiver<Item>) = tokio::sync::mpsc::channel(concurrency);

    for i in 0..count {
        if rate.is_some() {
            interval.tick().await;
        }
        let permit = semaphore.clone().acquire_owned().await;
        let concurrent_count = concurrency - semaphore.available_permits();
        stats.enqueued(concurrent_count);

        if Instant::now() - stats.last_sample_time() > sampling_period {
            stats.sample();
        }

        while let Ok(d) = rx.try_recv() {
            stats.record(d)
        }
        let mut tx = tx.clone();
        let context = context.clone();
        let progress = progress.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            match action(context, i).await {
                Ok(result) => {
                    let end = Instant::now();
                    let s = ActionStats {
                        duration: max(Duration::from_micros(1), end - start),
                        row_count: result.row_count,
                        partition_count: result.partition_count,
                    };
                    tx.send(Ok(s)).await.unwrap();
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
        workload.populate_count(),
        conf.concurrency,
        None, // make it as fast as possible
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.populate(i),
    )
    .await;

    par_execute(
        "Warming up...",
        conf.warmup_count,
        conf.concurrency,
        None,
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    let stats = par_execute(
        "Running...",
        conf.count,
        conf.concurrency,
        conf.rate,
        Duration::from_secs_f64(conf.sampling_period),
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

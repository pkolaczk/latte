use std::cmp::max;
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cassandra_cpp::{stmt, BindRustType, Cluster, Session};
use clap::Clap;
use tokio::macros::support::Future;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::time::Interval;

use config::Config;

use crate::progress::FastProgressBar;
use crate::stats::Stats;

mod config;
mod progress;
mod stats;

/// Prepares the connection to Cassandra.
/// Reports an error and exits the program if the connection cannot be established.
fn connect(conf: &Config) -> cassandra_cpp::Result<Session> {
    let mut cluster = Cluster::default();
    for addr in conf.addresses.iter() {
        cluster.set_contact_points(addr).unwrap();
    }
    cluster
        .set_core_connections_per_host(conf.connections)
        .unwrap();
    cluster
        .set_max_connections_per_host(conf.connections)
        .unwrap();
    cluster
        .set_queue_size_event(conf.parallelism as u32)
        .unwrap();
    cluster.set_queue_size_io(conf.parallelism as u32).unwrap();
    cluster.set_num_threads_io(conf.threads).unwrap();
    cluster.set_connect_timeout(time::Duration::seconds(5));
    cluster.set_load_balance_round_robin();
    cluster.connect()
}

/// Sets up test schema - creates keyspace and tables
async fn setup_schema(_conf: &Config, session: &Session) -> cassandra_cpp::Result<()> {
    session
        .execute(&stmt!(
            "CREATE KEYSPACE IF NOT EXISTS latte \
             WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"
        ))
        .await?;

    session
        .execute(&stmt!(
            "CREATE TABLE IF NOT EXISTS latte.tiny\
            (pk BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT)"
        ))
        .await?;
    Ok(())
}

async fn setup_data(session: &Session) -> cassandra_cpp::Result<()> {
    session
        .execute(&stmt!(
            "INSERT INTO latte.tiny(pk, c1, c2, c3, c4, c5) VALUES (1, 1, 2, 3, 4, 5)"
        ))
        .await?;
    Ok(())
}

/// Runs action as fast as possible within the parallelism limit, without measuring anything
async fn warmup<F, C, R, RR, RE>(conf: &Config, context: Arc<C>, action: F)
where
    F: Fn(&C, u64) -> R + Send + Sync + Copy + 'static,
    R: Future<Output = Result<RR, RE>> + Send,
    C: Send + Sync + 'static,
    RR: Send,
    RE: Send,
{
    let progress = Arc::new(FastProgressBar::new_progress_bar(
        "Warming up...",
        conf.count,
    ));
    let (tx, mut rx): (Sender<()>, Receiver<()>) = tokio::sync::mpsc::channel(conf.parallelism);
    let semaphore = Arc::new(Semaphore::new(conf.parallelism));
    for i in 0..conf.warmup_count {
        let permit = semaphore.clone().acquire_owned().await;
        let context = context.clone();
        let progress = progress.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let _discard = action(&context, i).await;
            progress.tick();
            // need to move the permit and tx to inside of the spawned task,
            // so we drop it not earlier than the task is done
            drop(permit);
            drop(tx);
        });
        //progress.tick();
    }
    drop(tx);
    rx.next().await; // wait until all coroutines finish
}

fn interval(rate: f32) -> Interval {
    let interval = Duration::from_nanos(max(1, (1000000000.0 / rate) as u64));
    tokio::time::interval(interval)
}

async fn benchmark<F, C, R, RR, RE>(conf: &Config, context: Arc<C>, action: F) -> Stats
where
    F: Fn(&C, u64) -> R + Send + Sync + Copy + 'static,
    R: Future<Output = Result<RR, RE>> + Send,
    C: Send + Sync + 'static,
    RR: Send,
    RE: Send,
{
    if conf.warmup_count > 0 {
        warmup(conf, context.clone(), action).await;
    }

    let progress = Arc::new(FastProgressBar::new_progress_bar("Running...", conf.count));
    let mut stats = Stats::start();
    let mut interval = interval(conf.rate);
    let semaphore = Arc::new(Semaphore::new(conf.parallelism));

    type Item = Result<Duration, ()>;
    let (tx, mut rx): (Sender<Item>, Receiver<Item>) = tokio::sync::mpsc::channel(conf.parallelism);

    for i in 0..conf.count {
        interval.tick().await;
        let permit = semaphore.clone().acquire_owned().await;
        let concurrent_count = conf.parallelism - semaphore.available_permits();
        stats.enqueued(concurrent_count);
        while let Ok(d) = rx.try_recv() {
            stats.record(d)
        }
        let mut tx = tx.clone();
        let context = context.clone();
        let progress = progress.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            match action(&context, i).await {
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
    let opt: Config = Config::parse();
    let session = match connect(&opt) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to Cassandra: {}", e);
            exit(1)
        }
    };

    match setup_schema(&opt, &session).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Failed to setup schema: {}", e);
            exit(1);
        }
    }

    match setup_data(&session).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Failed to prepare data: {}", e);
            exit(1);
        }
    }

    let statement = session
        .prepare("SELECT c1, c2, c3, c4, c5 FROM latte.tiny WHERE pk = ?")
        .unwrap()
        .await
        .unwrap();
    let ctx = Arc::new((session, statement));

    let stats = benchmark(&opt, ctx, |(session, statement), _i| {
        session.execute(&statement.bind().bind(0, 1 as i64).unwrap())
    })
    .await;

    opt.print();
    stats.print(&opt);
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

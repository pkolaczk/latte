use std::cmp::max;
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cassandra_cpp::{stmt, Cluster, Session};
use clap::Clap;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::time::Interval;

use config::Config;
use tokio::macros::support::Future;
use crate::stats::Stats;

mod config;
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
        .set_queue_size_event(conf.concurrency as u32)
        .unwrap();
    cluster.set_queue_size_io(conf.concurrency as u32).unwrap();
    cluster.set_num_threads_io(1).unwrap();
    cluster.set_connect_timeout(time::Duration::seconds(5));
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
            "CREATE TABLE IF NOT EXISTS latte.espresso(pk BIGINT PRIMARY KEY, value BIGINT)"
        ))
        .await?;
    Ok(())
}

fn interval(conf: &Config) -> Interval {
    let interval = Duration::from_nanos(max(1, (1000000000.0 / conf.rate) as u64));
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
    let mut stats = Stats::start();
    let mut interval = interval(conf);
    let semaphore = Arc::new(Semaphore::new(conf.concurrency));

    type Item = Result<Duration, ()>;
    let (tx, mut rx): (Sender<Item>, Receiver<Item>) = tokio::sync::mpsc::channel(conf.concurrency);

    for i in 0..conf.count {
        interval.tick().await;
        let permit = semaphore.clone().acquire_owned().await;
        while let Ok(d) = rx.try_recv() {
            stats.record(d)
        }
        let mut tx = tx.clone();
        let context = context.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let start = Instant::now();
            match action(&context, i).await {
                Ok(_) => {
                    let end = Instant::now();
                    let duration = max(Duration::from_micros(1), end - start);
                    tx.send(Ok(duration)).await.unwrap();
                }
                Err(_) => tx.send(Err(())).await.unwrap(),
            }
        });
    }
    drop(tx);

    while let Some(d) = rx.next().await {
        stats.record(d)
    }
    stats.finish();
    stats
}

#[tokio::main]
async fn main() {
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

    let statement = session
        .prepare("SELECT value FROM latte.espresso WHERE pk = 1")
        .unwrap()
        .await
        .unwrap();
    let ctx = Arc::new((session, statement));
    let stats = benchmark(&opt, ctx, |(session, statement), _| {
        session.execute(&statement.bind())
    })
    .await;
    stats.print();
}

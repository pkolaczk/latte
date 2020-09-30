use clap::Clap;
use core::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clap, Debug)]
pub enum Workload {
    Read,
    Write,
}

impl Display for Workload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Workload::Read => write!(f, "read")?,
            Workload::Write => write!(f, "write")?,
        };
        Ok(())
    }
}

/// Latency Tester for Apache Cassandra
#[derive(Clap, Debug)]
pub struct Config {
    /// Name of the keyspace
    #[clap(short("k"), long, default_value = "latte")]
    pub keyspace: String,

    /// Number of requests per second to send.
    /// If not given the requests will be sent as fast as possible within the parallelism limit
    #[clap(short("r"), long)]
    pub rate: Option<f64>,

    /// Number of non-measured, warmup requests
    #[clap(short("w"), long("warmup"), default_value = "1")]
    pub warmup_count: u64,

    /// Number of measured requests
    #[clap(short("n"), long, default_value = "1000000")]
    pub count: u64,

    /// Total number of distinct rows in the data-set.
    /// Applies to read and write workloads. Defaults to count.
    #[clap(short("d"), long)]
    pub rows: Option<u64>,

    /// Number of I/O threads used by the driver
    #[clap(short("t"), long, default_value = "1")]
    pub threads: u32,

    /// Number of connections per io_thread
    #[clap(short("c"), long, default_value = "1")]
    pub connections: u32,

    /// Max number of concurrent async requests
    #[clap(short("p"), long("concurrency"), default_value = "1024")]
    pub concurrency: usize,

    /// Throughput sampling period, in seconds
    #[clap(short("s"), long, default_value = "1.0")]
    pub sampling_period: f64,

    /// Workload type
    #[clap(arg_enum, name = "workload", required = true)]
    pub workload: Workload,

    /// List of Cassandra addresses to connect to
    #[clap(name = "addresses", required = true, default_value = "localhost")]
    pub addresses: Vec<String>,
}

impl Config {
    pub fn print(&self) {
        println!("CONFIG ====================================================================================");
        println!("           Workload: {:>11}", self.workload.to_string());
        println!("            Threads: {:11}", self.threads);
        println!(
            "  Total connections: {:11}",
            self.threads * self.connections
        );
        match self.rate {
            Some(rate) => println!("         Rate limit: {:11.1} req/s", rate),
            None => println!("         Rate limit: {:>11} req/s", "-"),
        }
        println!("  Concurrency limit: {:11} req", self.concurrency);
        println!("  Warmup iterations: {:11} req", self.warmup_count);
        println!("Measured iterations: {:11} req", self.count);
        println!("           Sampling: {:11.1} s", self.sampling_period);

        println!();
    }
}

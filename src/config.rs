use clap::Clap;

/// Latency Tester for Apache Cassandra
#[derive(Clap, Debug)]
pub struct Config {
    /// Number of requests per second to send.
    /// If not given the requests will be sent as fast as possible within the parallelism limit
    #[clap(short("r"), long)]
    pub rate: Option<f64>,

    /// Number of non-measured, warmup requests
    #[clap(short("w"), long("warmup"), default_value = "0")]
    pub warmup_count: u64,

    /// Number of measured requests
    #[clap(short("n"), long, default_value = "1000000")]
    pub count: u64,

    /// Number of I/O threads used by the driver
    #[clap(short("t"), long, default_value = "1")]
    pub threads: u32,

    /// Number of connections per io_thread
    #[clap(short, long, default_value = "1")]
    pub connections: u32,

    /// Max number of concurrent requests
    #[clap(short("p"), long, default_value = "1024")]
    pub parallelism: usize,

    /// List of Cassandra addresses to connect to
    #[clap(name = "addresses", required = true, default_value = "localhost")]
    pub addresses: Vec<String>,
}

impl Config {
    pub fn print(&self) {
        println!("CONFIG -----------------------------------------");
        println!("           Threads: {:11}", self.threads);
        println!(" Total connections: {:11}", self.threads * self.connections);
        match self.rate {
            Some(rate) =>
                println!("        Rate limit: {:11.1} req/s", rate),
            None =>
                println!("        Rate limit:         disabled"),
        }
        println!(" Concurrency limit: {:11} reqs", self.parallelism);
        println!();
    }
}

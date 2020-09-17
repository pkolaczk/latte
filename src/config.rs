use clap::Clap;

#[derive(Clap, Debug)]
pub struct Config {
    /// Number of requests per second to send
    #[clap(short("r"), long)]
    pub rate: f32,

    /// Total number of requests to make
    #[clap(short("n"), long)]
    pub count: u64,

    /// Number of io_threads used by the driver
    #[clap(short("t"), long, default_value = "1")]
    pub io_threads: u32,

    /// Number of connections per io_thread
    #[clap(short, long, default_value = "1")]
    pub connections: u32,

    /// Max number of concurrent requests
    #[clap(short("p"), long, default_value = "1024")]
    pub parallelism: usize,

    /// List of Cassandra addresses to connect to
    #[clap(name = "addresses", required = true)]
    pub addresses: Vec<String>,
}

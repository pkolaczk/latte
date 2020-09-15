use clap::Clap;

#[derive(Clap, Debug)]
pub struct Config {
    /// Number of requests per second
    #[clap(short("r"), long)]
    pub rate: f32,

    /// Total number of requests to make
    #[clap(short("n"), long)]
    pub count: u64,

    /// Number of connections per node
    #[clap(short, long, default_value = "1")]
    pub connections: u32,

    /// Max number of outstanding requests
    #[clap(short("p"), default_value = "1024")]
    pub concurrency: usize,

    /// List of Cassandra addresses to connect to
    #[clap(name = "addresses", required = true)]
    pub addresses: Vec<String>,
}

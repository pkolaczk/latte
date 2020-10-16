use crate::config::RunCommand;
use cassandra_cpp::{Cluster, Session};
use std::process::exit;

/// Configures connection to Cassandra.
pub fn cluster(conf: &RunCommand) -> Cluster {
    let mut cluster = Cluster::default();
    for addr in conf.addresses.iter() {
        cluster.set_contact_points(addr).unwrap();
    }
    cluster
        .set_core_connections_per_host(conf.connections as u32)
        .unwrap();
    cluster
        .set_max_connections_per_host(conf.connections as u32)
        .unwrap();
    cluster
        .set_queue_size_event(conf.parallelism as u32)
        .unwrap();
    cluster.set_queue_size_io(conf.parallelism as u32).unwrap();
    cluster.set_num_threads_io(conf.threads as u32).unwrap();
    cluster.set_connect_timeout(time::Duration::seconds(5));
    cluster.set_load_balance_round_robin();
    cluster
}

/// Connects to the cluster and returns a connected session object.
/// On failure, displays an error message and aborts the application.
pub async fn connect_or_abort(cluster: &mut Cluster) -> Session {
    match cluster.connect_async().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: Failed to connect to Cassandra: {}", e);
            exit(1)
        }
    }
}

/// Sets up the test keyspace
pub async fn setup_keyspace(conf: &RunCommand, session: &Session) -> cassandra_cpp::Result<()> {
    let statement = cassandra_cpp::Statement::new(
        format!(
            "CREATE KEYSPACE IF NOT EXISTS \"{}\" \
             WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}",
            conf.keyspace
        )
        .as_str(),
        0,
    );
    session.execute(&statement).await?;
    Ok(())
}

/// Sets up the test keyspace.
/// On failure, displays an error message and aborts the program.
pub async fn setup_keyspace_or_abort(conf: &RunCommand, session: &Session) {
    if let Err(e) = setup_keyspace(&conf, &session).await {
        eprintln!("error: Failed to setup keyspace {}: {}", &conf.keyspace, e);
        exit(1)
    }
}

/// Connects to the given keyspace.
/// On failure, displays an error message and aborts the program.
pub async fn connect_keyspace_or_abort(cluster: &mut Cluster, keyspace: &str) -> Session {
    let session = Session::new();
    if let Err(e) = session.connect_keyspace(&cluster, keyspace).unwrap().await {
        eprintln!("error: Failed to reconnect to keyspace {}: {}", keyspace, e);
        exit(1)
    }
    session
}

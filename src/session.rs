use crate::config::RunCommand;
use itertools::Itertools;
use scylla::transport::errors::{NewSessionError, QueryError};
use scylla::{Session, SessionBuilder};
use std::process::exit;

/// Configures connection to Cassandra.
pub async fn connect(conf: &RunCommand) -> Result<Session, NewSessionError> {
    SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .build()
        .await
}

/// Connects to the cluster and returns a connected session object.
/// On failure, displays an error message and aborts the application.
pub async fn connect_or_abort(conf: &RunCommand) -> Session {
    match connect(conf).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "error: Failed to connect to Cassandra at [{}]: {}",
                conf.addresses.iter().join(", "),
                e
            );
            exit(1)
        }
    }
}

/// Sets up the test keyspace
pub async fn setup_keyspace(conf: &RunCommand, session: &Session) -> Result<(), QueryError> {
    let cql = format!(
        "CREATE KEYSPACE IF NOT EXISTS \"{}\" \
             WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}",
        conf.keyspace
    );
    session.query(cql, &[]).await?;
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
pub async fn use_keyspace_or_abort(session: &Session, keyspace: &str) {
    if let Err(e) = session.use_keyspace(keyspace, true).await {
        eprintln!("error: Failed to use keyspace {}: {}", keyspace, e);
        exit(1)
    }
}

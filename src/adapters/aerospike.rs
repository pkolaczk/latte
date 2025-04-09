use crate::scripting::executor::Executor;
use crate::stats::session::SessionStats;
use aerospike::{as_key, Bin, Bins, Client, Error, Key, ReadPolicy, WritePolicy};
use std::sync::Arc;
use tracing::error;

#[derive(Clone)]
pub struct AerospikeAdapter {
    connection: Arc<Client>,
    executor: Arc<Executor<(), Error>>,
    read_policy: ReadPolicy,
    write_policy: WritePolicy,
    namespace: String,
    set: String,
}

impl AerospikeAdapter {
    pub fn new(
        client: Client,
        executor: Executor<(), Error>,
        read_policy: ReadPolicy,
        write_policy: WritePolicy,
        namespace: String,
        set: String,
    ) -> Self {
        Self {
            connection: Arc::new(client),
            executor: Arc::new(executor),
            read_policy,
            write_policy,
            namespace,
            set,
        }
    }

    pub async fn get(&self, key: &str) -> Result<(), Error> {
        let aero_key = as_key!(&self.namespace, &self.set, key);
        if let Err(err) = self
            .executor
            .execute_inner(|| self.get_inner(&aero_key), |_| 1)
            .await
        {
            error!("{}", err);
            return Err(err);
        }

        Ok(())
    }

    async fn get_inner(&self, aero_key: &Key) -> Result<(), Error> {
        self.connection
            .get(&self.read_policy, aero_key, Bins::All)
            .await
            .map(|_| ())
    }

    pub async fn put(&self, key: &str, values: Vec<Bin>) -> Result<(), Error> {
        let aero_key = as_key!(&self.namespace, &self.set, key);
        if let Err(err) = self
            .executor
            .execute_inner(
                || {
                    self.connection
                        .put(&self.write_policy, &aero_key, values.clone().leak())
                },
                |_| 1,
            )
            .await
        {
            error!("{}", err);
            return Err(err);
        }

        Ok(())
    }

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        self.executor.take_session_stats()
    }

    /// Resets query and request counters
    pub fn reset(&self) {
        self.executor.reset();
    }
}

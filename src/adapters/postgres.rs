use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::connect::ClusterInfo;
use crate::scripting::executor::Executor;
use crate::stats::session::SessionStats;
use itertools::Itertools;
use rune::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_postgres::types::ToSql;
use tokio_postgres::Error;
use tokio_postgres::{Client, Statement};
use tracing::error;

#[derive(Clone)]
pub struct PostgresAdapter {
    client: Arc<Client>,
    statements: HashMap<String, Statement>,
    executor: Arc<Executor<u64, Error>>,
    request_timeout: Duration,
}

impl PostgresAdapter {
    pub fn new(
        client: Client,
        request_timeout: Duration,
        executor: Executor<u64, Error>,
    ) -> PostgresAdapter {
        PostgresAdapter {
            client: Arc::new(client),
            statements: HashMap::new(),
            executor: Arc::new(executor),
            request_timeout,
        }
    }

    /// Returns cluster metadata such as cluster name and cassandra version.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, CassError> {
        Ok(None)
    }

    /// Prepares a statement and stores it in an internal statement map for future use.
    pub async fn prepare(&mut self, key: &str, cql: &str) -> Result<(), CassError> {
        let statement = self
            .client
            .prepare(cql)
            .await
            .map_err(|e| CassError::pg_prepare_error(cql, e))?;
        self.statements.insert(key.to_string(), statement);

        Ok(())
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    pub async fn execute(&self, cql: &str) -> Result<(), CassError> {
        if let Err(err) = self
            .executor
            .execute_inner(
                || self.client.execute(cql, &[]),
                |x|
                    // SELECT will always return 0
                    x.to_owned(),
            )
            .await
        {
            let err = CassError(CassErrorKind::PgExecution(cql.to_string(), err));
            error!("{}", err);
            return Err(err);
        }
        Ok(())
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare`.
    pub async fn execute_prepared(&self, key: &str, params: Value) -> Result<(), CassError> {
        let statement = self
            .statements
            .get(key)
            .ok_or_else(|| CassError(CassErrorKind::PreparedStatementNotFound(key.to_string())))?;

        let params = crate::scripting::bind::to_pg_query_params(&params, statement.params())
            .unwrap()
            .into_iter()
            .map(|x| Box::leak(x) as &(dyn ToSql + Sync))
            .collect_vec();

        let rs = self
            .executor
            .execute_inner(
                || self.client.execute(statement, params.as_slice()),
                |x| x.to_owned(),
            )
            .await;

        if let Err(err) = rs {
            let err = CassError(CassErrorKind::PgExecution(key.to_string(), err));
            error!("{}", err);
            return Err(err);
        }

        Ok(())
    }

    pub async fn query_to_row_count(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let count = self
            .client
            .query(statement, params)
            .await
            .map(|x| x.len() as u64);

        count
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare`.
    pub async fn query_prepared(&self, key: &str, params: Value) -> Result<(), CassError> {
        let statement = self
            .statements
            .get(key)
            .ok_or_else(|| CassError(CassErrorKind::PreparedStatementNotFound(key.to_string())))?;

        let params = crate::scripting::bind::to_pg_query_params(&params, statement.params())
            .unwrap()
            .into_iter()
            .map(|x| Box::leak(x) as &(dyn ToSql + Sync))
            .collect_vec();

        let rs = self
            .executor
            .execute_inner(
                || self.query_to_row_count(statement, params.as_slice()),
                |x| x.to_owned(),
            )
            .await;

        if let Err(err) = rs {
            let err = CassError(CassErrorKind::PgExecution(key.to_string(), err));
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

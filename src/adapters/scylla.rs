use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::connect::ClusterInfo;
use crate::scripting::executor::Executor;
use crate::stats::session::SessionStats;
use rune::Value;
use scylla::client::session::Session;
use scylla::errors::ExecutionError;
use scylla::response::query_result::QueryResult;
use scylla::statement::prepared::PreparedStatement;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

#[derive(Clone)]
pub struct ScyllaAdapter {
    session: Arc<Session>,
    statements: HashMap<String, Arc<PreparedStatement>>,
    executor: Arc<Executor<QueryResult, ExecutionError>>,
}

impl ScyllaAdapter {
    pub fn new(session: Session, executor: Executor<QueryResult, ExecutionError>) -> ScyllaAdapter {
        ScyllaAdapter {
            session: Arc::new(session),
            statements: HashMap::new(),
            executor: Arc::new(executor),
        }
    }

    /// Returns cluster metadata such as cluster name and cassandra version.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, CassError> {
        let cql = "SELECT cluster_name, release_version FROM system.local";
        let rs = self
            .session
            .query_unpaged(cql, ())
            .await
            .map_err(|e| CassError::query_execution_error(cql, &[], e))?;
        if let Ok(rows) = rs.into_rows_result() {
            if let Ok((name, cassandra_version)) = rows.single_row() {
                return Ok(Some(ClusterInfo {
                    name,
                    cassandra_version,
                }));
            }
        }
        Ok(None)
    }

    /// Prepares a statement and stores it in an internal statement map for future use.
    pub async fn prepare(&mut self, key: &str, cql: &str) -> Result<(), CassError> {
        let statement = self
            .session
            .prepare(cql)
            .await
            .map_err(|e| CassError::prepare_error(cql, e))?;
        self.statements.insert(key.to_string(), Arc::new(statement));
        Ok(())
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    pub async fn execute(&self, cql: &str) -> Result<(), CassError> {
        if let Err(err) = self
            .executor
            .execute_inner(
                || self.session.query_unpaged(cql, ()),
                |x| {
                    x.clone()
                        .into_rows_result()
                        .map(|x| x.rows_num())
                        .unwrap_or(0) as u64
                },
            )
            .await
        {
            let err = CassError::query_execution_error(cql, &[], err);
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

        let params = crate::scripting::bind::to_scylla_query_params(
            &params,
            statement.get_variable_col_specs(),
        )
        .unwrap();

        let rs = self
            .executor
            .execute_inner(
                || self.session.execute_unpaged(statement, params.clone()),
                |x| {
                    x.clone()
                        .into_rows_result()
                        .map(|x| x.rows_num())
                        .unwrap_or(0) as u64
                },
            )
            .await;

        if let Err(err) = rs {
            let err = CassError::query_execution_error(statement.get_statement(), &params, err);
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

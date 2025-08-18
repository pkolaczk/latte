use crate::config::RetryStrategy;
use crate::error::LatteError;
use crate::scripting::bind;
use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::connect::ClusterInfo;
use crate::stats::session::SessionStats;
use rand::prelude::ThreadRng;
use rand::random;
use rune::runtime::{Object, Shared};
use rune::{Any, Value};
use scylla::client::session::Session;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::response::query_result::QueryResult;
use scylla::statement::prepared::PreparedStatement;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::error;
use try_lock::TryLock;

/// This is the main object that a workload script uses to interface with the outside world.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Context {
    start_time: TryLock<Instant>,
    session: Arc<Session>,
    statements: HashMap<String, Arc<PreparedStatement>>,
    stats: TryLock<SessionStats>,
    retry_strategy: RetryStrategy,
    #[rune(get, set, add_assign, copy)]
    pub load_cycle_count: u64,
    #[rune(get)]
    pub data: Value,
    pub rng: ThreadRng,
}

// Needed, because Rune `Value` is !Send, as it may contain some internal pointers.
// Therefore, it is not safe to pass a `Value` to another thread by cloning it, because
// both objects could accidentally share some unprotected, `!Sync` data.
// To make it safe, the same `Context` is never used by more than one thread at once, and
// we make sure in `clone` to make a deep copy of the `data` field by serializing
// and deserializing it, so no pointers could get through.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(session: Session, retry_strategy: RetryStrategy) -> Context {
        Context {
            start_time: TryLock::new(Instant::now()),
            session: Arc::new(session),
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
            retry_strategy,
            load_cycle_count: 0,
            data: Value::Object(Shared::new(Object::new()).unwrap()),
            rng: rand::thread_rng(),
        }
    }

    /// Clones the context for use by another thread.
    /// The new clone gets fresh statistics.
    /// The user data gets passed through serialization and deserialization to avoid
    /// accidental data sharing.
    #[allow(clippy::result_large_err)]
    pub fn clone(&self) -> Result<Self, LatteError> {
        let serialized = rmp_serde::to_vec(&self.data)?;
        let deserialized: Value = rmp_serde::from_slice(&serialized)?;
        Ok(Context {
            session: self.session.clone(),
            statements: self.statements.clone(),
            stats: TryLock::new(SessionStats::default()),
            data: deserialized,
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
            rng: rand::thread_rng(),
            ..*self
        })
    }

    /// Returns cluster metadata such as cluster name and cassandra version.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, CassError> {
        let cql = "SELECT cluster_name, release_version FROM system.local";
        let rs = self
            .session
            .query_unpaged(cql, ())
            .await
            .map_err(|e| CassError::query_execution_error(cql, &[], e))?
            .into_rows_result()
            .map_err(|e| CassError::result_set_conversion_error(cql, &[], e))?;

        if let Ok(rows) = rs.rows() {
            if let Some(Ok((name, cassandra_version))) = rows.into_iter().next() {
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
            .execute_inner(|| self.session.query_unpaged(cql, ()))
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
        let statement = self.statements.get(key).ok_or_else(|| {
            CassError::new(CassErrorKind::PreparedStatementNotFound(key.to_string()))
        })?;

        let params =
            bind::to_scylla_query_params(&params, statement.get_variable_col_specs().as_slice())?;
        let rs = self
            .execute_inner(|| self.session.execute_unpaged(statement, params.clone()))
            .await;

        if let Err(err) = rs {
            let err = CassError::query_execution_error(statement.get_statement(), &params, err);
            error!("{}", err);
            return Err(err);
        }

        Ok(())
    }

    async fn execute_inner<R>(&self, f: impl Fn() -> R) -> Result<(), ExecutionError>
    where
        R: Future<Output = Result<QueryResult, ExecutionError>>,
    {
        let start_time = self.stats.try_lock().unwrap().start_request();

        let mut rs: Result<QueryResult, ExecutionError>;
        let mut attempts = 0;
        let retry_strategy = &self.retry_strategy;
        loop {
            rs = f().await;
            if rs.is_ok()
                || attempts >= retry_strategy.retries
                || !should_retry(&rs, retry_strategy)
            {
                break;
            }

            attempts += 1;

            let current_retry_interval = get_exponential_retry_interval(
                retry_strategy.retry_delay.min,
                retry_strategy.retry_delay.max,
                attempts,
            );
            tokio::time::sleep(current_retry_interval).await;
        }
        let duration = Instant::now() - start_time;
        self.stats
            .try_lock()
            .unwrap()
            .complete_request(duration, rs, attempts);
        Ok(())
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start_time.try_lock().unwrap().elapsed().as_secs_f64()
    }

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        let mut stats = self.stats.try_lock().unwrap();
        let result = stats.clone();
        stats.reset();
        result
    }

    /// Resets query and request counters
    pub fn reset(&self) {
        self.stats.try_lock().unwrap().reset();
        *self.start_time.try_lock().unwrap() = Instant::now();
    }
}

pub fn get_exponential_retry_interval(
    min_interval: Duration,
    max_interval: Duration,
    current_attempt_num: u64,
) -> Duration {
    let min_interval_float: f64 = min_interval.as_secs_f64();
    let mut current_interval: f64 =
        min_interval_float * (2u64.pow(current_attempt_num.try_into().unwrap_or(0)) as f64);

    // Add jitter
    current_interval += random::<f64>() * min_interval_float;
    current_interval -= min_interval_float / 2.0;

    Duration::from_secs_f64(current_interval.min(max_interval.as_secs_f64()))
}

fn should_retry<R>(result: &Result<R, ExecutionError>, retry_strategy: &RetryStrategy) -> bool {
    if !result.is_err() {
        return false;
    }
    if retry_strategy.retry_on_all_errors {
        return true;
    }
    matches!(
        result,
        Err(ExecutionError::RequestTimeout(_))
            | Err(ExecutionError::LastAttemptError(
                RequestAttemptError::DbError(
                    DbError::ReadTimeout { .. }
                        | DbError::WriteTimeout { .. }
                        | DbError::Overloaded
                        | DbError::RateLimitReached { .. },
                    _
                )
            ))
    )
}

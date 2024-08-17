use crate::config::RetryStrategy;
use crate::error::LatteError;
use crate::scripting::bind;
use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::connect::ClusterInfo;
use crate::scripting::rng::Rng;
use crate::stats::session::SessionStats;
use rand::random;
use rand::rngs::SmallRng;
use rune::runtime::{AnyObj, BorrowRef, Object, Shared, VmResult};
use rune::{vm_try, Any, ToValue, Value};
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::{DbError, QueryError};
use scylla::QueryResult;
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
pub struct LocalContext {
    #[rune(get)]
    pub cycle: i64,
    #[rune(get)]
    pub rng: Value,
    #[rune(get)]
    pub global: Value,
}

impl From<LocalContext> for Value {
    fn from(value: LocalContext) -> Self {
        Value::Any(Shared::new(AnyObj::new(value).unwrap()).unwrap())
    }
}

#[derive(Any)]
pub struct GlobalContext {
    start_time: TryLock<Instant>,
    session: Arc<scylla::Session>,
    statements: HashMap<String, Arc<PreparedStatement>>,
    stats: TryLock<SessionStats>,
    retry_strategy: RetryStrategy,
    #[rune(get, set, add_assign, copy)]
    pub load_cycle_count: u64,
    #[rune(get)]
    pub data: Value,
}

// Needed, because Rune `Value` is !Send, as it may contain some internal pointers.
// Therefore, it is not safe to pass a `Value` to another thread by cloning it, because
// both objects could accidentally share some unprotected, `!Sync` data.
// To make it safe, we make sure in `clone` to make a deep copy of the `data` field by serializing
// and deserializing it, so no pointers could get through.
unsafe impl Send for GlobalContext {}

impl GlobalContext {
    pub fn new(session: scylla::Session, retry_strategy: RetryStrategy) -> Self {
        Self {
            start_time: TryLock::new(Instant::now()),
            session: Arc::new(session),
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
            retry_strategy,
            load_cycle_count: 0,
            data: Value::Object(Shared::new(Object::new()).unwrap()),
        }
    }

    /// Clones the context for use by another thread.
    /// The new clone gets fresh statistics.
    /// The user data gets passed through serialization and deserialization to avoid
    /// accidental data sharing.
    pub fn clone(&self) -> Result<Self, LatteError> {
        let serialized = rmp_serde::to_vec(&self.data)?;
        let deserialized: Value = rmp_serde::from_slice(&serialized)?;
        Ok(Self {
            session: self.session.clone(),
            statements: self.statements.clone(),
            stats: TryLock::new(SessionStats::default()),
            data: deserialized,
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
            ..*self
        })
    }

    /// Returns cluster metadata such as cluster name and cassandra version.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, CassError> {
        let cql = "SELECT cluster_name, release_version FROM system.local";
        let rs = self
            .session
            .query(cql, ())
            .await
            .map_err(|e| CassError::query_execution_error(cql, &[], e))?;
        if let Some(rows) = rs.rows {
            if let Some(row) = rows.into_iter().next() {
                if let Ok((name, cassandra_version)) = row.into_typed() {
                    return Ok(Some(ClusterInfo {
                        name,
                        cassandra_version,
                    }));
                }
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
        if let Err(err) = self.execute_inner(|| self.session.query(cql, ())).await {
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

        let params = bind::to_scylla_query_params(&params, statement.get_variable_col_specs())?;
        let rs = self
            .execute_inner(|| self.session.execute(statement, params.clone()))
            .await;

        if let Err(err) = rs {
            let err = CassError::query_execution_error(statement.get_statement(), &params, err);
            error!("{}", err);
            return Err(err);
        }

        Ok(())
    }

    async fn execute_inner<R>(&self, f: impl Fn() -> R) -> Result<QueryResult, QueryError>
    where
        R: Future<Output = Result<QueryResult, QueryError>>,
    {
        let start_time = self.stats.try_lock().unwrap().start_request();

        let mut rs: Result<QueryResult, QueryError> = Err(QueryError::TimeoutError);
        let mut attempts = 0;
        let retry_strategy = &self.retry_strategy;
        while attempts <= retry_strategy.retries && should_retry(&rs, retry_strategy) {
            if attempts > 0 {
                let current_retry_interval = get_exponential_retry_interval(
                    retry_strategy.retry_delay.min,
                    retry_strategy.retry_delay.max,
                    attempts,
                );
                tokio::time::sleep(current_retry_interval).await;
            }
            rs = f().await;
            attempts += 1;
        }

        let duration = Instant::now() - start_time;
        self.stats
            .try_lock()
            .unwrap()
            .complete_request(duration, &rs, attempts - 1);
        rs
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

impl LocalContext {
    pub fn new(cycle: i64, global: GlobalContextRef, rng: SmallRng) -> Self {
        Self {
            cycle,
            global: global.to_value().into_result().unwrap(),
            rng: Value::Any(Shared::new(AnyObj::new(Rng::with_rng(rng)).unwrap()).unwrap()),
        }
    }

    pub fn global(&self) -> BorrowRef<GlobalContext> {
        let Value::Any(obj) = &self.global else {
            panic!("global must be an object")
        };
        obj.downcast_borrow_ref().unwrap()
    }
}

/// Wraps a reference to `Context` that can be converted to a Rune `Value`
/// and passed as one of `Args` arguments to a function.
pub struct GlobalContextRef<'a> {
    context: &'a GlobalContext,
}

impl GlobalContextRef<'_> {
    pub fn new(context: &GlobalContext) -> GlobalContextRef {
        GlobalContextRef { context }
    }
}

/// We need this to be able to pass a reference to `Session` as an argument
/// to Rune function.
///
/// Caution! Be careful using this trait. Undefined Behaviour possible.
/// This is unsound - it is theoretically
/// possible that the underlying `Session` gets dropped before the `Value` produced by this trait
/// implementation and the compiler is not going to catch that.
/// The receiver of a `Value` must ensure that it is dropped before `Session`!
impl<'a> ToValue for GlobalContextRef<'a> {
    fn to_value(self) -> VmResult<Value> {
        let obj = unsafe { AnyObj::from_ref(self.context) };
        VmResult::Ok(Value::from(vm_try!(Shared::new(obj))))
    }
}

/// Wraps a mutable reference to Session that can be converted to a Rune `Value` and passed
/// as one of `Args` arguments to a function.
pub struct GlobalContextRefMut<'a> {
    context: &'a mut GlobalContext,
}

impl GlobalContextRefMut<'_> {
    pub fn new(context: &mut GlobalContext) -> GlobalContextRefMut {
        GlobalContextRefMut { context }
    }
}

/// Caution! See `impl ToValue for SessionRef`.
impl<'a> ToValue for GlobalContextRefMut<'a> {
    fn to_value(self) -> VmResult<Value> {
        let obj = unsafe { AnyObj::from_mut(self.context) };
        VmResult::Ok(Value::from(vm_try!(Shared::new(obj))))
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

fn should_retry<R>(result: &Result<R, QueryError>, retry_strategy: &RetryStrategy) -> bool {
    if !result.is_err() {
        return false;
    }
    if retry_strategy.retry_on_all_errors {
        return true;
    }
    matches!(
        result,
        Err(QueryError::RequestTimeout(_))
            | Err(QueryError::TimeoutError)
            | Err(QueryError::DbError(
                DbError::ReadTimeout { .. } | DbError::WriteTimeout { .. } | DbError::Overloaded,
                _
            ))
    )
}

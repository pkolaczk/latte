use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::process::exit;
use std::sync::Arc;

use hdrhistogram::Histogram;
use itertools::Itertools;
use rune::runtime::TypeInfo;
use rune::{Any, Value};
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::{DbError, NewSessionError, QueryError};
use scylla::transport::session::PoolSize;
use scylla::{QueryResult, SessionBuilder};

use tokio::time::{Duration, Instant};
use try_lock::TryLock;

use crate::config::RunCommand;

/// Configures connection to Cassandra.
pub async fn connect(conf: &RunCommand) -> Result<scylla::Session, NewSessionError> {
    SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .pool_size(PoolSize::PerShard(conf.connections))
        .build()
        .await
}

/// Connects to the cluster and returns a connected session object.
/// On failure, displays an error message and aborts the application.
pub async fn connect_or_abort(conf: &RunCommand) -> scylla::Session {
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

#[derive(Any, Debug)]
pub struct CassError(pub CassErrorKind);

#[derive(Debug)]
pub enum CassErrorKind {
    PreparedStatementNotFound(String),
    UnsupportedType(TypeInfo),
    Overloaded(QueryError),
    Other(QueryError),
}

impl CassError {
    pub fn display(&self, buf: &mut String) -> std::fmt::Result {
        use std::fmt::Write;
        match &self.0 {
            CassErrorKind::PreparedStatementNotFound(s) => {
                write!(buf, "Prepared statement not found: {}", s)
            }
            CassErrorKind::UnsupportedType(s) => {
                write!(buf, "Unsupported type in Cassandra query: {}", s)
            }
            CassErrorKind::Overloaded(e) => write!(buf, "Overloaded: {}", e),
            CassErrorKind::Other(e) => write!(buf, "Other error: {}", e),
        }
    }
}

impl Display for CassError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buf = String::new();
        self.display(&mut buf).unwrap();
        write!(f, "{}", buf)
    }
}

impl From<QueryError> for CassError {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::TimeoutError
            | QueryError::DbError(
                DbError::Overloaded | DbError::ReadTimeout { .. } | DbError::WriteTimeout { .. },
                _,
            ) => CassError(CassErrorKind::Overloaded(err)),
            _ => CassError(CassErrorKind::Other(err)),
        }
    }
}

impl std::error::Error for CassError {}

#[derive(Clone, Debug)]
pub struct SessionStats {
    pub req_count: u64,
    pub req_errors: HashSet<String>,
    pub req_error_count: u64,
    pub row_count: u64,
    pub queue_length: u64,
    pub mean_queue_length: f32,
    pub resp_times_ns: Histogram<u64>,
}

impl SessionStats {
    pub fn new() -> SessionStats {
        Default::default()
    }

    pub fn start_request(&mut self) -> Instant {
        if self.req_count > 0 {
            self.mean_queue_length +=
                (self.queue_length as f32 - self.mean_queue_length) / self.req_count as f32;
        }
        self.queue_length += 1;
        Instant::now()
    }

    pub fn complete_request(&mut self, duration: Duration, rs: &Result<QueryResult, QueryError>) {
        self.queue_length -= 1;
        let duration_ns = duration.as_nanos().clamp(1, u64::MAX as u128) as u64;
        self.resp_times_ns.record(duration_ns).unwrap();
        self.req_count += 1;
        match rs {
            Ok(rs) => self.row_count += rs.rows.as_ref().map(|r| r.len()).unwrap_or(0) as u64,
            Err(e) => {
                self.req_error_count += 1;
                self.req_errors.insert(format!("{}", e));
            }
        }
    }

    /// Resets all accumulators
    pub fn reset(&mut self) {
        self.req_error_count = 0;
        self.row_count = 0;
        self.req_count = 0;
        self.mean_queue_length = 0.0;
        self.req_errors.clear();
        self.resp_times_ns.clear();

        // note that current queue_length is *not* reset to zero because there
        // might be pending requests and if we set it to zero, that would underflow
    }
}

impl Default for SessionStats {
    fn default() -> Self {
        SessionStats {
            req_count: 0,
            req_errors: HashSet::new(),
            req_error_count: 0,
            row_count: 0,
            queue_length: 0,
            mean_queue_length: 0.0,
            resp_times_ns: Histogram::new(3).unwrap(),
        }
    }
}

/// Cassandra session object exposed in a workload script.
/// This is the main object that a workload script can execute queries through.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Session {
    inner: Arc<scylla::Session>,
    prepared_statements: HashMap<String, Arc<PreparedStatement>>,
    stats: TryLock<SessionStats>,
}

impl Clone for Session {
    /// Clones the session. The new clone gets fresh statistics.
    fn clone(&self) -> Self {
        Session {
            inner: self.inner.clone(),
            prepared_statements: self.prepared_statements.clone(),
            stats: TryLock::new(SessionStats::default()),
        }
    }
}

impl Session {
    pub fn new(session: scylla::Session) -> Session {
        Session {
            inner: Arc::new(session),
            prepared_statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
        }
    }

    /// Prepares a statement and stores it in an internal statement map for future use.
    pub async fn prepare(&mut self, key: &str, cql: &str) -> Result<(), CassError> {
        let statement = self.inner.prepare(cql).await?;
        self.prepared_statements
            .insert(key.to_string(), Arc::new(statement));
        Ok(())
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    pub async fn execute(&self, cql: &str) -> Result<(), CassError> {
        let start_time = self.stats.try_lock().unwrap().start_request();
        let rs = self.inner.query(cql, ()).await;
        let duration = Instant::now() - start_time;
        self.stats
            .try_lock()
            .unwrap()
            .complete_request(duration, &rs);
        rs?;
        Ok(())
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare`.
    pub async fn execute_prepared(&self, key: &str, params: Value) -> Result<(), CassError> {
        let statement = self
            .prepared_statements
            .get(key)
            .ok_or_else(|| CassError(CassErrorKind::PreparedStatementNotFound(key.to_string())))?;
        let params = bind::to_scylla_query_params(&params)?;
        let start_time = self.stats.try_lock().unwrap().start_request();
        let rs = self.inner.execute(statement, params).await;
        let duration = Instant::now() - start_time;
        self.stats
            .try_lock()
            .unwrap()
            .complete_request(duration, &rs);
        rs?;
        Ok(())
    }

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_stats(&self) -> SessionStats {
        let mut stats = self.stats.try_lock().unwrap();
        let result = stats.clone();
        stats.reset();
        result
    }

    /// Resets query and request counters
    pub fn reset_stats(&self) {
        self.stats.try_lock().unwrap().reset();
    }
}

/// Functions for binding rune values to CQL parameters
mod bind {
    use scylla::frame::response::result::CqlValue;

    use crate::workload::context;
    use crate::workload::context::Uuid;
    use crate::CassErrorKind;

    use super::*;

    fn to_scylla_value(v: &Value) -> Result<CqlValue, CassError> {
        match v {
            Value::Bool(v) => Ok(CqlValue::Boolean(*v)),
            Value::Byte(v) => Ok(CqlValue::TinyInt(*v as i8)),
            Value::Integer(v) => Ok(CqlValue::BigInt(*v)),
            Value::Float(v) => Ok(CqlValue::Double(*v)),
            Value::StaticString(v) => Ok(CqlValue::Text(v.as_str().to_string())),
            Value::String(v) => Ok(CqlValue::Text(v.borrow_ref().unwrap().as_str().to_string())),
            Value::Bytes(v) => Ok(CqlValue::Blob(v.borrow_ref().unwrap().to_vec())),
            Value::Option(v) => match v.borrow_ref().unwrap().as_ref() {
                Some(v) => to_scylla_value(v),
                None => Ok(CqlValue::Empty),
            },
            Value::Vec(v) => {
                let v = v.borrow_ref().unwrap();
                let elements = v.as_ref().iter().map(to_scylla_value).try_collect()?;
                Ok(CqlValue::List(elements))
            }
            Value::Any(obj) => {
                let obj = obj.borrow_ref().unwrap();
                if obj.type_hash() == Uuid::type_hash() {
                    let uuid: &context::Uuid = obj.downcast_borrow_ref().unwrap();
                    Ok(CqlValue::Uuid(uuid.0))
                } else {
                    Err(CassError(CassErrorKind::UnsupportedType(
                        v.type_info().unwrap(),
                    )))
                }
            }
            other => Err(CassError(CassErrorKind::UnsupportedType(
                other.type_info().unwrap(),
            ))),
        }
    }

    /// Binds parameters passed as a single rune value to the arguments of the statement.
    /// The `params` value can be a tuple, a vector, a struct or an object.
    pub fn to_scylla_query_params(params: &Value) -> Result<Vec<CqlValue>, CassError> {
        let mut values = Vec::new();
        match params {
            Value::Tuple(tuple) => {
                let tuple = tuple.borrow_ref().unwrap();
                for v in tuple.iter() {
                    values.push(to_scylla_value(v)?);
                }
            }
            Value::Vec(vec) => {
                let vec = vec.borrow_ref().unwrap();
                for v in vec.iter() {
                    values.push(to_scylla_value(v)?);
                }
            }
            other => {
                return Err(CassError(CassErrorKind::UnsupportedType(
                    other.type_info().unwrap(),
                )));
            }
        }
        Ok(values)
    }
}

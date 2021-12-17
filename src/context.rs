use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
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

use crate::config::ConnectionConf;

/// Configures connection to Cassandra.
pub async fn connect(conf: &ConnectionConf) -> Result<scylla::Session, CassError> {
    SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .pool_size(PoolSize::PerShard(conf.count))
        .build()
        .await
        .map_err(|e| CassError(CassErrorKind::FailedToConnect(conf.addresses.clone(), e)))
}

pub struct ClusterInfo {
    pub name: String,
    pub cassandra_version: String,
}

#[derive(Any, Debug)]
pub struct CassError(pub CassErrorKind);

#[derive(Debug)]
pub enum CassErrorKind {
    FailedToConnect(Vec<String>, NewSessionError),
    PreparedStatementNotFound(String),
    UnsupportedType(TypeInfo),
    Overloaded(QueryError),
    Other(QueryError),
}

impl CassError {
    pub fn display(&self, buf: &mut String) -> std::fmt::Result {
        use std::fmt::Write;
        match &self.0 {
            CassErrorKind::FailedToConnect(hosts, e) => {
                write!(buf, "Could not connect to {}: {}", hosts.join(","), e)
            }
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

/// This is the main object that a workload script uses to interface with the outside world.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Context {
    session: Arc<scylla::Session>,
    statements: HashMap<String, Arc<PreparedStatement>>,
    stats: TryLock<SessionStats>,
    #[rune(get, set, add_assign, copy)]
    pub load_cycle_count: u64,
}

impl Clone for Context {
    /// Clones the session. The new clone gets fresh statistics.
    fn clone(&self) -> Self {
        Context {
            session: self.session.clone(),
            statements: self.statements.clone(),
            stats: TryLock::new(SessionStats::default()),
            load_cycle_count: self.load_cycle_count,
        }
    }
}

impl Context {
    pub fn new(session: scylla::Session) -> Context {
        Context {
            session: Arc::new(session),
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
            load_cycle_count: 0,
        }
    }

    /// Returns cluster metadata such as cluster name and cassandra version.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, CassError> {
        let rs = self
            .session
            .query("SELECT cluster_name, release_version FROM system.local", ())
            .await?;
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
        let statement = self.session.prepare(cql).await?;
        self.statements.insert(key.to_string(), Arc::new(statement));
        Ok(())
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    pub async fn execute(&self, cql: &str) -> Result<(), CassError> {
        let start_time = self.stats.try_lock().unwrap().start_request();
        let rs = self.session.query(cql, ()).await;
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
            .statements
            .get(key)
            .ok_or_else(|| CassError(CassErrorKind::PreparedStatementNotFound(key.to_string())))?;
        let params = bind::to_scylla_query_params(&params)?;
        let start_time = self.stats.try_lock().unwrap().start_request();
        let rs = self.session.execute(statement, params).await;
        let duration = Instant::now() - start_time;
        self.stats
            .try_lock()
            .unwrap()
            .complete_request(duration, &rs);
        rs?;
        Ok(())
    }

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        let mut stats = self.stats.try_lock().unwrap();
        let result = stats.clone();
        stats.reset();
        result
    }

    /// Resets query and request counters
    pub fn reset_session_stats(&self) {
        self.stats.try_lock().unwrap().reset();
    }
}

/// Functions for binding rune values to CQL parameters
mod bind {
    use scylla::frame::response::result::CqlValue;

    use crate::workload::globals;
    use crate::workload::globals::{Int16, Int32, Int8, Uuid};
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
                let h = obj.type_hash();
                if h == Uuid::type_hash() {
                    let uuid: &globals::Uuid = obj.downcast_borrow_ref().unwrap();
                    Ok(CqlValue::Uuid(uuid.0))
                } else if h == Int32::type_hash() {
                    let int32: &Int32 = obj.downcast_borrow_ref().unwrap();
                    Ok(CqlValue::Int(int32.0))
                } else if h == Int16::type_hash() {
                    let int16: &Int16 = obj.downcast_borrow_ref().unwrap();
                    Ok(CqlValue::SmallInt(int16.0))
                } else if h == Int8::type_hash() {
                    let int8: &Int8 = obj.downcast_borrow_ref().unwrap();
                    Ok(CqlValue::TinyInt(int8.0))
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

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::process::exit;
use std::sync::Arc;

use cassandra_cpp::{CassErrorCode, CassResult, Cluster, ErrorKind, PreparedStatement, Statement};
use cassandra_cpp_sys::{cass_log_set_level, CassLogLevel_};
use hdrhistogram::Histogram;
use rune::runtime::TypeInfo;
use rune::{Any, Value};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

use crate::config::RunCommand;

/// Configures connection to Cassandra.
pub fn cluster(conf: &RunCommand) -> Cluster {
    unsafe { cass_log_set_level(CassLogLevel_::CASS_LOG_DISABLED) }

    let mut cluster = Cluster::default();
    for addr in conf.addresses.iter() {
        cluster.set_contact_points(addr).unwrap();
    }
    cluster.set_protocol_version(4).unwrap();
    cluster
        .set_core_connections_per_host(conf.connections as u32)
        .unwrap();
    cluster
        .set_max_connections_per_host(conf.connections as u32)
        .unwrap();
    cluster
        .set_queue_size_event(conf.concurrency as u32)
        .unwrap();
    cluster.set_queue_size_io(conf.concurrency as u32).unwrap();
    cluster.set_num_threads_io(conf.threads as u32).unwrap();
    cluster.set_connect_timeout(std::time::Duration::from_secs(5));
    cluster.set_load_balance_round_robin();
    cluster
}

/// Connects to the cluster and returns a connected session object.
/// On failure, displays an error message and aborts the application.
pub async fn connect_or_abort(cluster: &mut Cluster) -> cassandra_cpp::Session {
    match cluster.connect_async().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: Failed to connect to Cassandra: {}", e);
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
    Overloaded(cassandra_cpp::Error),
    Other(cassandra_cpp::Error),
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

impl From<cassandra_cpp::Error> for CassError {
    fn from(e: cassandra_cpp::Error) -> Self {
        match e {
            cassandra_cpp::Error(
                ErrorKind::CassErrorResult(
                    CassErrorCode::SERVER_OVERLOADED
                    | CassErrorCode::SERVER_READ_TIMEOUT
                    | CassErrorCode::SERVER_READ_FAILURE
                    | CassErrorCode::SERVER_WRITE_TIMEOUT
                    | CassErrorCode::SERVER_WRITE_FAILURE,
                    ..,
                ),
                _,
            )
            | cassandra_cpp::Error(
                ErrorKind::CassError(
                    CassErrorCode::LIB_REQUEST_QUEUE_FULL | CassErrorCode::LIB_REQUEST_TIMED_OUT,
                    ..,
                ),
                _,
            ) => CassError(CassErrorKind::Overloaded(e)),
            _ => CassError(CassErrorKind::Other(e)),
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
    pub resp_times_us: Histogram<u64>,
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

    pub fn complete_request(&mut self, duration: Duration, rs: &cassandra_cpp::Result<CassResult>) {
        self.queue_length -= 1;
        let duration_us = duration.as_micros().clamp(1, u64::MAX as u128) as u64;
        self.resp_times_us.record(duration_us).unwrap();
        self.req_count += 1;
        match rs {
            Ok(rs) => self.row_count += rs.row_count(),
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
        self.resp_times_us.clear();

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
            resp_times_us: Histogram::new(3).unwrap(),
        }
    }
}

/// Cassandra session object exposed in a workload script.
/// This is the main object that a workload script can execute queries through.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Session {
    inner: Arc<cassandra_cpp::Session>,
    prepared_statements: HashMap<String, Arc<PreparedStatement>>,
    stats: Mutex<SessionStats>,
}

impl Clone for Session {
    /// Clones the session. The new clone gets fresh statistics.
    fn clone(&self) -> Self {
        Session {
            inner: self.inner.clone(),
            prepared_statements: self.prepared_statements.clone(),
            stats: Mutex::new(SessionStats::default()),
        }
    }
}

impl Session {
    pub fn new(session: cassandra_cpp::Session) -> Session {
        Session {
            inner: Arc::new(session),
            prepared_statements: HashMap::new(),
            stats: Mutex::new(SessionStats::new()),
        }
    }

    /// Prepares a statement and stores it in an internal statement map for future use.
    pub async fn prepare(&mut self, key: &str, cql: &str) -> Result<(), CassError> {
        let statement = self.inner.prepare(cql)?.await?;
        self.prepared_statements
            .insert(key.to_string(), Arc::new(statement));
        Ok(())
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    pub async fn execute(&self, cql: &str) -> Result<(), CassError> {
        let statement = Statement::new(cql, 0);
        let start_time = self.stats.try_lock().unwrap().start_request();
        let rs = self.inner.execute(&statement).await;
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
        let mut statement = statement.bind();
        bind::bind(&mut statement, &params)?;
        let start_time = self.stats.try_lock().unwrap().start_request();
        let rs = self.inner.execute(&statement).await;
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
    use cassandra_cpp::CassCollection;

    use crate::workload::context;
    use crate::workload::context::Uuid;
    use crate::CassErrorKind;

    use super::*;

    /// Appends a single item to a CQL list.
    fn append_list(list: &mut cassandra_cpp::List, value: &Value) -> Result<(), CassError> {
        match value {
            Value::Bool(v) => {
                list.append_bool(*v)?;
            }
            Value::Byte(v) => {
                list.append_int8((*v) as i8)?;
            }
            Value::Bytes(v) => {
                list.append_bytes(v.borrow_ref().unwrap().to_vec())?;
            }
            Value::Integer(v) => {
                list.append_int64(*v)?;
            }
            Value::Float(v) => {
                list.append_double(*v)?;
            }
            Value::StaticString(v) => {
                list.append_string(v.as_str())?;
            }
            Value::String(v) => {
                list.append_string(v.borrow_ref().unwrap().as_str())?;
            }
            Value::Option(v) => {
                if let Some(v) = v.borrow_ref().unwrap().as_ref() {
                    append_list(list, v)?;
                }
            }
            other => {
                return Err(CassError(CassErrorKind::UnsupportedType(
                    other.type_info().unwrap(),
                )));
            }
        }
        Ok(())
    }

    /// Builds a CQL list from an array of rune values.
    fn build_list(values: &[Value]) -> Result<cassandra_cpp::List, CassError> {
        let mut list = cassandra_cpp::List::with_capacity(values.len());
        for v in values {
            append_list(&mut list, v)?;
        }
        Ok(list)
    }

    /// Binds a statement key to a `Value` passed from the workload script.
    fn bind_name(statement: &mut Statement, key: &str, v: &Value) -> Result<(), CassError> {
        match v {
            Value::Bool(v) => {
                statement.bind_bool_by_name(key, *v)?;
            }
            Value::Byte(v) => {
                statement.bind_int8_by_name(key, (*v) as i8)?;
            }
            Value::Bytes(v) => {
                statement.bind_bytes_by_name(key, v.borrow_ref().unwrap().to_vec())?;
            }
            Value::Integer(v) => {
                statement.bind_int64_by_name(key, *v)?;
            }
            Value::Float(v) => {
                statement.bind_double_by_name(key, *v)?;
            }
            Value::StaticString(v) => {
                statement.bind_string_by_name(key, v.as_str())?;
            }
            Value::String(v) => {
                statement.bind_string_by_name(key, v.borrow_ref().unwrap().as_str())?;
            }
            Value::Option(v) => match v.borrow_ref().unwrap().as_ref() {
                Some(v) => {
                    bind_name(statement, key, v)?;
                }
                None => {
                    statement.bind_null_by_name(key)?;
                }
            },
            Value::Vec(v) => {
                let v = v.borrow_ref().unwrap();
                statement.bind_list_by_name(key, build_list(v.as_ref())?)?;
            }
            Value::Any(obj) => {
                let obj = obj.borrow_ref().unwrap();
                if obj.type_hash() == Uuid::type_hash() {
                    let uuid: &Uuid = obj.downcast_borrow_ref().unwrap();
                    statement.bind_uuid_by_name(key, uuid.0.into())?;
                } else {
                    return Err(CassError(CassErrorKind::UnsupportedType(
                        v.type_info().unwrap(),
                    )));
                }
            }
            other => {
                return Err(CassError(CassErrorKind::UnsupportedType(
                    other.type_info().unwrap(),
                )));
            }
        }
        Ok(())
    }

    /// Binds i-th statement argument to a `Value` passed from the workload script.
    // TODO: unify with `bind_name` using traits
    fn bind_index(statement: &mut Statement, index: usize, v: &Value) -> Result<(), CassError> {
        match v {
            Value::Bool(v) => {
                statement.bind_bool(index, *v)?;
            }
            Value::Byte(v) => {
                statement.bind_int8(index, (*v) as i8)?;
            }
            Value::Integer(v) => {
                statement.bind_int64(index, *v)?;
            }
            Value::Float(v) => {
                statement.bind_double(index, *v)?;
            }
            Value::StaticString(v) => {
                statement.bind_string(index, v.as_str())?;
            }
            Value::String(v) => {
                statement.bind_string(index, v.borrow_ref().unwrap().as_str())?;
            }
            Value::Bytes(v) => {
                statement.bind_bytes(index, v.borrow_ref().unwrap().to_vec())?;
            }
            Value::Option(v) => match v.borrow_ref().unwrap().as_ref() {
                Some(v) => {
                    bind_index(statement, index, v)?;
                }
                None => {
                    statement.bind_null(index)?;
                }
            },
            Value::Vec(v) => {
                let v = v.borrow_ref().unwrap();
                statement.bind_list(index, build_list(v.as_ref())?)?;
            }
            Value::Any(obj) => {
                let obj = obj.borrow_ref().unwrap();
                if obj.type_hash() == Uuid::type_hash() {
                    let uuid: &context::Uuid = obj.downcast_borrow_ref().unwrap();
                    statement.bind_uuid(index, uuid.0.into())?;
                } else {
                    return Err(CassError(CassErrorKind::UnsupportedType(
                        v.type_info().unwrap(),
                    )));
                }
            }
            other => {
                return Err(CassError(CassErrorKind::UnsupportedType(
                    other.type_info().unwrap(),
                )));
            }
        }
        Ok(())
    }

    /// Binds parameters passed as a single rune value to the arguments of the statement.
    /// The `params` value can be a tuple, a vector, a struct or an object.
    pub fn bind(statement: &mut Statement, params: &Value) -> Result<(), CassError> {
        match params {
            Value::Object(obj) => {
                let obj = obj.borrow_ref().unwrap();
                for (k, v) in obj.iter() {
                    bind_name(statement, k.as_str(), v)?
                }
            }
            Value::Struct(obj) => {
                let obj = obj.borrow_ref().unwrap();
                for (k, v) in obj.data().iter() {
                    bind_name(statement, k.as_str(), v)?
                }
            }
            Value::Tuple(tuple) => {
                let tuple = tuple.borrow_ref().unwrap();
                for (i, v) in tuple.iter().enumerate() {
                    bind_index(statement, i, v)?
                }
            }
            Value::Vec(vec) => {
                let vec = vec.borrow_ref().unwrap();
                for (i, v) in vec.iter().enumerate() {
                    bind_index(statement, i, v)?
                }
            }
            other => {
                return Err(CassError(CassErrorKind::UnsupportedType(
                    other.type_info().unwrap(),
                )));
            }
        }
        Ok(())
    }
}

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::sync::Arc;

use anyhow::anyhow;
use chrono::Utc;
use hdrhistogram::Histogram;
use itertools::Itertools;
use metrohash::{MetroHash128, MetroHash64};
use openssl::error::ErrorStack;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod};
use rand::distributions::Distribution;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rune::ast;
use rune::ast::Kind;
use rune::macros::{quote, MacroContext, TokenStream};
use rune::parse::Parser;
use rune::runtime::{Object, Shared, TypeInfo, VmError};
use rune::{Any, Value};
use rust_embed::RustEmbed;
use scylla::frame::response::result::CqlValue;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::{DbError, NewSessionError, QueryError};
use scylla::transport::session::PoolSize;
use scylla::{ExecutionProfile, QueryResult, SessionBuilder};
use statrs::distribution::Normal;
use tokio::time::{Duration, Instant};
use try_lock::TryLock;
use uuid::{Variant, Version};

use crate::config::{ConnectionConf, PRINT_RETRY_ERROR_LIMIT, RetryInterval};
use crate::LatteError;

fn ssl_context(conf: &&ConnectionConf) -> Result<Option<SslContext>, CassError> {
    if conf.ssl {
        let mut ssl = SslContextBuilder::new(SslMethod::tls())?;
        if let Some(path) = &conf.ssl_ca_cert_file {
            ssl.set_ca_file(path)?;
        }
        if let Some(path) = &conf.ssl_cert_file {
            ssl.set_certificate_file(path, SslFiletype::PEM)?;
        }
        if let Some(path) = &conf.ssl_key_file {
            ssl.set_private_key_file(path, SslFiletype::PEM)?;
        }
        Ok(Some(ssl.build()))
    } else {
        Ok(None)
    }
}

/// Configures connection to Cassandra.
pub async fn connect(conf: &ConnectionConf) -> Result<Context, CassError> {
    let profile = ExecutionProfile::builder()
        .consistency(conf.consistency.scylla_consistency())
        .request_timeout(Some(Duration::from_secs(conf.request_timeout.get() as u64)))
        .build();

    let scylla_session = SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .pool_size(PoolSize::PerShard(conf.count))
        .user(&conf.user, &conf.password)
        .ssl_context(ssl_context(&conf)?)
        .default_execution_profile_handle(profile.into_handle())
        .build()
        .await
        .map_err(|e| CassError(CassErrorKind::FailedToConnect(conf.addresses.clone(), e)))?;
    Ok(Context::new(scylla_session, conf.retry_number, conf.retry_interval))
}

pub struct ClusterInfo {
    pub name: String,
    pub cassandra_version: String,
}

/// Transforms a CqlValue object to a string dedicated to be part of CassError message
pub fn cql_value_obj_to_string(v: &CqlValue) -> String {
    let no_transformation_size_limit = 32;
    match v {
        // Replace big string- and bytes-alike object values with it's size labels
        CqlValue::Text(param) if param.len() > no_transformation_size_limit => {
            format!("Text(<size>={})", param.len())
        },
        CqlValue::Ascii(param) if param.len() > no_transformation_size_limit => {
            format!("Ascii(<size>={})", param.len())
        },
        CqlValue::Blob(param) if param.len() > no_transformation_size_limit => {
            format!("Blob(<size>={})", param.len())
        },
        CqlValue::UserDefinedType { keyspace, type_name, fields } => {
            let mut result = format!(
                "UDT {{ keyspace: \"{}\", type_name: \"{}\", fields: [",
                keyspace, type_name,
            );
            for (field_name, field_value) in fields {
                let field_string = match field_value {
                    Some(field) => cql_value_obj_to_string(field),
                    None => String::from("None"),
                };
                result.push_str(&format!("(\"{}\", {}), ", field_name, field_string));
            }
            if result.len() >= 2 {
                result.truncate(result.len() - 2);
            }
            result.push_str(&format!("] }}"));
            result
        },
        CqlValue::List(elements) => {
            let mut result = String::from("List([");
            for element in elements {
                let element_string = cql_value_obj_to_string(element);
                result.push_str(&element_string);
                result.push_str(", ");
            }
            if result.len() >= 2 {
                result.truncate(result.len() - 2);
            }
            result.push_str("])");
            result
        },
        CqlValue::Set(elements) => {
            let mut result = String::from("Set([");
            for element in elements {
                let element_string = cql_value_obj_to_string(element);
                result.push_str(&element_string);
                result.push_str(", ");
            }
            if result.len() >= 2 {
                result.truncate(result.len() - 2);
            }
            result.push_str("])");
            result
        },
        CqlValue::Map(pairs) => {
            let mut result = String::from("Map({");
            for (key, value) in pairs {
                let key_string = cql_value_obj_to_string(key);
                let value_string = cql_value_obj_to_string(value);
                result.push_str(&format!("({}: {}), ", key_string, value_string));
            }
            if result.len() >= 2 {
                result.truncate(result.len() - 2);
            }
            result.push_str("})");
            result
        },
        _ => format!("{v:?}"),
    }
}

#[derive(Any, Debug)]
pub struct CassError(pub CassErrorKind);

impl CassError {
    fn prepare_error(cql: &str, err: QueryError) -> CassError {
        CassError(CassErrorKind::Prepare(cql.to_string(), err))
    }

    fn query_execution_error(cql: &str, params: &[CqlValue], err: QueryError) -> CassError {
        let query = QueryInfo {
            cql: cql.to_string(),
            params: params.iter().map(|v| cql_value_obj_to_string(v)).collect(),
        };
        let kind = match err {
            QueryError::RequestTimeout(_)
            | QueryError::TimeoutError
            | QueryError::DbError(
                DbError::Overloaded | DbError::ReadTimeout { .. } | DbError::WriteTimeout { .. },
                _,
            ) => CassErrorKind::Overloaded(query, err),
            _ => CassErrorKind::QueryExecution(query, err),
        };
        CassError(kind)
    }
}

#[derive(Debug)]
pub struct QueryInfo {
    cql: String,
    params: Vec<String>,
}

impl Display for QueryInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "\"{}\" with params [{}]",
            self.cql,
            self.params.join(", ")
        )
    }
}

#[derive(Debug)]
pub enum CassErrorKind {
    SslConfiguration(ErrorStack),
    FailedToConnect(Vec<String>, NewSessionError),
    PreparedStatementNotFound(String),
    WrongDataStructure(String),
    UnsupportedType(TypeInfo),
    Prepare(String, QueryError),
    Overloaded(QueryInfo, QueryError),
    QueryExecution(QueryInfo, QueryError),
}

impl CassError {
    pub fn display(&self, buf: &mut String) -> std::fmt::Result {
        use std::fmt::Write;
        match &self.0 {
            CassErrorKind::SslConfiguration(e) => {
                write!(buf, "SSL configuration error: {e}")
            }
            CassErrorKind::FailedToConnect(hosts, e) => {
                write!(buf, "Could not connect to {}: {}", hosts.join(","), e)
            }
            CassErrorKind::PreparedStatementNotFound(s) => {
                write!(buf, "Prepared statement not found: {s}")
            }
            CassErrorKind::WrongDataStructure(s) => {
                write!(buf, "Wrong data structure: {s}")
            }
            CassErrorKind::UnsupportedType(s) => {
                write!(buf, "Unsupported type: {s}")
            }
            CassErrorKind::Prepare(q, e) => {
                write!(buf, "Failed to prepare query \"{q}\": {e}")
            }
            CassErrorKind::Overloaded(q, e) => {
                write!(buf, "Overloaded when executing query {q}: {e}")
            }
            CassErrorKind::QueryExecution(q, e) => {
                write!(buf, "Failed to execute query {q}: {e}")
            }
        }
    }
}

impl Display for CassError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buf = String::new();
        self.display(&mut buf).unwrap();
        write!(f, "{buf}")
    }
}

impl From<ErrorStack> for CassError {
    fn from(e: ErrorStack) -> CassError {
        CassError(CassErrorKind::SslConfiguration(e))
    }
}

impl std::error::Error for CassError {}

#[derive(Clone, Debug)]
pub struct SessionStats {
    pub req_count: u64,
    pub retry_errors: HashSet<String>,
    pub retry_error_count: u64,
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
                self.req_errors.insert(format!("{e}"));
            }
        }
    }

    pub fn store_retry_error(&mut self, error_str: String) {
        self.retry_error_count += 1;
        if self.retry_error_count <= PRINT_RETRY_ERROR_LIMIT {
            self.retry_errors.insert(error_str);
        }
    }

    /// Resets all accumulators
    pub fn reset(&mut self) {
        self.req_error_count = 0;
        self.row_count = 0;
        self.req_count = 0;
        self.mean_queue_length = 0.0;
        self.retry_error_count = 0;
        self.retry_errors.clear();
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
            retry_errors: HashSet::new(),
            retry_error_count: 0,
            req_errors: HashSet::new(),
            req_error_count: 0,
            row_count: 0,
            queue_length: 0,
            mean_queue_length: 0.0,
            resp_times_ns: Histogram::new(3).unwrap(),
        }
    }
}

pub fn get_expoinential_retry_interval(min_interval: u64,
                                       max_interval: u64,
                                       current_attempt_num: u64) -> u64 {
    let min_interval_float: f64 = min_interval as f64;
    let mut current_interval: f64 = min_interval_float * (
        2u64.pow((current_attempt_num - 1).try_into().unwrap_or(0)) as f64
    );

    // Add jitter
    current_interval += rand::thread_rng().gen::<f64>() * min_interval_float;
    current_interval -= min_interval_float / 2.0;

    std::cmp::min(current_interval as u64, max_interval as u64) as u64
}

pub async fn handle_retry_error(ctxt: &Context, current_attempt_num: u64, current_error: CassError) {
    let current_retry_interval = get_expoinential_retry_interval(
        ctxt.retry_interval.min_ms, ctxt.retry_interval.max_ms, current_attempt_num,
    );

    let mut next_attempt_str = String::new();
    let is_last_attempt = current_attempt_num == ctxt.retry_number;
    if !is_last_attempt {
        next_attempt_str += &format!("[Retry in {}ms]", current_retry_interval);
    }
    let err_msg = format!(
        "{}: [ERROR][Attempt {}/{}]{} {}",
        Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        current_attempt_num, ctxt.retry_number, next_attempt_str, current_error,
    );
    if !is_last_attempt {
        ctxt.stats.try_lock().unwrap().store_retry_error(err_msg);
        tokio::time::sleep(Duration::from_millis(current_retry_interval)).await;
    } else {
        eprintln!("{}", err_msg);
    }
}

/// This is the main object that a workload script uses to interface with the outside world.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Context {
    session: Arc<scylla::Session>,
    statements: HashMap<String, Arc<PreparedStatement>>,
    stats: TryLock<SessionStats>,
    retry_number: u64,
    retry_interval: RetryInterval,
    #[rune(get, set, add_assign, copy)]
    pub load_cycle_count: u64,
    #[rune(get)]
    pub data: Value,
}

// Needed, because Rune `Value` is !Send, as it may contain some internal pointers.
// Therefore it is not safe to pass a `Value` to another thread by cloning it, because
// both objects could accidentally share some unprotected, `!Sync` data.
// To make it safe, the same `Context` is never used by more than one thread at once and
// we make sure in `clone` to make a deep copy of the `data` field by serializing
// and deserializing it, so no pointers could get through.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(session: scylla::Session, retry_number: u64, retry_interval: RetryInterval) -> Context {
        Context {
            session: Arc::new(session),
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
            retry_number: retry_number,
            retry_interval: retry_interval,
            load_cycle_count: 0,
            data: Value::Object(Shared::new(Object::new())),
        }
    }

    /// Clones the context for use by another thread.
    /// The new clone gets fresh statistics.
    /// The user data gets passed through serialization and deserialization to avoid
    /// accidental data sharing.
    pub fn clone(&self) -> Result<Self, LatteError> {
        let serialized = rmp_serde::to_vec(&self.data)?;
        let deserialized: Value = rmp_serde::from_slice(&serialized)?;
        Ok(Context {
            session: self.session.clone(),
            statements: self.statements.clone(),
            stats: TryLock::new(SessionStats::default()),
            retry_number: self.retry_number,
            retry_interval: self.retry_interval,
            load_cycle_count: self.load_cycle_count,
            data: deserialized,
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
        for current_attempt_num in 0..self.retry_number+1 {
            let start_time = self.stats.try_lock().unwrap().start_request();
            let rs = self.session.query(cql, ()).await;
            let duration = Instant::now() - start_time;
            match rs {
                Ok(_) => {}
                Err(e) => {
                    let current_error = CassError::query_execution_error(cql, &[], e.clone());
                    handle_retry_error(self, current_attempt_num, current_error).await;
                    continue
                }
            }
            self.stats.try_lock().unwrap().complete_request(duration, &rs);
            rs.map_err(|e| CassError::query_execution_error(cql, &[], e.clone()))?;
            return Ok(())
        }
        let final_error = QueryError::ProtocolError(
            Box::leak(format!("Max retry attempts ({}) reached", self.retry_number).into_boxed_str())
        );
        Err(CassError::query_execution_error(cql, &[], final_error))
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare`.
    pub async fn execute_prepared(&self, key: &str, params: Value) -> Result<(), CassError> {
        let statement = self
            .statements
            .get(key)
            .ok_or_else(|| CassError(CassErrorKind::PreparedStatementNotFound(key.to_string())))?;
        let params = bind::to_scylla_query_params(&params)?;
        for current_attempt_num in 0..self.retry_number+1 {
            let start_time = self.stats.try_lock().unwrap().start_request();
            let rs = self.session.execute(statement, params.clone()).await;
            let duration = Instant::now() - start_time;
            match rs {
                Ok(_) => {}
                Err(e) => {
                    let current_error = CassError::query_execution_error(
                        statement.get_statement(), &params, e.clone()
                    );
                    handle_retry_error(self, current_attempt_num, current_error).await;
                    continue
                }
            }
            self.stats.try_lock().unwrap().complete_request(duration, &rs);
            rs.map_err(|e| CassError::query_execution_error(statement.get_statement(), &params, e))?;
            return Ok(());
        }
        let final_error = QueryError::ProtocolError(
            Box::leak(format!("Max retry attempts ({}) reached", self.retry_number).into_boxed_str())
        );
        Err(CassError::query_execution_error(statement.get_statement(), &params, final_error))
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
    use crate::CassErrorKind;
    use scylla::frame::response::result::CqlValue;

    use super::*;

    fn to_scylla_value(v: &Value) -> Result<CqlValue, CassError> {
        match v {
            // TODO: add support for the following native CQL types:
            //       'counter', 'date', 'decimal', 'duration', 'float', 'inet', 'time', 'timeuuid'
            //       and 'variant'.
            //       Also, for the 'tuple'.
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
            Value::Object(v) => {
                let borrowed = v.borrow_ref().unwrap();
                let set_key_name = "_set";
                let list_key_name = "_list";
                let map_key_name = "_map";
                let timestamp_key_name = "_timestamp";
                let udt_keyspace = "_keyspace";
                let udt_key_name = "_type_name";

                // Check that we don't have a mess of different data types in scope of single object
                let mutually_exclusive_keys = vec![
                    set_key_name, list_key_name, map_key_name, udt_key_name, timestamp_key_name,
                ];
                let mut found_mutually_exclusive_keys = HashSet::new();
                for mutually_exclusive_key in &mutually_exclusive_keys {
                    if borrowed.contains_key(&mutually_exclusive_key as &str) {
                        found_mutually_exclusive_keys.insert(mutually_exclusive_key);
                    }
                }
                if found_mutually_exclusive_keys.len() > 1 {
                    return Err(CassError(CassErrorKind::WrongDataStructure(format!(
                        "Following mutually exclusive keys were found: {:?}",
                        found_mutually_exclusive_keys,
                    ))));
                } else if found_mutually_exclusive_keys.len() == 0 {
                    return Err(CassError(CassErrorKind::WrongDataStructure(format!(
                        "None of the expected keys were provided: {:?}",
                        mutually_exclusive_keys,
                    ))));
                }

                // Check if "_timestamp" field exists and is of integer type
                if let Some(timestamp_value) = borrowed.get(timestamp_key_name) {
                    if let Value::Integer(timestamp) = timestamp_value {
                        return Ok(CqlValue::Timestamp(scylla::frame::value::CqlTimestamp(*timestamp)));
                    } else {
                        return Err(CassError(CassErrorKind::WrongDataStructure(format!(
                            "Unexpected data type provided for the 'timestamp': {:?}",
                            timestamp_value.type_info().unwrap(),
                        ))));
                    }
                }

                // Check if "_set" field exists and is a vector of values
                if let Some(set_value) = borrowed.get(set_key_name) {
                    if let Value::Vec(elements) = set_value {
                        let elements = elements.borrow_ref().unwrap().as_ref()
                            .iter().map(to_scylla_value).try_collect()?;
                        return Ok(CqlValue::Set(elements));
                    } else {
                        return Err(CassError(CassErrorKind::WrongDataStructure(format!(
                            "Unexpected data type provided for the 'set': {:?}",
                            set_value.type_info().unwrap(),
                        ))));
                    }
                }

                // Check for "_list" field exists and is a vector of values
                if let Some(list_value) = borrowed.get(list_key_name) {
                    if let Value::Vec(elements) = list_value {
                        let elements = elements.borrow_ref().unwrap().as_ref()
                            .iter().map(to_scylla_value).try_collect()?;
                        return Ok(CqlValue::List(elements));
                    } else {
                        return Err(CassError(CassErrorKind::WrongDataStructure(format!(
                            "Unexpected data type provided for the 'list': {:?}",
                            list_value.type_info().unwrap(),
                        ))));
                    }
                }

                // Check for "_map" field exists and is a vector of tuples
                if let Some(map_value) = borrowed.get(map_key_name) {
                    if let Value::Vec(vec_value) = map_value {
                        let vec_unwrapped = vec_value.borrow_ref().unwrap();
                        if vec_unwrapped.len() > 0 {
                            if let Value::Tuple(first_tuple) = &vec_unwrapped[0] {
                                if first_tuple.borrow_ref().unwrap().len() == 2 {
                                    let map_values: Vec<(CqlValue, CqlValue)> = vec_unwrapped.iter()
                                        .filter_map(|tuple_wrapped| {
                                            if let Value::Tuple(tuple_wrapped) = &tuple_wrapped {
                                                let tuple = tuple_wrapped.borrow_ref().unwrap();
                                                let key = to_scylla_value(tuple.get(0).unwrap()).unwrap();
                                                let value = to_scylla_value(tuple.get(1).unwrap()).unwrap();
                                                Some((key, value))
                                            } else { None }
                                        }).collect();
                                    return Ok(CqlValue::Map(map_values));
                                } else {
                                    return Err(CassError(CassErrorKind::WrongDataStructure(
                                        "Vector's tuple must have exactly 2 elements".to_string(),
                                    )))
                                }
                            } else {
                                return Err(CassError(CassErrorKind::WrongDataStructure(
                                    "'_map' is expected to contain vector of tuples only".to_string(),
                                )))
                            }
                        } else {
                            return Ok(CqlValue::Map(vec![]));
                        }
                    } else {
                        return Err(CassError(CassErrorKind::WrongDataStructure(
                            "'_map' field is expected to contain only Vector type of data".to_string(),
                        )))
                    }
                }

                // Handle last supported case - User Defined Type (UDT)
                let keyspace = match borrowed.get_value::<str, String>(udt_keyspace) {
                    Ok(Some(value)) => value,
                    _ => "unknown".to_string(),
                };
                let type_name = match borrowed.get_value::<str, String>(udt_key_name) {
                    Ok(Some(value)) => value,
                    _ => "unknown".to_string(),
                };
                let keys = borrowed.keys();
                let values: Result<Vec<Option<CqlValue>>, _> = borrowed.values()
                    .map(|value| to_scylla_value(&value.clone())
                    .map(Some)).collect();
                let fields: Vec<(String, Option<CqlValue>)> = keys.into_iter()
                    .zip(values?.into_iter())
                    .filter(|&(key, _)| key != udt_keyspace && key != udt_key_name)
                    .map(|(key, value)| (key.to_string(), value))
                    .collect();
                let udt = CqlValue::UserDefinedType{
                    keyspace: keyspace,
                    type_name: type_name,
                    fields: fields,
                };
                Ok(udt)
            },
            Value::Any(obj) => {
                let obj = obj.borrow_ref().unwrap();
                let h = obj.type_hash();
                if h == Uuid::type_hash() {
                    let uuid: &Uuid = obj.downcast_borrow_ref().unwrap();
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

#[derive(RustEmbed)]
#[folder = "resources/"]
struct Resources;

#[derive(Clone, Debug, Any)]
pub struct Uuid(pub uuid::Uuid);

impl Uuid {
    pub fn new(i: i64) -> Uuid {
        let mut hash = MetroHash128::new();
        i.hash(&mut hash);
        let (h1, h2) = hash.finish128();
        let h = ((h1 as u128) << 64) | (h2 as u128);
        let mut builder = uuid::Builder::from_u128(h);
        builder.set_variant(Variant::RFC4122);
        builder.set_version(Version::Random);
        Uuid(builder.into_uuid())
    }

    pub fn display(&self, buf: &mut String) -> std::fmt::Result {
        use std::fmt::Write;
        write!(buf, "{}", self.0)
    }
}

#[derive(Clone, Debug, Any)]
pub struct Int8(pub i8);

#[derive(Clone, Debug, Any)]
pub struct Int16(pub i16);

#[derive(Clone, Debug, Any)]
pub struct Int32(pub i32);

/// Returns the literal value stored in the `params` map under the key given as the first
/// macro arg, and if not found, returns the expression from the second arg.
pub fn param(
    ctx: &mut MacroContext,
    params: &HashMap<String, String>,
    ts: &TokenStream,
) -> rune::Result<TokenStream> {
    let mut parser = Parser::from_token_stream(ts, ctx.macro_span());
    let name = parser.parse::<ast::LitStr>()?;
    let name = ctx.resolve(name)?.to_string();
    let sep = parser.next()?;
    if sep.kind != Kind::Comma {
        return Err(anyhow!("Expected comma"));
    }
    let expr = parser.parse::<ast::Expr>()?;
    let rhs = match params.get(&name) {
        Some(value) => {
            let src_id = ctx.insert_source(&name, value);
            let value = ctx.parse_source::<ast::Expr>(src_id)?;
            quote!(#value)
        }
        None => quote!(#expr),
    };
    Ok(rhs.into_token_stream(ctx))
}

/// Converts a Rune integer to i8 (Cassandra tinyint)
pub fn int_to_i8(value: i64) -> Option<Int8> {
    Some(Int8(value.try_into().ok()?))
}

pub fn float_to_i8(value: f64) -> Option<Int8> {
    int_to_i8(value as i64)
}

/// Converts a Rune integer to i16 (Cassandra smallint)
pub fn int_to_i16(value: i64) -> Option<Int16> {
    Some(Int16(value.try_into().ok()?))
}

pub fn float_to_i16(value: f64) -> Option<Int16> {
    int_to_i16(value as i64)
}

/// Converts a Rune integer to i32 (Cassandra int)
pub fn int_to_i32(value: i64) -> Option<Int32> {
    Some(Int32(value.try_into().ok()?))
}

pub fn float_to_i32(value: f64) -> Option<Int32> {
    int_to_i32(value as i64)
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..i64::MAX`.
pub fn hash(i: i64) -> i64 {
    let mut hash = MetroHash64::new();
    i.hash(&mut hash);
    (hash.finish() & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Computes hash of two integer values.
pub fn hash2(a: i64, b: i64) -> i64 {
    let mut hash = MetroHash64::new();
    a.hash(&mut hash);
    b.hash(&mut hash);
    (hash.finish() & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..max`.
pub fn hash_range(i: i64, max: i64) -> i64 {
    hash(i) % max
}

/// Generates a floating point value with normal distribution
pub fn normal(i: i64, mean: f64, std_dev: f64) -> Result<f64, VmError> {
    let mut rng = StdRng::seed_from_u64(i as u64);
    let distribution = Normal::new(mean, std_dev).map_err(|e| VmError::panic(format!("{e}")))?;
    Ok(distribution.sample(&mut rng))
}

/// Restricts a value to a certain interval unless it is NaN.
pub fn clamp_float(value: f64, min: f64, max: f64) -> f64 {
    value.clamp(min, max)
}

/// Restricts a value to a certain interval.
pub fn clamp_int(value: i64, min: i64, max: i64) -> i64 {
    value.clamp(min, max)
}

/// Generates random blob of data of given length.
/// Parameter `seed` is used to seed the RNG.
pub fn blob(seed: i64, len: usize) -> rune::runtime::Bytes {
    let mut rng = StdRng::seed_from_u64(seed as u64);
    let v = (0..len).map(|_| rng.gen()).collect_vec();
    rune::runtime::Bytes::from_vec(v)
}

/// Generates random string of given length.
/// Parameter `seed` is used to seed the RNG.
pub fn text(seed: i64, len: usize) -> rune::runtime::StaticString {
    let mut rng = StdRng::seed_from_u64(seed as u64);
    let s: String = (0..len).map(|_| {
        let code_point = rng.gen_range(0x0061u32..=0x007Au32); // Unicode range for 'a-z'
        std::char::from_u32(code_point).unwrap()
    }).collect();
    rune::runtime::StaticString::new(s)
}

/// Generates 'now' timestamp
pub fn now_timestamp() -> i64 {
    Utc::now().timestamp()
}

/// Selects one item from the collection based on the hash of the given value.
pub fn hash_select(i: i64, collection: &[Value]) -> &Value {
    &collection[hash_range(i, collection.len() as i64) as usize]
}

/// Reads a file into a string.
pub fn read_to_string(filename: &str) -> io::Result<String> {
    let mut file = File::open(filename).expect("no such file");

    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;

    Ok(buffer)
}

/// Reads a file into a vector of lines.
pub fn read_lines(filename: &str) -> io::Result<Vec<String>> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    let result = buf
        .lines()
        .map(|l| l.expect("Could not parse line"))
        .collect();
    Ok(result)
}

/// Reads a resource file as a string.
pub fn read_resource_to_string(path: &str) -> io::Result<String> {
    let resource = Resources::get(path).ok_or_else(|| {
        io::Error::new(ErrorKind::NotFound, format!("Resource not found: {path}"))
    })?;
    let contents = std::str::from_utf8(resource.data.as_ref())
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("Invalid UTF8 string: {e}")))?;
    Ok(contents.to_string())
}

pub fn read_resource_lines(path: &str) -> io::Result<Vec<String>> {
    Ok(read_resource_to_string(path)?
        .split('\n')
        .map(|s| s.to_string())
        .collect_vec())
}

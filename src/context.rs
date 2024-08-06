use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::net::IpAddr;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;
use hdrhistogram::Histogram;
use itertools::Itertools;
use metrohash::{MetroHash128, MetroHash64};
use openssl::error::ErrorStack;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod};
use rand::distributions::Distribution;
use rand::prelude::ThreadRng;
use rand::rngs::StdRng;
use rand::{random, Rng, SeedableRng};
use rune::alloc::fmt::TryWrite;
use rune::macros::{quote, MacroContext, TokenStream};
use rune::parse::Parser;
use rune::runtime::{Mut, Object, Ref, Shared, TypeInfo, VmError, VmResult};
use rune::{ast, vm_try, vm_write};
use rune::{Any, Value};
use rust_embed::RustEmbed;
use scylla::_macro_internal::ColumnType;
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::CqlTimeuuid;
use scylla::load_balancing::DefaultPolicy;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::{DbError, NewSessionError, QueryError};
use scylla::transport::session::PoolSize;
use scylla::{ExecutionProfile, QueryResult, SessionBuilder};
use statrs::distribution::{Normal, Uniform};
use tokio::time::{Duration, Instant};
use tracing::error;
use try_lock::TryLock;
use uuid::{Variant, Version};

use crate::config::{ConnectionConf, RetryDelay};
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
    let mut policy_builder = DefaultPolicy::builder().token_aware(true);
    if let Some(dc) = &conf.datacenter {
        policy_builder = policy_builder
            .prefer_datacenter(dc.to_owned())
            .permit_dc_failover(true);
    }
    let profile = ExecutionProfile::builder()
        .consistency(conf.consistency.scylla_consistency())
        .load_balancing_policy(policy_builder.build())
        .request_timeout(Some(conf.request_timeout))
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
    Ok(Context::new(
        scylla_session,
        conf.retries,
        conf.retry_interval,
    ))
}

pub struct ClusterInfo {
    pub name: String,
    pub cassandra_version: String,
}

/// Transforms a CqlValue object to a string dedicated to be part of CassError message
pub fn cql_value_obj_to_string(v: &CqlValue) -> String {
    let no_transformation_size_limit = 32;
    match v {
        // Replace big string- and bytes-alike object values with its size labels
        CqlValue::Text(param) if param.len() > no_transformation_size_limit => {
            format!("Text(<size>={})", param.len())
        }
        CqlValue::Ascii(param) if param.len() > no_transformation_size_limit => {
            format!("Ascii(<size>={})", param.len())
        }
        CqlValue::Blob(param) if param.len() > no_transformation_size_limit => {
            format!("Blob(<size>={})", param.len())
        }
        CqlValue::UserDefinedType {
            keyspace,
            type_name,
            fields,
        } => {
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
            result.push_str("] }");
            result
        }
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
        }
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
        }
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
        }
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
            params: params.iter().map(cql_value_obj_to_string).collect(),
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
    QueryRetriesExceeded(String),
    QueryParamConversion(String, ColumnType, Option<String>),
    ValueOutOfRange(String, ColumnType),
    InvalidNumberOfQueryParams,
    InvalidQueryParamsObject(TypeInfo),
    Prepare(String, QueryError),
    Overloaded(QueryInfo, QueryError),
    QueryExecution(QueryInfo, QueryError),
}

impl CassError {
    #[rune::function(protocol = STRING_DISPLAY)]
    pub fn string_display(&self, f: &mut rune::runtime::Formatter) -> VmResult<()> {
        vm_write!(f, "{}", self.to_string());
        VmResult::Ok(())
    }

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
            CassErrorKind::QueryRetriesExceeded(s) => {
                write!(buf, "QueryRetriesExceeded: {s}")
            }
            CassErrorKind::ValueOutOfRange(v, t) => {
                write!(buf, "Value {v} out of range for Cassandra type {t:?}")
            }
            CassErrorKind::QueryParamConversion(v, t, None) => {
                write!(buf, "Cannot convert value {v} to Cassandra type {t:?}")
            }
            CassErrorKind::QueryParamConversion(v, t, Some(e)) => {
                write!(buf, "Cannot convert value {v} to Cassandra type {t:?}: {e}")
            }
            CassErrorKind::InvalidNumberOfQueryParams => {
                write!(buf, "Incorrect number of query parameters")
            }
            CassErrorKind::InvalidQueryParamsObject(t) => {
                write!(buf, "Value of type {t} cannot by used as query parameters; expected a list or object")
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
    pub req_errors: HashSet<String>,
    pub req_error_count: u64,
    pub req_retry_count: u64,
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

    pub fn complete_request(
        &mut self,
        duration: Duration,
        rs: &Result<QueryResult, QueryError>,
        retries: u64,
    ) {
        self.queue_length -= 1;
        let duration_ns = duration.as_nanos().clamp(1, u64::MAX as u128) as u64;
        self.resp_times_ns.record(duration_ns).unwrap();
        self.req_count += 1;
        self.req_retry_count += retries;
        match rs {
            Ok(rs) => self.row_count += rs.rows.as_ref().map(|r| r.len()).unwrap_or(0) as u64,
            Err(e) => {
                self.req_error_count += 1;
                self.req_errors.insert(format!("{e}"));
            }
        }
    }

    /// Resets all accumulators
    pub fn reset(&mut self) {
        self.req_error_count = 0;
        self.row_count = 0;
        self.req_count = 0;
        self.req_retry_count = 0;
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
            req_retry_count: 0,
            row_count: 0,
            queue_length: 0,
            mean_queue_length: 0.0,
            resp_times_ns: Histogram::new(3).unwrap(),
        }
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

/// This is the main object that a workload script uses to interface with the outside world.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Context {
    start_time: TryLock<Instant>,
    session: Arc<scylla::Session>,
    statements: HashMap<String, Arc<PreparedStatement>>,
    stats: TryLock<SessionStats>,
    retry_number: u64,
    retry_interval: RetryDelay,
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
    pub fn new(session: scylla::Session, retry_number: u64, retry_interval: RetryDelay) -> Context {
        Context {
            start_time: TryLock::new(Instant::now()),
            session: Arc::new(session),
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
            retry_number,
            retry_interval,
            load_cycle_count: 0,
            data: Value::Object(Shared::new(Object::new()).unwrap()),
            rng: rand::thread_rng(),
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
        while attempts <= self.retry_number && Self::should_retry(&rs) {
            if attempts > 0 {
                let current_retry_interval = get_exponential_retry_interval(
                    self.retry_interval.min,
                    self.retry_interval.max,
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

    fn should_retry<R>(result: &Result<R, QueryError>) -> bool {
        matches!(
            result,
            Err(QueryError::RequestTimeout(_))
                | Err(QueryError::TimeoutError)
                | Err(QueryError::DbError(
                    DbError::ReadTimeout { .. }
                        | DbError::WriteTimeout { .. }
                        | DbError::Overloaded,
                    _
                ))
        )
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

/// Functions for binding rune values to CQL parameters
mod bind {
    use crate::CassErrorKind;
    use rune::ToValue;
    use scylla::_macro_internal::ColumnType;
    use scylla::frame::response::result::{ColumnSpec, CqlValue};

    use super::*;

    fn to_scylla_value(v: &Value, typ: &ColumnType) -> Result<CqlValue, CassError> {
        // TODO: add support for the following native CQL types:
        //       'counter', 'date', 'decimal', 'duration', 'inet', 'time',
        //       'timestamp', 'timeuuid' and 'variant'.
        //       Also, for the 'tuple'.
        match (v, typ) {
            (Value::Bool(v), ColumnType::Boolean) => Ok(CqlValue::Boolean(*v)),

            (Value::Byte(v), ColumnType::TinyInt) => Ok(CqlValue::TinyInt(*v as i8)),
            (Value::Byte(v), ColumnType::SmallInt) => Ok(CqlValue::SmallInt(*v as i16)),
            (Value::Byte(v), ColumnType::Int) => Ok(CqlValue::Int(*v as i32)),
            (Value::Byte(v), ColumnType::BigInt) => Ok(CqlValue::BigInt(*v as i64)),

            (Value::Integer(v), ColumnType::TinyInt) => {
                convert_int(*v, ColumnType::TinyInt, CqlValue::TinyInt)
            }
            (Value::Integer(v), ColumnType::SmallInt) => {
                convert_int(*v, ColumnType::SmallInt, CqlValue::SmallInt)
            }
            (Value::Integer(v), ColumnType::Int) => convert_int(*v, ColumnType::Int, CqlValue::Int),
            (Value::Integer(v), ColumnType::BigInt) => Ok(CqlValue::BigInt(*v)),
            (Value::Integer(v), ColumnType::Timestamp) => {
                Ok(CqlValue::Timestamp(scylla::frame::value::CqlTimestamp(*v)))
            }

            (Value::Float(v), ColumnType::Float) => Ok(CqlValue::Float(*v as f32)),
            (Value::Float(v), ColumnType::Double) => Ok(CqlValue::Double(*v)),

            (Value::String(s), ColumnType::Timeuuid) => {
                let timeuuid_str = s.borrow_ref().unwrap();
                let timeuuid = CqlTimeuuid::from_str(timeuuid_str.as_str());
                match timeuuid {
                    Ok(timeuuid) => Ok(CqlValue::Timeuuid(timeuuid)),
                    Err(e) => Err(CassError(CassErrorKind::QueryParamConversion(
                        format!("{:?}", v),
                        ColumnType::Timeuuid,
                        Some(format!("{}", e)),
                    ))),
                }
            }
            (Value::String(v), ColumnType::Text | ColumnType::Ascii) => {
                Ok(CqlValue::Text(v.borrow_ref().unwrap().as_str().to_string()))
            }
            (Value::String(s), ColumnType::Inet) => {
                let ipaddr_str = s.borrow_ref().unwrap();
                let ipaddr = IpAddr::from_str(ipaddr_str.as_str());
                match ipaddr {
                    Ok(ipaddr) => Ok(CqlValue::Inet(ipaddr)),
                    Err(e) => Err(CassError(CassErrorKind::QueryParamConversion(
                        format!("{:?}", v),
                        ColumnType::Inet,
                        Some(format!("{}", e)),
                    ))),
                }
            }
            (Value::Bytes(v), ColumnType::Blob) => {
                Ok(CqlValue::Blob(v.borrow_ref().unwrap().to_vec()))
            }
            (Value::Option(v), typ) => match v.borrow_ref().unwrap().as_ref() {
                Some(v) => to_scylla_value(v, typ),
                None => Ok(CqlValue::Empty),
            },
            (Value::Vec(v), ColumnType::List(elt)) => {
                let v = v.borrow_ref().unwrap();
                let elements = v
                    .as_ref()
                    .iter()
                    .map(|v| to_scylla_value(v, elt))
                    .try_collect()?;
                Ok(CqlValue::List(elements))
            }
            (Value::Vec(v), ColumnType::Set(elt)) => {
                let v = v.borrow_ref().unwrap();
                let elements = v
                    .as_ref()
                    .iter()
                    .map(|v| to_scylla_value(v, elt))
                    .try_collect()?;
                Ok(CqlValue::Set(elements))
            }
            (Value::Vec(v), ColumnType::Map(key_elt, value_elt)) => {
                let v = v.borrow_ref().unwrap();
                let mut map_vec = Vec::with_capacity(v.len());
                for tuple in v.iter() {
                    match tuple {
                        Value::Tuple(tuple) if tuple.borrow_ref().unwrap().len() == 2 => {
                            let tuple = tuple.borrow_ref().unwrap();
                            let key = to_scylla_value(tuple.first().unwrap(), key_elt)?;
                            let value = to_scylla_value(tuple.get(1).unwrap(), value_elt)?;
                            map_vec.push((key, value));
                        }
                        _ => {
                            return Err(CassError(CassErrorKind::QueryParamConversion(
                                format!("{:?}", tuple),
                                ColumnType::Tuple(vec![
                                    key_elt.as_ref().clone(),
                                    value_elt.as_ref().clone(),
                                ]),
                                None,
                            )));
                        }
                    }
                }
                Ok(CqlValue::Map(map_vec))
            }
            (Value::Object(obj), ColumnType::Map(key_elt, value_elt)) => {
                let obj = obj.borrow_ref().unwrap();
                let mut map_vec = Vec::with_capacity(obj.keys().len());
                for (k, v) in obj.iter() {
                    let key = String::from(k.as_str());
                    let key = to_scylla_value(&(key.to_value().unwrap()), key_elt)?;
                    let value = to_scylla_value(v, value_elt)?;
                    map_vec.push((key, value));
                }
                Ok(CqlValue::Map(map_vec))
            }
            (
                Value::Object(v),
                ColumnType::UserDefinedType {
                    keyspace,
                    type_name,
                    field_types,
                },
            ) => {
                let obj = v.borrow_ref().unwrap();
                let fields = read_fields(|s| obj.get(s), field_types)?;
                Ok(CqlValue::UserDefinedType {
                    keyspace: keyspace.to_string(),
                    type_name: type_name.to_string(),
                    fields,
                })
            }
            (
                Value::Struct(v),
                ColumnType::UserDefinedType {
                    keyspace,
                    type_name,
                    field_types,
                },
            ) => {
                let obj = v.borrow_ref().unwrap();
                let fields = read_fields(|s| obj.get(s), field_types)?;
                Ok(CqlValue::UserDefinedType {
                    keyspace: keyspace.to_string(),
                    type_name: type_name.to_string(),
                    fields,
                })
            }

            (Value::Any(obj), ColumnType::Uuid) => {
                let obj = obj.borrow_ref().unwrap();
                let h = obj.type_hash();
                if h == Uuid::type_hash() {
                    let uuid: &Uuid = obj.downcast_borrow_ref().unwrap();
                    Ok(CqlValue::Uuid(uuid.0))
                } else {
                    Err(CassError(CassErrorKind::QueryParamConversion(
                        format!("{:?}", v),
                        ColumnType::Uuid,
                        None,
                    )))
                }
            }
            (value, typ) => Err(CassError(CassErrorKind::QueryParamConversion(
                format!("{:?}", value),
                typ.clone(),
                None,
            ))),
        }
    }

    fn convert_int<T: TryFrom<i64>, R>(
        value: i64,
        typ: ColumnType,
        f: impl Fn(T) -> R,
    ) -> Result<R, CassError> {
        let converted = value.try_into().map_err(|_| {
            CassError(CassErrorKind::ValueOutOfRange(
                value.to_string(),
                typ.clone(),
            ))
        })?;
        Ok(f(converted))
    }

    /// Binds parameters passed as a single rune value to the arguments of the statement.
    /// The `params` value can be a tuple, a vector, a struct or an object.
    pub fn to_scylla_query_params(
        params: &Value,
        types: &[ColumnSpec],
    ) -> Result<Vec<CqlValue>, CassError> {
        Ok(match params {
            Value::Tuple(tuple) => {
                let mut values = Vec::new();
                let tuple = tuple.borrow_ref().unwrap();
                if tuple.len() != types.len() {
                    return Err(CassError(CassErrorKind::InvalidNumberOfQueryParams));
                }
                for (v, t) in tuple.iter().zip(types) {
                    values.push(to_scylla_value(v, &t.typ)?);
                }
                values
            }
            Value::Vec(vec) => {
                let mut values = Vec::new();

                let vec = vec.borrow_ref().unwrap();
                for (v, t) in vec.iter().zip(types) {
                    values.push(to_scylla_value(v, &t.typ)?);
                }
                values
            }
            Value::Object(obj) => {
                let obj = obj.borrow_ref().unwrap();
                read_params(|f| obj.get(f), types)?
            }
            Value::Struct(obj) => {
                let obj = obj.borrow_ref().unwrap();
                read_params(|f| obj.get(f), types)?
            }
            other => {
                return Err(CassError(CassErrorKind::InvalidQueryParamsObject(
                    other.type_info().unwrap(),
                )));
            }
        })
    }

    fn read_params<'a, 'b>(
        get_value: impl Fn(&str) -> Option<&'a Value>,
        params: &[ColumnSpec],
    ) -> Result<Vec<CqlValue>, CassError> {
        let mut values = Vec::with_capacity(params.len());
        for column in params {
            let value = match get_value(&column.name) {
                Some(value) => to_scylla_value(value, &column.typ)?,
                None => CqlValue::Empty,
            };
            values.push(value)
        }
        Ok(values)
    }

    fn read_fields<'a, 'b>(
        get_value: impl Fn(&str) -> Option<&'a Value>,
        fields: &[(String, ColumnType)],
    ) -> Result<Vec<(String, Option<CqlValue>)>, CassError> {
        let mut values = Vec::with_capacity(fields.len());
        for (field_name, field_type) in fields {
            if let Some(value) = get_value(field_name) {
                let value = Some(to_scylla_value(value, field_type)?);
                values.push((field_name.to_string(), value))
            };
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

    #[rune::function(protocol = STRING_DISPLAY)]
    pub fn string_display(&self, f: &mut rune::runtime::Formatter) -> VmResult<()> {
        vm_write!(f, "{}", self.0);
        VmResult::Ok(())
    }
}

#[derive(Clone, Debug, Any)]
pub struct Int8(pub i8);

#[derive(Clone, Debug, Any)]
pub struct Int16(pub i16);

#[derive(Clone, Debug, Any)]
pub struct Int32(pub i32);

#[derive(Clone, Debug, Any)]
pub struct Float32(pub f32);

/// Returns the literal value stored in the `params` map under the key given as the first
/// macro arg, and if not found, returns the expression from the second arg.
pub fn param(
    ctx: &mut MacroContext,
    params: &HashMap<String, String>,
    ts: &TokenStream,
) -> rune::compile::Result<TokenStream> {
    let mut parser = Parser::from_token_stream(ts, ctx.macro_span());
    let name = parser.parse::<ast::LitStr>()?;
    let name = ctx.resolve(name)?.to_string();
    let _ = parser.parse::<ast::Comma>()?;
    let expr = parser.parse::<ast::Expr>()?;
    let rhs = match params.get(&name) {
        Some(value) => {
            let src_id = ctx.insert_source(&name, value)?;
            let value = ctx.parse_source::<ast::Expr>(src_id)?;
            quote!(#value)
        }
        None => quote!(#expr),
    };
    Ok(rhs.into_token_stream(ctx)?)
}

/// Creates a new UUID for current iteration
#[rune::function]
pub fn uuid(i: i64) -> Uuid {
    Uuid::new(i)
}

#[rune::function]
pub fn float_to_i8(value: f64) -> Option<Int8> {
    Some(Int8((value as i64).try_into().ok()?))
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..i64::MAX`.
fn hash_inner(i: i64) -> i64 {
    let mut hash = MetroHash64::new();
    i.hash(&mut hash);
    (hash.finish() & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..i64::MAX`.
#[rune::function]
pub fn hash(i: i64) -> i64 {
    hash_inner(i)
}

/// Computes hash of two integer values.
#[rune::function]
pub fn hash2(a: i64, b: i64) -> i64 {
    let mut hash = MetroHash64::new();
    a.hash(&mut hash);
    b.hash(&mut hash);
    (hash.finish() & 0x7FFFFFFFFFFFFFFF) as i64
}

/// Computes a hash of an integer value `i`.
/// Returns a value in range `0..max`.
#[rune::function]
pub fn hash_range(i: i64, max: i64) -> i64 {
    hash_inner(i) % max
}

/// Generates a floating point value with normal distribution
#[rune::function]
pub fn normal(i: i64, mean: f64, std_dev: f64) -> VmResult<f64> {
    let mut rng = StdRng::seed_from_u64(i as u64);
    let distribution =
        vm_try!(Normal::new(mean, std_dev).map_err(|e| VmError::panic(format!("{e}"))));
    VmResult::Ok(distribution.sample(&mut rng))
}

#[rune::function]
pub fn uniform(i: i64, min: f64, max: f64) -> VmResult<f64> {
    let mut rng = StdRng::seed_from_u64(i as u64);
    let distribution = vm_try!(Uniform::new(min, max).map_err(|e| VmError::panic(format!("{e}"))));
    VmResult::Ok(distribution.sample(&mut rng))
}

/// Generates random blob of data of given length.
/// Parameter `seed` is used to seed the RNG.
#[rune::function]
pub fn blob(seed: i64, len: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed as u64);
    (0..len).map(|_| rng.gen::<u8>()).collect()
}

/// Generates random string of given length.
/// Parameter `seed` is used to seed
/// the RNG.
#[rune::function]
pub fn text(seed: i64, len: usize) -> String {
    let mut rng = StdRng::seed_from_u64(seed as u64);
    (0..len)
        .map(|_| {
            let code_point = rng.gen_range(0x0061u32..=0x007Au32); // Unicode range for 'a-z'
            std::char::from_u32(code_point).unwrap()
        })
        .collect()
}

/// Generates 'now' timestamp
#[rune::function]
pub fn now_timestamp() -> i64 {
    Utc::now().timestamp()
}

/// Selects one item from the collection based on the hash of the given value.
#[rune::function]
pub fn hash_select(i: i64, collection: &[Value]) -> Value {
    collection[(hash_inner(i) % collection.len() as i64) as usize].clone()
}

/// Reads a file into a string.
#[rune::function]
pub fn read_to_string(filename: &str) -> io::Result<String> {
    let mut file = File::open(filename).expect("no such file");

    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;

    Ok(buffer)
}

/// Reads a file into a vector of lines.
#[rune::function]
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
fn read_resource_to_string_inner(path: &str) -> io::Result<String> {
    let resource = Resources::get(path).ok_or_else(|| {
        io::Error::new(ErrorKind::NotFound, format!("Resource not found: {path}"))
    })?;
    let contents = std::str::from_utf8(resource.data.as_ref())
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("Invalid UTF8 string: {e}")))?;
    Ok(contents.to_string())
}

#[rune::function]
pub fn read_resource_to_string(path: &str) -> io::Result<String> {
    read_resource_to_string_inner(path)
}

#[rune::function]
pub fn read_resource_lines(path: &str) -> io::Result<Vec<String>> {
    Ok(read_resource_to_string_inner(path)?
        .split('\n')
        .map(|s| s.to_string())
        .collect_vec())
}

#[rune::function(instance)]
pub async fn prepare(mut ctx: Mut<Context>, key: Ref<str>, cql: Ref<str>) -> Result<(), CassError> {
    ctx.prepare(&key, &cql).await
}

#[rune::function(instance)]
pub async fn execute(ctx: Ref<Context>, cql: Ref<str>) -> Result<(), CassError> {
    ctx.execute(cql.deref()).await
}

#[rune::function(instance)]
pub async fn execute_prepared(
    ctx: Ref<Context>,
    key: Ref<str>,
    params: Value,
) -> Result<(), CassError> {
    ctx.execute_prepared(&key, params).await
}

#[rune::function(instance)]
pub fn elapsed_secs(ctx: &Context) -> f64 {
    ctx.elapsed_secs()
}

pub mod i64 {
    use crate::context::{Float32, Int16, Int32, Int8};

    /// Converts a Rune integer to i8 (Cassandra tinyint)
    #[rune::function(instance)]
    pub fn to_i8(value: i64) -> Option<Int8> {
        Some(Int8(value.try_into().ok()?))
    }

    /// Converts a Rune integer to i16 (Cassandra smallint)
    #[rune::function(instance)]
    pub fn to_i16(value: i64) -> Option<Int16> {
        Some(Int16(value.try_into().ok()?))
    }

    /// Converts a Rune integer to i32 (Cassandra int)
    #[rune::function(instance)]
    pub fn to_i32(value: i64) -> Option<Int32> {
        Some(Int32(value.try_into().ok()?))
    }

    /// Converts a Rune integer to f32 (Cassandra float)
    #[rune::function(instance)]
    pub fn to_f32(value: i64) -> Float32 {
        Float32(value as f32)
    }

    /// Converts a Rune integer to a String
    #[rune::function(instance)]
    pub fn to_string(value: i64) -> String {
        value.to_string()
    }

    /// Restricts a value to a certain interval.
    #[rune::function(instance)]
    pub fn clamp(value: i64, min: i64, max: i64) -> i64 {
        value.clamp(min, max)
    }
}

pub mod f64 {
    use crate::context::{Float32, Int16, Int32, Int8};

    #[rune::function(instance)]
    pub fn to_i8(value: f64) -> Int8 {
        Int8(value as i8)
    }

    #[rune::function(instance)]
    pub fn to_i16(value: f64) -> Int16 {
        Int16(value as i16)
    }

    #[rune::function(instance)]
    pub fn to_i32(value: f64) -> Int32 {
        Int32(value as i32)
    }

    #[rune::function(instance)]
    pub fn to_f32(value: f64) -> Float32 {
        Float32(value as f32)
    }

    #[rune::function(instance)]
    pub fn to_string(value: f64) -> String {
        value.to_string()
    }

    /// Restricts a value to a certain interval unless it is NaN.
    #[rune::function(instance)]
    pub fn clamp(value: f64, min: f64, max: f64) -> f64 {
        value.clamp(min, max)
    }
}

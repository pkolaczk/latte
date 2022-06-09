use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
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
use scylla::{QueryResult, SessionBuilder};
use statrs::distribution::Normal;
use tokio::time::{Duration, Instant};
use try_lock::TryLock;
use uuid::{Variant, Version};

use crate::config::ConnectionConf;
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
pub async fn connect(conf: &ConnectionConf) -> Result<scylla::Session, CassError> {
    SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .pool_size(PoolSize::PerShard(conf.count))
        .user(&conf.user, &conf.password)
        .ssl_context(ssl_context(&conf)?)
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

impl CassError {
    fn prepare_error(cql: &str, err: QueryError) -> CassError {
        CassError(CassErrorKind::Prepare(cql.to_string(), err))
    }

    fn query_execution_error(cql: &str, params: &[CqlValue], err: QueryError) -> CassError {
        let query = QueryInfo {
            cql: cql.to_string(),
            params: params.iter().map(|v| format!("{:?}", v)).collect(),
        };
        let kind = match err {
            QueryError::TimeoutError
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
                write!(buf, "SSL configuration error: {}", e)
            }
            CassErrorKind::FailedToConnect(hosts, e) => {
                write!(buf, "Could not connect to {}: {}", hosts.join(","), e)
            }
            CassErrorKind::PreparedStatementNotFound(s) => {
                write!(buf, "Prepared statement not found: {}", s)
            }
            CassErrorKind::UnsupportedType(s) => {
                write!(buf, "Unsupported type: {}", s)
            }
            CassErrorKind::Prepare(q, e) => {
                write!(buf, "Failed to prepare query \"{}\": {}", q, e)
            }
            CassErrorKind::Overloaded(q, e) => {
                write!(buf, "Overloaded when executing query {}: {}", q, e)
            }
            CassErrorKind::QueryExecution(q, e) => {
                write!(buf, "Failed to execute query {}: {}", q, e)
            }
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
    pub fn new(session: scylla::Session) -> Context {
        Context {
            session: Arc::new(session),
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
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
        let start_time = self.stats.try_lock().unwrap().start_request();
        let rs = self.session.query(cql, ()).await;
        let duration = Instant::now() - start_time;
        self.stats
            .try_lock()
            .unwrap()
            .complete_request(duration, &rs);
        rs.map_err(|e| CassError::query_execution_error(cql, &[], e))?;
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
        let rs = self.session.execute(statement, params.clone()).await;
        let duration = Instant::now() - start_time;
        self.stats
            .try_lock()
            .unwrap()
            .complete_request(duration, &rs);
        rs.map_err(|e| CassError::query_execution_error(statement.get_statement(), &params, e))?;
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
    use crate::CassErrorKind;
    use scylla::frame::response::result::CqlValue;

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
    let distribution = Normal::new(mean, std_dev).map_err(|e| VmError::panic(format!("{}", e)))?;
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

/// Selects one item from the collection based on the hash of the given value.
pub fn hash_select(i: i64, collection: &[Value]) -> &Value {
    &collection[hash_range(i, collection.len() as i64) as usize]
}

/// Reads a file into a vector of lines.
pub fn read_lines(path: &str) -> io::Result<Vec<String>> {
    let mut reader = BufReader::new(File::open(PathBuf::from(path))?);
    let mut result = Vec::new();
    let mut line = String::new();
    while reader.read_line(&mut line)? != 0 {
        result.push(line.clone());
    }
    Ok(result)
}

/// Reads a resource file as a string.
pub fn read_resource_to_string(path: &str) -> io::Result<String> {
    let resource = Resources::get(path).ok_or_else(|| {
        io::Error::new(ErrorKind::NotFound, format!("Resource not found: {}", path))
    })?;
    let contents = std::str::from_utf8(resource.data.as_ref()).map_err(|e| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("Invalid UTF8 string: {}", e),
        )
    })?;
    Ok(contents.to_string())
}

pub fn read_resource_lines(path: &str) -> io::Result<Vec<String>> {
    Ok(read_resource_to_string(path)?
        .split('\n')
        .map(|s| s.to_string())
        .collect_vec())
}

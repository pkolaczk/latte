use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use chrono::Utc;
use hdrhistogram::Histogram;
use itertools::{Itertools, enumerate};
use metrohash::{MetroHash128, MetroHash64};
use openssl::error::ErrorStack;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod};
use rand::distributions::Distribution;
use rand::rngs::StdRng;
use rand::{random, Rng, SeedableRng};
use rune::ast;
use rune::ast::Kind;
use rune::macros::{quote, MacroContext, TokenStream};
use rune::parse::Parser;
use rune::runtime::{Object, Shared, TypeInfo, VmError};
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
use try_lock::TryLock;
use uuid::{Variant, Version};

use crate::config::{ConnectionConf, RetryInterval, PRINT_RETRY_ERROR_LIMIT};
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
    let dc = &conf.datacenter;
    if !dc.is_empty() {
        policy_builder = policy_builder.prefer_datacenter(dc.to_owned()).permit_dc_failover(true);
    }
    let profile = ExecutionProfile::builder()
        .consistency(conf.consistency.scylla_consistency())
        .load_balancing_policy(policy_builder.build())
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
    Ok(Context::new(
        scylla_session,
        dc.to_string(),
        conf.retry_number,
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

    fn query_retries_exceeded(retry_number: u64) -> CassError {
        CassError(CassErrorKind::QueryRetriesExceeded(format!(
            "Max retry attempts ({}) reached",
            retry_number
        )))
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
    PartitionRowPresetNotFound(String),
    QueryRetriesExceeded(String),
    QueryParamConversion(TypeInfo, ColumnType),
    ValueOutOfRange(String, ColumnType),
    InvalidNumberOfQueryParams,
    InvalidQueryParamsObject(TypeInfo),
    WrongDataStructure(String),
    Prepare(String, QueryError),
    Overloaded(QueryInfo, QueryError),
    QueryExecution(QueryInfo, QueryError),
    Error(String),
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
            CassErrorKind::PartitionRowPresetNotFound(s) => {
                write!(buf, "Partition-row preset not found: {s}")
            }
            CassErrorKind::QueryRetriesExceeded(s) => {
                write!(buf, "QueryRetriesExceeded: {s}")
            }
            CassErrorKind::ValueOutOfRange(v, t) => {
                write!(buf, "Value {v} out of range for Cassandra type {t:?}")
            }
            CassErrorKind::QueryParamConversion(s, t) => {
                write!(
                    buf,
                    "Cannot convert value of type {s} to Cassandra type {t:?}"
                )
            }
            CassErrorKind::InvalidNumberOfQueryParams => {
                write!(buf, "Incorrect number of query parameters")
            }
            CassErrorKind::InvalidQueryParamsObject(t) => {
                write!(buf, "Value of type {t} cannot by used as query parameters; expected a list or object")
            }
            CassErrorKind::WrongDataStructure(s) => {
                write!(buf, "Wrong data structure: {s}")
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
            CassErrorKind::Error(s) => {
                write!(buf, "Error: {s}")
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

pub fn get_exponential_retry_interval(
    min_interval: u64,
    max_interval: u64,
    current_attempt_num: u64,
) -> u64 {
    let min_interval_float: f64 = min_interval as f64;
    let mut current_interval: f64 =
        min_interval_float * (2u64.pow(current_attempt_num.try_into().unwrap_or(0)) as f64);

    // Add jitter
    current_interval += random::<f64>() * min_interval_float;
    current_interval -= min_interval_float / 2.0;

    std::cmp::min(current_interval as u64, max_interval)
}

pub async fn handle_retry_error(
    ctxt: &Context,
    current_attempt_num: u64,
    current_error: CassError,
) {
    let current_retry_interval = get_exponential_retry_interval(
        ctxt.retry_interval.min_ms,
        ctxt.retry_interval.max_ms,
        current_attempt_num,
    );

    let mut next_attempt_str = String::new();
    let is_last_attempt = current_attempt_num == ctxt.retry_number;
    if !is_last_attempt {
        next_attempt_str += &format!("[Retry in {} ms]", current_retry_interval);
    }
    let err_msg = format!(
        "{}: [ERROR][Attempt {}/{}]{} {}",
        Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
        current_attempt_num,
        ctxt.retry_number,
        next_attempt_str,
        current_error,
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
    partition_row_presets: HashMap<String, (u64, Vec<(f64, u64, u64, f64)>)>,
    #[rune(get, set, add_assign, copy)]
    pub load_cycle_count: u64,
    #[rune(get)]
    pub preferred_datacenter: String,
    #[rune(get)]
    pub data: Value,
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
    pub fn new(
        session: scylla::Session,
        preferred_datacenter: String,
        retry_number: u64,
        retry_interval: RetryInterval,
    ) -> Context {
        Context {
            session: Arc::new(session),
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
            retry_number,
            retry_interval,
            partition_row_presets: HashMap::new(),
            load_cycle_count: 0,
            preferred_datacenter: preferred_datacenter,
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
            partition_row_presets: self.partition_row_presets.clone(),
            load_cycle_count: self.load_cycle_count,
            preferred_datacenter: self.preferred_datacenter.clone(),
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
            .map_err(|e| CassError::query_execution_error(cql, &[], e));
        match rs {
            Ok(rs) => {
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
            Err(e) => {
                eprintln!("WARNING: {e}", e=e);
                Ok(None)
            }
        }
    }

    /// Creates a preset for uneven row distribution among partitions
    pub async fn init_partition_row_distribution_preset(
        &mut self,
        preset_name: &str,
        row_count: u64,
        rows_per_partitions_base: u64,
        mut rows_per_partitions_groups: &str,  // "percent:base_multiplier, ..." -> "80:1,15:2,5:4"
    ) -> Result<(), CassError> {
        // Validate input data
        if preset_name.is_empty() {
            return Err(CassError(CassErrorKind::Error(
                "init_partition_row_distribution_preset: 'preset_name' cannot be empty".to_string()
            )))
        }
        if row_count < 1 {
            return Err(CassError(CassErrorKind::Error(
                "init_partition_row_distribution_preset: 'row_count' cannot be less than '1'.".to_string()
            )))
        }
        if rows_per_partitions_base < 1 {
            return Err(CassError(CassErrorKind::Error(
                "init_partition_row_distribution_preset: 'rows_per_partitions_base' cannot be less than '1'.".to_string()
            )))
        }

        // Parse the 'rows_per_partitions_groups' string parameter into a HashMap
        let mut partn_multipliers: HashMap<String, (f64, f64)> = HashMap::new();
        if rows_per_partitions_groups.is_empty() {
            rows_per_partitions_groups = "95:1,4:2,1:4";
        }
        let mut summary_percentage: f64 = 0.0;
        let mut duplicates_dump: Vec<String> = Vec::new();
        for pair in rows_per_partitions_groups.split(',') {
            let processed_pair = &pair.replace(" ", "");
            if duplicates_dump.contains(processed_pair) {
                return Err(CassError(CassErrorKind::Error(format!(
                    "init_partition_row_distribution_preset: found duplicates pairs - '{processed_pair}'")
                )))
            }
            let parts: Vec<&str> = processed_pair.split(':').collect();
            if let (Some(key), Some(value)) = (parts.get(0), parts.get(1)) {
                if let (Ok(k), Ok(v)) = (key.parse::<f64>(), value.parse::<f64>()) {
                    let current_pair_key = format!("{k}:{v}");
                    partn_multipliers.insert(current_pair_key.clone(), (k, v));
                    summary_percentage += k;
                    duplicates_dump.push(current_pair_key);
                } else {
                    return Err(CassError(CassErrorKind::Error(format!(
                        "init_partition_row_distribution_preset: \
                        Wrong sub-value provided in the 'rows_per_partitions_groups' parameter: '{processed_pair}'. \
                        It must be set of integer pairs separated with a ':' symbol. Example: '49.1:1,49:2,1.9:2.5'")
                    )))
                }
            }
        }
        if (summary_percentage - 100.0).abs() > 0.01 {
            return Err(CassError(CassErrorKind::Error(format!(
                "init_partition_row_distribution_preset: \
                summary of partition percentage must be '100'. Got '{summary_percentage}' instead")
            )))
        }

        // Calculate values
        let mut partn_sizes: HashMap<String, (f64, u64)> = HashMap::new();
        let mut partn_counts: HashMap<String, (f64, u64)> = HashMap::new();
        let mut partn_cycle_size: f64 = 0.0;
        for (key, (partn_percent, partn_multiplier)) in &partn_multipliers {
            partn_sizes.insert(
                key.to_string(),
                (*partn_percent, ((rows_per_partitions_base as f64) * partn_multiplier) as u64)
            );
            let partition_type_size: f64 = rows_per_partitions_base as f64 * partn_multiplier * partn_percent / 100.0;
            partn_cycle_size += partition_type_size;
        }
        let mut partn_count: u64 = (row_count as f64 / partn_cycle_size) as u64;
        for (key, (partn_percent, _partn_multiplier)) in &partn_multipliers {
            let current_partn_count: u64 = ((partn_count as f64) * partn_percent / 100.0) as u64;
            partn_counts.insert(key.to_string(), (*partn_percent, current_partn_count));
        }
        partn_count = partn_counts.values().map(|&(_, last)| last).sum();

        // Combine calculated data into a vector of tuples
        let mut actual_row_count: u64 = 0;
        let mut partitions: Vec<(f64, u64, u64, f64)> = Vec::new();
        for (key, (_partn_percent, partn_cnt)) in &partn_counts {
            if let Some((_partn_percent, partn_size)) = partn_sizes.get(key) {
                if let Some((partn_percent, partn_multiplier)) = partn_multipliers.get(key) {
                    partitions.push((*partn_percent, *partn_cnt, *partn_size, *partn_multiplier));
                    actual_row_count += partn_cnt * partn_size;
                }
            }
        }
        partitions.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));

        // Adjust partitions based on the difference between requested and total row count
        let mut row_count_diff: u64 = 0;
        if row_count > actual_row_count {
            row_count_diff = row_count - actual_row_count;
            let smallest_partn_count_diff = row_count_diff / partitions[0].2;
            if smallest_partn_count_diff > 0 {
                partn_count += smallest_partn_count_diff;
                partitions[0].1 += smallest_partn_count_diff;
                let additional_rows: u64 = smallest_partn_count_diff * partitions[0].2;
                actual_row_count += additional_rows;
                row_count_diff -= additional_rows;
            }
        } else if row_count < actual_row_count {
            row_count_diff = actual_row_count - row_count;
            let mut smallest_partn_count_diff = row_count_diff / partitions[0].2;
            if row_count_diff % partitions[0].2 > 0 {
                smallest_partn_count_diff += 1;
            }
            if smallest_partn_count_diff > 0 {
                partn_count -= smallest_partn_count_diff;
                partitions[0].1 -= smallest_partn_count_diff;
                actual_row_count -= smallest_partn_count_diff * partitions[0].2;
                let additional_rows: u64 = smallest_partn_count_diff * partitions[0].2;
                actual_row_count -= additional_rows;
                row_count_diff = additional_rows - row_count_diff;
            }
        }
        if row_count_diff > 0 {
            partn_count += 1;
            let mut same_size_exists = false;
            for (i, partition) in enumerate(partitions.clone()) {
                if partition.2 == row_count_diff {
                    partitions[i].1 += 1;
                    same_size_exists = true;
                    break
                }
            }
            if !same_size_exists {
                partitions.push(((100000.0 / (partn_count as f64)).round() / 1000.0, 1, row_count_diff, 1.0));
            }
            actual_row_count += row_count_diff;
        }
        partitions.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));

        // Print calculated values
        let partitions_str = partitions
            .iter()
            .map(|(_percent, partns, rows, _multiplier)| {
                let percent = *partns as f64 / partn_count as f64 * 100.0;
                let percent_str = format!("{:.10}", percent);
                let parts = percent_str.split('.').collect::<Vec<_>>();
                if parts.len() == 2 {
                    let int_part = parts[0];
                    let mut frac_part: String = "".to_string();
                    if parts[1].matches("0").count() != parts[1].len() {
                        frac_part = parts[1].chars()
                            .take_while(|&ch| ch == '0')
                            .chain(parts[1].chars().filter(|&ch| ch != '0').take(2))
                            .collect::<String>();
                    }
                    if frac_part.len() > 0 {
                        frac_part = format!(".{}", frac_part);
                    }
                    format!("{}(~{}{}%):{}", partns, int_part, frac_part, rows)
                } else {
                    format!("{}(~{}%):{}", partns, parts[0], rows)
                }
            })
            .collect::<Vec<String>>()
            .join(", ");
        println!(
            "info: init_partition_row_distribution_preset: \
             preset_name={preset_name}, total_partitions={partn_count}, total_rows={total_rows}\
            , partitions/rows -> {partitions}",
            preset_name=preset_name,
            partn_count=partn_count,
            total_rows=actual_row_count,
            partitions=partitions_str,
        );

        // Save data for further usage
        partitions.retain(|&(_, current_partn_count, _, _)| current_partn_count != 0);
        self.partition_row_presets.insert(preset_name.to_string(), (actual_row_count, partitions.clone()));

        Ok(())
    }

    /// Returns a partition index based on the stress operation index and a preset of values
    pub async fn get_partition_idx(&self, preset_name: &str, idx: u64) -> Result<u64, CassError> {
        let preset = self.partition_row_presets
            .get(preset_name)
            .ok_or_else(|| CassError(CassErrorKind::PartitionRowPresetNotFound(preset_name.to_string())))?;
        let row_count = preset.0;
        let partitions = &preset.1;
        let current_idx = idx - ((idx / row_count) * row_count);

        let mut idx_offset: u64 = 0;
        let mut partition_offset: u64 = 0;
        for current_partition in partitions {
            let current_partition_count = current_partition.1;
            let current_partition_size = current_partition.2;
            if current_idx < current_partition_count * current_partition_size + idx_offset {
                return Ok(partition_offset + ((current_idx - idx_offset) / current_partition_size))
            }
            idx_offset += current_partition_count * current_partition_size;
            partition_offset += current_partition_count;
        }

        Ok(partition_offset)
    }

    /// Returns list of datacenters used by nodes
    pub async fn get_datacenters(&self) -> Result<Vec<String>, CassError> {
        let dc_info = self.session.get_cluster_data().get_datacenters_info();
        let mut datacenters: Vec<String> = dc_info.keys().cloned().collect();
        datacenters.sort();
        Ok(datacenters)
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
        for current_attempt_num in 0..self.retry_number + 1 {
            let start_time = self.stats.try_lock().unwrap().start_request();
            let rs = self.session.query(cql, ()).await;
            let duration = Instant::now() - start_time;
            match rs {
                Ok(_) => {}
                Err(e) => {
                    let current_error = CassError::query_execution_error(cql, &[], e.clone());
                    handle_retry_error(self, current_attempt_num, current_error).await;
                    continue;
                }
            }
            self.stats
                .try_lock()
                .unwrap()
                .complete_request(duration, &rs);
            rs.map_err(|e| CassError::query_execution_error(cql, &[], e.clone()))?;
            return Ok(());
        }
        Err(CassError::query_retries_exceeded(self.retry_number))
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare`.
    pub async fn execute_prepared(&self, key: &str, params: Value) -> Result<(), CassError> {
        let statement = self
            .statements
            .get(key)
            .ok_or_else(|| CassError(CassErrorKind::PreparedStatementNotFound(key.to_string())))?;

        let params = bind::to_scylla_query_params(&params, statement.get_variable_col_specs())?;
        for current_attempt_num in 0..self.retry_number + 1 {
            let start_time = self.stats.try_lock().unwrap().start_request();
            let rs = self.session.execute(statement, params.clone()).await;
            let duration = Instant::now() - start_time;
            match rs {
                Ok(_) => {}
                Err(e) => {
                    let current_error = CassError::query_execution_error(
                        statement.get_statement(),
                        &params,
                        e.clone(),
                    );
                    handle_retry_error(self, current_attempt_num, current_error).await;
                    continue;
                }
            }
            self.stats
                .try_lock()
                .unwrap()
                .complete_request(duration, &rs);
            rs.map_err(|e| {
                CassError::query_execution_error(statement.get_statement(), &params, e)
            })?;
            return Ok(());
        }
        Err(CassError::query_retries_exceeded(self.retry_number))
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
    use scylla::_macro_internal::ColumnType;
    use scylla::frame::response::result::{ColumnSpec, CqlValue};

    use super::*;

    fn to_scylla_value(v: &Value, typ: &ColumnType) -> Result<CqlValue, CassError> {
        // TODO: add support for the following native CQL types:
        //       'counter', 'date', 'decimal', 'duration', 'time' and 'variant'.
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

            (Value::StaticString(v), ColumnType::Timeuuid) => {
                let timeuuid = CqlTimeuuid::from_str(v);
                match timeuuid {
                    Ok(timeuuid) => Ok(CqlValue::Timeuuid(timeuuid)),
                    Err(e) => {
                        Err(CassError(CassErrorKind::WrongDataStructure(
                            format!("Failed to parse '{}' StaticString as Timeuuid: {}", v.as_str(), e),
                        )))
                    }
                }
            }
            (Value::String(v), ColumnType::Timeuuid) => {
                let timeuuid_str = v.borrow_ref().unwrap();
                let timeuuid = CqlTimeuuid::from_str(timeuuid_str.as_str());
                match timeuuid {
                    Ok(timeuuid) => Ok(CqlValue::Timeuuid(timeuuid)),
                    Err(e) => {
                        Err(CassError(CassErrorKind::WrongDataStructure(
                            format!("Failed to parse '{}' String as Timeuuid: {}", timeuuid_str.as_str(), e),
                        )))
                    }
                }
            }
            (Value::StaticString(v), ColumnType::Text | ColumnType::Ascii) => {
                Ok(CqlValue::Text(v.as_str().to_string()))
            }
            (Value::String(v), ColumnType::Text | ColumnType::Ascii) => {
                Ok(CqlValue::Text(v.borrow_ref().unwrap().as_str().to_string()))
            }
            (Value::StaticString(v), ColumnType::Inet) => {
                let ipaddr = IpAddr::from_str(v);
                match ipaddr {
                    Ok(ipaddr) => Ok(CqlValue::Inet(ipaddr)),
                    Err(e) => {
                        Err(CassError(CassErrorKind::WrongDataStructure(
                            format!("Failed to parse '{}' StaticString as IP address: {}", v.as_str(), e),
                        )))
                    }
                }
            }
            (Value::String(v), ColumnType::Inet) => {
                let ipaddr_str = v.borrow_ref().unwrap();
                let ipaddr = IpAddr::from_str(ipaddr_str.as_str());
                match ipaddr {
                    Ok(ipaddr) => Ok(CqlValue::Inet(ipaddr)),
                    Err(e) => {
                        Err(CassError(CassErrorKind::WrongDataStructure(
                            format!("Failed to parse '{}' String as IP address: {}", ipaddr_str.as_str(), e),
                        )))
                    }
                }
            }

            (Value::Bytes(v), ColumnType::Blob) => {
                Ok(CqlValue::Blob(v.borrow_ref().unwrap().to_vec()))
            }
            (Value::Option(v), typ) => match v.borrow_ref().unwrap().as_ref() {
                Some(v) => to_scylla_value(v, typ),
                None => Ok(CqlValue::Empty),
            }
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
                if v.len() > 0 {
                    if let Value::Tuple(first_tuple) = &v[0] {
                        if first_tuple.borrow_ref().unwrap().len() == 2 {
                            let map_values: Vec<(CqlValue, CqlValue)> = v
                                .iter()
                                .filter_map(|tuple_wrapped| {
                                    if let Value::Tuple(tuple_wrapped) = &tuple_wrapped {
                                        let tuple = tuple_wrapped.borrow_ref().unwrap();
                                        let key = to_scylla_value(tuple.get(0).unwrap(), key_elt).unwrap();
                                        let value = to_scylla_value(tuple.get(1).unwrap(), value_elt).unwrap();
                                        Some((key, value))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            Ok(CqlValue::Map(map_values))
                        } else {
                            Err(CassError(CassErrorKind::WrongDataStructure(
                                "Vector's tuple must have exactly 2 elements".to_string(),
                            )))
                        }
                    } else {
                        Err(CassError(CassErrorKind::WrongDataStructure(
                            "ColumnType::Map expects only vector of tuples".to_string(),
                        )))
                    }
                } else {
                    Ok(CqlValue::Map(vec![]))
                }
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
                        v.type_info().unwrap(),
                        ColumnType::Uuid,
                    )))
                }
            }
            (value, typ) => Err(CassError(CassErrorKind::QueryParamConversion(
                value.type_info().unwrap(),
                typ.clone(),
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
        get_value: impl Fn(&String) -> Option<&'a Value>,
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
        get_value: impl Fn(&String) -> Option<&'a Value>,
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

#[derive(Clone, Debug, Any)]
pub struct Float32(pub f32);

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

pub fn int_to_f32(value: i64) -> Option<Float32> {
    Some(Float32(value as f32))
}

pub fn float_to_f32(value: f64) -> Option<Float32> {
    Some(Float32(value as f32))
}

pub fn int_to_string(value: i64) -> Option<String> {
    Some(value.to_string())
}

pub fn float_to_string(value: f64) -> Option<String> {
    Some(value.to_string())
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

pub fn uniform(i: i64, min: f64, max: f64) -> Result<f64, VmError> {
    let mut rng = StdRng::seed_from_u64(i as u64);
    let distribution = Uniform::new(min, max).map_err(|e| VmError::panic(format!("{e}")))?;
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
    let s: String = (0..len)
        .map(|_| {
            let code_point = rng.gen_range(0x0061u32..=0x007Au32); // Unicode range for 'a-z'
            std::char::from_u32(code_point).unwrap()
        })
        .collect();
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

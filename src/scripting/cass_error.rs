use anyhow::Error;
use openssl::error::ErrorStack;
use rune::alloc::fmt::TryWrite;
use rune::runtime::{TypeInfo, VmResult};
use rune::{vm_write, Any};
use scylla::errors::{ExecutionError, NewSessionError, PrepareError};
use scylla::value::CqlValue;
use std::fmt::{Display, Formatter};
use tokio_postgres::Error as pgError;

#[derive(Any, Debug)]
pub struct CassError(pub CassErrorKind);

impl CassError {
    pub fn prepare_error(cql: &str, err: PrepareError) -> CassError {
        CassError(CassErrorKind::Prepare(cql.to_string(), err))
    }

    pub fn pg_prepare_error(cql: &str, err: pgError) -> CassError {
        CassError(CassErrorKind::PgPrepare(cql.to_string(), err))
    }

    pub fn query_execution_error(cql: &str, params: &[CqlValue], err: ExecutionError) -> CassError {
        let query = QueryInfo {
            cql: cql.to_string(),
            params: params.iter().map(cql_value_obj_to_string).collect(),
        };
        let kind = match err {
            ExecutionError::RequestTimeout(_) => CassErrorKind::Overloaded(query, err),
            _ => CassErrorKind::QueryExecution(query, err),
        };
        CassError(kind)
    }
}

#[derive(Debug)]
pub enum CassErrorKind {
    SslConfiguration(ErrorStack),
    FailedToConnect(Vec<String>, NewSessionError),
    PreparedStatementNotFound(String),
    QueryRetriesExceeded(String),
    QueryParamConversion(String, Option<String>),
    ValueOutOfRange(String),
    InvalidNumberOfQueryParams,
    InvalidQueryParamsObject(TypeInfo),
    Prepare(String, PrepareError),
    PgPrepare(String, pgError),
    PgExecution(String, pgError),
    Overloaded(QueryInfo, ExecutionError),
    QueryExecution(QueryInfo, ExecutionError),
    AerospikeError(aerospike::Error),
    Unsupported,
    UnsupportedEngine,
    InitFailure(Error),
}

#[derive(Debug)]
pub struct QueryInfo {
    cql: String,
    params: Vec<String>,
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
            CassErrorKind::ValueOutOfRange(v) => {
                write!(buf, "Value {v} out of range")
            }
            CassErrorKind::QueryParamConversion(v, None) => {
                write!(buf, "Cannot convert value {v}")
            }
            CassErrorKind::QueryParamConversion(v, Some(e)) => {
                write!(buf, "Cannot convert value {v}: {e}")
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
            CassErrorKind::PgPrepare(q, e) => {
                write!(buf, "Failed to prepare query \"{q}\": {e}")
            }
            CassErrorKind::PgExecution(q, e) => {
                write!(buf, "Failed to execute query \"{q}\": {e}")
            }
            CassErrorKind::Overloaded(q, e) => {
                write!(buf, "Overloaded when executing query {q}: {e}")
            }
            CassErrorKind::QueryExecution(q, e) => {
                write!(buf, "Failed to execute query {q}: {e}")
            }
            CassErrorKind::Unsupported => {
                write!(buf, "Unsupported query type")
            }
            CassErrorKind::UnsupportedEngine => {
                write!(buf, "Unsupported DB type")
            }
            CassErrorKind::InitFailure(e) => {
                write!(buf, "Failed to init: {e}")
            }
            CassErrorKind::AerospikeError(e) => {
                write!(buf, "Failed to execute Aerospike query: {e}")
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
            name: type_name,
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

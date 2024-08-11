use crate::scripting::cass_error::CassError;
use err_derive::*;
use hdrhistogram::serialization::interval_log::IntervalLogWriterError;
use hdrhistogram::serialization::V2DeflateSerializeError;
use rune::alloc;
use std::path::PathBuf;

#[derive(Debug, Error)]
pub enum LatteError {
    #[error(display = "Context data could not be serialized: {}", _0)]
    ContextDataEncode(#[source] rmp_serde::encode::Error),

    #[error(display = "Context data could not be deserialized: {}", _0)]
    ContextDataDecode(#[source] rmp_serde::decode::Error),

    #[error(display = "Cassandra error: {}", _0)]
    Cassandra(#[source] CassError),

    #[error(display = "Failed to read file {:?}: {}", _0, _1)]
    ScriptRead(PathBuf, #[source] rune::source::FromPathError),

    #[error(display = "Failed to load script: {}", _0)]
    ScriptBuildError(#[source] rune::BuildError),

    #[error(display = "Failed to execute script function {}: {}", _0, _1)]
    ScriptExecError(String, rune::runtime::VmError),

    #[error(display = "Function {} returned error: {}", _0, _1)]
    FunctionResult(String, String),

    #[error(display = "{}", _0)]
    Diagnostics(#[source] rune::diagnostics::EmitError),

    #[error(display = "Failed to create output file {:?}: {}", _0, _1)]
    OutputFileCreate(PathBuf, std::io::Error),

    #[error(display = "Failed to create log file {:?}: {}", _0, _1)]
    LogFileCreate(PathBuf, std::io::Error),

    #[error(display = "Error writing HDR log: {}", _0)]
    HdrLogWrite(#[source] IntervalLogWriterError<V2DeflateSerializeError>),

    #[error(display = "Failed to launch external editor {}: {}", _0, _1)]
    ExternalEditorLaunch(String, std::io::Error),

    #[error(display = "Invalid configuration: {}", _0)]
    Configuration(String),

    #[error(display = "Memory allocation failure: {}", _0)]
    OutOfMemory(#[source] alloc::Error),
}

impl LatteError {}

pub type Result<T> = std::result::Result<T, LatteError>;

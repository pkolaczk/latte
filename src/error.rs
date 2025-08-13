use crate::scripting::cass_error::CassError;
use hdrhistogram::serialization::interval_log::IntervalLogWriterError;
use hdrhistogram::serialization::V2DeflateSerializeError;
use rune::alloc;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LatteError {
    #[error("Context data could not be serialized: {0}")]
    ContextDataEncode(#[from] rmp_serde::encode::Error),

    #[error("Context data could not be deserialized: {0}")]
    ContextDataDecode(#[from] rmp_serde::decode::Error),

    #[error("Cassandra error: {0}")]
    Cassandra(#[from] CassError),

    #[error("Failed to read file {0:?}: {1}")]
    ScriptRead(PathBuf, #[source] rune::source::FromPathError),

    #[error("Failed to load script: {0}")]
    ScriptBuildError(#[from] rune::BuildError),

    #[error("Failed to execute script function {0}: {1}")]
    ScriptExecError(String, rune::runtime::VmError),

    #[error("Function {0} returned error: {1}")]
    FunctionResult(String, String),

    #[error("{0}")]
    Diagnostics(#[from] rune::diagnostics::EmitError),

    #[error("Failed to create output file {0:?}: {1}")]
    OutputFileCreate(PathBuf, std::io::Error),

    #[error("Failed to create log file {0:?}: {1}")]
    LogFileCreate(PathBuf, std::io::Error),

    #[error("Error writing HDR log: {0}")]
    HdrLogWrite(#[from] IntervalLogWriterError<V2DeflateSerializeError>),

    #[error("Failed to launch external editor {0}: {1}")]
    ExternalEditorLaunch(String, std::io::Error),

    #[error("Invalid configuration: {0}")]
    Configuration(String),

    #[error("Memory allocation failure: {0}")]
    OutOfMemory(#[from] alloc::Error),
}

pub type Result<T> = std::result::Result<T, LatteError>;

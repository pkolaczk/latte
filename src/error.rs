use crate::session::CassError;
use err_derive::*;
use std::path::PathBuf;

#[derive(Debug, Error)]
pub enum LatteError {
    #[error(display = "Cassandra error: {}", _0)]
    Cassandra(#[source] CassError),

    #[error(display = "Failed to read file {:?}: {}", _0, _1)]
    ScriptRead(PathBuf, #[source] std::io::Error),

    #[error(display = "Failed to load script: {}", _0)]
    ScriptBuildError(#[source] rune::BuildError),

    #[error(display = "Failed to execute script function {}: {}", _0, _1)]
    ScriptExecError(&'static str, rune::runtime::VmError),

    #[error(display = "Function {} returned error: {}", _0, _1)]
    FunctionResult(&'static str, String),

    #[error(display = "{}", _0)]
    Diagnostics(#[source] rune::diagnostics::EmitError),

    #[error(display = "Interrupted")]
    Interrupted,
}

pub type Result<T> = std::result::Result<T, LatteError>;

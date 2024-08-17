use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::error::LatteError;
use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::context::{
    GlobalContext, GlobalContextRef, GlobalContextRefMut, LocalContext,
};
use crate::stats::latency::LatencyDistributionRecorder;
use crate::stats::session::SessionStats;
use rand::distributions::{Distribution, WeightedIndex};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rune::alloc::clone::TryClone;
use rune::compile::meta::Kind;
use rune::compile::{CompileVisitor, MetaError, MetaRef};
use rune::runtime::{Args, RuntimeContext, VmError};
use rune::termcolor::{ColorChoice, StandardStream};
use rune::{Any, Diagnostics, Source, Sources, Unit, Value, Vm};
use serde::{Deserialize, Serialize};
use try_lock::TryLock;

/// Stores the name and hash together.
/// Name is used for message formatting, hash is used for fast function lookup.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FnRef {
    pub name: String,
    pub hash: rune::Hash,
}

impl Hash for FnRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl FnRef {
    pub fn new(name: &str) -> FnRef {
        FnRef {
            name: name.to_string(),
            hash: rune::Hash::type_hash([name]),
        }
    }
}

pub const SCHEMA_FN: &str = "schema";
pub const PREPARE_FN: &str = "prepare";
pub const ERASE_FN: &str = "erase";
pub const LOAD_FN: &str = "load";

/// Compiled workload program
#[derive(Clone)]
pub struct Program {
    sources: Arc<Sources>,
    context: Arc<RuntimeContext>,
    unit: Arc<Unit>,
    meta: ProgramMetadata,
}

impl Program {
    /// Performs some basic sanity checks of the workload script source and prepares it
    /// for fast execution. Does not create VM yet.
    ///
    /// # Parameters
    /// - `script`: source code in Rune language
    /// - `params`: parameter values that will be exposed to the script by the `params!` macro
    pub fn new(source: Source, params: HashMap<String, String>) -> Result<Program, LatteError> {
        let mut context = rune::Context::with_default_modules().unwrap();
        crate::scripting::install(&mut context, params);

        let mut options = rune::Options::default();
        options.debug_info(true);

        let mut diagnostics = Diagnostics::new();
        let mut sources = Self::load_sources(source)?;
        let mut meta = ProgramMetadata::new();
        let unit = rune::prepare(&mut sources)
            .with_context(&context)
            .with_diagnostics(&mut diagnostics)
            .with_visitor(&mut meta)?
            .build();

        if !diagnostics.is_empty() {
            let mut writer = StandardStream::stderr(ColorChoice::Always);
            diagnostics.emit(&mut writer, &sources)?;
        }
        let unit = unit?;

        Ok(Program {
            sources: Arc::new(sources),
            context: Arc::new(context.runtime().unwrap()),
            unit: Arc::new(unit),
            meta,
        })
    }

    fn load_sources(source: Source) -> Result<Sources, LatteError> {
        let mut sources = Sources::new();
        if let Some(path) = source.path() {
            if let Some(parent) = path.parent() {
                Self::try_insert_lib_source(parent, &mut sources)?
            }
        }
        sources.insert(source)?;
        Ok(sources)
    }

    // Tries to add `lib.rn` to `sources` if it exists in the same directory as the main source.
    fn try_insert_lib_source(parent: &Path, sources: &mut Sources) -> Result<(), LatteError> {
        let lib_src = parent.join("lib.rn");
        if lib_src.is_file() {
            sources.insert(
                Source::from_path(&lib_src)
                    .map_err(|e| LatteError::ScriptRead(lib_src.clone(), e))?,
            )?;
        }
        Ok(())
    }

    /// Makes a deep copy of context and unit.
    /// Calling this method instead of `clone` ensures that Rune runtime structures
    /// are separate and can be moved to different CPU cores efficiently without accidental
    /// sharing of Arc references.
    fn unshare(&self) -> Program {
        Program {
            meta: self.meta.clone(),
            sources: self.sources.clone(),
            context: Arc::new(self.context.as_ref().try_clone().unwrap()),
            unit: Arc::new(self.unit.as_ref().try_clone().unwrap()),
        }
    }

    /// Initializes a fresh virtual machine needed to execute this program.
    /// This is extremely lightweight.
    fn vm(&self) -> Vm {
        Vm::new(self.context.clone(), self.unit.clone())
    }

    /// Checks if Rune function call result is an error and if so, converts it into [`LatteError`].
    /// Cassandra errors are returned as [`LatteError::Cassandra`].
    /// All other errors are returned as [`LatteError::FunctionResult`].
    /// If result is not an `Err`, it is returned as-is.
    ///
    /// This is needed because execution of the function could actually run till completion just
    /// fine, but the function could return an error value, and in this case we should not
    /// ignore it.
    fn convert_error(&self, function_name: &str, result: Value) -> Result<Value, LatteError> {
        match result {
            Value::Result(result) => match result.take().unwrap() {
                Ok(value) => Ok(value),
                Err(Value::Any(e)) => {
                    if e.borrow_ref().unwrap().type_hash() == CassError::type_hash() {
                        let e = e.take_downcast::<CassError>().unwrap();
                        return Err(LatteError::Cassandra(e));
                    }

                    let e = Value::Any(e);
                    let msg = self.vm().with(|| format!("{e:?}"));
                    Err(LatteError::FunctionResult(function_name.to_string(), msg))
                }
                Err(other) => Err(LatteError::FunctionResult(
                    function_name.to_string(),
                    format!("{other:?}"),
                )),
            },
            other => Ok(other),
        }
    }

    /// Executes given async function with args.
    /// If execution fails, emits diagnostic messages, e.g. stacktrace to standard error stream.
    /// Also signals an error if the function execution succeeds, but the function returns
    /// an error value.
    pub async fn async_call(
        &self,
        fun: &FnRef,
        args: impl Args + Send,
    ) -> Result<Value, LatteError> {
        let handle_err = |e: VmError| {
            let mut out = StandardStream::stderr(ColorChoice::Auto);
            let _ = e.emit(&mut out, &self.sources);
            LatteError::ScriptExecError(fun.name.to_string(), e)
        };
        let execution = self.vm().send_execute(fun.hash, args).map_err(handle_err)?;
        let result = execution
            .async_complete()
            .await
            .into_result()
            .map_err(handle_err)?;
        self.convert_error(fun.name.as_str(), result)
    }

    pub fn has_prepare(&self) -> bool {
        self.has_function(&FnRef::new(PREPARE_FN))
    }

    pub fn has_schema(&self) -> bool {
        self.has_function(&FnRef::new(SCHEMA_FN))
    }

    pub fn has_erase(&self) -> bool {
        self.has_function(&FnRef::new(ERASE_FN))
    }

    pub fn has_load(&self) -> bool {
        self.has_function(&FnRef::new(LOAD_FN))
    }

    pub fn has_function(&self, function: &FnRef) -> bool {
        self.meta.functions.contains(function)
    }

    /// Calls the script's `init` function.
    /// Called once at the beginning of the benchmark.
    /// Typically used to prepare statements.
    pub async fn prepare(&mut self, context: &mut GlobalContext) -> Result<(), LatteError> {
        let context = GlobalContextRefMut::new(context);
        self.async_call(&FnRef::new(PREPARE_FN), (context,)).await?;
        Ok(())
    }

    /// Calls the script's `schema` function.
    /// Typically used to create database schema.
    pub async fn schema(&mut self, context: &mut GlobalContext) -> Result<(), LatteError> {
        let context = GlobalContextRefMut::new(context);
        self.async_call(&FnRef::new(SCHEMA_FN), (context,)).await?;
        Ok(())
    }

    /// Calls the script's `erase` function.
    /// Typically used to remove the data from the database before running the benchmark.
    pub async fn erase(&mut self, context: &mut GlobalContext) -> Result<(), LatteError> {
        let context = GlobalContextRefMut::new(context);
        self.async_call(&FnRef::new(ERASE_FN), (context,)).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct ProgramMetadata {
    functions: HashSet<FnRef>,
}

impl ProgramMetadata {
    pub fn new() -> Self {
        Self {
            functions: HashSet::new(),
        }
    }
}

impl CompileVisitor for ProgramMetadata {
    fn register_meta(&mut self, meta: MetaRef<'_>) -> Result<(), MetaError> {
        if let Kind::Function { .. } = meta.kind {
            let name = meta.item.last().unwrap().to_string();
            self.functions.insert(FnRef::new(name.as_str()));
        }
        Ok(())
    }
}

/// Tracks statistics of the Rune function invoked by the workload
#[derive(Clone, Debug)]
pub struct FnStats {
    pub function: FnRef,
    pub call_count: u64,
    pub error_count: u64,
    pub call_latency: LatencyDistributionRecorder,
}

impl FnStats {
    pub fn new(function: FnRef) -> FnStats {
        FnStats {
            function,
            call_count: 0,
            error_count: 0,
            call_latency: LatencyDistributionRecorder::default(),
        }
    }

    pub fn reset(&mut self) {
        self.call_count = 0;
        self.error_count = 0;
        self.call_latency.clear();
    }

    pub fn operation_completed(&mut self, duration: Duration) {
        self.call_count += 1;
        self.call_latency.record(duration)
    }

    pub fn operation_failed(&mut self, duration: Duration) {
        self.call_count += 1;
        self.error_count += 1;
        self.call_latency.record(duration);
    }
}

/// Statistics of operations (function calls) and Cassandra requests.
pub struct WorkloadStats {
    pub start_time: Instant,
    pub end_time: Instant,
    pub function_stats: Vec<FnStats>,
    pub session_stats: SessionStats,
}

/// Mutable part of Workload
pub struct FnStatsCollector {
    start_time: Instant,
    fn_stats: Vec<FnStats>,
}

impl FnStatsCollector {
    pub fn new(functions: impl IntoIterator<Item = FnRef>) -> FnStatsCollector {
        let mut fn_stats = Vec::new();
        for f in functions {
            fn_stats.push(FnStats::new(f));
        }
        FnStatsCollector {
            start_time: Instant::now(),
            fn_stats,
        }
    }

    pub fn functions(&self) -> impl Iterator<Item = FnRef> + '_ {
        self.fn_stats.iter().map(|f| f.function.clone())
    }

    /// Records the duration of a successful operation
    pub fn operation_completed(&mut self, function: &FnRef, duration: Duration) {
        self.fn_stats_mut(function).operation_completed(duration);
    }

    /// Records the duration of a failed operation
    pub fn operation_failed(&mut self, function: &FnRef, duration: Duration) {
        self.fn_stats_mut(function).operation_failed(duration);
    }

    /// Finds the stats for given function.
    /// The function must exist! Otherwise, it will panic.
    fn fn_stats_mut(&mut self, function: &FnRef) -> &mut FnStats {
        self.fn_stats
            .iter_mut()
            .find(|f| f.function.hash == function.hash)
            .unwrap()
    }

    /// Clears any collected stats and sets the start time
    pub fn reset(&mut self, start_time: Instant) {
        self.fn_stats.iter_mut().for_each(FnStats::reset);
        self.start_time = start_time;
    }

    /// Returns the collected stats and resets this object
    pub fn take(&mut self, end_time: Instant) -> FnStatsCollector {
        let mut state = FnStatsCollector::new(self.functions());
        state.start_time = end_time;
        mem::swap(self, &mut state);
        state
    }
}

pub struct Workload {
    context: GlobalContext,
    program: Program,
    router: FunctionRouter,
    state: TryLock<FnStatsCollector>,
}

impl Workload {
    pub fn new(context: GlobalContext, program: Program, functions: &[(FnRef, f64)]) -> Workload {
        let state = FnStatsCollector::new(functions.iter().map(|x| x.0.clone()));
        Workload {
            context,
            program,
            router: FunctionRouter::new(functions),
            state: TryLock::new(state),
        }
    }

    pub fn clone(&self) -> Result<Self, LatteError> {
        Ok(Workload {
            context: self.context.clone()?,
            // make a deep copy to avoid congestion on Arc ref counts used heavily by Rune
            program: self.program.unshare(),
            router: self.router.clone(),
            state: TryLock::new(FnStatsCollector::new(
                self.state.try_lock().unwrap().functions(),
            )),
        })
    }

    /// Executes a single cycle of a workload.
    /// This should be idempotent –
    /// the generated action should be a function of the iteration number.
    /// Returns the cycle number and the end time of the query.
    pub async fn run(&self, cycle: i64) -> Result<(i64, Instant), LatteError> {
        let start_time = Instant::now();
        let mut rng = SmallRng::seed_from_u64(cycle as u64);
        let function = self.router.select(&mut rng);
        let global_ctx = GlobalContextRef::new(&self.context);
        let local_ctx = LocalContext::new(cycle, global_ctx, rng);
        let result = self.program.async_call(function, (local_ctx, cycle)).await;
        let end_time = Instant::now();
        let mut state = self.state.try_lock().unwrap();
        let duration = end_time - start_time;
        match result {
            Ok(_) => {
                state.operation_completed(function, duration);
                Ok((cycle, end_time))
            }
            Err(LatteError::Cassandra(CassError(CassErrorKind::Overloaded(_, _)))) => {
                // don't stop on overload errors;
                // they are being counted by the context stats anyways
                state.operation_failed(function, duration);
                Ok((cycle, end_time))
            }
            Err(e) => {
                state.operation_failed(function, duration);
                Err(e)
            }
        }
    }

    /// Returns the reference to the contained context.
    /// Allows to e.g. access context stats.
    pub fn context(&self) -> &GlobalContext {
        &self.context
    }

    /// Sets the workload start time and resets the counters.
    /// Needed for producing `WorkloadStats` with
    /// recorded start and end times of measurement.
    pub fn reset(&self, start_time: Instant) {
        self.state.try_lock().unwrap().reset(start_time);
        self.context.reset();
    }

    /// Returns statistics of the operations invoked by this workload so far.
    /// Resets the internal statistic counters.
    pub fn take_stats(&self, end_time: Instant) -> WorkloadStats {
        let state = self.state.try_lock().unwrap().take(end_time);
        let result = WorkloadStats {
            start_time: state.start_time,
            end_time,
            function_stats: state.fn_stats.clone(),
            session_stats: self.context().take_session_stats(),
        };
        result
    }
}

#[derive(Clone, Debug)]
struct FunctionRouter {
    selector: WeightedIndex<f64>,
    functions: Vec<FnRef>,
}

impl FunctionRouter {
    pub fn new(functions: &[(FnRef, f64)]) -> Self {
        let (functions, weights): (Vec<_>, Vec<_>) = functions.iter().cloned().unzip();
        let selector = WeightedIndex::new(weights).unwrap();
        FunctionRouter {
            selector,
            functions,
        }
    }

    pub fn select(&self, rng: &mut impl Rng) -> &FnRef {
        &self.functions[self.selector.sample(rng)]
    }
}

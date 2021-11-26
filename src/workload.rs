use std::cell::{Cell, RefCell};
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use hdrhistogram::Histogram;
use rune::runtime::{GuardedArgs, RuntimeContext};
use rune::termcolor::{ColorChoice, StandardStream};
use rune::{Any, Diagnostics, Module, Source, Sources, Unit, Value, Vm};
use tokio::time::Duration;
use tokio::time::Instant;

use crate::error::LatteError;
use crate::{CassError, Session, SessionStats};

/// Stores the name and hash together.
/// Name is used for message formatting, hash is used for fast function lookup.
#[derive(Debug, Copy, Clone)]
pub struct FnRef {
    name: &'static str,
    hash: rune::Hash,
}

impl FnRef {
    pub fn new(name: &'static str) -> FnRef {
        FnRef {
            name,
            hash: rune::Hash::type_hash(&[name]),
        }
    }
}

pub const SCHEMA_FN: &str = "schema";
pub const PREPARE_FN: &str = "prepare";
pub const ERASE_FN: &str = "erase";
pub const LOAD_FN: &str = "load";
pub const RUN_FN: &str = "run";

/// Compiled workload program
pub struct Program {
    sources: Sources,
    context: Arc<RuntimeContext>,
    unit: Arc<Unit>,
}

impl Program {
    /// Performs some basic sanity checks of the workload script source and prepares it
    /// for fast execution. Does not create VM yet.
    ///
    /// # Parameters
    /// - `script`: source code in Rune language
    /// - `params`: parameter values that will be exposed to the script by the `params!` macro
    pub fn new(source: Source, params: HashMap<String, String>) -> Result<Program, LatteError> {
        let mut session_module = Module::default();
        session_module.ty::<Session>().unwrap();
        session_module
            .async_inst_fn("execute", Session::execute)
            .unwrap();
        session_module
            .async_inst_fn("prepare", Session::prepare)
            .unwrap();
        session_module
            .async_inst_fn("execute_prepared", Session::execute_prepared)
            .unwrap();

        let mut err_module = Module::default();
        err_module.ty::<CassError>().unwrap();
        err_module
            .inst_fn(rune::Protocol::STRING_DISPLAY, CassError::display)
            .unwrap();

        let mut uuid_module = Module::default();
        uuid_module.ty::<context::Uuid>().unwrap();
        uuid_module
            .inst_fn(rune::Protocol::STRING_DISPLAY, context::Uuid::display)
            .unwrap();

        let mut latte_module = Module::with_crate("latte");
        latte_module.function(&["blob"], context::blob).unwrap();
        latte_module.function(&["hash"], context::hash).unwrap();
        latte_module.function(&["hash2"], context::hash2).unwrap();
        latte_module
            .function(&["hash_range"], context::hash_range)
            .unwrap();
        latte_module
            .function(&["uuid"], context::Uuid::new)
            .unwrap();
        latte_module
            .macro_(&["param"], move |ctx, ts| context::param(ctx, &params, ts))
            .unwrap();

        let mut context = rune::Context::with_default_modules().unwrap();
        context.install(&session_module).unwrap();
        context.install(&err_module).unwrap();
        context.install(&uuid_module).unwrap();
        context.install(&latte_module).unwrap();

        let mut options = rune::Options::default();
        options.debug_info(true);

        let mut diagnostics = Diagnostics::new();
        let mut sources = Sources::new();
        sources.insert(source);

        let unit = rune::prepare(&mut sources)
            .with_context(&context)
            .with_diagnostics(&mut diagnostics)
            .build();

        if !diagnostics.is_empty() {
            let mut writer = StandardStream::stderr(ColorChoice::Always);
            diagnostics.emit(&mut writer, &sources)?;
        }
        let unit = unit?;

        Ok(Program {
            sources,
            context: Arc::new(context.runtime()),
            unit: Arc::new(unit),
        })
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
    fn convert_error(function_name: &'static str, result: Value) -> Result<Value, LatteError> {
        match result {
            Value::Result(result) => match result.take().unwrap() {
                Ok(value) => Ok(value),
                Err(Value::Any(e)) => {
                    if e.borrow_ref().unwrap().type_hash() == CassError::type_hash() {
                        let e = e.take_downcast::<CassError>().unwrap();
                        return Err(LatteError::Cassandra(e));
                    }
                    let mut msg = String::new();
                    let mut buf = String::new();
                    let e = Value::Any(e);
                    if e.string_display(&mut msg, &mut buf).unwrap().is_err() {
                        msg = format!("{:?}", e)
                    }
                    Err(LatteError::FunctionResult(function_name, msg))
                }
                Err(other) => Err(LatteError::FunctionResult(
                    function_name,
                    format!("{:?}", other),
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
        fun: FnRef,
        args: impl GuardedArgs,
    ) -> Result<Value, LatteError> {
        let mut vm = self.vm();
        let result = vm.async_call(fun.hash, args).await.map_err(|e| {
            let mut out = StandardStream::stderr(ColorChoice::Auto);
            let _ = e.emit(&mut out, &self.sources);
            LatteError::ScriptExecError(fun.name, e)
        })?;
        vm.with(|| Self::convert_error(fun.name, result))
    }

    pub fn has_prepare(&self) -> bool {
        self.unit.lookup(FnRef::new(PREPARE_FN).hash).is_some()
    }

    pub fn has_schema(&self) -> bool {
        self.unit.lookup(FnRef::new(SCHEMA_FN).hash).is_some()
    }

    pub fn has_erase(&self) -> bool {
        self.unit.lookup(FnRef::new(ERASE_FN).hash).is_some()
    }

    pub fn has_load(&self) -> bool {
        self.unit.lookup(FnRef::new(LOAD_FN).hash).is_some()
    }

    pub fn has_run(&self) -> bool {
        self.unit.lookup(FnRef::new(RUN_FN).hash).is_some()
    }

    /// Calls the script's `init` function.
    /// Called once at the beginning of the benchmark.
    /// Typically used to prepare statements.
    pub async fn prepare(&mut self, session: &mut Session) -> Result<(), LatteError> {
        self.async_call(FnRef::new(PREPARE_FN), (session, )).await?;
        Ok(())
    }

    /// Calls the script's `schema` function.
    /// Typically used to create database schema.
    pub async fn schema(&mut self, session: &mut Session) -> Result<(), LatteError> {
        self.async_call(FnRef::new(SCHEMA_FN), (session, )).await?;
        Ok(())
    }

    /// Calls the script's `erase` function.
    /// Typically used to remove the data from the database before running the benchmark.
    pub async fn erase(&mut self, session: &mut Session) -> Result<(), LatteError> {
        self.async_call(FnRef::new(ERASE_FN), (session, )).await?;
        Ok(())
    }

    /// Returns the preferred number of `load` invocations, determined by `LOAD_COUNT`
    /// constant of the script. If the constant is missing or incorrect type, returns 0.
    pub fn load_count(&self) -> u64 {
        match self.unit.constant(rune::Hash::type_hash(&["LOAD_COUNT"])) {
            Some(cv) => max(0, cv.clone().into_value().into_integer().unwrap_or(0)) as u64,
            None => 0,
        }
    }
}

/// Tracks statistics of the Rune function invoked by the workload
#[derive(Clone, Debug)]
pub struct FnStats {
    pub call_count: u64,
    pub call_durations_us: Histogram<u64>,
}

impl FnStats {
    pub fn operation_completed(&mut self, duration: Duration) {
        self.call_count += 1;
        self.call_durations_us
            .record(duration.as_micros().clamp(1, u64::MAX as u128) as u64)
            .unwrap();
    }
}

impl Default for FnStats {
    fn default() -> Self {
        FnStats {
            call_count: 0,
            call_durations_us: Histogram::new(3).unwrap(),
        }
    }
}

/// Statistics of operations (function calls) and Cassandra requests.
pub struct WorkloadStats {
    pub start_time: Instant,
    pub end_time: Instant,
    pub function_stats: FnStats,
    pub session_stats: SessionStats,
}

#[derive(Clone)]
pub struct Workload {
    session: Session,
    program: Arc<Program>,
    function: FnRef,
    iter_offset: i64,
    iter_step: i64,
    start_time: Cell<Instant>,
    fn_stats: RefCell<FnStats>,
}

impl Workload {
    pub fn new(session: Session, program: Arc<Program>, function: FnRef) -> Workload {
        Workload {
            session,
            program,
            function,
            iter_offset: 0,
            iter_step: 1,
            start_time: Cell::new(Instant::now()),
            fn_stats: RefCell::new(Default::default()),
        }
    }

    /// Splits this workload into subworkloads, each producing a different subsequence of iterations.
    /// Each subworkload gets fresh metrics.
    pub fn split(&self, n: usize) -> Vec<Workload> {
        (0..n)
            .into_iter()
            .map(|offset| Workload {
                session: self.session.clone(),
                program: self.program.clone(),
                function: self.function,
                iter_step: n as i64,
                iter_offset: offset as i64,
                start_time: Cell::new(Instant::now()),
                fn_stats: Default::default(),
            })
            .collect()
    }

    /// Executes a single iteration of a workload.
    /// This should be idempotent –
    /// the generated action should be a function of the iteration number.
    pub async fn run(&self, iteration: i64) -> Result<(), LatteError> {
        let start_time = Instant::now();
        let result = self
            .program
            .async_call(self.function, (&self.session, iteration))
            .await;
        let end_time = Instant::now();
        self.fn_stats
            .borrow_mut()
            .operation_completed(end_time - start_time);
        result?;
        Ok(())
    }

    /// Returns the reference to the contained session.
    /// Allows to e.g. access session stats.
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Sets the workload start time and resets the counters.
    /// Needed for producing `WorkloadStats` with
    /// recorded start and end times of measurement.
    pub fn reset(&self, start_time: Instant) {
        self.fn_stats.replace(FnStats::default());
        self.session.reset_stats();
        self.start_time.set(start_time)
    }

    /// Returns statistics of the operations invoked by this workload so far.
    /// Resets the internal statistic counters.
    pub fn take_stats(&self, end_time: Instant) -> WorkloadStats {
        WorkloadStats {
            start_time: self.start_time.replace(end_time),
            end_time,
            function_stats: self.fn_stats.replace(FnStats::default()),
            session_stats: self.session().take_stats(),
        }
    }
}

pub mod context {
    use std::collections::HashMap;
    use std::hash::{Hash, Hasher};

    use anyhow::anyhow;
    use itertools::Itertools;
    use metrohash::{MetroHash128, MetroHash64};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rune::ast::{Kind, Lit};
    use rune::macros::{quote, MacroContext, TokenStream};
    use rune::parse::Parser;
    use rune::{ast, Any};
    use uuid::{Variant, Version};

    #[derive(Clone, Debug, Any)]
    pub struct Uuid(pub uuid::Uuid);

    impl Uuid {
        pub fn new(i: i64) -> Uuid {
            let mut hash = MetroHash128::new();
            i.hash(&mut hash);
            let (h1, h2) = hash.finish128();
            let h = ((h1 as u128) << 64) | (h2 as u128);
            Uuid(
                uuid::Builder::from_u128(h)
                    .set_variant(Variant::RFC4122)
                    .set_version(Version::Random)
                    .build(),
            )
        }

        pub fn display(&self, buf: &mut String) -> std::fmt::Result {
            use std::fmt::Write;
            write!(buf, "{}", self.0)
        }
    }

    /// Returns the literal value stored in the `params` map under the key given as the first
    /// macro arg, and if not found, returns the expression from the second arg.
    pub fn param(
        ctx: &mut MacroContext,
        params: &HashMap<String, String>,
        ts: &TokenStream,
    ) -> rune::Result<TokenStream> {
        let mut parser = Parser::from_token_stream(ts, ctx.macro_span());
        let name = parser.parse::<ast::LitStr>()?;
        let name = ctx.resolve(name)?;
        let sep = parser.next()?;
        if sep.kind != Kind::Comma {
            return Err(anyhow!("Expected comma"));
        }
        let expr = parser.parse::<ast::Expr>()?;
        let rhs = match params.get(name.as_ref()) {
            Some(value) => {
                let value = Lit::new(ctx, value.parse::<i64>()?);
                quote!(#value)
            }
            None => quote!(#expr),
        };
        Ok(rhs.into_token_stream(ctx))
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

    /// Generates random blob of data of given length.
    /// Parameter `seed` is used to seed the RNG.
    pub fn blob(seed: i64, len: usize) -> rune::runtime::Bytes {
        let mut rng = StdRng::seed_from_u64(seed as u64);
        let v = (0..len).map(|_| rng.gen()).collect_vec();
        rune::runtime::Bytes::from_vec(v)
    }
}

use std::ffi::OsStr;
use std::fs::File;
use std::io::{stdout, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Duration;
use std::{env, fs};

use clap::Parser;
use config::RunCommand;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hdrhistogram::serialization::interval_log::Tag;
use hdrhistogram::serialization::{interval_log, V2DeflateSerializer};
use itertools::Itertools;
use rune::Source;
use search_path::SearchPath;
use tokio::runtime::{Builder, Runtime};
use tokio::task::spawn_blocking;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use walkdir::WalkDir;

use crate::config::{
    AppConfig, Command, ConnectionConf, EditCommand, HdrCommand, Interval, ListCommand,
    LoadCommand, SchemaCommand, ShowCommand,
};
use crate::error::{LatteError, Result};
use crate::exec::{par_execute, ExecutionOptions};
use crate::report::{PathAndSummary, Report, RunConfigCmp};
use crate::scripting::connect::ClusterInfo;
use crate::scripting::context::Context;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Recorder};
use exec::cycle::BoundedCycleCounter;
use exec::progress::Progress;
use exec::workload::{FnRef, Program, Workload, WorkloadStats, LOAD_FN};
use report::plot::plot_graph;
use report::table::{Alignment, Table};

mod adapters;
mod config;
mod error;
mod exec;
mod report;
mod scripting;
mod stats;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn load_report_or_abort(path: &Path) -> Report {
    match Report::load(path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "error: Failed to read report from {}: {}",
                path.display(),
                e
            );
            exit(1)
        }
    }
}

/// Reads the workload script from a file and compiles it.
fn load_workload_script(workload: &Path, params: &[(String, String)]) -> Result<Program> {
    let workload = find_workload(workload)
        .canonicalize()
        .unwrap_or_else(|_| workload.to_path_buf());
    eprintln!("info: Loading workload script {}...", workload.display());
    let src =
        Source::from_path(&workload).map_err(|e| LatteError::ScriptRead(workload.clone(), e))?;
    let program = Program::new(src, params.iter().cloned().collect())?;
    info!("Loaded workload script {}", workload.display());
    Ok(program)
}

/// Locates the workload and returns an absolute path to it.
/// If not found, returns the original path unchanged.
/// If the workload path is relative, it is searched in the directories
/// listed by `LATTE_WORKLOAD_PATH` environment variable.
/// If the variable is not set, workload is searched in
/// `.local/share/latte/workloads` and `/usr/share/latte/workloads`.
fn find_workload(workload: &Path) -> PathBuf {
    if workload.starts_with(".") || workload.is_absolute() {
        return workload.to_path_buf();
    }
    let search_path = SearchPath::new("LATTE_WORKLOAD_PATH").unwrap_or_else(|_| {
        let relative_to_exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf))
            .map(|p| p.join("workloads"));
        SearchPath::from(
            [
                PathBuf::from(".local/share/latte/workloads"),
                PathBuf::from("/usr/share/latte/workloads"),
            ]
            .into_iter()
            .chain(relative_to_exe)
            .collect_vec(),
        )
    });
    search_path
        .find_file(workload)
        .unwrap_or_else(|| workload.to_path_buf())
}

/// Connects to the server and returns the session
async fn connect(conf: &ConnectionConf) -> Result<(Context, Option<ClusterInfo>)> {
    eprintln!("info: Connecting to {:?}... ", conf.addresses);
    let session = scripting::connect::connect(conf).await?;
    Ok((session, None))
}

/// Runs the `schema` function of the workload script.
/// Exits with error if the `schema` function is not present or fails.
async fn schema(conf: SchemaCommand) -> Result<()> {
    let mut program = load_workload_script(&conf.workload, &conf.params)?;
    let (mut session, _) = connect(&conf.connection).await?;
    if !program.has_schema() {
        eprintln!("error: Function `schema` not found in the workload script.");
        exit(255);
    }
    eprintln!("info: Creating schema...");
    if let Err(e) = program.schema(&mut session).await {
        eprintln!("error: Failed to create schema: {e}");
        exit(255);
    }
    eprintln!("info: Schema created successfully");
    Ok(())
}

/// Loads the data into the database.
/// Exits with error if the `load` function is not present or fails.
async fn load(conf: LoadCommand) -> Result<()> {
    let mut program = load_workload_script(&conf.workload, &conf.params)?;
    let (mut session, _) = connect(&conf.connection).await?;

    if program.has_prepare() {
        eprintln!("info: Preparing...");
        if let Err(e) = program.prepare(&mut session).await {
            eprintln!("error: Failed to prepare: {e}");
            exit(255);
        }
    }

    let load_count = session.load_cycle_count;
    if load_count > 0 && !program.has_load() {
        eprintln!("error: Function `load` not found in the workload script.");
        exit(255);
    }

    if program.has_erase() {
        eprintln!("info: Erasing data...");
        if let Err(e) = program.erase(&mut session).await {
            eprintln!("error: Failed to erase: {e}");
            exit(255);
        }
    }

    eprintln!("info: Loading data...");
    let loader = Workload::new(
        session.clone()?,
        program.clone(),
        &[(FnRef::new(LOAD_FN), 1.0)],
    );
    let load_options = ExecutionOptions {
        duration: config::Interval::Count(load_count),
        cycle_range: (0, i64::MAX),
        rate: conf.rate,
        threads: conf.threads,
        concurrency: conf.concurrency,
    };
    let result = par_execute(
        "Loading...",
        &load_options,
        config::Interval::Unbounded,
        loader,
        !conf.quiet,
        false,
    )
    .await?;

    if result.error_count > 0 {
        for e in result.errors {
            eprintln!("error: {e}");
        }
        eprintln!("error: Errors encountered when loading data. Some data might be missing.");
        exit(255)
    }
    Ok(())
}

async fn run(conf: RunCommand) -> Result<()> {
    let mut conf = conf.set_timestamp_if_empty();
    let compare = conf.baseline.as_ref().map(|p| load_report_or_abort(p));

    let mut program = load_workload_script(&conf.workload, &conf.params)?;

    let mut functions = Vec::new();
    for f in &conf.functions {
        let function = FnRef::new(f.name.as_str());
        if !program.has_function(&function) {
            eprintln!(
                "error: Function {} not found in the workload script.",
                f.name.as_str()
            );
            exit(255);
        }
        functions.push((function, f.weight))
    }

    let (mut session, cluster_info) = connect(&conf.connection).await?;
    if let Some(cluster_info) = cluster_info {
        conf.cluster_name = Some(cluster_info.name);
        conf.cass_version = Some(cluster_info.cassandra_version);
    }

    if program.has_prepare() {
        eprintln!("info: Preparing...");
        if let Err(e) = program.prepare(&mut session).await {
            eprintln!("error: Failed to prepare: {e}");
            exit(255);
        }
    }

    let runner = Workload::new(session.clone()?, program.clone(), &functions);
    if conf.warmup_duration.is_not_zero() {
        eprintln!("info: Warming up...");
        let warmup_options = ExecutionOptions {
            duration: conf.warmup_duration,
            cycle_range: (conf.start_cycle, conf.end_cycle),
            rate: None,
            threads: conf.threads,
            concurrency: conf.concurrency,
        };
        par_execute(
            "Warming up...",
            &warmup_options,
            Interval::Unbounded,
            runner.clone()?,
            !conf.quiet,
            false,
        )
        .await?;
    }

    eprintln!("info: Running benchmark...");

    println!(
        "{}",
        RunConfigCmp {
            v1: &conf,
            v2: compare.as_ref().map(|c| &c.conf),
        }
    );

    let exec_options = ExecutionOptions {
        duration: conf.run_duration,
        cycle_range: (conf.start_cycle, conf.end_cycle),
        concurrency: conf.concurrency,
        rate: conf.rate,
        threads: conf.threads,
    };

    report::print_log_header();
    let stats = match par_execute(
        "Running...",
        &exec_options,
        conf.sampling_interval,
        runner,
        !conf.quiet,
        !conf.drop_sampling_log,
    )
    .await
    {
        Ok(stats) => stats,
        Err(e) => {
            return Err(e);
        }
    };

    let stats_cmp = BenchmarkCmp {
        v1: &stats,
        v2: compare.as_ref().map(|c| &c.result),
    };
    println!();
    println!("{}", &stats_cmp);

    let path = conf
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("latte-{}.json", conf.id.as_ref().unwrap())));

    let report = Report::new(conf, stats);
    match report.save(&path) {
        Ok(()) => {
            eprintln!("info: Saved report to {}", path.display());
        }
        Err(e) => {
            eprintln!("error: Failed to save report to {}: {}", path.display(), e);
            exit(1);
        }
    }
    Ok(())
}

async fn list(conf: ListCommand) -> Result<()> {
    let max_depth = if conf.recursive { usize::MAX } else { 1 };

    // Loading reports is a bit slow, so we do it in parallel:
    let mut report_futures = FuturesUnordered::new();
    for path in &conf.output {
        let walk = WalkDir::new(path).max_depth(max_depth);
        for entry in walk.into_iter().flatten() {
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }

            let path = entry.path().to_path_buf();
            report_futures.push(spawn_blocking(move || (path.clone(), Report::load(&path))));
        }
    }

    let mut reports = Vec::new();
    while let Some(report) = report_futures.next().await {
        match report.unwrap() {
            (path, Ok(report)) if should_list(&report, &conf) => {
                reports.push(PathAndSummary(path, report.summary()))
            }
            (path, Err(e)) => eprintln!("Failed to load report {}: {}", path.display(), e),
            _ => {}
        };
    }

    if !reports.is_empty() {
        reports.sort_unstable_by_key(|s| {
            (
                s.1.workload.clone(),
                s.1.functions.clone(),
                s.1.params.clone(),
                s.1.tags.clone(),
                s.1.timestamp,
            )
        });
        let mut table = Table::new(PathAndSummary::COLUMNS);
        table.align(7, Alignment::Right);
        table.align(8, Alignment::Right);
        table.align(9, Alignment::Right);
        for r in reports {
            table.push(r);
        }
        println!("{}", table);
    }
    Ok(())
}

fn should_list(report: &Report, conf: &ListCommand) -> bool {
    if let Some(workload_pattern) = &conf.workload {
        if !report
            .conf
            .workload
            .to_string_lossy()
            .contains(workload_pattern)
        {
            return false;
        }
    }
    if let Some(function) = &conf.function {
        if !report
            .conf
            .functions
            .iter()
            .map(|f| &f.name)
            .contains(function)
        {
            return false;
        }
    }
    if !conf.tags.is_empty() && !conf.tags.iter().any(|t| report.conf.tags.contains(t)) {
        return false;
    }
    true
}

async fn show(conf: ShowCommand) -> Result<()> {
    let report1 = load_report_or_abort(&conf.report);
    let report2 = conf.baseline.map(|p| load_report_or_abort(&p));

    let config_cmp = RunConfigCmp {
        v1: &report1.conf,
        v2: report2.as_ref().map(|r| &r.conf),
    };
    println!("{config_cmp}");

    let results_cmp = BenchmarkCmp {
        v1: &report1.result,
        v2: report2.as_ref().map(|r| &r.result),
    };
    println!("{results_cmp}");
    Ok(())
}

/// Reads histograms from the report and dumps them to an hdr log
async fn export_hdr_log(conf: HdrCommand) -> Result<()> {
    let tag_prefix = conf.tag.map(|t| t + ".").unwrap_or_default();
    if tag_prefix.chars().any(|c| ", \n\t".contains(c)) {
        eprintln!("error: Hdr histogram tags are not allowed to contain commas nor whitespace.");
        exit(255);
    }

    let report = load_report_or_abort(&conf.report);
    let stdout = stdout();
    let output_file: File;
    let stdout_stream;
    let mut out: Box<dyn Write> = match conf.output {
        Some(path) => {
            output_file = File::create(&path).map_err(|e| LatteError::OutputFileCreate(path, e))?;
            Box::new(output_file)
        }
        None => {
            stdout_stream = stdout.lock();
            Box::new(stdout_stream)
        }
    };

    let mut serializer = V2DeflateSerializer::new();
    let mut log_writer = interval_log::IntervalLogWriterBuilder::new()
        .add_comment(format!("[Logged with Latte {VERSION}]").as_str())
        .with_start_time(report.result.start_time.into())
        .with_base_time(report.result.start_time.into())
        .with_max_value_divisor(1000000.0) // ms
        .begin_log_with(&mut out, &mut serializer)
        .unwrap();

    for sample in &report.result.log {
        let interval_start_time = Duration::from_millis((sample.time_s * 1000.0) as u64);
        let interval_duration = Duration::from_millis((sample.duration_s * 1000.0) as u64);
        log_writer.write_histogram(
            &sample.cycle_latency.histogram.0,
            interval_start_time,
            interval_duration,
            Tag::new(format!("{tag_prefix}cycles").as_str()),
        )?;
        log_writer.write_histogram(
            &sample.request_latency.histogram.0,
            interval_start_time,
            interval_duration,
            Tag::new(format!("{tag_prefix}requests").as_str()),
        )?;
    }
    Ok(())
}

async fn async_main(run_id: String, command: Command) -> Result<()> {
    match command {
        Command::Edit(config) => edit(config)?,
        Command::Schema(config) => schema(config).await?,
        Command::Load(config) => load(config).await?,
        Command::Run(mut config) => {
            config.id = Some(run_id);
            run(config).await?
        }
        Command::List(config) => list(config).await?,
        Command::Show(config) => show(config).await?,
        Command::Hdr(config) => export_hdr_log(config).await?,
        Command::Plot(config) => plot_graph(config).await?,
    }
    Ok(())
}

fn edit(config: EditCommand) -> Result<()> {
    let workload = find_workload(&config.workload)
        .canonicalize()
        .unwrap_or_else(|_| config.workload.to_path_buf());
    File::open(&workload).map_err(|err| LatteError::ScriptRead(workload.clone(), err.into()))?;
    edit_workload(workload)
}

fn edit_workload(workload: PathBuf) -> Result<()> {
    let editor = env::var("LATTE_EDITOR")
        .or_else(|_| env::var("EDITOR"))
        .unwrap_or("vi".to_string());
    std::process::Command::new(&editor)
        .current_dir(workload.parent().unwrap_or(Path::new(".")))
        .arg(workload)
        .status()
        .map_err(|e| LatteError::ExternalEditorLaunch(editor, e))?;
    Ok(())
}

fn init_runtime(thread_count: usize) -> std::io::Result<Runtime> {
    if thread_count == 1 {
        Builder::new_current_thread().enable_all().build()
    } else {
        Builder::new_multi_thread()
            .worker_threads(thread_count)
            .enable_all()
            .build()
    }
}

fn setup_logging(run_id: &str, config: &AppConfig) -> Result<WorkerGuard> {
    let log_file = match &config.log_file {
        Some(file) if file.is_absolute() => file.clone(),
        Some(file) => config.log_dir.clone().join(file),
        None => config.log_dir.join(format!("latte-{}.log", run_id)),
    };
    fs::create_dir_all(&config.log_dir)
        .map_err(|e| LatteError::LogFileCreate(log_file.clone(), e))?;
    let log_file = File::create(&log_file).map_err(|e| LatteError::LogFileCreate(log_file, e))?;
    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("LATTE_LOG")
        .from_env()
        .map_err(|e| LatteError::Configuration(e.to_string()))?
        .add_directive("rune=off".parse().unwrap()); // turn off rune tracing for performance reasons

    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(non_blocking)
        .with_env_filter(filter)
        .init();
    Ok(guard)
}

fn run_id() -> String {
    chrono::Local::now().format("%Y%m%d-%H%M%S").to_string()
}

fn main() {
    let run_id = run_id();
    let config = AppConfig::parse();
    let _guard = match setup_logging(run_id.as_str(), &config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("error: {e}");
            exit(1);
        }
    };

    let command = config.command;
    let thread_count = match &command {
        Command::Run(cmd) => cmd.threads.get(),
        Command::Load(cmd) => cmd.threads.get(),
        _ => 1,
    };
    let runtime = init_runtime(thread_count);
    if let Err(e) = runtime.unwrap().block_on(async_main(run_id, command)) {
        eprintln!("error: {e}");
        exit(128);
    }
}

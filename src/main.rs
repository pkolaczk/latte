use std::fs::File;
use std::io::{stdout, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use hdrhistogram::serialization::interval_log::Tag;
use hdrhistogram::serialization::{interval_log, V2DeflateSerializer};
use rune::Source;
use tokio::runtime::{Builder, Runtime};

use config::RunCommand;

use crate::config::{
    AppConfig, Command, ConnectionConf, HdrCommand, Interval, LoadCommand, ShowCommand,
};
use crate::context::*;
use crate::context::{CassError, CassErrorKind, Context, SessionStats};
use crate::cycle::BoundedCycleCounter;
use crate::error::{LatteError, Result};
use crate::exec::{par_execute, ExecutionOptions};
use crate::interrupt::InterruptHandler;
use crate::progress::Progress;
use crate::report::{Report, RunConfigCmp};
use crate::sampler::Sampler;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Recorder};
use crate::workload::{FnRef, Program, Workload, WorkloadStats, LOAD_FN, RUN_FN};

mod config;
mod context;
mod cycle;
mod error;
mod exec;
mod histogram;
mod interrupt;
mod progress;
mod report;
mod sampler;
mod stats;
mod workload;

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

/// Constructs the output report file name from the parameters in the command.
/// Separates parameters with underscores.
fn get_default_output_name(conf: &RunCommand) -> PathBuf {
    let mut components = vec![conf.name()];
    components.extend(conf.cluster_name.iter().map(|x| x.replace(" ", "_")));
    components.extend(conf.cass_version.iter().cloned());
    components.extend(conf.tags.iter().cloned());
    components.extend(conf.rate.map(|r| format!("r{}", r)));
    components.push(format!("p{}", conf.concurrency));
    components.push(format!("t{}", conf.threads));
    components.push(format!("c{}", conf.connection.count));
    let params = conf.params.iter().map(|(k, v)| format!("{}{}", k, v));
    components.extend(params);
    components.push(chrono::Local::now().format("%Y%m%d.%H%M%S").to_string());
    PathBuf::from(format!("{}.json", components.join(".")))
}

/// Reads the workload script from a file and compiles it.
fn load_workload_script(workload: &Path, params: &[(String, String)]) -> Result<Program> {
    eprintln!("info: Loading workload script {}...", workload.display());
    let src = Source::from_path(workload)
        .map_err(|e| LatteError::ScriptRead(PathBuf::from(workload), e))?;
    workload::Program::new(src, params.iter().cloned().collect())
}

/// Connects to the server and returns the session
async fn connect(conf: &ConnectionConf) -> Result<(Context, Option<ClusterInfo>)> {
    eprintln!("info: Connecting to {:?}... ", conf.addresses);
    let session = context::connect(conf).await?;
    let session = Context::new(session);
    let cluster_info = session.cluster_info().await?;
    eprintln!(
        "info: Connected to {} running Cassandra version {}",
        cluster_info
            .as_ref()
            .map(|c| c.name.as_str())
            .unwrap_or("unknown"),
        cluster_info
            .as_ref()
            .map(|c| c.cassandra_version.as_str())
            .unwrap_or("unknown")
    );
    Ok((session, cluster_info))
}

async fn load(conf: LoadCommand) -> Result<()> {
    let mut program = load_workload_script(&conf.workload, &conf.params)?;
    let (mut session, _) = connect(&conf.connection).await?;

    if program.has_schema() {
        eprintln!("info: Creating schema...");
        if let Err(e) = program.schema(&mut session).await {
            eprintln!("error: Failed to create schema: {}", e);
            exit(255);
        }
    }

    if program.has_prepare() {
        eprintln!("info: Preparing...");
        if let Err(e) = program.prepare(&mut session).await {
            eprintln!("error: Failed to prepare: {}", e);
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
            eprintln!("error: Failed to erase: {}", e);
            exit(255);
        }
    }

    let interrupt = Arc::new(InterruptHandler::install());
    eprintln!("info: Loading data...");
    let loader = Workload::new(session.clone()?, program.clone(), FnRef::new(LOAD_FN));
    let load_options = ExecutionOptions {
        duration: config::Interval::Count(load_count),
        rate: None,
        threads: conf.threads,
        concurrency: conf.concurrency,
    };
    let result = par_execute(
        "Loading...",
        &load_options,
        config::Interval::Unbounded,
        loader,
        interrupt.clone(),
        !conf.quiet,
    )
    .await?;

    if result.error_count > 0 {
        for e in result.errors {
            eprintln!("error: {}", e);
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
    if !program.has_run() {
        eprintln!("error: Function `run` not found in the workload script.");
        exit(255);
    }

    let (mut session, cluster_info) = connect(&conf.connection).await?;
    if let Some(cluster_info) = cluster_info {
        conf.cluster_name = Some(cluster_info.name);
        conf.cass_version = Some(cluster_info.cassandra_version);
    }

    if program.has_schema() {
        eprintln!("info: Creating schema...");
        if let Err(e) = program.schema(&mut session).await {
            eprintln!("error: Failed to create schema: {}", e);
            exit(255);
        }
    }

    if program.has_prepare() {
        eprintln!("info: Preparing...");
        if let Err(e) = program.prepare(&mut session).await {
            eprintln!("error: Failed to prepare: {}", e);
            exit(255);
        }
    }

    let runner = Workload::new(session.clone()?, program.clone(), FnRef::new(RUN_FN));
    let interrupt = Arc::new(InterruptHandler::install());
    if conf.warmup_duration.is_not_zero() {
        eprintln!("info: Warming up...");
        let warmup_options = ExecutionOptions {
            duration: conf.warmup_duration,
            rate: None,
            threads: conf.threads,
            concurrency: conf.concurrency,
        };
        par_execute(
            "Warming up...",
            &warmup_options,
            Interval::Unbounded,
            runner.clone()?,
            interrupt.clone(),
            !conf.quiet,
        )
        .await?;
    }

    if interrupt.is_interrupted() {
        return Err(LatteError::Interrupted);
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
        concurrency: conf.concurrency,
        rate: conf.rate,
        threads: conf.threads,
    };

    report::print_log_header();
    let stats = par_execute(
        "Running...",
        &exec_options,
        conf.sampling_interval,
        runner,
        interrupt.clone(),
        !conf.quiet,
    )
    .await?;

    let stats_cmp = BenchmarkCmp {
        v1: &stats,
        v2: compare.as_ref().map(|c| &c.result),
    };
    println!();
    println!("{}", &stats_cmp);

    let path = conf
        .output
        .clone()
        .unwrap_or_else(|| get_default_output_name(&conf));

    let report = Report::new(conf, stats);
    match report.save(&path) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("error: Failed to save report to {}: {}", path.display(), e);
            exit(1)
        }
    }
    Ok(())
}

async fn show(conf: ShowCommand) -> Result<()> {
    let report1 = load_report_or_abort(&conf.report);
    let report2 = conf.baseline.map(|p| load_report_or_abort(&p));

    let config_cmp = RunConfigCmp {
        v1: &report1.conf,
        v2: report2.as_ref().map(|r| &r.conf),
    };
    println!("{}", config_cmp);

    let results_cmp = BenchmarkCmp {
        v1: &report1.result,
        v2: report2.as_ref().map(|r| &r.result),
    };
    println!("{}", results_cmp);
    Ok(())
}

/// Reads histograms from the report and dumps them to an hdr log
async fn export_hdr_log(conf: HdrCommand) -> Result<()> {
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
        .add_comment(format!("[Logged with Latte {}]", VERSION).as_str())
        .with_start_time(report.result.start_time.into())
        .with_base_time(report.result.start_time.into())
        .with_max_value_divisor(1000000.0) // ms
        .begin_log_with(&mut out, &mut serializer)
        .unwrap();

    for sample in &report.result.log {
        let interval_start_time = Duration::from_millis((sample.time_s * 1000.0) as u64);
        let interval_duration = Duration::from_millis((sample.duration_s * 1000.0) as u64);
        log_writer.write_histogram(
            &sample.cycle_time_histogram_ns.0,
            interval_start_time,
            interval_duration,
            Tag::new("cycles"),
        )?;
        log_writer.write_histogram(
            &sample.resp_time_histogram_ns.0,
            interval_start_time,
            interval_duration,
            Tag::new("requests"),
        )?;
    }
    Ok(())
}

async fn async_main(command: Command) -> Result<()> {
    match command {
        Command::Load(config) => load(config).await?,
        Command::Run(config) => run(config).await?,
        Command::Show(config) => show(config).await?,
        Command::Hdr(config) => export_hdr_log(config).await?,
    }
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

fn main() {
    let command = AppConfig::parse().command;
    let thread_count = match &command {
        Command::Run(cmd) => cmd.threads.get(),
        Command::Load(cmd) => cmd.threads.get(),
        _ => 1,
    };
    let runtime = init_runtime(thread_count);
    if let Err(e) = runtime.unwrap().block_on(async_main(command)) {
        eprintln!("error: {}", e);
        exit(128);
    }
}

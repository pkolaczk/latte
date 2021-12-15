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
use tokio::runtime::Builder;

use config::RunCommand;

use crate::config::{AppConfig, Command, HdrCommand, Interval, ShowCommand};
use crate::cycle::BoundedCycleCounter;
use crate::error::{LatteError, Result};
use crate::exec::{par_execute, ExecutionOptions};
use crate::interrupt::InterruptHandler;
use crate::progress::Progress;
use crate::report::{Report, RunConfigCmp};
use crate::sampler::Sampler;
use crate::session::*;
use crate::session::{CassError, CassErrorKind, Session, SessionStats};
use crate::stats::{BenchmarkCmp, BenchmarkStats, Recorder};
use crate::workload::{FnRef, Workload, WorkloadStats, LOAD_FN, RUN_FN};

mod config;
mod cycle;
mod error;
mod exec;
mod histogram;
mod interrupt;
mod progress;
mod report;
mod sampler;
mod session;
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
    components.push(format!("c{}", conf.connections));
    let params = conf.params.iter().map(|(k, v)| format!("{}{}", k, v));
    components.extend(params);
    components.push(chrono::Local::now().format("%Y%m%d.%H%M%S").to_string());
    PathBuf::from(format!("{}.json", components.join(".")))
}

async fn run(conf: RunCommand) -> Result<()> {
    let mut conf = conf.set_timestamp_if_empty();
    let compare = conf.baseline.as_ref().map(|p| load_report_or_abort(p));
    eprintln!(
        "info: Loading workload script {}...",
        conf.workload.display()
    );
    let script = Source::from_path(&conf.workload)
        .map_err(|e| LatteError::ScriptRead(conf.workload.clone(), e))?;

    let mut program = workload::Program::new(script, conf.params.iter().cloned().collect())?;

    if !program.has_run() {
        eprintln!("error: Function `run` not found in the workload script.");
        exit(255);
    }

    eprintln!("info: Connecting to {:?}... ", conf.addresses);
    let session = connect_or_abort(&conf).await;
    let cluster_info = session
        .query("SELECT cluster_name, release_version FROM system.local", ())
        .await;
    if let Ok(rs) = cluster_info {
        if let Some(rows) = rs.rows {
            if let Some(row) = rows.into_iter().next() {
                if let Ok((cluster_name, cass_version)) = row.into_typed() {
                    conf.cluster_name = Some(cluster_name);
                    conf.cass_version = Some(cass_version);
                }
            }
        }
    }

    eprintln!(
        "info: Connected to {} running Cassandra version {}",
        conf.cluster_name.as_deref().unwrap_or("unknown"),
        conf.cass_version.as_deref().unwrap_or("unknown")
    );

    let mut session = Session::new(session);

    if program.has_schema() {
        eprintln!("info: Creating schema...");
        if let Err(e) = program.schema(&mut session).await {
            eprintln!("error: Failed to create schema: {}", e);
            exit(255);
        }
    }

    if program.has_erase() && !conf.no_load {
        eprintln!("info: Erasing data...");
        if let Err(e) = program.erase(&mut session).await {
            eprintln!("error: Failed to erase: {}", e);
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

    let loader = Workload::new(session.clone(), program.clone(), FnRef::new(LOAD_FN));
    let runner = Workload::new(session.clone(), program.clone(), FnRef::new(RUN_FN));

    let interrupt = Arc::new(InterruptHandler::install());

    if !conf.no_load {
        let load_count = program.load_count();
        if load_count > 0 && program.has_load() {
            eprintln!("info: Loading data...");
            let load_options = ExecutionOptions {
                duration: config::Interval::Count(load_count),
                rate: None,
                threads: conf.threads,
                concurrency: conf.load_concurrency,
            };
            par_execute(
                "Loading...",
                &load_options,
                config::Interval::Unbounded,
                loader,
                interrupt.clone(),
                !conf.quiet,
            )
            .await?;
        }
    }

    if interrupt.is_interrupted() {
        return Err(LatteError::Interrupted);
    }

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
            runner.clone(),
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
        Command::Run(config) => run(config).await?,
        Command::Show(config) => show(config).await?,
        Command::Hdr(config) => export_hdr_log(config).await?,
    }
    Ok(())
}

fn main() {
    let command = AppConfig::parse().command;
    let runtime = match &command {
        Command::Run(cmd) if cmd.threads.get() >= 1 => Builder::new_multi_thread()
            .worker_threads(cmd.threads.get())
            .enable_all()
            .build(),
        _ => Builder::new_current_thread().enable_all().build(),
    };
    if let Err(e) = runtime.unwrap().block_on(async_main(command)) {
        eprintln!("error: {}", e);
        exit(128);
    }
}

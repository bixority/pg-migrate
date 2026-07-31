use chrono::Local;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use pg_migrate::config::{Args, build_config};
use pg_migrate::error::{Error, Result};
use pg_migrate::{MultiLogger, run_migration_workflow};
use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();
    let args = Args::parse();

    let log_name = Local::now().format("%Y-%m-%dT%H:%M:%S.log").to_string();
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_name)
        .map_err(|e| Error::Env(format!("failed to create log file {log_name}: {e}").into()))?;

    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();

    let mp = Arc::new(MultiProgress::with_draw_target(
        ProgressDrawTarget::stderr_with_hz(1),
    ));

    let multi_logger = MultiLogger {
        file: Mutex::new(log_file),
        inner: logger,
    };

    indicatif_log_bridge::LogWrapper::new((*mp).clone(), multi_logger)
        .try_init()
        .map_err(|e| Error::Env(format!("failed to init log wrapper: {e}").into()))?;

    let total_time_pb = mp.add(ProgressBar::new_spinner());
    total_time_pb.set_style(
        ProgressStyle::with_template("{spinner:.green} Total elapsed time: {elapsed_precise}")
            .map_err(|e| Error::Config(format!("Invalid progress style template: {e}").into()))?,
    );
    total_time_pb.enable_steady_tick(Duration::from_millis(100));

    let config = build_config(args)?;

    run_migration_workflow(config, mp, total_time_pb, start_time).await
}

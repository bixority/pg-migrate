mod db;
mod phases;
mod tui;
mod verification;

use crate::phases::phase_migrate_all;
use crate::tui::{migration_style, redraw_loop, shared_migration_states};
use anyhow::Result;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use log::info;
use std::{
    env, fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub struct Config {
    pub from_host: String,
    pub from_port: String,
    pub from_user: String,
    pub from_pass: String,
    pub from_db: String,

    pub to_host: String,
    pub to_port: String,
    pub to_user: String,
    pub to_pass: String,
    pub to_db: String,

    pub dump_jobs: usize,
    pub restore_jobs: usize,
    pub max_parallel: usize,

    pub dump_root: PathBuf,
    pub migrate_globals: bool,
    pub disable_dst_optimizations: bool,
    pub exclude_table_data: Vec<String>,
}

/// Returns the user's home directory.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn home() -> PathBuf {
    PathBuf::from(env::var("HOME").expect("HOME not set"))
}

/// Returns the directory used for state markers.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn state_dir() -> PathBuf {
    home().join("pg_migrate_state")
}

/// Returns the directory used for verification markers.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn verify_dir() -> PathBuf {
    home().join("pg_verify_state")
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "localhost")]
    from_host: String,
    #[arg(long, default_value = "5432")]
    from_port: String,
    #[arg(long, default_value = "postgres")]
    from_user: String,
    #[arg(long, default_value = "oldpass")]
    from_pass: String,
    #[arg(long, default_value = "postgres")]
    from_db: String,

    #[arg(long, default_value = "localhost")]
    to_host: String,
    #[arg(long, default_value = "5432")]
    to_port: String,
    #[arg(long, default_value = "postgres")]
    to_user: String,
    #[arg(long, default_value = "newpass")]
    to_pass: String,
    #[arg(long, default_value = "postgres")]
    to_db: String,

    #[arg(long, default_value_t = 24)]
    dump_jobs: usize,
    #[arg(long, default_value_t = 12)]
    restore_jobs: usize,
    #[arg(short = 'p', long, default_value_t = 6)]
    max_parallel: usize,
    #[arg(long, default_value = "pg_dumps")]
    dump_root: String,
    #[arg(long, default_value_t = true)]
    migrate_globals: bool,
    #[arg(long, default_value_t = false)]
    disable_dst_optimizations: bool,
    #[arg(long, value_name = "DATABASE.TABLE_PATTERN")]
    exclude_table_data: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();
    let args = Args::parse();

    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();

    let mp = Arc::new(MultiProgress::with_draw_target(
        ProgressDrawTarget::stderr_with_hz(10),
    ));

    indicatif_log_bridge::LogWrapper::new((*mp).clone(), logger)
        .try_init()
        .expect("failed to init log wrapper");

    let total_time_pb = mp.add(ProgressBar::new_spinner());
    total_time_pb.set_style(
        ProgressStyle::with_template("{spinner:.green} Total elapsed time: {elapsed_precise}")
            .expect("Invalid template"),
    );
    total_time_pb.enable_steady_tick(Duration::from_millis(100));

    let config = Arc::new(Config {
        from_host: args.from_host,
        from_port: args.from_port,
        from_user: args.from_user,
        from_pass: args.from_pass,
        from_db: args.from_db,
        to_host: args.to_host,
        to_port: args.to_port,
        to_user: args.to_user,
        to_pass: args.to_pass,
        to_db: args.to_db,
        dump_jobs: args.dump_jobs,
        restore_jobs: args.restore_jobs,
        max_parallel: args.max_parallel,
        dump_root: args.dump_root.into(),
        migrate_globals: args.migrate_globals,
        disable_dst_optimizations: args.disable_dst_optimizations,
        exclude_table_data: args.exclude_table_data,
    });

    fs::create_dir_all(state_dir())?;
    fs::create_dir_all(verify_dir())?;

    let cancel = CancellationToken::new();
    let cancel_signal = cancel.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
        eprintln!("\nInterrupt received, killing child processes…");
        cancel_signal.cancel();
    });

    let dbs_with_sizes = db::discover_databases(&config, cancel.clone()).await?;
    let db_names_owned: Vec<String> = dbs_with_sizes.iter().map(|(n, _)| n.clone()).collect();

    info!("Databases: {db_names_owned:?}");

    if dbs_with_sizes.is_empty() {
        info!("No databases found to migrate.");
        return Ok(());
    }

    let states = shared_migration_states(&dbs_with_sizes);

    let table_pb = mp.add(ProgressBar::new_spinner());
    table_pb.set_style(migration_style()?);
    table_pb.enable_steady_tick(Duration::from_secs(1));

    let redraw_cancel = cancel.clone();
    let redraw_states = states.clone();
    let redraw_pb = table_pb.clone();

    let redraw_task = tokio::spawn(async move {
        redraw_loop(redraw_states, redraw_pb, redraw_cancel).await;
    });

    if !config.disable_dst_optimizations {
        db::enable_fast_restore(&config, cancel.clone()).await?;
    }

    if config.migrate_globals {
        db::migrate_globals(&config, cancel.clone()).await?;
    }

    db::create_dbs(&config, &db_names_owned, cancel.clone()).await?;

    let sem = Arc::new(Semaphore::new(config.max_parallel));

    let migrate_result = phase_migrate_all(
        config.clone(),
        &dbs_with_sizes,
        states.clone(),
        &cancel,
        sem,
    )
    .await;

    cancel.cancel();
    let _ = redraw_task.await;

    migrate_result?;

    if !config.disable_dst_optimizations {
        db::restore_safe_settings(&config, CancellationToken::new()).await?;
    }

    table_pb.finish_with_message(states.read().await.render_table());
    total_time_pb.finish_and_clear();

    let elapsed = start_time.elapsed();

    info!(
        "Migration complete in {}.",
        indicatif::HumanDuration(elapsed)
    );

    Ok(())
}

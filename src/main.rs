mod config;
pub mod copy_engine;
mod db;
mod error;
mod phases;
mod plan;
mod tls;
mod tui;
mod verification;

use crate::config::{Args, Config, build_config, state_dir, verify_dir};
use crate::error::{Error, Result};
use crate::phases::phase_migrate_all;
use crate::tui::{migration_style, redraw_loop, shared_migration_states};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use log::info;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();
    let args = Args::parse();

    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();

    let mp = Arc::new(MultiProgress::with_draw_target(
        ProgressDrawTarget::stderr_with_hz(1),
    ));

    indicatif_log_bridge::LogWrapper::new((*mp).clone(), logger)
        .try_init()
        .map_err(|e| Error::Env(format!("failed to init log wrapper: {e}")))?;

    let total_time_pb = mp.add(ProgressBar::new_spinner());
    total_time_pb.set_style(
        ProgressStyle::with_template("{spinner:.green} Total elapsed time: {elapsed_precise}")
            .map_err(|e| Error::Config(format!("Invalid progress style template: {e}")))?,
    );
    total_time_pb.enable_steady_tick(Duration::from_millis(100));

    let config = build_config(args)?;

    run_migration_workflow(config, mp, total_time_pb, start_time).await
}

/// A single copy-engine migration target: the table to copy and how to split it.
pub struct CopyTarget<'a> {
    pub table: &'a str,
    pub column: &'a str,
    pub from: Option<&'a str>,
    pub till: Option<&'a str>,
    pub method: Option<&'a str>,
}

/// Runs the copy engine for a specific table.
///
/// `on_progress` receives a [`copy_engine::CopyProgress`] snapshot before the
/// copy starts and after each partition finishes.
///
/// # Errors
///
/// Returns an error if:
/// - Partitioning fails.
/// - The copy operation fails.
pub async fn run_copy_engine(
    config: &Config,
    db_name: &str,
    target: CopyTarget<'_>,
    on_progress: impl FnMut(copy_engine::CopyProgress),
) -> Result<()> {
    let source_conn = format!(
        "host={} port={} user={} password={} dbname={} sslmode={}",
        config.source.host,
        config.source.port,
        config.source.user,
        config.source.pass,
        db_name,
        config.ssl_mode
    );
    let dest_conn = format!(
        "host={} port={} user={} password={} dbname={} sslmode={}",
        config.destination.host,
        config.destination.port,
        config.destination.user,
        config.destination.pass,
        db_name,
        config.ssl_mode
    );

    let orchestrator = copy_engine::Orchestrator::new(
        source_conn,
        dest_conn,
        target.table.to_string(),
        config.max_parallel,
    );

    let partitions = copy_engine::Splitter::split(
        target.column,
        target.from,
        target.till,
        target.method,
        config.max_parallel,
    )?;

    orchestrator.run(partitions, on_progress).await?;

    info!("Copy migration for {} finished successfully", target.table);
    Ok(())
}

async fn run_migration_workflow(
    config: Arc<Config>,
    mp: Arc<MultiProgress>,
    total_time_pb: ProgressBar,
    start_time: Instant,
) -> Result<()> {
    fs::create_dir_all(state_dir()?)?;
    fs::create_dir_all(verify_dir()?)?;

    let cancel = CancellationToken::new();
    let cancel_signal = cancel.clone();

    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("failed to listen for ctrl-c: {e}");
        }
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

    prepare_destination(&config, &db_names_owned, cancel.clone()).await?;

    // The plan is the single source of truth for what runs, so build it before
    // the UI and derive the migration rows (including delayed/copy-engine rows)
    // from it. This keeps the TUI rows in sync with `phase_migrate_all`.
    let plan = plan::create_plan(config.clone(), &dbs_with_sizes, cancel.clone()).await?;
    plan.print();

    let (states, table_pb, redraw_task) = setup_ui(&mp, &plan, cancel.clone())?;

    let dump_sem = Arc::new(Semaphore::new(config.dump_parallel));
    let restore_sem = Arc::new(Semaphore::new(config.restore_parallel));

    let migrate_result = phase_migrate_all(
        config.clone(),
        plan,
        states.clone(),
        &cancel,
        dump_sem,
        restore_sem,
    )
    .await;

    cancel.cancel();
    let _ = redraw_task.await;

    let (regular_duration, migration_duration) = migrate_result?;

    let final_table = states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string()))?
        .render_table();
    table_pb.finish_with_message(final_table);
    total_time_pb.finish_and_clear();

    let elapsed = start_time.elapsed();

    info!(
        "Migration complete.\nSummary:\n  Regular phase: {}\n  Migration:     {}\n  \
         Total time:    {}",
        indicatif::HumanDuration(regular_duration),
        indicatif::HumanDuration(migration_duration),
        indicatif::HumanDuration(elapsed)
    );

    Ok(())
}

fn setup_ui(
    mp: &Arc<MultiProgress>,
    plan: &plan::MigrationPlan,
    cancel: CancellationToken,
) -> Result<(
    tui::SharedMigrationStates,
    ProgressBar,
    tokio::task::JoinHandle<()>,
)> {
    let states = shared_migration_states(plan);

    let table_pb = mp.add(ProgressBar::new_spinner());
    table_pb.set_style(migration_style()?);
    table_pb.enable_steady_tick(Duration::from_secs(1));

    let redraw_cancel = cancel;
    let redraw_states = states.clone();
    let redraw_pb = table_pb.clone();

    let redraw_task = tokio::spawn(async move {
        redraw_loop(redraw_states, redraw_pb, redraw_cancel).await;
    });

    Ok((states, table_pb, redraw_task))
}

async fn prepare_destination(
    config: &Config,
    db_names: &[String],
    cancel: CancellationToken,
) -> Result<()> {
    if config.migrate_globals {
        db::migrate_globals(config, cancel.clone()).await?;
    }

    db::create_dbs(config, db_names, cancel.clone()).await?;
    Ok(())
}

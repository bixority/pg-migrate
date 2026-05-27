mod db;
mod error;
mod phases;
mod tui;
mod verification;

use crate::error::{Error, Result};
use crate::phases::phase_migrate_all;
use crate::tui::{migration_style, redraw_loop, shared_migration_states};
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
    pub source: db::DbArgs,
    pub source_db: String,

    pub destination: db::DbArgs,
    pub destination_db: String,

    pub dump_jobs: usize,
    pub restore_jobs: usize,
    pub max_parallel: usize,
    pub dump_parallel: usize,
    pub restore_parallel: usize,

    pub dump_root: PathBuf,
    pub migrate_globals: bool,
    pub delay_table_data: Vec<String>,

    pub fast_verify: bool,
    pub verify_concurrency: usize,

    pub pool_cache: db::PoolCache,
    pub verify_sem: Arc<Semaphore>,
}

/// Returns the user's home directory.
///
/// # Errors
///
/// Returns an error if the `HOME` environment variable is not set.
pub fn home() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| Error::Other("HOME environment variable not set".to_string()))
}

/// Returns the directory used for state markers.
///
/// # Errors
///
/// See [`home`].
pub fn state_dir() -> Result<PathBuf> {
    Ok(home()?.join("pg_migrate_state"))
}

/// Returns the directory used for verification markers.
///
/// # Errors
///
/// See [`home`].
pub fn verify_dir() -> Result<PathBuf> {
    Ok(home()?.join("pg_verify_state"))
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "localhost")]
    from_host: String,
    #[arg(long, default_value_t = 5432)]
    from_port: u16,
    #[arg(long, default_value = "postgres")]
    from_user: String,
    #[arg(long, default_value = "oldpass")]
    from_pass: String,
    #[arg(long, default_value = "postgres")]
    from_db: String,

    #[arg(long, default_value = "localhost")]
    to_host: String,
    #[arg(long, default_value_t = 5432)]
    to_port: u16,
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
    #[arg(long)]
    dump_parallel: Option<usize>,
    #[arg(long)]
    restore_parallel: Option<usize>,
    #[arg(long, default_value = "pg_dumps")]
    dump_root: String,
    #[arg(long, default_value_t = true)]
    migrate_globals: bool,
    #[arg(long, value_name = "DATABASE.TABLE_PATTERN")]
    delay_table_data: Vec<String>,
    #[arg(long, default_value_t = false)]
    fast_verify: bool,
    #[arg(long, default_value_t = 16)]
    verify_concurrency: usize,
}

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
        .map_err(|e| Error::Other(format!("failed to init log wrapper: {e}")))?;

    let total_time_pb = mp.add(ProgressBar::new_spinner());
    total_time_pb.set_style(
        ProgressStyle::with_template("{spinner:.green} Total elapsed time: {elapsed_precise}")
            .map_err(|e| Error::Other(format!("Invalid template: {e}")))?,
    );
    total_time_pb.enable_steady_tick(Duration::from_millis(100));

    let config = build_config(args);

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

    let (states, table_pb, redraw_task) = setup_ui(&mp, &dbs_with_sizes, &config, cancel.clone())?;

    prepare_destination(&config, &db_names_owned, cancel.clone()).await?;

    let dump_sem = Arc::new(Semaphore::new(config.dump_parallel));
    let restore_sem = Arc::new(Semaphore::new(config.restore_parallel));

    let migrate_result = phase_migrate_all(
        config.clone(),
        &dbs_with_sizes,
        states.clone(),
        &cancel,
        dump_sem,
        restore_sem,
    )
    .await;

    cancel.cancel();
    let _ = redraw_task.await;

    let (regular_duration, migration_duration) = match migrate_result {
        Ok(res) => res,
        Err(e) => {
            return Err(e);
        }
    };

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
    dbs_with_sizes: &[(String, u64)],
    config: &Config,
    cancel: CancellationToken,
) -> Result<(
    tui::SharedMigrationStates,
    ProgressBar,
    tokio::task::JoinHandle<()>,
)> {
    let states = shared_migration_states(dbs_with_sizes, config);

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

fn build_config(args: Args) -> Arc<Config> {
    let verify_concurrency = args.verify_concurrency.max(1);
    let dump_parallel = args.dump_parallel.unwrap_or(args.max_parallel).max(1);
    let restore_parallel = args.restore_parallel.unwrap_or(args.max_parallel).max(1);
    let pool_cap = u32::try_from(args.restore_jobs.max(verify_concurrency).max(4)).unwrap_or(16);

    Arc::new(Config {
        source: db::DbArgs {
            host: args.from_host,
            port: args.from_port,
            user: args.from_user,
            pass: args.from_pass,
        },
        source_db: args.from_db,
        destination: db::DbArgs {
            host: args.to_host,
            port: args.to_port,
            user: args.to_user,
            pass: args.to_pass,
        },
        destination_db: args.to_db,
        dump_jobs: args.dump_jobs,
        restore_jobs: args.restore_jobs,
        max_parallel: args.max_parallel,
        dump_parallel,
        restore_parallel,
        dump_root: args.dump_root.into(),
        migrate_globals: args.migrate_globals,
        delay_table_data: args.delay_table_data,
        fast_verify: args.fast_verify,
        verify_concurrency,
        pool_cache: db::PoolCache::new(pool_cap),
        verify_sem: Arc::new(Semaphore::new(verify_concurrency)),
    })
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

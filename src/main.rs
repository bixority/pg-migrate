pub mod copy_engine;
mod db;
mod error;
mod phases;
mod plan;
mod tui;
mod verification;

use crate::error::{Error, Result};
use crate::phases::phase_migrate_all;
use crate::tui::{migration_style, redraw_loop, shared_migration_states};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use log::{info, warn};
use serde::Deserialize;
use std::{
    env, fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

fn default_split_by_column() -> String {
    "created_at".to_string()
}

#[derive(Debug, Deserialize, Clone, Hash)]
pub struct CopyRule {
    pub table: String,
    #[serde(default = "default_split_by_column")]
    pub split_by_column: String,
    pub from: Option<String>,
    pub till: Option<String>,
    pub method: Option<String>,
}

impl CopyRule {
    #[must_use]
    pub fn rule_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

pub struct Config {
    pub source: db::DbArgs,
    pub source_db: String,

    pub destination: db::DbArgs,
    pub destination_db: String,

    pub dump_root: PathBuf,
    pub dump_parallel: usize,
    pub dump_jobs: usize,
    pub zstd_level: u8,

    pub restore_jobs: usize,
    pub restore_parallel: usize,

    pub max_parallel: usize,
    pub migrate_globals: bool,
    pub delay_table_data: Vec<String>,

    pub verify_sem: Arc<Semaphore>,
    pub fast_verify: bool,
    pub verify_concurrency: usize,

    pub pool_cache: db::PoolCache,

    pub copy_rules: Vec<CopyRule>,
}

/// Returns the user's home directory.
///
/// # Errors
///
/// Returns an error if the `HOME` environment variable is not set.
pub fn home() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| Error::Env("HOME environment variable not set".to_string()))
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

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct TomlConfig {
    pub dump_jobs: usize,
    pub restore_jobs: usize,
    pub max_parallel: usize,
    pub dump_parallel: Option<usize>,
    pub restore_parallel: Option<usize>,
    pub dump_root: String,
    pub migrate_globals: bool,
    pub delay_table_data: Option<Vec<String>>,
    pub fast_verify: bool,
    pub verify_concurrency: usize,
    pub zstd_level: u8,
    pub copy_rules: Option<Vec<CopyRule>>,
}

impl Default for TomlConfig {
    fn default() -> Self {
        Self {
            dump_jobs: 24,
            restore_jobs: 12,
            max_parallel: 6,
            dump_parallel: None,
            restore_parallel: None,
            dump_root: "pg_dumps".to_string(),
            migrate_globals: true,
            delay_table_data: None,
            fast_verify: false,
            verify_concurrency: 16,
            zstd_level: 5,
            copy_rules: None,
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

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

/// Runs the copy engine for a specific table.
///
/// # Errors
///
/// Returns an error if:
/// - Partitioning fails.
/// - The copy operation fails.
pub async fn run_copy_engine(
    config: &Config,
    db_name: &str,
    table_name: &str,
    column: &str,
    from: Option<&str>,
    till: Option<&str>,
    method: Option<&str>,
) -> Result<()> {
    let source_conn = format!(
        "host={} port={} user={} password={} dbname={}",
        config.source.host, config.source.port, config.source.user, config.source.pass, db_name
    );
    let dest_conn = format!(
        "host={} port={} user={} password={} dbname={}",
        config.destination.host,
        config.destination.port,
        config.destination.user,
        config.destination.pass,
        db_name
    );

    let orchestrator = copy_engine::Orchestrator::new(
        source_conn,
        dest_conn,
        table_name.to_string(),
        config.max_parallel,
    );

    let partitions = copy_engine::Splitter::split(column, from, till, method, config.max_parallel)?;

    orchestrator.run(partitions).await?;

    info!("Copy migration for {table_name} finished successfully");
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

    let (states, table_pb, redraw_task) = setup_ui(&mp, &dbs_with_sizes, &config, cancel.clone())?;

    prepare_destination(&config, &db_names_owned, cancel.clone()).await?;

    let plan = plan::create_plan(config.clone(), &dbs_with_sizes, cancel.clone()).await?;
    plan.print();

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

fn build_config(args: Args) -> Result<Arc<Config>> {
    let toml_config: TomlConfig = if args.config.exists() {
        let content = fs::read_to_string(&args.config)?;
        toml::from_str(&content)?
    } else {
        info!("Config file not found, using defaults");
        TomlConfig::default()
    };

    let verify_concurrency = toml_config.verify_concurrency.max(1);
    let dump_parallel = toml_config
        .dump_parallel
        .unwrap_or(toml_config.max_parallel)
        .max(1);
    let restore_parallel = toml_config
        .restore_parallel
        .unwrap_or(toml_config.max_parallel)
        .max(1);

    let zstd_level = if (1..=22).contains(&toml_config.zstd_level) {
        toml_config.zstd_level
    } else {
        warn!(
            "Invalid zstd_level: {}, must be between 1 and 22. Using default: 5",
            toml_config.zstd_level
        );
        5
    };

    Ok(Arc::new(Config {
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
        dump_jobs: toml_config.dump_jobs,
        restore_jobs: toml_config.restore_jobs,
        max_parallel: toml_config.max_parallel,
        dump_parallel,
        restore_parallel,
        dump_root: toml_config.dump_root.into(),
        migrate_globals: toml_config.migrate_globals,
        delay_table_data: toml_config.delay_table_data.unwrap_or_default(),
        fast_verify: toml_config.fast_verify,
        verify_concurrency,
        pool_cache: db::PoolCache::new(),
        verify_sem: Arc::new(Semaphore::new(verify_concurrency)),
        zstd_level,
        copy_rules: toml_config.copy_rules.unwrap_or_default(),
    }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toml_config_parsing_missing_delay_table_data() -> Result<()> {
        let toml = "
dump_jobs = 1
restore_jobs = 1
max_parallel = 1
dump_root = \"/tmp\"
migrate_globals = true
fast_verify = false
verify_concurrency = 1
zstd_level = 1
";
        let config: TomlConfig = toml::from_str(toml)?;
        assert!(config.delay_table_data.is_none());
        Ok(())
    }

    #[test]
    fn test_toml_config_parsing_empty_string() -> Result<()> {
        let toml = "";
        let config: TomlConfig = toml::from_str(toml)?;
        assert!(config.delay_table_data.is_none());
        Ok(())
    }

    #[test]
    fn test_toml_config_parsing_empty_list_delay_table_data() -> Result<()> {
        let toml = "
dump_jobs = 1
restore_jobs = 1
max_parallel = 1
dump_root = \"/tmp\"
migrate_globals = true
fast_verify = false
verify_concurrency = 1
zstd_level = 1
delay_table_data = []
";
        let config: TomlConfig = toml::from_str(toml)?;
        assert!(
            config
                .delay_table_data
                .as_ref()
                .ok_or_else(|| Error::Config("delay_table_data should be Some".into()))?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn test_toml_config_parsing_copy_rules() -> Result<()> {
        let toml = "
dump_jobs = 1
restore_jobs = 1
max_parallel = 1
dump_root = \"/tmp\"

[[copy_rules]]
table = \"mydb.large_table\"
split_by_column = \"created_at\"
from = \"2023-01-01\"
till = \"2024-01-01\"
";
        let config: TomlConfig = toml::from_str(toml)?;
        let rules = config
            .copy_rules
            .ok_or_else(|| Error::Config("copy_rules should be Some".into()))?;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].table, "mydb.large_table");
        assert_eq!(rules[0].split_by_column, "created_at");
        assert_eq!(rules[0].from.as_deref(), Some("2023-01-01"));
        assert_eq!(rules[0].till.as_deref(), Some("2024-01-01"));
        assert!(rules[0].method.is_none());
        Ok(())
    }

    #[test]
    fn test_toml_config_parsing_copy_rules_hash() -> Result<()> {
        let toml = "
[[copy_rules]]
table = \"mydb.skewed_table\"
method = \"hash\"
";
        let config: TomlConfig = toml::from_str(toml)?;
        let rules = config
            .copy_rules
            .ok_or_else(|| Error::Config("copy_rules should be Some".into()))?;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].method.as_deref(), Some("hash"));
        Ok(())
    }

    #[test]
    fn test_toml_config_parsing_multiple_copy_rules_same_table() -> Result<()> {
        let toml = "
[[copy_rules]]
table = \"mydb.table1\"
from = \"2023-01-01\"
till = \"2023-02-01\"

[[copy_rules]]
table = \"mydb.table1\"
from = \"2023-02-01\"
till = \"2023-03-01\"
";
        let config: TomlConfig = toml::from_str(toml)?;
        let rules = config
            .copy_rules
            .ok_or_else(|| Error::Config("copy_rules should be Some".into()))?;
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].table, "mydb.table1");
        assert_eq!(rules[1].table, "mydb.table1");
        assert_ne!(rules[0].rule_hash(), rules[1].rule_hash());
        Ok(())
    }
}

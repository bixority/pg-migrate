#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

pub mod config;
pub mod copy_engine;
pub mod db;
pub mod error;
pub mod phases;
pub mod plan;
pub mod tls;
pub mod tui;
pub mod verification;

use crate::config::{Config, state_dir, verify_dir};
use crate::error::{Error, Result};
use crate::phases::phase_migrate_all;
use crate::tui::{migration_style, redraw_loop, shared_migration_states};
use chrono::Local;
use indicatif::{MultiProgress, ProgressBar};
use log::info;
use std::fs;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// A single copy-engine migration target: the table to copy and how to split it.
#[derive(Copy, Clone, Debug)]
pub struct CopyTarget<'a> {
    pub table: &'a str,
    pub column: &'a str,
    pub from: Option<&'a str>,
    pub till: Option<&'a str>,
    pub method: Option<&'a str>,
}

impl CopyTarget<'_> {
    #[must_use]
    pub fn to_owned(&self) -> CopyTargetOwned {
        CopyTargetOwned {
            table: self.table.to_string(),
            column: self.column.to_string(),
            from: self.from.map(str::to_string),
            till: self.till.map(str::to_string),
            method: self.method.map(str::to_string),
        }
    }
}

/// An owned version of [`CopyTarget`] for use in async blocks.
#[derive(Clone, Debug)]
pub struct CopyTargetOwned {
    pub table: String,
    pub column: String,
    pub from: Option<String>,
    pub till: Option<String>,
    pub method: Option<String>,
}

impl CopyTargetOwned {
    #[must_use]
    pub fn as_target(&self) -> CopyTarget<'_> {
        CopyTarget {
            table: &self.table,
            column: &self.column,
            from: self.from.as_deref(),
            till: self.till.as_deref(),
            method: self.method.as_deref(),
        }
    }
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
    semaphore: Arc<Semaphore>,
    cancel: CancellationToken,
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
        target.table,
        copy_engine::CopySettings {
            worker_count: config.max_parallel,
            buffer_size: config.copy_buffer_size,
            report_interval: config.copy_report_interval,
        },
        semaphore,
        cancel,
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

pub async fn run_migration_workflow(
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
    let dbs_with_sizes: Vec<(String, u64)> = dbs_with_sizes
        .into_iter()
        .filter(|(name, _)| {
            if config.is_db_excluded(name) {
                info!("Excluding database: {name}");
                false
            } else {
                true
            }
        })
        .collect();

    let db_names_owned: Vec<String> = dbs_with_sizes.iter().map(|(n, _)| n.clone()).collect();

    info!("Databases: {db_names_owned:?}");

    if dbs_with_sizes.is_empty() {
        info!("No databases found to migrate.");
        return Ok(());
    }

    prepare_destination(&config, &db_names_owned, cancel.clone()).await?;

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
        mp.clone(),
    )
    .await;

    cancel.cancel();
    let _ = redraw_task.await;

    let (regular_duration, migration_total_duration) = migrate_result?;

    let final_table = states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .render_table();
    table_pb.finish_with_message(final_table);
    total_time_pb.finish_and_clear();

    let elapsed = start_time.elapsed();
    let delayed_duration = migration_total_duration.saturating_sub(regular_duration);

    info!(
        "Migration complete.\nSummary:\n  Regular phase:     {}\n  Delayed migration: {}\n  \
         Total time:        {}",
        format_duration(regular_duration),
        format_duration(delayed_duration),
        format_duration(elapsed)
    );

    Ok(())
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    format!("{hours} hours, {minutes} minutes, {seconds} seconds")
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

pub struct MultiLogger {
    pub file: Mutex<fs::File>,
    pub inner: env_logger::Logger,
}

impl log::Log for MultiLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        self.inner.log(record);

        if self.enabled(record.metadata()) {
            let msg = format!("{}", record.args());

            if msg.contains("Table Name | Source Rows | Dest Rows")
                || msg.contains("Database | Size | Phase | %")
            {
                return;
            }

            if let Ok(mut file) = self.file.lock() {
                let timestamp = Local::now().format("%Y-%m-%dT%H:%M:%S");
                let stripped_msg = strip_ansi(&msg);
                let _ = writeln!(
                    file,
                    "{} [{:<5}] {} - {}",
                    timestamp,
                    record.level(),
                    record.target(),
                    stripped_msg
                );
            }
        }
    }

    fn flush(&self) {
        self.inner.flush();
        if let Ok(mut file) = self.file.lock() {
            let _ = file.flush();
        }
    }
}

#[must_use]
pub fn strip_ansi(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut iter = s.chars();
    while let Some(c) = iter.next() {
        if c == '\x1b' {
            if iter.next() == Some('[') {
                for c in iter.by_ref() {
                    if (0x40..=0x7E).contains(&(c as u32)) {
                        break;
                    }
                }
            }
            continue;
        }
        result.push(c);
    }
    result
}

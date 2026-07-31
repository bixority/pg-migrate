use crate::Config;
use crate::error::{Error, Result};
use crate::plan::MigrationPlan;
use crate::tui::SharedMigrationStates;
use indicatif::MultiProgress;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub mod copy;
pub mod delayed;
pub mod regular;

pub use regular::phase_migrate_one;

use crate::plan::DatabasePlan;

#[derive(Clone)]
pub struct PipelineArgs {
    pub config: Arc<Config>,
    pub db_plan: Arc<DatabasePlan>,
    pub states: SharedMigrationStates,
    pub cancel: CancellationToken,
    pub dump_sem: Arc<Semaphore>,
    pub restore_sem: Arc<Semaphore>,
}

pub async fn phase_migrate_all(
    config: Arc<Config>,
    plan: MigrationPlan,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    dump_sem: Arc<Semaphore>,
    restore_sem: Arc<Semaphore>,
    mp: Arc<MultiProgress>,
) -> Result<(Duration, Duration)> {
    let start = Instant::now();
    let (proceed_tx, proceed_rx) = watch::channel(false);

    let mut regular_tasks = JoinSet::new();
    let mut delayed_tasks = JoinSet::new();

    for db_plan in plan.databases {
        let args = PipelineArgs {
            config: config.clone(),
            db_plan: db_plan.clone(),
            states: states.clone(),
            cancel: cancel.clone(),
            dump_sem: dump_sem.clone(),
            restore_sem: restore_sem.clone(),
        };

        // Spawn regular pipeline
        regular_tasks.spawn(regular::run_regular_pipeline(args.clone(), None));

        // Spawn delayed pipeline if matching flags
        if !db_plan.delayed_tables.is_empty() || !db_plan.copy_rules.is_empty() {
            delayed_tasks.spawn(delayed::run_delayed_pipeline(args, proceed_rx.clone()));
        }
    }

    // Wait for all regular pipelines to succeed
    wait_for_regular_tasks(&mut regular_tasks, &mut delayed_tasks, cancel).await?;

    config.pool_cache.clear().await;
    log::info!("Regular migration phase finished. All database connections closed.");

    confirm_delayed_part(&config, &mp, cancel, &mut delayed_tasks)?;

    let _ = proceed_tx.send(true);

    // Wait for all delayed pipelines to succeed
    wait_for_delayed_tasks(&mut delayed_tasks, cancel).await?;

    if cancel.is_cancelled() {
        return Err(Error::Cancelled("user interruption".into()));
    }

    let total_duration = start.elapsed();
    let regular_duration = states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .latest_regular_completion()
        .map_or(total_duration, |t| t.duration_since(start));

    Ok((regular_duration, total_duration))
}

pub(crate) async fn acquire(
    sem: &Arc<Semaphore>,
    cancel: &CancellationToken,
) -> Result<OwnedSemaphorePermit> {
    tokio::select! {
        res = sem.clone().acquire_owned() => Ok(res?),
        () = cancel.cancelled() => Err(Error::Cancelled("semaphore acquisition".into())),
    }
}

async fn wait_for_regular_tasks(
    regular_tasks: &mut JoinSet<Result<()>>,
    delayed_tasks: &mut JoinSet<Result<()>>,
    cancel: &CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            res = regular_tasks.join_next() => {
                match res {
                    Some(res) => {
                        if let Err(e) = res? {
                            cancel.cancel();
                            regular_tasks.abort_all();
                            delayed_tasks.abort_all();
                            return Err(e);
                        }
                    }
                    None => break,
                }
            }
            () = cancel.cancelled() => {
                regular_tasks.abort_all();
                delayed_tasks.abort_all();
                return Err(Error::Cancelled("user interruption".into()));
            }
        }
    }
    Ok(())
}

pub async fn wait_for_delayed_tasks(
    delayed_tasks: &mut JoinSet<Result<()>>,
    cancel: &CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            res = delayed_tasks.join_next() => {
                match res {
                    Some(res) => {
                        if let Err(e) = res? {
                            cancel.cancel();
                            delayed_tasks.abort_all();
                            return Err(e);
                        }
                    }
                    None => break,
                }
            }
            () = cancel.cancelled() => {
                delayed_tasks.abort_all();
                return Err(Error::Cancelled("user interruption".into()));
            }
        }
    }
    Ok(())
}

fn confirm_delayed_part(
    config: &Config,
    mp: &MultiProgress,
    cancel: &CancellationToken,
    delayed_tasks: &mut JoinSet<Result<()>>,
) -> Result<()> {
    if config.confirm_delayed {
        let confirmed = mp.suspend(|| {
            use std::io::{self, Write};
            print!("\nRegular phase finished. Continue with delayed migration? [y/N]: ");
            let _ = io::stdout().flush();
            let mut input = String::new();
            if io::stdin().read_line(&mut input).is_ok() {
                input.trim().to_lowercase() == "y"
            } else {
                false
            }
        });

        if !confirmed {
            cancel.cancel();
            delayed_tasks.abort_all();
            return Err(Error::Cancelled(
                "User declined to continue delayed migration".into(),
            ));
        }
    }
    Ok(())
}

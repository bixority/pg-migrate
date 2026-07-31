use crate::db;
use crate::error::{Error, MigrationPhase, Result};
use crate::tui::SharedMigrationStates;
use crate::verification;
use crate::Config;
use std::sync::Arc;
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use super::{PipelineArgs, acquire, copy};

pub async fn run_delayed_pipeline(
    args: PipelineArgs,
    proceed: watch::Receiver<bool>,
) -> Result<()> {
    run_delayed_pipeline_steps_internal(args, proceed, run_delayed_pipeline_steps).await
}

pub async fn run_delayed_pipeline_steps(
    args: PipelineArgs,
    mut proceed: watch::Receiver<bool>,
) -> Result<()> {
    let PipelineArgs {
        config,
        db_plan,
        states,
        cancel,
        dump_sem,
        restore_sem,
    } = args;

    let db_name = db_plan.name.clone();
    let delayed_name = format!("{db_name} (delayed)");

    proceed
        .wait_for(|&done| done)
        .await
        .map_err(|_| Error::Cancelled("regular pipelines did not complete".into()))?;

    if cancel.is_cancelled() {
        return Err(Error::Cancelled("migration process interrupted".into()));
    }

    delayed_dump_phase(
        &config,
        &db_plan,
        &delayed_name,
        &states,
        &dump_sem,
        &cancel,
    )
    .await?;

    if let Ok(mut lock) = states.lock() {
        lock.start_timing(&delayed_name);
    }

    if cancel.is_cancelled() {
        return Err(Error::Cancelled("migration process interrupted".into()));
    }

    let _restore_permit = acquire(&restore_sem, &cancel).await?;

    delayed_restore_phase(&config, &db_plan, &delayed_name, &states, &cancel).await?;

    copy::migrate_copy_rules(config.clone(), &db_plan, &delayed_name, &states, &cancel).await?;

    delayed_verify_phase(&config, &db_plan, &delayed_name, &states, &cancel).await?;

    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .update(
            &delayed_name,
            MigrationPhase::Complete,
            5,
            "migration complete (with delayed data)",
        );

    Ok(())
}

async fn delayed_dump_phase(
    config: &Config,
    db_plan: &crate::plan::DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    dump_sem: &Arc<Semaphore>,
    cancel: &CancellationToken,
) -> Result<()> {
    let _dump_permit = acquire(dump_sem, cancel).await?;
    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .update(
            delayed_name,
            MigrationPhase::DelayedDumping,
            1,
            "dumping delayed table data",
        );
    db::dump_delayed_data(
        config,
        &db_plan.name,
        &db_plan.delayed_tables,
        &db_plan
            .copy_rules
            .iter()
            .map(|r| r.table.clone())
            .collect::<Vec<_>>(),
        cancel.clone(),
    )
    .await
}

async fn delayed_restore_phase(
    config: &Config,
    db_plan: &crate::plan::DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    cancel: &CancellationToken,
) -> Result<()> {
    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .update(
            delayed_name,
            MigrationPhase::DelayedRestoring,
            2,
            "restoring delayed table data",
        );
    db::restore_delayed_data(
        config,
        &db_plan.name,
        !db_plan.delayed_tables.is_empty(),
        cancel.clone(),
    )
    .await
}

async fn delayed_verify_phase(
    config: &Config,
    db_plan: &crate::plan::DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    cancel: &CancellationToken,
) -> Result<()> {
    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .update(
            delayed_name,
            MigrationPhase::DelayedVerifying,
            4,
            "verifying all row counts (including delayed)",
        );
    verification::verify_db(config, &db_plan.name, true, cancel.clone()).await
}

pub async fn run_delayed_pipeline_steps_internal<F, Fut>(
    args: PipelineArgs,
    proceed: watch::Receiver<bool>,
    work_fn: F,
) -> Result<()>
where
    F: FnOnce(PipelineArgs, watch::Receiver<bool>) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let states = args.states.clone();
    let db_name = args.db_plan.name.clone();
    let delayed_name = format!("{db_name} (delayed)");

    let res = work_fn(args, proceed).await;

    if let Err(error) = &res
        && let Ok(mut lock) = states.lock()
    {
        lock.fail(&delayed_name, error.to_string());
    }

    res.map_err(|e: Error| {
        let (phase, step) = states
            .lock()
            .ok()
            .and_then(|s| s.get_state(&delayed_name))
            .unwrap_or((MigrationPhase::Pending, 0));
        e.with_context(delayed_name, phase, step)
    })
}

use crate::db;
use crate::error::{Error, MigrationPhase, Result};
use crate::plan::DatabasePlan;
use crate::tui::SharedMigrationStates;
use crate::verification;
use crate::Config;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;
use super::{PipelineArgs, acquire};

pub async fn run_regular_pipeline(
    args: PipelineArgs,
    regular_done_tx: Option<watch::Sender<bool>>,
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
    let res: Result<()> = async {
        phase_migrate_one(
            &config,
            &db_plan,
            states.clone(),
            &cancel,
            acquire(&dump_sem, &cancel).await?,
            &restore_sem,
        )
        .await?;

        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
            .mark_regular_done(&db_name);

        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
            .update(&db_name, MigrationPhase::Complete, 4, "migration complete");

        if let Some(tx) = regular_done_tx {
            let _ = tx.send(true);
        }

        Ok(())
    }
    .await;

    if let Err(error) = &res
        && let Ok(mut lock) = states.lock()
    {
        lock.fail(&db_name, error.to_string());
    }

    res.map_err(|e: Error| {
        let (phase, step) = states
            .lock()
            .ok()
            .and_then(|s| s.get_state(&db_name))
            .unwrap_or((MigrationPhase::Pending, 0));
        e.with_context(db_name, phase, step)
    })
}

pub async fn phase_migrate_one(
    config: &Config,
    db_plan: &DatabasePlan,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    dump_permit: OwnedSemaphorePermit,
    restore_sem: &Arc<Semaphore>,
) -> Result<()> {
    let db_name = &db_plan.name;
    let size = db_plan.size;
    {
        let _dump_permit = dump_permit;

        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
            .update(db_name, MigrationPhase::Dumping, 1, "dumping database");

        db::dump_db(
            config,
            db_name,
            size,
            &db_plan.regular_data_excludes,
            &db_plan.full_excludes,
            cancel.clone(),
        )
        .await?;
    }

    let _restore_permit = acquire(restore_sem, cancel).await?;

    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .update(db_name, MigrationPhase::Restoring, 2, "restoring database");

    db::restore_db(config, db_name, size, cancel.clone()).await?;

    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string().into()))?
        .update(
            db_name,
            MigrationPhase::Verifying,
            3,
            "verifying row counts",
        );

    verification::verify_db(config, db_name, false, cancel.clone()).await?;

    Ok(())
}

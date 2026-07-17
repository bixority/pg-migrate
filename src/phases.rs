use crate::Config;
use crate::copy_engine::CopyProgress;
use crate::db;
use crate::error::{Error, MigrationPhase, Result};
use crate::plan::{DatabasePlan, MigrationPlan};
use crate::tui::SharedMigrationStates;
use crate::verification;
use indicatif::HumanBytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct PipelineArgs {
    config: Arc<Config>,
    db_plan: DatabasePlan,
    states: SharedMigrationStates,
    cancel: CancellationToken,
    dump_sem: Arc<Semaphore>,
    restore_sem: Arc<Semaphore>,
}

pub async fn phase_migrate_all(
    config: Arc<Config>,
    plan: MigrationPlan,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    dump_sem: Arc<Semaphore>,
    restore_sem: Arc<Semaphore>,
) -> Result<(Duration, Duration)> {
    let start = Instant::now();
    // A `watch` (rather than `Notify`) latches the "all regular pipelines
    // finished" state. A delayed pipeline that reaches its wait point *after*
    // the signal is sent still observes it, instead of missing the wake and
    // hanging — which `Notify::notify_waiters` allows for a late-scheduled
    // waiter. This gate is what guarantees a database's delayed restore never
    // begins until every regular pipeline (including that same database's
    // regular dump and restore) has completed.
    let mut regular_tasks = JoinSet::new();
    let mut delayed_tasks = JoinSet::new();

    for db_plan in plan.databases {
        let (regular_done_tx, regular_done_rx) = watch::channel(false);
        let args = PipelineArgs {
            config: config.clone(),
            db_plan: db_plan.clone(),
            states: states.clone(),
            cancel: cancel.clone(),
            dump_sem: dump_sem.clone(),
            restore_sem: restore_sem.clone(),
        };

        // Spawn regular pipeline
        regular_tasks.spawn(run_regular_pipeline(args.clone(), Some(regular_done_tx)));

        // Spawn delayed pipeline if matching flags
        if !db_plan.delayed_tables.is_empty() || !db_plan.copy_rules.is_empty() {
            delayed_tasks.spawn(run_delayed_pipeline(args, regular_done_rx));
        }
    }

    // Wait for all regular pipelines to succeed
    loop {
        tokio::select! {
            res = regular_tasks.join_next() => {
                match res {
                    Some(res) => {
                        match res? {
                            Ok(()) => {}
                            Err(e) => {
                                cancel.cancel();
                                regular_tasks.abort_all();
                                delayed_tasks.abort_all();
                                return Err(e);
                            }
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

    // Wait for all delayed pipelines to succeed
    loop {
        tokio::select! {
            res = delayed_tasks.join_next() => {
                match res {
                    Some(res) => {
                        match res? {
                            Ok(()) => {}
                            Err(e) => {
                                cancel.cancel();
                                delayed_tasks.abort_all();
                                return Err(e);
                            }
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

    if cancel.is_cancelled() {
        return Err(Error::Cancelled("user interruption".into()));
    }

    let total_duration = start.elapsed();
    let regular_duration = states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string()))?
        .latest_regular_completion()
        .map_or(total_duration, |t| t.duration_since(start));

    Ok((regular_duration, total_duration))
}

async fn acquire(sem: &Arc<Semaphore>, cancel: &CancellationToken) -> Result<OwnedSemaphorePermit> {
    tokio::select! {
        res = sem.clone().acquire_owned() => Ok(res?),
        () = cancel.cancelled() => Err(Error::Cancelled("semaphore acquisition".into())),
    }
}

async fn run_regular_pipeline(
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
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
            .mark_regular_done(&db_name);

        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
            .update(&db_name, MigrationPhase::Complete, 6, "migration complete");

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

async fn run_delayed_pipeline(
    args: PipelineArgs,
    mut regular_done: watch::Receiver<bool>,
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

    let res: Result<()> = async {
        // Phase 1: Delayed Dumping
        {
            let _dump_permit = acquire(&dump_sem, &cancel).await?;
            states
                .lock()
                .map_err(|e| Error::LockPoisoned(e.to_string()))?
                .update(
                    &delayed_name,
                    MigrationPhase::DelayedDumping,
                    1,
                    "dumping delayed table data",
                );
            db::dump_delayed_data(
                &config,
                &db_name,
                &db_plan.delayed_tables,
                &db_plan
                    .copy_rules
                    .iter()
                    .map(|r| r.table.clone())
                    .collect::<Vec<_>>(),
                cancel.clone(),
            )
            .await?;
        }

        // Wait for regular pipelines to finish before proceeding to restore
        // phases. This is what guarantees the delayed restore below never starts
        // before this database's own regular dump and restore have completed.
        // `wait_for` returns immediately if the gate is already open, and errors
        // only if the sender was dropped without opening it (a torn-down run).
        regular_done
            .wait_for(|&done| done)
            .await
            .map_err(|_| Error::Cancelled("regular pipelines did not complete".into()))?;
        if cancel.is_cancelled() {
            return Err(Error::Cancelled("migration process interrupted".into()));
        }

        let _restore_permit = acquire(&restore_sem, &cancel).await?;

        // Phase 2: Delayed Restoring
        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
            .update(
                &delayed_name,
                MigrationPhase::DelayedRestoring,
                3,
                "restoring delayed table data",
            );
        db::restore_delayed_data(
            &config,
            &db_name,
            !db_plan.delayed_tables.is_empty(),
            cancel.clone(),
        )
        .await?;

        // Phase 2.5: Copy Engine
        migrate_copy_rules(&config, &db_plan, &delayed_name, &states).await?;

        // Phase 3: Delayed Verifying
        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
            .update(
                &delayed_name,
                MigrationPhase::DelayedVerifying,
                5,
                "verifying all row counts (including delayed)",
            );
        verification::verify_db(&config, &db_name, true, cancel.clone()).await?;

        // Phase 4: Complete
        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
            .update(
                &delayed_name,
                MigrationPhase::Complete,
                6,
                "migration complete (with delayed data)",
            );

        Ok(())
    }
    .await;

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

async fn migrate_copy_rules(
    config: &Config,
    db_plan: &DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
) -> Result<()> {
    for rule in &db_plan.copy_rules {
        let table_name = &rule.table;
        let db_name = &db_plan.name;
        let marker = db::copy_rule_done_marker(db_name, table_name, rule.rule_hash)?;
        if marker.exists() {
            continue;
        }

        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
            .update(
                delayed_name,
                MigrationPhase::DelayedRestoring,
                4,
                format!("preparing copy of {table_name} via copy engine"),
            );

        // Reflect copy-engine partition progress in the delayed row as each
        // partition completes; UI updates are best-effort, so a poisoned lock
        // is ignored rather than aborting the migration.
        let on_progress = |p: CopyProgress| {
            if let Ok(mut lock) = states.lock() {
                lock.update(
                    delayed_name,
                    MigrationPhase::DelayedRestoring,
                    4,
                    format!(
                        "copying {table_name} via copy engine ({}/{} partitions, {})",
                        p.completed_partitions,
                        p.total_partitions,
                        HumanBytes(p.total_bytes)
                    ),
                );
            }
        };

        crate::run_copy_engine(
            config,
            db_name,
            crate::CopyTarget {
                table: table_name,
                column: &rule.column,
                from: rule.from.as_deref(),
                till: rule.till.as_deref(),
                method: Some(&rule.method),
            },
            on_progress,
        )
        .await?;

        std::fs::write(marker, "")?;
    }
    Ok(())
}

async fn phase_migrate_one(
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
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
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
        .map_err(|e| Error::LockPoisoned(e.to_string()))?
        .update(db_name, MigrationPhase::Restoring, 2, "restoring database");

    db::restore_db(config, db_name, size, cancel.clone()).await?;

    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string()))?
        .update(
            db_name,
            MigrationPhase::SourceCounts,
            3,
            "computing source row counts",
        );

    verification::get_or_compute_counts(
        config,
        &config.source,
        db_name,
        false,
        true,
        cancel.clone(),
    )
    .await?;

    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string()))?
        .update(
            db_name,
            MigrationPhase::DestinationCounts,
            4,
            "computing destination row counts",
        );

    verification::get_or_compute_counts(
        config,
        &config.destination,
        db_name,
        false,
        false,
        cancel.clone(),
    )
    .await?;

    states
        .lock()
        .map_err(|e| Error::LockPoisoned(e.to_string()))?
        .update(
            db_name,
            MigrationPhase::Verifying,
            5,
            "verifying row counts",
        );

    verification::verify_db(config, db_name, false, cancel.clone()).await?;

    Ok(())
}

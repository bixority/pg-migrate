use crate::Config;
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
    db_plan: Arc<DatabasePlan>,
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

        if let Ok(mut lock) = states.lock() {
            lock.start_timing(&delayed_name);
        }

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
                2,
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
        migrate_copy_rules(config.clone(), &db_plan, &delayed_name, &states, &cancel).await?;

        // Phase 3: Delayed Verifying
        states
            .lock()
            .map_err(|e| Error::LockPoisoned(e.to_string()))?
            .update(
                &delayed_name,
                MigrationPhase::DelayedVerifying,
                4,
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
                5,
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
    config: Arc<Config>,
    db_plan: &DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    cancel: &CancellationToken,
) -> Result<()> {
    if db_plan.copy_rules.is_empty() {
        return Ok(());
    }

    let mut tasks = JoinSet::new();
    let sem = Arc::new(Semaphore::new(config.max_parallel));
    let progress = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));

    for rule in &db_plan.copy_rules {
        let marker = db::copy_rule_done_marker(&db_plan.name, &rule.table, rule.rule_hash)?;
        if marker.exists() {
            continue;
        }

        let rule = rule.clone();
        let config = config.clone();
        let db_name = db_plan.name.clone();
        let delayed_name = delayed_name.to_string();
        let states = states.clone();
        let cancel = cancel.clone();
        let sem = sem.clone();
        let progress = progress.clone();
        let rule_key = format!("{}:{}", rule.table, rule.rule_hash);

        tasks.spawn(async move {
            let table_name = rule.table.clone();
            let column = rule.column.clone();
            let from = rule.from.clone();
            let till = rule.till.clone();
            let method = rule.method.clone();
            let sem_inner = sem.clone();
            let cancel_inner = cancel.clone();

            crate::run_copy_engine(
                &config,
                &db_name,
                crate::CopyTarget {
                    table: &table_name,
                    column: &column,
                    from: from.as_deref(),
                    till: till.as_deref(),
                    method: Some(&method),
                },
                sem_inner,
                cancel_inner,
                |p| {
                    if let Ok(mut lock) = progress.lock() {
                        lock.insert(rule_key.clone(), p);
                        let total_p: usize = lock.values().map(|v| v.total_partitions).sum();
                        let comp_p: usize = lock.values().map(|v| v.completed_partitions).sum();
                        let bytes: u64 = lock.values().map(|v| v.total_bytes).sum();
                        let table_count = lock.len();

                        if let Ok(mut ui_lock) = states.lock() {
                            ui_lock.update(
                                &delayed_name,
                                MigrationPhase::DelayedRestoring,
                                3,
                                format!(
                                    "copying {table_count} tables via copy engine ({comp_p}/{total_p} partitions, {})",
                                    HumanBytes(bytes)
                                ),
                            );
                        }
                    }
                },
            )
            .await?;

            let marker = db::copy_rule_done_marker(&db_name, &table_name, rule.rule_hash)?;
            std::fs::write(marker, "")?;

            Ok::<(), Error>(())
        });
    }

    while let Some(res) = tasks.join_next().await {
        res??;
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
            MigrationPhase::Verifying,
            3,
            "verifying row counts",
        );

    verification::verify_db(config, db_name, false, cancel.clone()).await?;

    Ok(())
}

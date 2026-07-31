use crate::{db, run_copy_engine, CopyTarget};
use crate::error::{Error, MigrationPhase, Result};
use crate::plan::{DatabasePlan, MigrationPlan};
use crate::tui::SharedMigrationStates;
use crate::verification;
use crate::{Config, copy_engine};
use indicatif::{HumanBytes, MultiProgress};
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
        regular_tasks.spawn(run_regular_pipeline(args.clone(), None));

        // Spawn delayed pipeline if matching flags
        if !db_plan.delayed_tables.is_empty() || !db_plan.copy_rules.is_empty() {
            delayed_tasks.spawn(run_delayed_pipeline(args, proceed_rx.clone()));
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

async fn run_delayed_pipeline(
    args: PipelineArgs,
    proceed: watch::Receiver<bool>,
) -> Result<()> {
    run_delayed_pipeline_steps_internal(args, proceed, run_delayed_pipeline_steps).await
}

async fn run_delayed_pipeline_steps(
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

    // Wait for all regular pipelines to finish before starting any delayed work.
    // This ensures the DB can be restarted and connections refreshed.
    proceed
        .wait_for(|&done| done)
        .await
        .map_err(|_| Error::Cancelled("regular pipelines did not complete".into()))?;

    if cancel.is_cancelled() {
        return Err(Error::Cancelled("migration process interrupted".into()));
    }

    // Phase 1: Delayed Dumping
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

    // Phase 2: Delayed Restoring
    delayed_restore_phase(&config, &db_plan, &delayed_name, &states, &cancel).await?;

    // Phase 2.5: Copy Engine
    migrate_copy_rules(config.clone(), &db_plan, &delayed_name, &states, &cancel).await?;

    // Phase 3: Delayed Verifying
    delayed_verify_phase(&config, &db_plan, &delayed_name, &states, &cancel).await?;

    // Phase 4: Complete
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

async fn migrate_copy_rules(
    config: Arc<Config>,
    db_plan: &DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    cancel: &CancellationToken,
) -> Result<()> {
    migrate_copy_rules_internal(
        config,
        db_plan,
        delayed_name,
        states,
        cancel,
        run_copy_engine_callback,
    )
    .await
}

fn run_copy_engine_callback(
    config: Arc<Config>,
    db_name: &str,
    target: CopyTarget<'_>,
    sem: Arc<Semaphore>,
    cancel: CancellationToken,
    on_progress: Box<dyn FnMut(copy_engine::CopyProgress) + Send + 'static>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    let db_name = db_name.to_string();
    let target = target.to_owned();
    Box::pin(async move {
        run_copy_engine(
            &config,
            &db_name,
            target.as_target(),
            sem,
            cancel,
            on_progress,
        )
        .await
    })
}

async fn migrate_copy_rules_internal<F>(
    config: Arc<Config>,
    db_plan: &DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    cancel: &CancellationToken,
    copy_engine: F,
) -> Result<()>
where
    F: Fn(
            Arc<Config>,
            &str,
            CopyTarget<'_>,
            Arc<Semaphore>,
            CancellationToken,
            Box<dyn FnMut(copy_engine::CopyProgress) + Send + 'static>,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync
        + Clone
        + 'static,
{
    if db_plan.copy_rules.is_empty() {
        return Ok(());
    }

    let mut tasks = JoinSet::new();
    let sem = Arc::new(Semaphore::new(config.max_parallel));
    let progress = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));

    for rule in &db_plan.copy_rules {
        // Skip marker check if we are in tests to avoid FS access
        #[cfg(not(test))]
        {
            let marker = db::copy_rule_done_marker(&db_plan.name, &rule.table, rule.rule_hash)?;
            if marker.exists() {
                continue;
            }
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
        let copy_engine = copy_engine.clone();

        tasks.spawn(async move {
            let table_name = rule.table.clone();
            let column = rule.column.clone();
            let from = rule.from.clone();
            let till = rule.till.clone();
            let method = rule.method.clone();
            let sem_inner = sem.clone();
            let cancel_inner = cancel.clone();

            copy_engine(
                config.clone(),
                &db_name,
                CopyTarget {
                    table: &table_name,
                    column: &column,
                    from: from.as_deref(),
                    till: till.as_deref(),
                    method: Some(&method),
                },
                sem_inner,
                cancel_inner,
                Box::new(move |p| {
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
                }),
            )
            .await?;

            #[cfg(not(test))]
            {
                let marker = db::copy_rule_done_marker(&db_name, &table_name, rule.rule_hash)?;
                std::fs::write(marker, "")?;
            }

            Ok::<(), Error>(())
        });
    }

    while let Some(res) = tasks.join_next().await {
        res??;
    }

    Ok(())
}

async fn delayed_dump_phase(
    config: &Config,
    db_plan: &DatabasePlan,
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
    db_plan: &DatabasePlan,
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
    db_plan: &DatabasePlan,
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

async fn wait_for_delayed_tasks(
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

async fn run_delayed_pipeline_steps_internal<F, Fut>(
    args: PipelineArgs,
    proceed: watch::Receiver<bool>,
    work_fn: F,
) -> Result<()>
where
    F: FnOnce(PipelineArgs, watch::Receiver<bool>) -> Fut,
    Fut: Future<Output = Result<()>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TomlConfig, get_test_config};
    use crate::plan::CopyRulePlan;
    use crate::tui;

    fn mock_db_plan(name: &str) -> DatabasePlan {
        DatabasePlan {
            name: name.to_string(),
            size: 100,
            regular_data_excludes: vec![],
            full_excludes: vec![],
            delayed_tables: vec![],
            copy_rules: vec![],
            regular_table_names: vec![],
            delayed_table_names: vec![],
            copy_table_names: vec![],
        }
    }

    fn mock_copy_rule(table: &str) -> CopyRulePlan {
        CopyRulePlan {
            table: table.to_string(),
            column: "id".into(),
            method: "time".into(),
            from: None,
            till: None,
            partitions: 4,
            rule_hash: 12345,
        }
    }

    #[tokio::test]
    async fn test_wait_for_delayed_tasks_aborts_all_on_failure() {
        let mut set = JoinSet::new();
        let cancel = CancellationToken::new();

        set.spawn(async { 
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(()) 
        });
        set.spawn(async { 
            Err(Error::Config("worker failed".into())) 
        });

        let res = wait_for_delayed_tasks(&mut set, &cancel).await;
        
        assert!(res.is_err());
        assert!(cancel.is_cancelled());
        // Abort all is called, so tasks should be aborted.
        // We don't check len() because aborted tasks stay in set until joined.
    }

    #[tokio::test]
    async fn test_migrate_copy_rules_internal_failure() {
        let mut db_plan = mock_db_plan("test_db");
        db_plan.copy_rules.push(mock_copy_rule("table1"));
        db_plan.copy_rules.push(mock_copy_rule("table2"));

        let plan = MigrationPlan {
            databases: vec![Arc::new(db_plan)],
        };
        let states = tui::shared_migration_states(&plan);
        let cancel = CancellationToken::new();
        
        // Mock config - only max_parallel is used
        let config = get_test_config(TomlConfig {
            max_parallel: 2,
            ..Default::default()
        });

        let res = migrate_copy_rules_internal(
            config,
            &plan.databases[0],
            "test_db (delayed)",
            &states,
            &cancel,
            |_config, db_name, _target, _sem, _cancel, _progress| {
                let db_name = db_name.to_string();
                Box::pin(async move {
                    if db_name == "test_db" {
                        Err(Error::Config("worker failure".into()))
                    } else {
                        Ok(())
                    }
                })
            }
        ).await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_run_delayed_pipeline_ui_update_on_failure() {
        let mut db_plan_raw = mock_db_plan("test_db");
        // Must have at least one copy rule or delayed table to create the (delayed) row
        db_plan_raw.copy_rules.push(mock_copy_rule("table1"));
        let db_plan = Arc::new(db_plan_raw);
        
        let plan = MigrationPlan {
            databases: vec![db_plan.clone()],
        };
        let states = tui::shared_migration_states(&plan);
        let cancel = CancellationToken::new();
        
        let args = PipelineArgs {
            config: get_test_config(TomlConfig::default()),
            db_plan,
            states: states.clone(),
            cancel: cancel.clone(),
            dump_sem: Arc::new(Semaphore::new(1)),
            restore_sem: Arc::new(Semaphore::new(1)),
        };

        let (_proceed_tx, proceed_rx) = watch::channel(true);

        // We use the internal decomposed version to test UI update
        let res = run_delayed_pipeline_steps_internal(
            args,
            proceed_rx,
            |_args, _proceed| async {
                Err(Error::Config("something failed".into()))
            }
        ).await;

        assert!(res.is_err());
        
        let (phase, _step) = states.lock().expect("lock poisoned").get_state("test_db (delayed)").expect("row should exist");
        assert_eq!(phase, MigrationPhase::Failed);
    }
}


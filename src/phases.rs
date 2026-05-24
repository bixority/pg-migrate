use crate::db::MigrationPhase;
use crate::tui::SharedMigrationStates;
use crate::{Config, db, verification};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn phase_migrate_all(
    config: Arc<Config>,
    db_names_with_sizes: &[(String, u64)],
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    dump_sem: Arc<Semaphore>,
    restore_sem: Arc<Semaphore>,
) -> anyhow::Result<(Duration, Duration)> {
    let start = Instant::now();
    let mut pipeline_tasks = JoinSet::new();

    for (db_name, size) in db_names_with_sizes {
        let config_clone = config.clone();
        let cancel_clone = cancel.clone();
        let dump_sem_clone = dump_sem.clone();
        let restore_sem_clone = restore_sem.clone();
        let states_clone = states.clone();
        let db_clone = db_name.clone();
        let size_val = *size;

        pipeline_tasks.spawn(async move {
            let result = run_pipeline(
                &config_clone,
                &db_clone,
                size_val,
                states_clone.clone(),
                &cancel_clone,
                &dump_sem_clone,
                &restore_sem_clone,
            )
            .await;

            if let Err(error) = &result {
                states_clone
                    .lock()
                    .expect("states lock poisoned")
                    .fail(&db_clone, error.to_string());
            }

            result
        });
    }

    loop {
        tokio::select! {
            pipeline_result = pipeline_tasks.join_next() => {
                match pipeline_result {
                    Some(res) => {
                        match res? {
                            Ok(()) => {}
                            Err(e) => {
                                let was_cancelled = cancel.is_cancelled();

                                if !was_cancelled {
                                    cancel.cancel();
                                }

                                pipeline_tasks.abort_all();

                                if was_cancelled {
                                    anyhow::bail!("Migration cancelled by user");
                                }

                                return Err(e);
                            }
                        }
                    }
                    None => break,
                }
            }
            () = cancel.cancelled() => {
                pipeline_tasks.abort_all();
                anyhow::bail!("Migration cancelled by user");
            }
        }
    }

    if cancel.is_cancelled() {
        anyhow::bail!("Migration cancelled by user");
    }

    let total_duration = start.elapsed();
    let regular_duration = states
        .lock()
        .expect("states lock poisoned")
        .latest_regular_completion()
        .map_or(total_duration, |t| t.duration_since(start));

    Ok((regular_duration, total_duration))
}

async fn acquire(
    sem: &Arc<Semaphore>,
    cancel: &CancellationToken,
) -> anyhow::Result<OwnedSemaphorePermit> {
    tokio::select! {
        res = sem.clone().acquire_owned() => Ok(res?),
        () = cancel.cancelled() => anyhow::bail!("cancelled while waiting for semaphore"),
    }
}

async fn run_pipeline(
    config: &Config,
    db_name: &str,
    size: u64,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    dump_sem: &Arc<Semaphore>,
    restore_sem: &Arc<Semaphore>,
) -> anyhow::Result<()> {
    phase_migrate_one(
        config,
        db_name,
        size,
        states.clone(),
        cancel,
        dump_sem,
        restore_sem,
    )
    .await?;
    states
        .lock()
        .expect("states lock poisoned")
        .mark_regular_done(db_name);
    phase_finalize_one(config, db_name, size, states, cancel, dump_sem, restore_sem).await?;
    Ok(())
}

async fn phase_migrate_one(
    config: &Config,
    db_name: &str,
    size: u64,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    dump_sem: &Arc<Semaphore>,
    restore_sem: &Arc<Semaphore>,
) -> anyhow::Result<()> {
    {
        let _dump_permit = acquire(dump_sem, cancel).await?;

        states.lock().expect("states lock poisoned").update(
            db_name,
            MigrationPhase::Dumping,
            1,
            "dumping database",
        );

        db::dump_db(config, db_name, size, cancel.clone()).await?;

        states.lock().expect("states lock poisoned").update(
            db_name,
            MigrationPhase::SourceCounts,
            2,
            "computing source row counts",
        );

        compute_source_counts(config, db_name, cancel.clone()).await?;
    }

    let _restore_permit = acquire(restore_sem, cancel).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::Restoring,
        3,
        "restoring database",
    );

    db::restore_db(config, db_name, size, cancel.clone()).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::DestinationCounts,
        4,
        "computing destination row counts",
    );

    compute_destination_counts(config, db_name, cancel.clone()).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::Verifying,
        5,
        "verifying row counts",
    );

    verification::verify_db(config, db_name, false, cancel.clone()).await?;

    Ok(())
}

async fn phase_finalize_one(
    config: &Config,
    db_name: &str,
    _size: u64,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    dump_sem: &Arc<Semaphore>,
    restore_sem: &Arc<Semaphore>,
) -> anyhow::Result<()> {
    let db_prefix = format!("{db_name}.");
    let has_delayed = config
        .delay_table_data
        .iter()
        .any(|d| d.starts_with(&db_prefix));

    if !has_delayed {
        states.lock().expect("states lock poisoned").update(
            db_name,
            MigrationPhase::Complete,
            6,
            "migration complete",
        );
        return Ok(());
    }

    {
        let _dump_permit = acquire(dump_sem, cancel).await?;

        states.lock().expect("states lock poisoned").update(
            db_name,
            MigrationPhase::DelayedDumping,
            7,
            "dumping delayed table data",
        );

        db::dump_data(config, db_name, cancel.clone()).await?;
    }

    let _restore_permit = acquire(restore_sem, cancel).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::DelayedDroppingIndexes,
        8,
        "dropping secondary indexes on delayed tables",
    );

    db::drop_delayed_indexes(config, db_name, cancel.clone()).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::DelayedRestoring,
        9,
        "restoring delayed table data",
    );

    db::restore_delayed_data(config, db_name, cancel.clone()).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::DelayedRecreatingIndexes,
        10,
        "recreating secondary indexes on delayed tables",
    );

    db::recreate_delayed_indexes(config, db_name, cancel.clone()).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::DelayedVerifying,
        11,
        "verifying all row counts (including delayed)",
    );

    verification::verify_db(config, db_name, true, cancel.clone()).await?;

    states.lock().expect("states lock poisoned").update(
        db_name,
        MigrationPhase::Complete,
        11,
        "migration complete (with delayed data)",
    );

    Ok(())
}

async fn compute_source_counts(
    config: &Config,
    db_name: &str,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let src_path = verification::src_counts_path(db_name, config.fast_verify);

    if !src_path.exists() {
        let counts = verification::stat_counts(
            config,
            &config.source,
            db_name,
            &config.delay_table_data,
            false,
            cancel,
        )
        .await?;

        let content = serde_json::to_string(&counts)?;
        fs::write(&src_path, content)?;
    }

    Ok(())
}

async fn compute_destination_counts(
    config: &Config,
    db_name: &str,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let dst_path = verification::dst_counts_path(db_name, config.fast_verify);

    if !dst_path.exists() {
        let counts = verification::stat_counts(
            config,
            &config.destination,
            db_name,
            &config.delay_table_data,
            false,
            cancel,
        )
        .await?;

        let content = serde_json::to_string(&counts)?;
        fs::write(&dst_path, content)?;
    }

    Ok(())
}

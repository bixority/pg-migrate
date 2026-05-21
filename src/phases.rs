use crate::db::MigrationPhase;
use crate::tui::SharedMigrationStates;
use crate::{Config, db, verification};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn phase_migrate_all(
    config: Arc<Config>,
    db_names_with_sizes: &[(String, u64)],
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    sem: Arc<Semaphore>,
) -> anyhow::Result<(Duration, Duration)> {
    let mut pipeline_tasks = JoinSet::new();

    let regular_start = Instant::now();

    for (db_name, size) in db_names_with_sizes {
        let config_clone = config.clone();
        let cancel_clone = cancel.clone();
        let sem_clone = sem.clone();
        let states_clone = states.clone();
        let db_clone = db_name.clone();
        let size_val = *size;

        pipeline_tasks.spawn(async move {
            let _permit = tokio::select! {
                res = sem_clone.acquire_owned() => res?,
                () = cancel_clone.cancelled() => anyhow::bail!("cancelled while waiting for semaphore"),
            };

            let result = phase_migrate_one(
                &config_clone,
                &db_clone,
                size_val,
                states_clone.clone(),
                &cancel_clone,
            )
            .await;

            if let Err(error) = &result {
                states_clone
                    .write()
                    .await
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

    let regular_duration = regular_start.elapsed();

    let delayed_start = Instant::now();
    phase_delay_migrate_all(
        config.clone(),
        db_names_with_sizes,
        states.clone(),
        cancel,
        sem,
    )
    .await?;
    let delayed_duration = delayed_start.elapsed();

    Ok((regular_duration, delayed_duration))
}

async fn phase_migrate_one(
    config: &Config,
    db_name: &str,
    size: u64,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    states
        .write()
        .await
        .update(db_name, MigrationPhase::Dumping, 1, "dumping database");

    db::dump_db(config, db_name, size, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::SourceCounts,
        2,
        "computing source row counts",
    );

    compute_source_counts(config, db_name, cancel.clone()).await?;

    states
        .write()
        .await
        .update(db_name, MigrationPhase::Restoring, 3, "restoring database");

    db::restore_db(config, db_name, size, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::DestinationCounts,
        4,
        "computing destination row counts",
    );

    compute_destination_counts(config, db_name, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::Verifying,
        5,
        "verifying row counts",
    );

    verification::verify_db(config, db_name, false, cancel.clone()).await?;

    Ok(())
}

pub async fn phase_delay_migrate_all(
    config: Arc<Config>,
    db_names_with_sizes: &[(String, u64)],
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    sem: Arc<Semaphore>,
) -> anyhow::Result<()> {
    let mut finalize_tasks = JoinSet::new();

    for (db_name, size) in db_names_with_sizes {
        let config_clone = config.clone();
        let cancel_clone = cancel.clone();
        let sem_clone = sem.clone();
        let states_clone = states.clone();
        let db_clone = db_name.clone();
        let size_val = *size;

        finalize_tasks.spawn(async move {
            let _permit = tokio::select! {
                res = sem_clone.acquire_owned() => res?,
                () = cancel_clone.cancelled() => anyhow::bail!("cancelled while waiting for semaphore"),
            };

            let result = phase_finalize_one(
                &config_clone,
                &db_clone,
                size_val,
                states_clone.clone(),
                &cancel_clone,
            )
            .await;

            if let Err(error) = &result {
                states_clone
                    .write()
                    .await
                    .fail(&db_clone, error.to_string());
            }

            result
        });
    }

    while let Some(res) = finalize_tasks.join_next().await {
        match res? {
            Ok(()) => {}
            Err(e) => {
                let was_cancelled = cancel.is_cancelled();
                if !was_cancelled {
                    cancel.cancel();
                }
                finalize_tasks.abort_all();
                if was_cancelled {
                    anyhow::bail!("Finalize phase cancelled by user");
                }
                return Err(e);
            }
        }
    }

    Ok(())
}

async fn phase_finalize_one(
    config: &Config,
    db_name: &str,
    _size: u64,
    states: SharedMigrationStates,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let db_prefix = format!("{db_name}.");
    let has_delayed = config
        .delay_table_data
        .iter()
        .any(|d| d.starts_with(&db_prefix));

    if !has_delayed {
        states
            .write()
            .await
            .update(db_name, MigrationPhase::Complete, 6, "migration complete");
        return Ok(());
    }

    states.write().await.update(
        db_name,
        MigrationPhase::DelayedDumping,
        7,
        "dumping delayed table data",
    );

    db::dump_data(config, db_name, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::DelayedDroppingIndexes,
        8,
        "dropping secondary indexes on delayed tables",
    );

    db::drop_delayed_indexes(config, db_name, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::DelayedRestoring,
        9,
        "restoring delayed table data",
    );

    db::restore_delayed_data(config, db_name, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::DelayedRecreatingIndexes,
        10,
        "recreating secondary indexes on delayed tables",
    );

    db::recreate_delayed_indexes(config, db_name, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::DelayedVerifying,
        11,
        "verifying all row counts (including delayed)",
    );

    verification::verify_db(config, db_name, true, cancel.clone()).await?;

    states.write().await.update(
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
    let src_path = verification::src_counts_path(db_name);

    if !src_path.exists() {
        let counts = verification::stat_counts(
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
    let dst_path = verification::dst_counts_path(db_name);

    if !dst_path.exists() {
        let counts = verification::stat_counts(
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

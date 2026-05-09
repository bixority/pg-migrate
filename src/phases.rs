use crate::db::MigrationPhase;
use crate::tui::SharedMigrationStates;
use crate::{Config, db, verification};
use log::info;
use std::fs;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn phase_migrate_all(
    config: Arc<Config>,
    db_names_with_sizes: &[(String, u64)],
    states: SharedMigrationStates,
    cancel: &CancellationToken,
    sem: Arc<Semaphore>,
) -> anyhow::Result<()> {
    let mut pipeline_tasks = JoinSet::new();

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

    Ok(())
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

    if db::done_marker(db_name).exists() {
        info!("Skipping restore for {db_name}");

        states.write().await.update(
            db_name,
            MigrationPhase::Skipped,
            4,
            "restore skipped; already done",
        );
    } else {
        states
            .write()
            .await
            .update(db_name, MigrationPhase::Restoring, 3, "restoring database");

        db::restore_db(config, db_name, size, cancel.clone()).await?;
    }

    states.write().await.update(
        db_name,
        MigrationPhase::DestinationCounts,
        5,
        "computing destination row counts",
    );

    compute_destination_counts(config, db_name, cancel.clone()).await?;

    states.write().await.update(
        db_name,
        MigrationPhase::Verifying,
        6,
        "verifying row counts",
    );

    verification::verify_db(config, db_name, cancel.clone()).await?;

    states
        .write()
        .await
        .update(db_name, MigrationPhase::Complete, 6, "migration complete");

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
            &config.from_host,
            &config.from_port,
            &config.from_pass,
            &config.from_user,
            db_name,
            &config.exclude_table_data,
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
            &config.to_host,
            &config.to_port,
            &config.to_pass,
            &config.to_user,
            db_name,
            &config.exclude_table_data,
            cancel,
        )
        .await?;

        let content = serde_json::to_string(&counts)?;
        fs::write(&dst_path, content)?;
    }

    Ok(())
}

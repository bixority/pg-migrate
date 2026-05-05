use crate::{Config, db, verification};
use indicatif::ProgressBar;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn phase_migrate_all(
    config: Arc<Config>,
    db_names_with_sizes: &[(String, u64)],
    pbs: &HashMap<&String, ProgressBar>,
    cancel: &CancellationToken,
    sem: Arc<Semaphore>,
) -> anyhow::Result<()> {
    let mut pipeline_tasks = JoinSet::new();

    for (db_name, size) in db_names_with_sizes {
        let config_clone = config.clone();
        let cancel_clone = cancel.clone();
        let sem_clone = sem.clone();
        let db_clone = db_name.clone();
        let size_val = *size;
        let pb = pbs.get(db_name).cloned().expect("missing pb");

        pipeline_tasks.spawn(async move {
            let _permit = sem_clone.acquire_owned().await?;

            phase_migrate_one(&config_clone, &db_clone, size_val, pb, &cancel_clone).await
        });
    }

    while let Some(pipeline_result) = pipeline_tasks.join_next().await {
        match pipeline_result? {
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

    Ok(())
}

async fn phase_migrate_one(
    config: &Config,
    db_name: &str,
    size: u64,
    pb: ProgressBar,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    db::dump_db(config, db_name, size, pb.clone(), cancel.clone()).await?;

    compute_source_counts(config, db_name).await?;

    if db::done_marker(db_name).exists() {
        info!("Skipping restore for {db_name}");
        pb.set_position(size.saturating_mul(2));
        pb.set_message(format!("Restoration skipped (already done) for {db_name}"));
    } else {
        db::restore_db(config, db_name, size, pb.clone(), cancel.clone()).await?;
    }

    compute_destination_counts(config, db_name).await?;

    verification::verify_db(config, db_name, pb).await?;

    Ok(())
}

async fn compute_source_counts(config: &Config, db_name: &str) -> anyhow::Result<()> {
    let src_path = verification::src_counts_path(db_name);

    if !src_path.exists() {
        let counts = verification::stat_counts(
            &config.from_host,
            &config.from_port,
            &config.from_pass,
            &config.from_user,
            db_name,
        )
        .await?;

        let content = serde_json::to_string(&counts)?;
        fs::write(&src_path, content)?;
    }

    Ok(())
}

async fn compute_destination_counts(config: &Config, db_name: &str) -> anyhow::Result<()> {
    let dst_path = verification::dst_counts_path(db_name);

    if !dst_path.exists() {
        let counts = verification::stat_counts(
            &config.to_host,
            &config.to_port,
            &config.to_pass,
            &config.to_user,
            db_name,
        )
        .await?;

        let content = serde_json::to_string(&counts)?;
        fs::write(&dst_path, content)?;
    }

    Ok(())
}

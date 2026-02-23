use crate::{Config, db, verification};
use indicatif::MultiProgress;
use log::info;
use std::fs;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub async fn phase_dump_all(
    config: &Config,
    dbs_with_sizes: &[(String, u64)],
    mp: Arc<MultiProgress>,
    cancel: &CancellationToken,
    sem: Arc<Semaphore>,
) -> anyhow::Result<()> {
    let mut dump_tasks = vec![];

    for (db, size) in dbs_with_sizes {
        let permit = sem.clone().acquire_owned().await?;
        let mp = mp.clone();
        let config_clone = Arc::new(Config {
            from_host: config.from_host.clone(),
            from_port: config.from_port.clone(),
            from_user: config.from_user.clone(),
            from_pass: config.from_pass.clone(),
            from_db: config.from_db.clone(),
            to_host: config.to_host.clone(),
            to_port: config.to_port.clone(),
            to_user: config.to_user.clone(),
            to_pass: config.to_pass.clone(),
            to_db: config.to_db.clone(),
            dump_jobs: config.dump_jobs,
            restore_jobs: config.restore_jobs,
            max_parallel: config.max_parallel,
            dump_root: config.dump_root.clone(),
            migrate_globals: config.migrate_globals,
            disable_dst_optimizations: config.disable_dst_optimizations,
        });
        let cancel_clone = cancel.clone();
        let db_clone = db.clone();
        let size_val = *size;

        dump_tasks.push(tokio::spawn(async move {
            let _p = permit;
            db::dump_db(&config_clone, &db_clone, size_val, mp, cancel_clone).await
        }));
    }

    for dump_task in dump_tasks {
        match dump_task.await? {
            Ok(()) => {}
            Err(e) => {
                if cancel.is_cancelled() {
                    anyhow::bail!("Migration cancelled by user");
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

pub async fn phase_compute_source_counts(
    config: &Config,
    db_names: &[String],
) -> anyhow::Result<()> {
    for db in db_names {
        let src_path = verification::src_counts_path(db);

        if !src_path.exists() {
            let counts = verification::stat_counts(
                &config.from_host,
                &config.from_port,
                &config.from_pass,
                &config.from_user,
                db,
            )
            .await?;
            let content = serde_json::to_string(&counts)?;
            fs::write(&src_path, content)?;
        }
    }
    Ok(())
}

pub async fn phase_restore_all(
    config: &Config,
    dbs_with_sizes: &[(String, u64)],
    mp: Arc<MultiProgress>,
    cancel: &CancellationToken,
    sem: Arc<Semaphore>,
) -> anyhow::Result<()> {
    let mut restore_tasks = vec![];

    for (db, size) in dbs_with_sizes {
        if db::done_marker(db).exists() {
            info!("Skipping restore for {db}");
            continue;
        }

        let permit = sem.clone().acquire_owned().await?;
        let mp = mp.clone();
        let config_clone = Arc::new(Config {
            from_host: config.from_host.clone(),
            from_port: config.from_port.clone(),
            from_user: config.from_user.clone(),
            from_pass: config.from_pass.clone(),
            from_db: config.from_db.clone(),
            to_host: config.to_host.clone(),
            to_port: config.to_port.clone(),
            to_user: config.to_user.clone(),
            to_pass: config.to_pass.clone(),
            to_db: config.to_db.clone(),
            dump_jobs: config.dump_jobs,
            restore_jobs: config.restore_jobs,
            max_parallel: config.max_parallel,
            dump_root: config.dump_root.clone(),
            migrate_globals: config.migrate_globals,
            disable_dst_optimizations: config.disable_dst_optimizations,
        });
        let cancel_clone = cancel.clone();
        let db_clone = db.clone();
        let size_val = *size;

        restore_tasks.push(tokio::spawn(async move {
            let _p = permit;
            db::restore_db(&config_clone, &db_clone, size_val, mp, cancel_clone).await
        }));
    }

    for restore_task in restore_tasks {
        match restore_task.await? {
            Ok(()) => {}
            Err(e) => {
                if cancel.is_cancelled() {
                    anyhow::bail!("Migration cancelled by user");
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

pub async fn phase_verify_all(
    config: &Config,
    db_names: &[String],
    mp: Arc<MultiProgress>,
) -> anyhow::Result<()> {
    for db in db_names {
        let dst_path = verification::dst_counts_path(db);

        if !dst_path.exists() {
            let counts = verification::stat_counts(
                &config.to_host,
                &config.to_port,
                &config.to_pass,
                &config.to_user,
                db,
            )
            .await?;

            let content = serde_json::to_string(&counts)?;
            fs::write(&dst_path, content)?;
        }
        verification::verify_db(config, db, mp.clone()).await?;
    }
    Ok(())
}

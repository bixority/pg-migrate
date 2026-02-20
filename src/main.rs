use anyhow::Result;
use clap::Parser;
use indicatif::{MultiProgress, ProgressDrawTarget};
use log::info;
use pg_migrate::{
    Config, create_dbs, discover_databases, done_marker, enable_fast_restore, migrate_db,
    migrate_globals, restore_safe_settings, state_dir, verify_all, verify_dir,
};
use std::{fs, sync::Arc};
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "localhost")]
    from_host: String,
    #[arg(long, default_value = "5432")]
    from_port: String,
    #[arg(long, default_value = "postgres")]
    from_user: String,
    #[arg(long, default_value = "oldpass")]
    from_pass: String,
    #[arg(long, default_value = "postgres")]
    from_db: String,

    #[arg(long, default_value = "localhost")]
    to_host: String,
    #[arg(long, default_value = "5432")]
    to_port: String,
    #[arg(long, default_value = "postgres")]
    to_user: String,
    #[arg(long, default_value = "newpass")]
    to_pass: String,
    #[arg(long, default_value = "postgres")]
    to_db: String,

    #[arg(long, default_value_t = 24)]
    dump_jobs: usize,
    #[arg(long, default_value_t = 12)]
    restore_jobs: usize,
    #[arg(short = 'p', long, default_value_t = 6)]
    max_parallel: usize,
    #[arg(long, default_value = "pg_dumps")]
    dump_root: String,
    #[arg(long, default_value_t = true)]
    migrate_globals: bool,
    #[arg(long, default_value_t = false)]
    disable_dst_optimizations: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();

    let mp = Arc::new(MultiProgress::with_draw_target(
        ProgressDrawTarget::stderr_with_hz(10),
    ));

    indicatif_log_bridge::LogWrapper::new((*mp).clone(), logger)
        .try_init()
        .expect("failed to init log wrapper");

    let config = Arc::new(Config {
        from_host: args.from_host,
        from_port: args.from_port,
        from_user: args.from_user,
        from_pass: args.from_pass,
        from_db: args.from_db,
        to_host: args.to_host,
        to_port: args.to_port,
        to_user: args.to_user,
        to_pass: args.to_pass,
        to_db: args.to_db,
        dump_jobs: args.dump_jobs,
        restore_jobs: args.restore_jobs,
        max_parallel: args.max_parallel,
        dump_root: args.dump_root.into(),
        migrate_globals: args.migrate_globals,
        disable_dst_optimizations: args.disable_dst_optimizations,
    });

    fs::create_dir_all(state_dir())?;
    fs::create_dir_all(verify_dir())?;

    let dbs_with_sizes = discover_databases(&config).await?;
    let db_names: Vec<String> = dbs_with_sizes.iter().map(|(n, _)| n.clone()).collect();
    info!("Databases: {db_names:?}");

    if dbs_with_sizes.is_empty() {
        info!("No databases found to migrate.");
        return Ok(());
    }

    if !config.disable_dst_optimizations {
        enable_fast_restore(&config).await?;
    }

    if config.migrate_globals {
        migrate_globals(&config).await?;
    }

    create_dbs(&config, &db_names).await?;

    let sem = Arc::new(Semaphore::new(config.max_parallel));

    let mut tasks = vec![];

    for (db, size) in dbs_with_sizes {
        if done_marker(&db).exists() {
            info!("Skipping {db}");
            continue;
        }

        let permit = sem.clone().acquire_owned().await?;
        let mp = mp.clone();
        let config = config.clone();

        tasks.push(tokio::spawn(async move {
            let _p = permit;
            migrate_db(&config, &db, size, mp).await
        }));
    }

    for t in tasks {
        t.await??;
    }

    verify_all(&config, &db_names).await?;

    if !config.disable_dst_optimizations {
        restore_safe_settings(&config).await?;
    }

    info!("Migration complete.");
    Ok(())
}

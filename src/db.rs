use crate::Config;
use crate::tui::{migration_style, render_verification_report};
use crate::{state_dir, verify_dir};
use anyhow::{Context, Result};
use indicatif::{HumanBytes, MultiProgress, ProgressBar};
use log::{info, warn};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{
    collections::BTreeMap,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::process::Command;
use tokio::select;
use tokio_util::sync::CancellationToken;

pub fn dump_dir(root: &Path, db: &str) -> PathBuf {
    root.join(db)
}

pub async fn pg_pool(host: &str, port: &str, user: &str, pass: &str, db: &str) -> Result<PgPool> {
    let url = format!("postgres://{user}:{pass}@{host}:{port}/{db}");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    Ok(pool)
}

pub async fn discover_databases(config: &Config) -> Result<Vec<(String, u64)>> {
    let pool = pg_pool(
        &config.from_host,
        &config.from_port,
        &config.from_user,
        &config.from_pass,
        &config.from_db,
    )
    .await?;

    let rows = sqlx::query(
        "SELECT datname, pg_database_size(datname) AS size \
         FROM pg_database \
         WHERE datname NOT IN ('postgres','template0','template1') \
         AND datallowconn IS TRUE \
         ORDER BY pg_database_size(datname) ASC;",
    )
    .fetch_all(&pool)
    .await?;

    let mut dbs = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let size: i64 = row.get(1);
        dbs.push((name, size.max(0).try_into().unwrap_or(0)));
    }
    Ok(dbs)
}

pub async fn migrate_db(
    config: &Config,
    db: &str,
    size: u64,
    mp: Arc<MultiProgress>,
    cancel: CancellationToken, // <-- add this
) -> Result<()> {
    let pb = mp.add(ProgressBar::new(0));
    pb.set_style(migration_style()?);
    pb.enable_steady_tick(Duration::from_secs(1));

    let mut bar_total = size.saturating_mul(2);
    if bar_total == 0 {
        bar_total = 100;
    }
    let phase_mid = bar_total / 2;
    let phase_end = bar_total;

    pb.set_length(bar_total);
    pb.set_message(format!("Dumping {db} ({})", HumanBytes(size)));

    let dump_path = dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        pb.set_message(format!("Dumping {db}"));

        let mut child = Command::new("pg_dump")
            .env("PGPASSWORD", &config.from_pass)
            .args([
                "-h",
                &config.from_host,
                "-p",
                &config.from_port,
                "-U",
                &config.from_user,
                "-Fd",
                "-j",
                &config.dump_jobs.to_string(),
                "-Z",
                "zstd:5",
                "-f",
                dump_path.to_str().expect("invalid dump path"),
                db,
            ])
            .spawn() // spawn, don't block
            .context("pg_dump failed to start")?;

        let status = select! {
            res = child.wait() => res.context("pg_dump wait failed")?,
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                anyhow::bail!("cancelled during pg_dump of {db}");
            }
        };

        if !status.success() {
            anyhow::bail!("pg_dump failed for {db}");
        }
    }

    pb.set_position(phase_mid);
    pb.set_message(format!("Restoring {db} ({})", HumanBytes(size)));

    let mut child = Command::new("pg_restore")
        .env("PGPASSWORD", &config.to_pass)
        .args([
            "-h",
            &config.to_host,
            "-p",
            &config.to_port,
            "-U",
            &config.to_user,
            "-j",
            &config.restore_jobs.to_string(),
            "--disable-triggers",
            "-d",
            db,
            dump_path.to_str().expect("invalid dump path"),
        ])
        .spawn()
        .context("pg_restore failed to start")?;

    let status = select! {
        res = child.wait() => res.context("pg_restore wait failed")?,
        () = cancel.cancelled() => {
            let _ = child.kill().await;
            anyhow::bail!("cancelled during pg_restore of {db}");
        }
    };

    if !status.success() {
        anyhow::bail!("pg_restore failed for {db}");
    }

    pb.set_position(phase_end);
    pb.finish_with_message(format!("{db} complete"));
    fs::write(done_marker(db), "")?;
    Ok(())
}

pub async fn verify_all(config: &Config, dbs: &[String]) -> Result<()> {
    for db in dbs {
        if verify_marker(db).exists() {
            continue;
        }
        verify_db(config, db).await?;
    }
    Ok(())
}

pub async fn verify_db(config: &Config, db: &str) -> Result<()> {
    let src_counts_str = stat_counts(
        &config.from_host,
        &config.from_port,
        &config.from_pass,
        &config.from_user,
        db,
    )
    .await?;
    let dst_counts_str = stat_counts(
        &config.to_host,
        &config.to_port,
        &config.to_pass,
        &config.to_user,
        db,
    )
    .await?;

    let src_map = parse_counts(&src_counts_str);
    let dst_map = parse_counts(&dst_counts_str);

    let (output, mismatch) = render_verification_report(db, &src_map, &dst_map);

    if mismatch {
        info!("{output}");
        anyhow::bail!("Verification failed for {db}: tables or row counts mismatch");
    }

    info!("{output}");
    info!("Verified {db}: {} tables, all rows match", src_map.len());
    fs::write(verify_marker(db), "")?;
    Ok(())
}

fn parse_counts(counts_str: &str) -> BTreeMap<String, String> {
    counts_str
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| {
            let parts: Vec<&str> = l.split(':').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
        .collect()
}

pub async fn stat_counts(
    host: &str,
    port: &str,
    pass: &str,
    user: &str,
    db: &str,
) -> Result<String> {
    let pool = pg_pool(host, port, user, pass, db).await?;
    let rows = sqlx::query(
        "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY 1, 2",
    )
    .fetch_all(&pool)
    .await?;

    let mut out = String::new();
    for row in rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let n: i64 = row.get(2);
        let _ = writeln!(out, "{}.{}:{}", schema, table, n.max(0));
    }
    Ok(out)
}

pub async fn enable_fast_restore(config: &Config) -> Result<()> {
    let settings = [
        ("fsync", "off"),
        ("synchronous_commit", "off"),
        ("full_page_writes", "off"),
        ("maintenance_work_mem", "'2GB'"),
        ("checkpoint_completion_target", "0.9"),
    ];

    let pool = pg_pool(
        &config.to_host,
        &config.to_port,
        &config.to_user,
        &config.to_pass,
        &config.to_db,
    )
    .await?;

    for (k, v) in settings {
        let sql = format!("ALTER SYSTEM SET {k} TO {v};");
        sqlx::query(&sql).execute(&pool).await?;
    }

    sqlx::query("SELECT pg_reload_conf();")
        .execute(&pool)
        .await?;
    Ok(())
}

pub async fn restore_safe_settings(config: &Config) -> Result<()> {
    let settings = ["fsync", "synchronous_commit", "full_page_writes"];

    let pool = pg_pool(
        &config.to_host,
        &config.to_port,
        &config.to_user,
        &config.to_pass,
        &config.to_db,
    )
    .await?;

    for s in settings {
        let sql = format!("ALTER SYSTEM RESET {s};");
        sqlx::query(&sql).execute(&pool).await?;
    }
    sqlx::query("SELECT pg_reload_conf();")
        .execute(&pool)
        .await?;
    Ok(())
}

pub async fn create_dbs(config: &Config, dbs: &[String]) -> Result<()> {
    let pool = pg_pool(
        &config.to_host,
        &config.to_port,
        &config.to_user,
        &config.to_pass,
        &config.to_db,
    )
    .await?;

    for db in dbs {
        let sql = format!("CREATE DATABASE \"{db}\"");
        if let Err(e) = sqlx::query(&sql).execute(&pool).await {
            warn!("Warning: CREATE DATABASE \"{db}\" failed or already exists: {e}");
        }
    }
    Ok(())
}

pub fn done_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.done"))
}

pub fn verify_marker(db: &str) -> PathBuf {
    verify_dir().join(format!("{db}.ok"))
}

pub fn globals_marker() -> PathBuf {
    state_dir().join("globals.done")
}

pub async fn migrate_globals(config: &Config) -> Result<()> {
    if globals_marker().exists() {
        return Ok(());
    }

    info!("Migrating global objects...");

    let globals_path = config.dump_root.join("globals.sql");
    fs::create_dir_all(&config.dump_root)?;

    let status = Command::new("pg_dumpall")
        .env("PGPASSWORD", &config.from_pass)
        .args([
            "-h",
            &config.from_host,
            "-p",
            &config.from_port,
            "-U",
            &config.from_user,
            "--globals-only",
            "-f",
            globals_path.to_str().expect("invalid globals path"),
        ])
        .status()
        .await
        .context("pg_dumpall --globals-only failed")?;

    if !status.success() {
        anyhow::bail!("pg_dumpall failed");
    }

    let globals_content = fs::read_to_string(&globals_path)?;
    let mut filtered_content = Vec::new();
    for line in globals_content.lines() {
        if (line.starts_with("CREATE ROLE ") || line.starts_with("ALTER ROLE "))
            && line.contains(&format!(" {} ", config.to_user))
        {
            info!(
                "Skipping migration of role '{}' to avoid password overwrite.",
                config.to_user
            );
            continue;
        }
        if (line.starts_with("CREATE ROLE ") || line.starts_with("ALTER ROLE "))
            && line.ends_with(&format!(" {};", config.to_user))
        {
            info!(
                "Skipping migration of role '{}' to avoid password overwrite.",
                config.to_user
            );
            continue;
        }

        filtered_content.push(line);
    }
    fs::write(&globals_path, filtered_content.join("\n"))?;

    let pool = pg_pool(
        &config.to_host,
        &config.to_port,
        &config.to_user,
        &config.to_pass,
        &config.to_db,
    )
    .await?;

    let sql = fs::read_to_string(&globals_path)?;
    for stmt in sql.split(";\n") {
        let s = stmt.trim();
        if s.is_empty() {
            continue;
        }
        let exec_sql = format!("{s};");
        if let Err(e) = sqlx::query(&exec_sql).execute(&pool).await {
            let msg = format!("{e}");
            if msg.contains("already exists")
                || msg.contains("MD5-encrypted password")
                || msg.contains("MD5 password support is deprecated")
            {
                continue;
            }
            warn!("Warning: executing globals statement failed: {msg}");
        }
    }

    fs::write(globals_marker(), "")?;
    Ok(())
}

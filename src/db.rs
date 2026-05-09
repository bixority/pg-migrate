use crate::Config;
use crate::state_dir;
use anyhow::{Context, Result};
use indicatif::HumanBytes;
use log::{info, warn};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::process::Stdio;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::select;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub enum MigrationPhase {
    Pending,
    Dumping,
    SourceCounts,
    Restoring,
    DestinationCounts,
    Verifying,
    DelayedDumping,
    DelayedRestoring,
    DelayedVerifying,
    Complete,
    Skipped,
    Failed,
}

impl MigrationPhase {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Dumping => "dumping",
            Self::SourceCounts => "source counts",
            Self::Restoring => "restoring",
            Self::DestinationCounts => "dest counts",
            Self::Verifying => "verifying",
            Self::DelayedDumping => "delayed dumping",
            Self::DelayedRestoring => "delayed restoring",
            Self::DelayedVerifying => "delayed verifying",
            Self::Complete => "complete",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct MigrationState {
    pub db: String,
    pub size: u64,
    pub phase: MigrationPhase,
    pub step: u8,
    pub total_steps: u8,
    pub display: String,
    pub error: Option<String>,
}

impl MigrationState {
    #[must_use]
    pub fn new(db: impl Into<String>, size: u64) -> Self {
        let db = db.into();

        Self {
            display: "waiting".to_string(),
            db,
            size,
            phase: MigrationPhase::Pending,
            step: 0,
            total_steps: 6,
            error: None,
        }
    }

    pub fn set_phase(&mut self, phase: MigrationPhase, step: u8, display: impl Into<String>) {
        self.phase = phase;
        self.step = step;
        self.display = display.into();
    }

    pub fn fail(&mut self, error: impl Into<String>) {
        let error = error.into();

        self.phase = MigrationPhase::Failed;
        self.display.clone_from(&error);
        self.error = Some(error);
    }

    #[must_use]
    pub fn percent(&self) -> u8 {
        if self.total_steps == 0 {
            return 0;
        }

        let percent = u16::from(self.step).saturating_mul(100) / u16::from(self.total_steps);
        percent.min(100) as u8
    }
}

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

pub async fn discover_databases(
    config: &Config,
    cancel: CancellationToken,
) -> Result<Vec<(String, u64)>> {
    let pool = select! {
        res = pg_pool(
            &config.from_host,
            &config.from_port,
            &config.from_user,
            &config.from_pass,
            &config.from_db,
        ) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during database connection"),
    };

    let rows = select! {
        res = sqlx::query(
            "SELECT datname, pg_database_size(datname) AS size \
             FROM pg_database \
             WHERE datname NOT IN ('postgres','template0','template1') \
             AND datallowconn IS TRUE \
             ORDER BY pg_database_size(datname) ASC;",
        )
        .fetch_all(&pool) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during database discovery"),
    };

    let mut dbs = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let size: i64 = row.get(1);
        dbs.push((name, size.max(0).try_into().unwrap_or(0)));
    }
    Ok(dbs)
}

pub fn dump_done_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.dumped"))
}

pub fn delayed_dump_done_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.delayed_dumped"))
}

pub fn delayed_done_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.delayed_done"))
}

pub async fn dump_db(
    config: &Config,
    db: &str,
    size: u64,
    cancel: CancellationToken,
) -> Result<()> {
    let human_size = HumanBytes(size);

    let dump_path = dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        let mut command = Command::new("pg_dump");
        command.env("PGPASSWORD", &config.from_pass).args([
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
        ]);

        let db_prefix = format!("{db}.");
        for delay in &config.delay_table_data {
            if let Some(table_pattern) = delay.strip_prefix(&db_prefix) {
                command.arg(format!("--exclude-table-data={table_pattern}"));
            }
        }

        let mut child = command
            .arg(db)
            .stderr(Stdio::piped())
            .spawn()
            .context("pg_dump failed to start")?;

        let stderr = child.stderr.take();

        let status = select! {
            res = child.wait() => res.context("pg_dump wait failed")?,
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                anyhow::bail!("cancelled during pg_dump of {db}");
            }
        };

        let mut err_output = String::new();
        if let Some(mut stderr) = stderr {
            let _ = stderr.read_to_string(&mut err_output).await;
        }

        if !status.success() {
            anyhow::bail!("pg_dump failed for {db}: {}", err_output.trim());
        }
    }

    info!("Dumped {db} ({human_size})");
    fs::write(dump_done_marker(db), "")?;
    Ok(())
}

pub async fn restore_db(
    config: &Config,
    db: &str,
    size: u64,
    cancel: CancellationToken,
) -> Result<()> {
    let human_size = HumanBytes(size);

    let dump_path = dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        anyhow::bail!("Dump not found for {db} at {}", dump_path.display());
    }

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
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("pg_restore failed to start")?;

    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();

    let status = select! {
        res = child.wait() => res.context("pg_restore wait failed")?,
        () = cancel.cancelled() => {
            let _ = child.kill().await;
            anyhow::bail!("cancelled during pg_restore of {db}");
        }
    };

    let mut stdout_output = String::new();
    if let Some(mut stdout) = stdout.take() {
        let _ = stdout.read_to_string(&mut stdout_output).await;
    }

    let mut stderr_output = String::new();
    if let Some(mut stderr) = stderr.take() {
        let _ = stderr.read_to_string(&mut stderr_output).await;
    }

    if !status.success() {
        anyhow::bail!(
            "pg_restore failed for {db} with status {status}\nstdout:\n{}\nstderr:\n{}",
            stdout_output.trim(),
            stderr_output.trim(),
        );
    }

    info!("Restored {db} ({human_size})");
    fs::write(done_marker(db), "")?;
    Ok(())
}

pub async fn dump_data(config: &Config, db: &str, cancel: CancellationToken) -> Result<()> {
    let dump_path = dump_dir(&config.dump_root, db).join("delayed");
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        let mut command = Command::new("pg_dump");
        command.env("PGPASSWORD", &config.from_pass).args([
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
            "--data-only",
            "-f",
            dump_path.to_str().expect("invalid dump path"),
        ]);

        let db_prefix = format!("{db}.");
        let mut has_delayed = false;
        for delay in &config.delay_table_data {
            if let Some(table_pattern) = delay.strip_prefix(&db_prefix) {
                command.arg(format!("--table={table_pattern}"));
                has_delayed = true;
            }
        }

        if !has_delayed {
            return Ok(());
        }

        let mut child = command
            .arg(db)
            .stderr(Stdio::piped())
            .spawn()
            .context("pg_dump (delayed) failed to start")?;

        let stderr = child.stderr.take();

        let status = select! {
            res = child.wait() => res.context("pg_dump (delayed) wait failed")?,
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                anyhow::bail!("cancelled during pg_dump (delayed) of {db}");
            }
        };

        let mut err_output = String::new();
        if let Some(mut stderr) = stderr {
            let _ = stderr.read_to_string(&mut err_output).await;
        }

        if !status.success() {
            anyhow::bail!("pg_dump (delayed) failed for {db}: {}", err_output.trim());
        }
    }

    info!("Dumped delayed data for {db}");
    fs::write(delayed_dump_done_marker(db), "")?;
    Ok(())
}

pub async fn restore_delayed_data(
    config: &Config,
    db: &str,
    cancel: CancellationToken,
) -> Result<()> {
    let dump_path = dump_dir(&config.dump_root, db).join("delayed");

    if !dump_path.join("toc.dat").exists() {
        return Ok(());
    }

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
            "--data-only",
            "-d",
            db,
            dump_path.to_str().expect("invalid dump path"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("pg_restore (delayed) failed to start")?;

    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();

    let status = select! {
        res = child.wait() => res.context("pg_restore (delayed) wait failed")?,
        () = cancel.cancelled() => {
            let _ = child.kill().await;
            anyhow::bail!("cancelled during pg_restore (delayed) of {db}");
        }
    };

    let mut stdout_output = String::new();
    if let Some(mut stdout) = stdout.take() {
        let _ = stdout.read_to_string(&mut stdout_output).await;
    }

    let mut stderr_output = String::new();
    if let Some(mut stderr) = stderr.take() {
        let _ = stderr.read_to_string(&mut stderr_output).await;
    }

    if !status.success() {
        anyhow::bail!(
            "pg_restore (delayed) failed for {db} with status {status}\nstdout:\n{}\nstderr:\n{}",
            stdout_output.trim(),
            stderr_output.trim(),
        );
    }

    info!("Restored delayed data for {db}");
    fs::write(delayed_done_marker(db), "")?;
    Ok(())
}

pub async fn enable_fast_restore(config: &Config, cancel: CancellationToken) -> Result<()> {
    let settings = [
        ("fsync", "off"),
        ("synchronous_commit", "off"),
        ("full_page_writes", "off"),
        ("maintenance_work_mem", "'2GB'"),
        ("checkpoint_completion_target", "0.9"),
    ];

    let pool = select! {
        res = pg_pool(
            &config.to_host,
            &config.to_port,
            &config.to_user,
            &config.to_pass,
            &config.to_db,
        ) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during database connection"),
    };

    for (k, v) in settings {
        let sql = format!("ALTER SYSTEM SET {k} TO {v};");
        select! {
            res = sqlx::query(&sql).execute(&pool) => res?,
            () = cancel.cancelled() => anyhow::bail!("cancelled during fast restore enablement"),
        };
    }

    select! {
        res = sqlx::query("SELECT pg_reload_conf();").execute(&pool) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during fast restore enablement (reload)"),
    };
    Ok(())
}

pub async fn restore_safe_settings(config: &Config, cancel: CancellationToken) -> Result<()> {
    let settings = ["fsync", "synchronous_commit", "full_page_writes"];

    let pool = select! {
        res = pg_pool(
            &config.to_host,
            &config.to_port,
            &config.to_user,
            &config.to_pass,
            &config.to_db,
        ) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during database connection"),
    };

    for s in settings {
        let sql = format!("ALTER SYSTEM RESET {s};");
        select! {
            res = sqlx::query(&sql).execute(&pool) => res?,
            () = cancel.cancelled() => anyhow::bail!("cancelled during safe settings restoration"),
        };
    }
    select! {
        res = sqlx::query("SELECT pg_reload_conf();").execute(&pool) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during safe settings restoration (reload)"),
    };
    Ok(())
}

pub async fn create_dbs(config: &Config, dbs: &[String], cancel: CancellationToken) -> Result<()> {
    let pool = select! {
        res = pg_pool(
            &config.to_host,
            &config.to_port,
            &config.to_user,
            &config.to_pass,
            &config.to_db,
        ) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during database connection"),
    };

    for db in dbs {
        let sql = format!("CREATE DATABASE \"{db}\"");
        select! {
            res = sqlx::query(&sql).execute(&pool) => {
                if let Err(e) = res {
                    warn!("Warning: CREATE DATABASE \"{db}\" failed or already exists: {e}");
                }
            }
            () = cancel.cancelled() => anyhow::bail!("cancelled during database creation of {db}"),
        }
    }
    Ok(())
}

pub fn done_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.done"))
}

pub fn globals_marker() -> PathBuf {
    state_dir().join("globals.done")
}

pub async fn migrate_globals(config: &Config, cancel: CancellationToken) -> Result<()> {
    if globals_marker().exists() {
        return Ok(());
    }

    info!("Migrating global objects...");

    let globals_path = config.dump_root.join("globals.sql");
    fs::create_dir_all(&config.dump_root)?;

    let mut child = Command::new("pg_dumpall")
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
        .spawn()
        .context("pg_dumpall --globals-only failed to start")?;

    let status = select! {
        res = child.wait() => res.context("pg_dumpall wait failed")?,
        () = cancel.cancelled() => {
            let _ = child.kill().await;
            anyhow::bail!("cancelled during pg_dumpall --globals-only");
        }
    };

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

    let pool = select! {
        res = pg_pool(
            &config.to_host,
            &config.to_port,
            &config.to_user,
            &config.to_pass,
            &config.to_db,
        ) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during database connection"),
    };

    let sql = fs::read_to_string(&globals_path)?;
    for stmt in sql.split(";\n") {
        let s = stmt.trim();
        if s.is_empty() {
            continue;
        }
        let exec_sql = format!("{s};");

        let res = select! {
            res = sqlx::query(&exec_sql).execute(&pool) => res,
            () = cancel.cancelled() => anyhow::bail!("cancelled during globals migration execution"),
        };

        if let Err(e) = res {
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

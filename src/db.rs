use crate::Config;
use crate::state_dir;
use crate::verification::is_delayed_table;
use anyhow::{Context, Result};
use indicatif::HumanBytes;
use log::{info, warn};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::select;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct DbArgs {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
}

#[derive(Clone, Debug)]
pub enum MigrationPhase {
    Pending,
    Dumping,
    SourceCounts,
    Restoring,
    DestinationCounts,
    Verifying,
    DelayedDumping,
    DelayedDroppingIndexes,
    DelayedRestoring,
    DelayedRecreatingIndexes,
    DelayedVerifying,
    Complete,
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
            Self::DelayedDroppingIndexes => "dropping indexes",
            Self::DelayedRestoring => "delayed restoring",
            Self::DelayedRecreatingIndexes => "recreating indexes",
            Self::DelayedVerifying => "delayed verifying",
            Self::Complete => "complete",
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
    pub regular_completed_at: Option<Instant>,
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
            regular_completed_at: None,
        }
    }

    pub fn set_phase(&mut self, phase: MigrationPhase, step: u8, display: impl Into<String>) {
        self.phase = phase;
        self.step = step;
        self.display = display.into();
    }

    pub fn mark_regular_done(&mut self) {
        self.regular_completed_at = Some(Instant::now());
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

type PoolKey = (String, u16, String, String);

#[derive(Clone)]
pub struct PoolCache {
    inner: Arc<AsyncMutex<HashMap<PoolKey, PgPool>>>,
    max_connections: u32,
}

impl PoolCache {
    #[must_use]
    pub fn new(max_connections: u32) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(HashMap::new())),
            max_connections,
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    pub async fn get(&self, args: &DbArgs, db: &str) -> Result<PgPool> {
        let key = (
            args.host.clone(),
            args.port,
            args.user.clone(),
            db.to_string(),
        );
        let mut guard = self.inner.lock().await;
        if let Some(p) = guard.get(&key) {
            return Ok(p.clone());
        }
        let opts = PgConnectOptions::new()
            .host(&args.host)
            .port(args.port)
            .username(&args.user)
            .password(&args.pass)
            .database(db);
        let pool = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(0)
            .idle_timeout(Some(Duration::from_secs(2)))
            .acquire_timeout(Duration::from_mins(1))
            .connect_with(opts)
            .await?;
        guard.insert(key, pool.clone());
        Ok(pool)
    }
}

pub async fn discover_databases(
    config: &Config,
    cancel: CancellationToken,
) -> Result<Vec<(String, u64)>> {
    let pool = select! {
        res = config.pool_cache.get(&config.source, &config.source_db) => res?,
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
        let port = config.source.port.to_string();
        let mut command = Command::new("pg_dump");
        command.env("PGPASSWORD", &config.source.pass).args([
            "-h",
            &config.source.host,
            "-p",
            &port,
            "-U",
            &config.source.user,
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
    let marker = done_marker(db);
    if marker.exists() {
        info!("Skipping restore for {db} (already done)");
        return Ok(());
    }

    let human_size = HumanBytes(size);

    let dump_path = dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        anyhow::bail!("Dump not found for {db} at {}", dump_path.display());
    }

    let port = config.destination.port.to_string();
    let mut child = Command::new("pg_restore")
        .env("PGPASSWORD", &config.destination.pass)
        .args([
            "-h",
            &config.destination.host,
            "-p",
            &port,
            "-U",
            &config.destination.user,
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

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let wait_fut = async {
        select! {
            res = child.wait() => res.context("pg_restore wait failed"),
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                anyhow::bail!("cancelled during pg_restore of {db}")
            }
        }
    };

    let read_stdout = async {
        let mut buf = String::new();
        if let Some(mut s) = stdout {
            let _ = s.read_to_string(&mut buf).await;
        }
        buf
    };

    let read_stderr = async {
        let mut buf = String::new();
        if let Some(mut s) = stderr {
            let _ = s.read_to_string(&mut buf).await;
        }
        buf
    };

    let (status_res, stdout_output, stderr_output) =
        tokio::join!(wait_fut, read_stdout, read_stderr);
    let status = status_res?;

    if !status.success() {
        anyhow::bail!(
            "pg_restore failed for {db} with status {status}\nstdout:\n{}\nstderr:\n{}",
            stdout_output.trim(),
            stderr_output.trim(),
        );
    }

    info!("Restored {db} ({human_size})");
    fs::write(&marker, "")?;
    Ok(())
}

pub async fn dump_data(config: &Config, db: &str, cancel: CancellationToken) -> Result<()> {
    let dump_path = dump_dir(&config.dump_root, db).join("delayed");
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        let port = config.source.port.to_string();
        let mut command = Command::new("pg_dump");
        command.env("PGPASSWORD", &config.source.pass).args([
            "-h",
            &config.source.host,
            "-p",
            &port,
            "-U",
            &config.source.user,
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

    let port = config.destination.port.to_string();
    let mut child = Command::new("pg_restore")
        .env("PGPASSWORD", &config.destination.pass)
        .args([
            "-h",
            &config.destination.host,
            "-p",
            &port,
            "-U",
            &config.destination.user,
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

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let wait_fut = async {
        select! {
            res = child.wait() => res.context("pg_restore (delayed) wait failed"),
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                anyhow::bail!("cancelled during pg_restore (delayed) of {db}")
            }
        }
    };

    let read_stdout = async {
        let mut buf = String::new();
        if let Some(mut s) = stdout {
            let _ = s.read_to_string(&mut buf).await;
        }
        buf
    };

    let read_stderr = async {
        let mut buf = String::new();
        if let Some(mut s) = stderr {
            let _ = s.read_to_string(&mut buf).await;
        }
        buf
    };

    let (status_res, stdout_output, stderr_output) =
        tokio::join!(wait_fut, read_stdout, read_stderr);
    let status = status_res?;

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

#[derive(Clone, Debug)]
struct DelayedIndex {
    schema: String,
    name: String,
    ddl: String,
}

async fn collect_delayed_indexes(
    config: &Config,
    db_name: &str,
    cancel: &CancellationToken,
) -> Result<Vec<DelayedIndex>> {
    let src_pool = select! {
        res = config.pool_cache.get(&config.source, db_name) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during source connection for {db_name}"),
    };

    let table_rows = select! {
        res = sqlx::query("SELECT schemaname, tablename FROM pg_tables").fetch_all(&src_pool) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during delayed table discovery for {db_name}"),
    };

    let mut indexes = Vec::new();
    for row in table_rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);

        if !is_delayed_table(db_name, &schema, &table, &config.delay_table_data) {
            continue;
        }

        let idx_rows = select! {
            res = sqlx::query(
                "SELECT i.relname, pg_get_indexdef(i.oid) \
                 FROM pg_index x \
                 JOIN pg_class i ON i.oid = x.indexrelid \
                 JOIN pg_class t ON t.oid = x.indrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 WHERE n.nspname = $1 AND t.relname = $2 \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM pg_constraint c WHERE c.conindid = x.indexrelid \
                   )"
            )
            .bind(&schema)
            .bind(&table)
            .fetch_all(&src_pool) => res?,
            () = cancel.cancelled() => anyhow::bail!("cancelled during index discovery for {schema}.{table}"),
        };

        for row in idx_rows {
            let name: String = row.get(0);
            let ddl: String = row.get(1);
            indexes.push(DelayedIndex {
                schema: schema.clone(),
                name,
                ddl,
            });
        }
    }

    Ok(indexes)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub async fn drop_delayed_indexes(
    config: &Config,
    db_name: &str,
    cancel: CancellationToken,
) -> Result<()> {
    let marker = delayed_indexes_dropped_marker(db_name);
    if marker.exists() {
        info!("Skipping delayed-index drop for {db_name} (already done)");
        return Ok(());
    }

    let indexes = collect_delayed_indexes(config, db_name, &cancel).await?;

    if indexes.is_empty() {
        info!("No delayed-table secondary indexes for {db_name}");
        fs::write(&marker, "")?;
        return Ok(());
    }

    let dst_pool = select! {
        res = config.pool_cache.get(&config.destination, db_name) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during destination connection for {db_name}"),
    };

    for idx in &indexes {
        let sql = format!(
            "DROP INDEX IF EXISTS {}.{}",
            quote_ident(&idx.schema),
            quote_ident(&idx.name),
        );
        info!("Dropping index {}.{} on {db_name}", idx.schema, idx.name);
        select! {
            res = sqlx::query(&sql).execute(&dst_pool) => res?,
            () = cancel.cancelled() => anyhow::bail!("cancelled during DROP INDEX of {}.{}", idx.schema, idx.name),
        };
    }

    fs::write(&marker, "")?;
    Ok(())
}

pub async fn recreate_delayed_indexes(
    config: &Config,
    db_name: &str,
    cancel: CancellationToken,
) -> Result<()> {
    let marker = delayed_indexes_recreated_marker(db_name);
    if marker.exists() {
        info!("Skipping delayed-index recreate for {db_name} (already done)");
        return Ok(());
    }

    let indexes = collect_delayed_indexes(config, db_name, &cancel).await?;

    if indexes.is_empty() {
        fs::write(&marker, "")?;
        return Ok(());
    }

    let dst_pool = select! {
        res = config.pool_cache.get(&config.destination, db_name) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during destination connection for {db_name}"),
    };

    let parallel = config.restore_jobs.max(1);
    let sem = Arc::new(Semaphore::new(parallel));
    let mut set: JoinSet<Result<()>> = JoinSet::new();

    for idx in indexes {
        let pool = dst_pool.clone();
        let sem = sem.clone();
        let cancel = cancel.clone();
        let db_name_owned = db_name.to_string();

        set.spawn(async move {
            let _permit = select! {
                res = sem.acquire_owned() => res?,
                () = cancel.cancelled() => anyhow::bail!("cancelled while waiting for index slot"),
            };

            let exists: bool = select! {
                res = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS ( \
                       SELECT 1 FROM pg_class c \
                       JOIN pg_namespace n ON n.oid = c.relnamespace \
                       WHERE c.relkind = 'i' AND c.relname = $1 AND n.nspname = $2 \
                     )"
                )
                .bind(&idx.name)
                .bind(&idx.schema)
                .fetch_one(&pool) => res?,
                () = cancel.cancelled() => anyhow::bail!("cancelled during index existence check for {}.{}", idx.schema, idx.name),
            };

            if exists {
                info!(
                    "Index {}.{} already exists on {db_name_owned}; skipping",
                    idx.schema, idx.name
                );
                return Ok(());
            }

            info!(
                "Recreating index {}.{} on {db_name_owned}",
                idx.schema, idx.name
            );
            select! {
                res = sqlx::query(&idx.ddl).execute(&pool) => { res?; }
                () = cancel.cancelled() => anyhow::bail!("cancelled during CREATE INDEX of {}.{}", idx.schema, idx.name),
            };
            Ok(())
        });
    }

    while let Some(res) = set.join_next().await {
        res??;
    }

    fs::write(&marker, "")?;
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
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
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
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
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
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
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

pub fn delayed_indexes_dropped_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.delayed_indexes_dropped"))
}

pub fn delayed_indexes_recreated_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.delayed_indexes_recreated"))
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

    let port = config.source.port.to_string();
    let mut child = Command::new("pg_dumpall")
        .env("PGPASSWORD", &config.source.pass)
        .args([
            "-h",
            &config.source.host,
            "-p",
            &port,
            "-U",
            &config.source.user,
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
            && line.contains(&format!(" {} ", config.destination.user))
        {
            info!(
                "Skipping migration of role '{}' to avoid password overwrite.",
                config.destination.user
            );
            continue;
        }
        if (line.starts_with("CREATE ROLE ") || line.starts_with("ALTER ROLE "))
            && line.ends_with(&format!(" {};", config.destination.user))
        {
            info!(
                "Skipping migration of role '{}' to avoid password overwrite.",
                config.destination.user
            );
            continue;
        }

        filtered_content.push(line);
    }
    fs::write(&globals_path, filtered_content.join("\n"))?;

    let pool = select! {
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
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

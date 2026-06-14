use crate::Config;
use crate::error::{Error, MigrationPhase, Result};
use crate::state_dir;
use indicatif::HumanBytes;
use log::{info, warn};
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
use tokio::sync::Mutex as AsyncMutex;
use tokio_postgres::config::SslMode;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct DbArgs {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
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

pub fn delayed_dump_dir(root: &Path, db: &str) -> PathBuf {
    root.join(format!("{db}_delayed"))
}

type PoolKey = (String, u16, String, String);

#[derive(Clone)]
pub struct PoolCache {
    inner: Arc<AsyncMutex<HashMap<PoolKey, Arc<tokio_postgres::Client>>>>,
    ssl_mode: SslMode,
}

impl PoolCache {
    #[must_use]
    pub fn new(ssl_mode: SslMode) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(HashMap::new())),
            ssl_mode,
        }
    }

    pub async fn get(&self, args: &DbArgs, db: &str) -> Result<Arc<tokio_postgres::Client>> {
        let key = (
            args.host.clone(),
            args.port,
            args.user.clone(),
            db.to_string(),
        );

        {
            let lock = self.inner.lock().await;
            if let Some(p) = lock.get(&key) {
                return Ok(p.clone());
            }
        }

        let mut config = tokio_postgres::Config::new();
        config
            .host(&args.host)
            .port(args.port)
            .user(&args.user)
            .password(&args.pass)
            .dbname(db)
            .ssl_mode(self.ssl_mode);

        let tls = crate::tls::make_tls();
        let (client, connection) =
            tokio::time::timeout(Duration::from_secs(30), config.connect(tls))
                .await
                .map_err(|_| Error::Timeout(format!("to {} database {}", args.host, db)))??;

        let db_name = db.to_string();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Database connection error for {db_name}: {e}");
            }
        });

        let client = Arc::new(client);

        self.inner.lock().await.insert(key, client.clone());

        Ok(client)
    }
}

pub async fn discover_databases(
    config: &Config,
    cancel: CancellationToken,
) -> Result<Vec<(String, u64)>> {
    let pool = select! {
        res = config.pool_cache.get(&config.source, &config.source_db) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database connection".to_string())),
    };

    let rows = select! {
        res = pool.query(
            "SELECT datname, pg_database_size(datname) AS size \
             FROM pg_database \
             WHERE datname NOT IN ('postgres','template0','template1') \
             AND datallowconn IS TRUE \
             ORDER BY pg_database_size(datname) ASC;",
             &[]
        ) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database discovery".to_string())),
    };

    let mut dbs = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let size: i64 = row.get(1);
        dbs.push((name, size.max(0).try_into().unwrap_or(0)));
    }

    dbs.sort_by_key(|&(_, size)| size);

    Ok(dbs)
}

pub fn dump_done_marker(db: &str) -> Result<PathBuf> {
    Ok(state_dir()?.join(format!("{db}.dumped")))
}

pub fn delayed_dump_done_marker(db: &str) -> Result<PathBuf> {
    Ok(state_dir()?.join(format!("{db}.delayed_dumped")))
}

pub fn delayed_done_marker(db: &str) -> Result<PathBuf> {
    Ok(state_dir()?.join(format!("{db}.delayed_done")))
}

pub fn copy_rule_done_marker(db: &str, table: &str, hash: u64) -> Result<PathBuf> {
    Ok(state_dir()?.join(format!("{db}.{table}.copy.{hash:x}.done")))
}

pub async fn dump_db(
    config: &Config,
    db: &str,
    size: u64,
    data_excludes: &[String],
    full_excludes: &[String],
    cancel: CancellationToken,
) -> Result<()> {
    let human_size = HumanBytes(size);

    let dump_path = dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        let port = config.source.port.to_string();
        let mut command = Command::new("pg_dump");
        let zstd_level = config.zstd_level;
        command.kill_on_drop(true);
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
            &format!("zstd:{zstd_level}"),
            "-f",
            dump_path
                .to_str()
                .ok_or_else(|| Error::InvalidPath(dump_path.display().to_string()))?,
        ]);

        for table_pattern in data_excludes {
            command.arg(format!("--exclude-table-data={table_pattern}"));
        }

        for table_pattern in full_excludes {
            command.arg(format!("--exclude-table={table_pattern}"));
        }

        let mut child = command
            .arg(db)
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::SpawnFailed {
                command: "pg_dump".to_string(),
                source: e,
            })?;

        let stderr = child.stderr.take();

        let status = select! {
            res = child.wait() => res?,
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                return Err(Error::Cancelled(format!("pg_dump of {db}")));
            }
        };

        let mut err_output = String::new();
        if let Some(mut stderr) = stderr {
            let _ = stderr.read_to_string(&mut err_output).await;
        }

        if !status.success() {
            return Err(Error::ProcessFailed {
                command: format!("pg_dump {db}"),
                stderr: err_output.trim().to_string(),
            });
        }
    }

    info!("Dumped {db} ({human_size})");
    fs::write(dump_done_marker(db)?, "")?;
    Ok(())
}

pub async fn restore_db(
    config: &Config,
    db: &str,
    size: u64,
    cancel: CancellationToken,
) -> Result<()> {
    let marker = done_marker(db)?;
    if marker.exists() {
        info!("Skipping restore for {db} (already done)");
        return Ok(());
    }

    let human_size = HumanBytes(size);

    let dump_path = dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        return Err(Error::DumpNotFound {
            database: db.to_string(),
            path: dump_path.display().to_string(),
        });
    }

    let port = config.destination.port.to_string();
    let mut child = Command::new("pg_restore")
        .kill_on_drop(true)
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
            dump_path
                .to_str()
                .ok_or_else(|| Error::InvalidPath(dump_path.display().to_string()))?,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::SpawnFailed {
            command: "pg_restore".to_string(),
            source: e,
        })?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let wait_fut = async {
        select! {
            res = child.wait() => res.map_err(Error::from),
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                Err(Error::Cancelled(format!("pg_restore of {db}")))
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
        return Err(Error::ProcessFailed {
            command: format!("pg_restore {db}"),
            stderr: format!(
                "status {status}\nstdout:\n{}\nstderr:\n{}",
                stdout_output.trim(),
                stderr_output.trim()
            ),
        });
    }

    info!("Restored {db} ({human_size})");
    fs::write(&marker, "")?;
    Ok(())
}

pub async fn dump_delayed_data(
    config: &Config,
    db: &str,
    tables: &[String],
    copy_excludes: &[String],
    cancel: CancellationToken,
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }

    let dump_path = delayed_dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        let port = config.source.port.to_string();
        let mut command = Command::new("pg_dump");
        let zstd_level = config.zstd_level;
        command.kill_on_drop(true);
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
            &format!("zstd:{zstd_level}"),
            "--data-only",
            "-f",
            dump_path
                .to_str()
                .ok_or_else(|| Error::InvalidPath(dump_path.display().to_string()))?,
        ]);

        for table_pattern in tables {
            command.arg(format!("--table={table_pattern}"));
        }

        for exclude in copy_excludes {
            command.arg(format!("--exclude-table={exclude}"));
        }

        let mut child = command
            .arg(db)
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::SpawnFailed {
                command: "pg_dump (delayed)".to_string(),
                source: e,
            })?;

        let stderr = child.stderr.take();

        let status = select! {
            res = child.wait() => res?,
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                return Err(Error::Cancelled(format!("pg_dump (delayed) of {db}")));
            }
        };

        let mut err_output = String::new();
        if let Some(mut stderr) = stderr {
            let _ = stderr.read_to_string(&mut err_output).await;
        }

        if !status.success() {
            return Err(Error::ProcessFailed {
                command: format!("pg_dump (delayed) {db}"),
                stderr: err_output.trim().to_string(),
            });
        }
    }

    info!("Dumped delayed data for {db}");
    fs::write(delayed_dump_done_marker(db)?, "")?;
    Ok(())
}

pub async fn restore_delayed_data(
    config: &Config,
    db: &str,
    has_delayed_tables: bool,
    cancel: CancellationToken,
) -> Result<()> {
    if !has_delayed_tables {
        return Ok(());
    }
    let dump_path = delayed_dump_dir(&config.dump_root, db);

    if !dump_path.join("toc.dat").exists() {
        return Ok(());
    }

    let port = config.destination.port.to_string();
    let mut child = Command::new("pg_restore")
        .kill_on_drop(true)
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
            dump_path
                .to_str()
                .ok_or_else(|| Error::InvalidPath(dump_path.display().to_string()))?,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::SpawnFailed {
            command: "pg_restore (delayed)".to_string(),
            source: e,
        })?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let wait_fut = async {
        select! {
            res = child.wait() => res.map_err(Error::from),
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                Err(Error::Cancelled(format!("pg_restore (delayed) of {db}")))
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
        return Err(Error::ProcessFailed {
            command: format!("pg_restore (delayed) {db}"),
            stderr: format!(
                "status {status}\nstdout:\n{}\nstderr:\n{}",
                stdout_output.trim(),
                stderr_output.trim()
            ),
        });
    }

    info!("Restored delayed data for {db}");
    fs::write(delayed_done_marker(db)?, "")?;
    Ok(())
}

pub async fn create_dbs(config: &Config, dbs: &[String], cancel: CancellationToken) -> Result<()> {
    let pool = select! {
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database connection".to_string())),
    };

    for db in dbs {
        let sql = format!("CREATE DATABASE \"{db}\"");
        select! {
            res = pool.execute(&sql, &[]) => {
                if let Err(e) = res {
                    warn!("Warning: CREATE DATABASE \"{db}\" failed or already exists: {e}");
                }
            }
            () = cancel.cancelled() => return Err(Error::Cancelled(format!("database creation of {db}"))),
        }
    }
    Ok(())
}

pub fn done_marker(db: &str) -> Result<PathBuf> {
    Ok(state_dir()?.join(format!("{db}.done")))
}

pub fn globals_marker() -> Result<PathBuf> {
    Ok(state_dir()?.join("globals.done"))
}

fn filter_globals_sql(content: &str, dest_user: &str) -> String {
    let mut filtered_content = Vec::new();
    let pattern1 = format!(" {dest_user} ");
    let pattern2 = format!(" {dest_user};");

    for line in content.lines() {
        if (line.starts_with("CREATE ROLE ") || line.starts_with("ALTER ROLE "))
            && (line.contains(&pattern1) || line.ends_with(&pattern2))
        {
            info!("Skipping migration of role '{dest_user}' to avoid password overwrite.");
            continue;
        }

        filtered_content.push(line);
    }
    filtered_content.join("\n")
}

pub async fn migrate_globals(config: &Config, cancel: CancellationToken) -> Result<()> {
    if globals_marker()?.exists() {
        return Ok(());
    }

    info!("Migrating global objects...");

    let globals_path = config.dump_root.join("globals.sql");
    fs::create_dir_all(&config.dump_root)?;

    let port = config.source.port.to_string();
    let mut child = Command::new("pg_dumpall")
        .kill_on_drop(true)
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
            globals_path
                .to_str()
                .ok_or_else(|| Error::InvalidPath(globals_path.display().to_string()))?,
        ])
        .spawn()
        .map_err(|e| Error::SpawnFailed {
            command: "pg_dumpall".to_string(),
            source: e,
        })?;

    let status = select! {
        res = child.wait() => res?,
        () = cancel.cancelled() => {
            let _ = child.kill().await;
            return Err(Error::Cancelled("pg_dumpall --globals-only".to_string()));
        }
    };

    if !status.success() {
        return Err(Error::ProcessFailed {
            command: "pg_dumpall".to_string(),
            stderr: "pg_dumpall failed".to_string(),
        });
    }

    let globals_content = fs::read_to_string(&globals_path)?;
    let filtered_content = filter_globals_sql(&globals_content, &config.destination.user);
    fs::write(&globals_path, filtered_content)?;

    let pool = select! {
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database connection".to_string())),
    };

    let sql = fs::read_to_string(&globals_path)?;
    for stmt in sql.split(";\n") {
        let s = stmt.trim();
        if s.is_empty() {
            continue;
        }
        let exec_sql = format!("{s};");

        let res = select! {
            res = pool.execute(&exec_sql, &[]) => res,
            () = cancel.cancelled() => return Err(Error::Cancelled("globals migration execution".to_string())),
        };

        if let Err(e) = res {
            // `tokio_postgres::Error`'s Display only yields "db error"; the real
            // server message, SQLSTATE and detail live in the underlying DbError.
            if let Some(db_err) = e.as_db_error() {
                let msg = db_err.message();
                if msg.contains("already exists")
                    || msg.contains("MD5-encrypted password")
                    || msg.contains("MD5 password support is deprecated")
                {
                    continue;
                }
                warn!(
                    "Warning: executing globals statement failed [{}]: {}{}{}\n  statement: {s}",
                    db_err.code().code(),
                    msg,
                    db_err
                        .detail()
                        .map(|d| format!(" (detail: {d})"))
                        .unwrap_or_default(),
                    db_err
                        .hint()
                        .map(|h| format!(" (hint: {h})"))
                        .unwrap_or_default(),
                );
            } else {
                warn!("Warning: executing globals statement failed: {e}\n  statement: {s}");
            }
        }
    }

    fs::write(globals_marker()?, "")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_globals_sql() {
        let sql = "\
CREATE ROLE postgres;
ALTER ROLE postgres WITH PASSWORD 'abc';
CREATE ROLE other;
ALTER ROLE other WITH PASSWORD 'xyz';
";
        let filtered = filter_globals_sql(sql, "postgres");
        assert!(!filtered.contains("CREATE ROLE postgres;"));
        assert!(!filtered.contains("ALTER ROLE postgres WITH PASSWORD"));
        assert!(filtered.contains("CREATE ROLE other;"));
        assert!(filtered.contains("ALTER ROLE other WITH PASSWORD"));
    }

    #[test]
    fn test_filter_globals_sql_with_spaces() {
        let sql = "CREATE ROLE postgres WITH LOGIN;";
        let filtered = filter_globals_sql(sql, "postgres");
        assert_eq!(filtered, "");
    }
}

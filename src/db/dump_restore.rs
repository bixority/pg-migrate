use crate::error::{Error, Result};
use crate::state_dir;
use crate::Config;
use indicatif::HumanBytes;
use log::info;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::select;
use tokio_util::sync::CancellationToken;

#[must_use]
pub fn dump_dir(root: &Path, db: &str) -> PathBuf {
    root.join(db)
}

#[must_use]
pub fn delayed_dump_dir(root: &Path, db: &str) -> PathBuf {
    root.join(format!("{db}_delayed"))
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

pub fn done_marker(db: &str) -> Result<PathBuf> {
    Ok(state_dir()?.join(format!("{db}.done")))
}

pub fn globals_marker() -> Result<PathBuf> {
    Ok(state_dir()?.join("globals.done"))
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
        command.env("PGPASSWORD", &*config.source.pass).args([
            "-h", &*config.source.host,
            "-p", &port,
            "-U", &*config.source.user,
            "-Fd",
            "-j", &config.dump_jobs.to_string(),
            "-Z", &format!("zstd:{zstd_level}"),
            "-f", dump_path.to_str().ok_or_else(|| Error::InvalidPath(dump_path.display().to_string().into()))?,
        ]);

        for table_pattern in data_excludes {
            command.arg(format!("--exclude-table-data={table_pattern}"));
        }
        for table_pattern in full_excludes {
            command.arg(format!("--exclude-table={table_pattern}"));
        }

        let mut child = command.arg(db).stderr(Stdio::piped()).spawn().map_err(|e| Error::SpawnFailed {
            command: "pg_dump".into(),
            source: e,
        })?;

        let stderr = child.stderr.take();
        let status = select! {
            res = child.wait() => res?,
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                return Err(Error::Cancelled(format!("pg_dump of {db}").into()));
            }
        };

        let mut err_output = String::new();
        if let Some(mut stderr) = stderr {
            let _ = stderr.read_to_string(&mut err_output).await;
        }

        if !status.success() {
            return Err(Error::ProcessFailed {
                command: format!("pg_dump {db}").into(),
                stderr: err_output.trim().to_string().into(),
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
            database: db.to_string().into(),
            path: dump_path.display().to_string().into(),
        });
    }

    let port = config.destination.port.to_string();
    let mut command = Command::new("pg_restore");
    command
        .kill_on_drop(true)
        .env("PGPASSWORD", &*config.destination.pass)
        .args([
            "-h", &*config.destination.host,
            "-p", &port,
            "-U", &*config.destination.user,
            "--disable-triggers",
            "-d", db,
            dump_path.to_str().ok_or_else(|| Error::InvalidPath(dump_path.display().to_string().into()))?,
        ]);

    if config.restore_single_transaction {
        command.arg("--single-transaction");
    } else {
        command.arg("-j").arg(config.restore_jobs.to_string());
    }

    let mut child = command.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e| Error::SpawnFailed {
        command: "pg_restore".into(),
        source: e,
    })?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let wait_fut = async {
        select! {
            res = child.wait() => res.map_err(Error::from),
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                Err(Error::Cancelled(format!("pg_restore of {db}").into()))
            }
        }
    };

    let read_stdout = async {
        let mut buf = String::new();
        if let Some(mut s) = stdout { let _ = s.read_to_string(&mut buf).await; }
        buf
    };

    let read_stderr = async {
        let mut buf = String::new();
        if let Some(mut s) = stderr { let _ = s.read_to_string(&mut buf).await; }
        buf
    };

    let (status_res, stdout_output, stderr_output) = tokio::join!(wait_fut, read_stdout, read_stderr);
    let status = status_res?;

    if !status.success() {
        return Err(Error::ProcessFailed {
            command: format!("pg_restore {db}").into(),
            stderr: format!("status {status}\nstdout:\n{}\nstderr:\n{}", stdout_output.trim(), stderr_output.trim()).into(),
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
    if tables.is_empty() { return Ok(()); }
    let dump_path = delayed_dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        let port = config.source.port.to_string();
        let mut command = Command::new("pg_dump");
        let zstd_level = config.zstd_level;
        command.kill_on_drop(true);
        command.env("PGPASSWORD", &*config.source.pass).args([
            "-h", &*config.source.host,
            "-p", &port,
            "-U", &*config.source.user,
            "-Fd",
            "-j", &config.dump_jobs.to_string(),
            "-Z", &format!("zstd:{zstd_level}"),
            "--data-only",
            "-f", dump_path.to_str().ok_or_else(|| Error::InvalidPath(dump_path.display().to_string().into()))?,
        ]);

        for table_pattern in tables { command.arg(format!("--table={table_pattern}")); }
        for exclude in copy_excludes { command.arg(format!("--exclude-table={exclude}")); }

        let mut child = command.arg(db).stderr(Stdio::piped()).spawn().map_err(|e| Error::SpawnFailed {
            command: "pg_dump (delayed)".into(),
            source: e,
        })?;

        let stderr = child.stderr.take();
        let status = select! {
            res = child.wait() => res?,
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                return Err(Error::Cancelled(format!("pg_dump (delayed) of {db}").into()));
            }
        };

        let mut err_output = String::new();
        if let Some(mut stderr) = stderr { let _ = stderr.read_to_string(&mut err_output).await; }

        if !status.success() {
            return Err(Error::ProcessFailed {
                command: format!("pg_dump (delayed) {db}").into(),
                stderr: err_output.trim().to_string().into(),
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
    if !has_delayed_tables { return Ok(()); }
    let dump_path = delayed_dump_dir(&config.dump_root, db);
    if !dump_path.join("toc.dat").exists() { return Ok(()); }

    let port = config.destination.port.to_string();
    let mut command = Command::new("pg_restore");
    command
        .kill_on_drop(true)
        .env("PGPASSWORD", &*config.destination.pass)
        .args([
            "-h", &*config.destination.host,
            "-p", &port,
            "-U", &*config.destination.user,
            "--disable-triggers",
            "--data-only",
            "-d", db,
            dump_path.to_str().ok_or_else(|| Error::InvalidPath(dump_path.display().to_string().into()))?,
        ]);

    if config.restore_single_transaction {
        command.arg("--single-transaction");
    } else {
        command.arg("-j").arg(config.restore_jobs.to_string());
    }

    let mut child = command.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e| Error::SpawnFailed {
        command: "pg_restore (delayed)".into(),
        source: e,
    })?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let wait_fut = async {
        select! {
            res = child.wait() => res.map_err(Error::from),
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                Err(Error::Cancelled(format!("pg_restore (delayed) of {db}").into()))
            }
        }
    };

    let read_stdout = async {
        let mut buf = String::new();
        if let Some(mut s) = stdout { let _ = s.read_to_string(&mut buf).await; }
        buf
    };

    let read_stderr = async {
        let mut buf = String::new();
        if let Some(mut s) = stderr { let _ = s.read_to_string(&mut buf).await; }
        buf
    };

    let (status_res, stdout_output, stderr_output) = tokio::join!(wait_fut, read_stdout, read_stderr);
    let status = status_res?;

    if !status.success() {
        return Err(Error::ProcessFailed {
            command: format!("pg_restore (delayed) {db}").into(),
            stderr: format!("status {status}\nstdout:\n{}\nstderr:\n{}", stdout_output.trim(), stderr_output.trim()).into(),
        });
    }

    info!("Restored delayed data for {db}");
    fs::write(delayed_done_marker(db)?, "")?;
    Ok(())
}

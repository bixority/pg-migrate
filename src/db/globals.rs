use crate::error::{Error, Result};
use crate::{Config, tls};
use log::info;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::process::Command;
use tokio::select;
use tokio_util::sync::CancellationToken;
use super::dump_restore::globals_marker;

pub async fn migrate_globals(config: &Config, cancel: CancellationToken) -> Result<()> {
    if globals_marker()?.exists() {
        return Ok(());
    }

    info!("Migrating global objects...");

    let globals_path = config.dump_root.join("globals.sql");

    dump_globals(config, &globals_path, cancel.clone()).await?;
    apply_globals(config, &globals_path, cancel).await?;

    fs::write(globals_marker()?, "")?;
    Ok(())
}

async fn dump_globals(
    config: &Config,
    globals_path: &Path,
    cancel: CancellationToken,
) -> Result<()> {
    fs::create_dir_all(&config.dump_root)?;

    let port = config.source.port.to_string();
    let mut child = Command::new("pg_dumpall")
        .kill_on_drop(true)
        .env("PGPASSWORD", &*config.source.pass)
        .args([
            "-h", &*config.source.host,
            "-p", &port,
            "-U", &*config.source.user,
            "--globals-only",
            "-f", globals_path.to_str().ok_or_else(|| Error::InvalidPath(globals_path.display().to_string().into()))?,
        ])
        .spawn()
        .map_err(|e| Error::SpawnFailed { command: "pg_dumpall".into(), source: e })?;

    let status = select! {
        res = child.wait() => res?,
        () = cancel.cancelled() => {
            let _ = child.kill().await;
            return Err(Error::Cancelled("pg_dumpall --globals-only".into()));
        }
    };

    if !status.success() {
        return Err(Error::ProcessFailed {
            command: "pg_dumpall".into(),
            stderr: "pg_dumpall failed".into(),
        });
    }

    let globals_content = fs::read_to_string(globals_path)?;
    let filtered_content = filter_globals_sql(&globals_content, &config.destination.user);
    fs::write(globals_path, filtered_content)?;

    Ok(())
}

async fn apply_globals(
    config: &Config,
    globals_path: &Path,
    cancel: CancellationToken,
) -> Result<()> {
    let mut db_config = tokio_postgres::Config::new();
    db_config
        .host(&*config.destination.host)
        .port(config.destination.port)
        .user(&*config.destination.user)
        .password(&*config.destination.pass)
        .dbname(&config.destination_db)
        .ssl_mode(config.pool_cache.ssl_mode);

    let (client, connection) = tokio::time::timeout(
        Duration::from_secs(30),
        db_config.connect(tls::make_tls()),
    )
    .await
    .map_err(|_| {
        Error::Timeout(
            format!("to {} database {} for globals", &*config.destination.host, config.destination_db).into(),
        )
    })??;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("Globals connection error: {e}");
        }
    });

    let sql = fs::read_to_string(globals_path)?;
    let sql_normalized = sql.replace(";\r\n", ";\n");
    for stmt in sql_normalized.split(";\n") {
        let s = stmt.trim();
        if s.is_empty() { continue; }
        let exec_sql = format!("{s};");

        let res = select! {
            res = client.execute(&exec_sql, &[]) => res,
            () = cancel.cancelled() => return Err(Error::Cancelled("globals migration execution".into())),
        };

        if let Err(e) = res {
            if let Some(db_err) = e.as_db_error() {
                let msg = db_err.message();
                if msg.contains("already exists") || msg.contains("MD5-encrypted password") || msg.contains("MD5 password support is deprecated") {
                    continue;
                }
                return Err(Error::ProcessFailed {
                    command: "execute globals statement".into(),
                    stderr: format!("Error [{}]: {}{}{}\n  statement: {s}", db_err.code().code(), msg, db_err.detail().map(|d| format!(" (detail: {d})")).unwrap_or_default(), db_err.hint().map(|h| format!(" (hint: {h})")).unwrap_or_default()).into(),
                });
            }
            return Err(Error::ProcessFailed {
                command: "execute globals statement".into(),
                stderr: format!("Error: {e}\n  statement: {s}").into(),
            });
        }
    }
    Ok(())
}

#[must_use]
pub fn filter_globals_sql(content: &str, dest_user: &str) -> String {
    let mut filtered_content = Vec::new();
    let quoted_user = format!("\"{dest_user}\"");
    let patterns = [
        format!(" {dest_user} "),
        format!(" {dest_user};"),
        format!(" {quoted_user} "),
        format!(" {quoted_user};"),
    ];

    for line in content.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('\\') { continue; }

        if (line.starts_with("CREATE ROLE ") || line.starts_with("ALTER ROLE "))
            && patterns.iter().any(|p| line.contains(p) || line.ends_with(p))
        {
            info!("Skipping migration of role '{dest_user}' to avoid password overwrite.");
            continue;
        }
        filtered_content.push(line);
    }
    filtered_content.join("\n")
}

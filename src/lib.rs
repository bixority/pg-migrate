use anyhow::{Context, Result};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use std::{
    collections::BTreeMap,
    env,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::process::Command;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

pub struct Config {
    pub from_host: String,
    pub from_port: String,
    pub from_user: String,
    pub from_pass: String,
    pub from_db: String,

    pub to_host: String,
    pub to_port: String,
    pub to_user: String,
    pub to_pass: String,
    pub to_db: String,

    pub dump_jobs: usize,
    pub restore_jobs: usize,
    pub max_parallel: usize,

    pub dump_root: PathBuf,
    pub migrate_globals: bool,
    pub disable_dst_optimizations: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            from_host: "localhost".into(),
            from_port: "5432".into(),
            from_user: "postgres".into(),
            from_pass: "oldpass".into(),
            from_db: "postgres".into(),
            to_host: "localhost".into(),
            to_port: "5432".into(),
            to_user: "postgres".into(),
            to_pass: "newpass".into(),
            to_db: "postgres".into(),
            dump_jobs: 10,
            restore_jobs: 10,
            max_parallel: 10,
            dump_root: home().join("pg_dumps"),
            migrate_globals: true,
            disable_dst_optimizations: false,
        }
    }
}

/// Returns the user's home directory.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn home() -> PathBuf {
    PathBuf::from(env::var("HOME").expect("HOME not set"))
}

/// Returns the directory used for state markers.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn state_dir() -> PathBuf {
    home().join("pg_migrate_state")
}

/// Returns the directory used for verification markers.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn verify_dir() -> PathBuf {
    home().join("pg_verify_state")
}

fn dump_dir(root: &Path, db: &str) -> PathBuf {
    root.join(db)
}

async fn pg_pool(host: &str, port: &str, user: &str, pass: &str, db: &str) -> Result<PgPool> {
    let url = format!("postgres://{user}:{pass}@{host}:{port}/{db}");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    Ok(pool)
}

/// Discovers databases to migrate and their sizes.
///
/// # Errors
///
/// Returns an error if the query fails or if the output cannot be parsed.
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

/// Migrates a single database.
///
/// # Errors
///
/// Returns an error if `pg_dump` or `pg_restore` fails.
///
/// # Panics
///
/// Panics if the dump path cannot be converted to a string.
pub async fn migrate_db(
    config: &Config,
    db: &str,
    size: u64,
    mp: Arc<MultiProgress>,
) -> Result<()> {
    let pb = mp.add(ProgressBar::new(0));
    pb.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {msg}")?
            .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_secs(1));

    // Total bar represents two phases: dump (0-50%) and restore (50-100%).
    // Use percentage display to avoid confusion with doubled byte counters.
    let mut bar_total = size.saturating_mul(2);
    if bar_total == 0 {
        // Fallback to a 0-100 percentage bar when size is unknown/unavailable
        bar_total = 100;
    }
    let phase_mid = bar_total / 2;
    let phase_end = bar_total;

    pb.set_length(bar_total);
    pb.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {percent:>3}% {msg}")?
            .progress_chars("#>-"),
    );
    pb.set_message(format!("Dumping {db} ({})", HumanBytes(size)));

    let dump_path = dump_dir(&config.dump_root, db);
    fs::create_dir_all(&dump_path)?;

    if !dump_path.join("toc.dat").exists() {
        pb.set_message(format!("Dumping {db}"));
        let out = Command::new("pg_dump")
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
                "9",
                "-f",
                dump_path.to_str().expect("invalid dump path"),
                db,
            ])
            .output()
            .await
            .context("pg_dump failed to execute")?;

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            anyhow::bail!("pg_dump failed for {db}: {stderr}");
        }
    }

    pb.set_position(phase_mid);
    pb.set_message(format!("Restoring {db} ({})", HumanBytes(size)));

    let out = Command::new("pg_restore")
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
        .output()
        .await
        .context("pg_restore failed to execute")?;

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::bail!("pg_restore failed for {db}: {stderr}");
    }

    pb.set_position(phase_end);
    pb.finish_with_message(format!("{db} complete"));
    fs::write(done_marker(db), "")?;
    Ok(())
}

/// Verifies all databases.
///
/// # Errors
///
/// Returns an error if verification fails for any database.
pub async fn verify_all(config: &Config, dbs: &[String]) -> Result<()> {
    for db in dbs {
        if verify_marker(db).exists() {
            continue;
        }
        verify_db(config, db).await?;
    }
    Ok(())
}

/// Verifies a single database.
///
/// # Errors
///
/// Returns an error if row counts or table counts mismatch between source and destination.
pub async fn verify_db(config: &Config, db: &str) -> Result<()> {
    let src_counts = stat_counts(
        &config.from_host,
        &config.from_port,
        &config.from_pass,
        &config.from_user,
        db,
    )
    .await?;
    let dst_counts = stat_counts(
        &config.to_host,
        &config.to_port,
        &config.to_pass,
        &config.to_user,
        db,
    )
    .await?;

    let src_map: BTreeMap<String, String> = src_counts
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
        .collect();

    let dst_map: BTreeMap<String, String> = dst_counts
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
        .collect();

    let mut tables: Vec<String> = src_map.keys().cloned().collect();
    for k in dst_map.keys() {
        if !src_map.contains_key(k) {
            tables.push(k.clone());
        }
    }
    tables.sort_unstable();

    let mut mismatch = false;
    let mut output = format!("Verification for {db}:\n");
    let _ = writeln!(
        output,
        "{:<40} | {:<15} | {:<15} | Status",
        "Table Name", "Source Rows", "Dest Rows"
    );
    let _ = writeln!(output, "{:-<40}-|-{:-<15}-|-{:-<15}-|--------", "", "", "");

    for t in &tables {
        let src_row = src_map.get(t).map_or("MISSING", String::as_str);
        let dst_row = dst_map.get(t).map_or("MISSING", String::as_str);

        let src_disp = if src_row == "MISSING" {
            format!("\x1b[31m{src_row}\x1b[0m")
        } else {
            src_row.to_string()
        };
        let dst_disp = if dst_row == "MISSING" {
            format!("\x1b[31m{dst_row}\x1b[0m")
        } else {
            dst_row.to_string()
        };

        let status_colored = if src_row == dst_row {
            "\x1b[32mOK\x1b[0m".to_string()
        } else {
            mismatch = true;
            "\x1b[31mMISMATCH\x1b[0m".to_string()
        };

        let _ = writeln!(
            output,
            "{t:<40} | {src_disp:<15} | {dst_disp:<15} | {status_colored}"
        );
    }

    if mismatch {
        println!("{output}");
        anyhow::bail!("Verification failed for {db}: tables or row counts mismatch");
    }

    println!("{output}");
    println!("Verified {db}: {} tables, all rows match", tables.len());
    fs::write(verify_marker(db), "")?;
    Ok(())
}

/// Returns row counts for user tables in a database.
///
/// # Errors
///
/// Returns an error if the `psql` command fails.
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

/// Returns the size of a database in bytes.
///
/// # Errors
///
/// Returns an error if the `psql` command fails or if the output cannot be parsed.
pub async fn db_size(host: &str, port: &str, pass: &str, user: &str, db: &str) -> Result<String> {
    let pool = pg_pool(host, port, user, pass, db).await?;
    let row = sqlx::query("SELECT pg_database_size($1) AS size")
        .bind(db)
        .fetch_one(&pool)
        .await?;
    let size: i64 = row.get("size");
    Ok(size.max(0).to_string())
}

/// Enables fast restore settings on the destination server.
///
/// # Errors
///
/// Returns an error if `ALTER SYSTEM` or `pg_reload_conf()` fails.
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

    sqlx::query("SELECT pg_reload_conf();").execute(&pool).await?;
    Ok(())
}

/// Restores safe settings on the destination server.
///
/// # Errors
///
/// Returns an error if `ALTER SYSTEM` or `pg_reload_conf()` fails.
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
    sqlx::query("SELECT pg_reload_conf();").execute(&pool).await?;
    Ok(())
}

/// Creates databases on the destination server if they do not exist.
///
/// # Errors
///
/// Returns an error if `CREATE DATABASE` fails.
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
            // It might already exist, which is fine for resume
            println!("Warning: CREATE DATABASE \"{db}\" failed or already exists: {e}");
        }
    }
    Ok(())
}

/// Returns the path to the done marker for a database.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn done_marker(db: &str) -> PathBuf {
    state_dir().join(format!("{db}.done"))
}

/// Returns the path to the verification marker for a database.
///
/// # Panics
///
/// Panics if the `HOME` environment variable is not set.
#[must_use]
pub fn verify_marker(db: &str) -> PathBuf {
    verify_dir().join(format!("{db}.ok"))
}

#[must_use]
pub fn globals_marker() -> PathBuf {
    state_dir().join("globals.done")
}

/// Migrates global objects (roles, etc.).
///
/// # Errors
///
/// Returns an error if `pg_dumpall` or `psql` fails.
///
/// # Panics
///
/// Panics if the globals path cannot be converted to a string.
pub async fn migrate_globals(config: &Config) -> Result<()> {
    if globals_marker().exists() {
        return Ok(());
    }

    println!("Migrating global objects...");

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

    // Filter globals.sql to avoid overwriting the password of the current to_user
    let globals_content = fs::read_to_string(&globals_path)?;
    let mut filtered_content = Vec::new();
    for line in globals_content.lines() {
        // Skip CREATE ROLE or ALTER ROLE for the current to_user
        if (line.starts_with("CREATE ROLE ") || line.starts_with("ALTER ROLE "))
            && line.contains(&format!(" {} ", config.to_user))
        {
            println!(
                "Skipping migration of role '{}' to avoid password overwrite.",
                config.to_user
            );
            continue;
        }
        // Also skip if it ends with the user name followed by a semicolon
        if (line.starts_with("CREATE ROLE ") || line.starts_with("ALTER ROLE "))
            && line.ends_with(&format!(" {};", config.to_user))
        {
            println!(
                "Skipping migration of role '{}' to avoid password overwrite.",
                config.to_user
            );
            continue;
        }

        filtered_content.push(line);
    }
    fs::write(&globals_path, filtered_content.join("\n"))?;

    // Execute globals via sqlx instead of psql
    let pool = pg_pool(
        &config.to_host,
        &config.to_port,
        &config.to_user,
        &config.to_pass,
        &config.to_db,
    )
    .await?;

    let sql = fs::read_to_string(&globals_path)?;
    // Naively split statements by semicolon followed by newline; skip empty chunks
    for stmt in sql.split(";\n") {
        let s = stmt.trim();
        if s.is_empty() { continue; }
        // add back the semicolon to be safe for certain commands
        let exec_sql = format!("{s};");
        if let Err(e) = sqlx::query(&exec_sql).execute(&pool).await {
            let msg = format!("{e}");
            if msg.contains("already exists") {
                continue;
            }
            if msg.contains("MD5-encrypted password")
                || msg.contains("MD5 password support is deprecated")
            {
                continue;
            }
            println!("Warning: executing globals statement failed: {msg}");
        }
    }

    fs::write(globals_marker(), "")?;
    Ok(())
}

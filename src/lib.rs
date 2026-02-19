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

/// Discovers databases to migrate.
///
/// # Errors
///
/// Returns an error if the `psql` command fails or if the output cannot be parsed.
pub async fn discover_databases(config: &Config) -> Result<Vec<String>> {
    let out = Command::new("psql")
        .env("PGPASSWORD", &config.from_pass)
        .args([
            "-h",
            &config.from_host,
            "-p",
            &config.from_port,
            "-U",
            &config.from_user,
            "-d",
            &config.from_db,
            "-At",
            "-c",
            "SELECT datname FROM pg_database \
             WHERE datname NOT IN ('postgres','template0','template1') \
             AND datallowconn IS TRUE \
             ORDER BY datname;",
        ])
        .output()
        .await?;

    if !out.status.success() {
        anyhow::bail!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
    }

    Ok(String::from_utf8(out.stdout)?
        .lines()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
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
pub async fn migrate_db(config: &Config, db: &str, mp: Arc<MultiProgress>) -> Result<()> {
    let pb = mp.add(ProgressBar::new(0));
    pb.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {msg}")?
            .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_secs(1));
    pb.set_message(format!("Calculating size of {db}"));

    let size = db_size(
        &config.from_host,
        &config.from_port,
        &config.from_pass,
        &config.from_user,
        db,
    )
    .await?
    .parse::<u64>()
    .unwrap_or(0);

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
    let out = Command::new("psql")
        .env("PGPASSWORD", pass)
        .args([
            "-h",
            host,
            "-p",
            port,
            "-U",
            user,
            "-d",
            db,
            "-At",
            "-c",
            "SELECT schemaname||'.'||relname||':'||n_live_tup FROM pg_stat_user_tables ORDER BY 1",
        ])
        .output()
        .await?;

    if !out.status.success() {
        anyhow::bail!(
            "stat_counts psql failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    Ok(String::from_utf8(out.stdout)?)
}

/// Returns the size of a database in bytes.
///
/// # Errors
///
/// Returns an error if the `psql` command fails or if the output cannot be parsed.
pub async fn db_size(host: &str, port: &str, pass: &str, user: &str, db: &str) -> Result<String> {
    let out = Command::new("psql")
        .env("PGPASSWORD", pass)
        .args([
            "-h",
            host,
            "-p",
            port,
            "-U",
            user,
            "-d",
            db, // ensure we can connect even if default DB doesn't exist
            "-At",
            "-c",
            &format!("SELECT pg_database_size('{db}')"),
        ])
        .output()
        .await?;

    if !out.status.success() {
        anyhow::bail!("psql failed: {}", String::from_utf8_lossy(&out.stderr));
    }

    Ok(String::from_utf8(out.stdout)?.trim().to_string())
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

    for (k, v) in settings {
        let status = Command::new("psql")
            .env("PGPASSWORD", &config.to_pass)
            .args([
                "-h",
                &config.to_host,
                "-p",
                &config.to_port,
                "-U",
                &config.to_user,
                "-d",
                &config.to_db,
                "-c",
                &format!("ALTER SYSTEM SET {k} TO {v};"),
            ])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Failed to set {k} to {v}");
        }
    }

    let status = Command::new("psql")
        .env("PGPASSWORD", &config.to_pass)
        .args([
            "-h",
            &config.to_host,
            "-p",
            &config.to_port,
            "-U",
            &config.to_user,
            "-d",
            &config.to_db,
            "-c",
            "SELECT pg_reload_conf();",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Failed to reload config");
    }
    Ok(())
}

/// Restores safe settings on the destination server.
///
/// # Errors
///
/// Returns an error if `ALTER SYSTEM` or `pg_reload_conf()` fails.
pub async fn restore_safe_settings(config: &Config) -> Result<()> {
    let settings = ["fsync", "synchronous_commit", "full_page_writes"];

    for s in settings {
        let status = Command::new("psql")
            .env("PGPASSWORD", &config.to_pass)
            .args([
                "-h",
                &config.to_host,
                "-p",
                &config.to_port,
                "-U",
                &config.to_user,
                "-d",
                &config.to_db,
                "-c",
                &format!("ALTER SYSTEM RESET {s};"),
            ])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Failed to reset {s}");
        }
    }
    Command::new("psql")
        .env("PGPASSWORD", &config.to_pass)
        .args([
            "-h",
            &config.to_host,
            "-p",
            &config.to_port,
            "-U",
            &config.to_user,
            "-d",
            &config.to_db,
            "-c",
            "SELECT pg_reload_conf();",
        ])
        .status()
        .await?;
    Ok(())
}

/// Creates databases on the destination server if they do not exist.
///
/// # Errors
///
/// Returns an error if `CREATE DATABASE` fails.
pub async fn create_dbs(config: &Config, dbs: &[String]) -> Result<()> {
    for db in dbs {
        let status = Command::new("psql")
            .env("PGPASSWORD", &config.to_pass)
            .args([
                "-h",
                &config.to_host,
                "-p",
                &config.to_port,
                "-U",
                &config.to_user,
                "-d",
                &config.to_db,
                "-c",
                &format!("CREATE DATABASE \"{db}\";"),
            ])
            .status()
            .await?;
        if !status.success() {
            // It might already exist, which is fine for resume
            println!("Warning: CREATE DATABASE \"{db}\" failed or already exists.");
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

    let out = Command::new("psql")
        .env("PGPASSWORD", &config.to_pass)
        .args([
            "-h",
            &config.to_host,
            "-p",
            &config.to_port,
            "-U",
            &config.to_user,
            "-d",
            &config.to_db,
            "-f",
            globals_path.to_str().expect("invalid globals path"),
        ])
        .output()
        .await
        .context("restoring globals failed")?;

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let mut filtered_stderr = Vec::new();
        for line in stderr.lines() {
            if line.contains("ERROR:  role") && line.contains("already exists") {
                continue;
            }
            if line.contains("WARNING:  setting an MD5-encrypted password")
                || line.contains("DETAIL:  MD5 password support is deprecated")
                || line.contains("HINT:  Refer to the PostgreSQL documentation")
            {
                continue;
            }
            filtered_stderr.push(line);
        }
        if !filtered_stderr.is_empty() {
            println!(
                "Warning: psql restored globals with some errors:\n{}",
                filtered_stderr.join("\n")
            );
        }
    }

    fs::write(globals_marker(), "")?;
    Ok(())
}

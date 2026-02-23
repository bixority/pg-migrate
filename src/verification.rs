use crate::Config;
use crate::db::pg_pool;
use crate::tui::render_verification_report;
use crate::verify_dir;
use anyhow::Result;
use indicatif::MultiProgress;
use sqlx::Row;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

pub fn verify_marker(db: &str) -> PathBuf {
    verify_dir().join(format!("{db}.verify"))
}

pub fn src_counts_path(db: &str) -> PathBuf {
    verify_dir().join(format!("{db}.src_counts.json"))
}

pub fn dst_counts_path(db: &str) -> PathBuf {
    verify_dir().join(format!("{db}.dst_counts.json"))
}

#[allow(dead_code)]
pub async fn verify_all(config: &Config, dbs: &[String], mp: Arc<MultiProgress>) -> Result<()> {
    for db in dbs {
        if verify_marker(db).exists() {
            continue;
        }
        verify_db(config, db, mp.clone()).await?;
    }
    Ok(())
}

pub async fn verify_db(config: &Config, db: &str, mp: Arc<MultiProgress>) -> Result<()> {
    let src_counts_path = src_counts_path(db);
    let dst_counts_path = dst_counts_path(db);

    let src_map: BTreeMap<String, String> = if src_counts_path.exists() {
        let content = fs::read_to_string(&src_counts_path)?;
        serde_json::from_str(&content)?
    } else {
        let counts = stat_counts(
            &config.from_host,
            &config.from_port,
            &config.from_pass,
            &config.from_user,
            db,
        )
        .await?;
        let content = serde_json::to_string(&counts)?;
        fs::write(&src_counts_path, content)?;
        counts
    };

    let dst_map: BTreeMap<String, String> = if dst_counts_path.exists() {
        let content = fs::read_to_string(&dst_counts_path)?;
        serde_json::from_str(&content)?
    } else {
        let counts = stat_counts(
            &config.to_host,
            &config.to_port,
            &config.to_pass,
            &config.to_user,
            db,
        )
        .await?;
        let content = serde_json::to_string(&counts)?;
        fs::write(&dst_counts_path, content)?;
        counts
    };

    let (output, mismatch) = render_verification_report(db, &src_map, &dst_map);

    if mismatch {
        let _ = mp.println(&output);
        anyhow::bail!("Verification failed for {db}: tables or row counts mismatch");
    }

    let _ = mp.println(&output);
    let _ = mp.println(format!(
        "Verified {db}: {} tables, all rows match",
        src_map.len()
    ));
    fs::write(verify_marker(db), "")?;
    Ok(())
}

pub async fn stat_counts(
    host: &str,
    port: &str,
    pass: &str,
    user: &str,
    db: &str,
) -> Result<BTreeMap<String, String>> {
    let pool = pg_pool(host, port, user, pass, db).await?;

    let tables = sqlx::query("SELECT schemaname, relname FROM pg_stat_user_tables ORDER BY 1, 2")
        .fetch_all(&pool)
        .await?;

    let mut counts = BTreeMap::new();

    for row in tables {
        let schema: String = row.get(0);
        let table: String = row.get(1);

        let full_name = format!("\"{schema}\".\"{table}\"");
        let count_query = format!("SELECT count(*) FROM {full_name}");
        let count: i64 = sqlx::query(&count_query).fetch_one(&pool).await?.get(0);
        counts.insert(format!("{schema}.{table}"), count.to_string());
    }

    Ok(counts)
}

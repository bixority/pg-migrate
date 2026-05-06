use crate::Config;
use crate::db::pg_pool;
use crate::tui::render_verification_report;
use crate::verify_dir;
use anyhow::Result;
use log::info;
use sqlx::Row;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

pub fn verify_marker(db_name: &str) -> PathBuf {
    verify_dir().join(format!("{db_name}.verify"))
}

pub fn src_counts_path(db_name: &str) -> PathBuf {
    verify_dir().join(format!("{db_name}.src_counts.json"))
}

pub fn dst_counts_path(db: &str) -> PathBuf {
    verify_dir().join(format!("{db}.dst_counts.json"))
}

#[allow(dead_code)]
pub async fn verify_all(config: &Config, db_names: &[String]) -> Result<()> {
    for db_name in db_names {
        if verify_marker(db_name).exists() {
            continue;
        }
        verify_db(config, db_name).await?;
    }
    Ok(())
}

pub async fn verify_db(config: &Config, db_name: &str) -> Result<()> {
    let src_counts_path = src_counts_path(db_name);
    let dst_counts_path = dst_counts_path(db_name);

    let mut src_map: BTreeMap<String, String> = if src_counts_path.exists() {
        let content = fs::read_to_string(&src_counts_path)?;
        serde_json::from_str(&content)?
    } else {
        let counts = stat_counts(
            &config.from_host,
            &config.from_port,
            &config.from_pass,
            &config.from_user,
            db_name,
            &config.exclude_table_data,
        )
        .await?;
        let content = serde_json::to_string(&counts)?;
        fs::write(&src_counts_path, content)?;
        counts
    };

    let mut dst_map: BTreeMap<String, String> = if dst_counts_path.exists() {
        let content = fs::read_to_string(&dst_counts_path)?;
        serde_json::from_str(&content)?
    } else {
        let counts = stat_counts(
            &config.to_host,
            &config.to_port,
            &config.to_pass,
            &config.to_user,
            db_name,
            &config.exclude_table_data,
        )
        .await?;
        let content = serde_json::to_string(&counts)?;
        fs::write(&dst_counts_path, content)?;
        counts
    };

    filter_excluded_counts(db_name, &config.exclude_table_data, &mut src_map);
    filter_excluded_counts(db_name, &config.exclude_table_data, &mut dst_map);

    let (output, mismatch) = render_verification_report(db_name, &src_map, &dst_map);

    if mismatch {
        info!("{output}");
        anyhow::bail!("Verification failed for {db_name}: tables or row counts mismatch");
    }

    info!("{output}");
    info!(
        "Verified {db_name}: {} tables, all rows match",
        src_map.len()
    );
    fs::write(verify_marker(db_name), "")?;
    Ok(())
}

pub async fn stat_counts(
    host: &str,
    port: &str,
    pass: &str,
    user: &str,
    db_name: &str,
    exclude_table_data: &[String],
) -> Result<BTreeMap<String, String>> {
    let pool = pg_pool(host, port, user, pass, db_name).await?;

    let tables = sqlx::query("SELECT schemaname, relname FROM pg_stat_user_tables ORDER BY 1, 2")
        .fetch_all(&pool)
        .await?;

    let mut counts = BTreeMap::new();

    for row in tables {
        let schema: String = row.get(0);
        let table: String = row.get(1);

        if is_excluded_table(db_name, &schema, &table, exclude_table_data) {
            continue;
        }

        let full_name = format!("\"{schema}\".\"{table}\"");
        let count_query = format!("SELECT count(*) FROM {full_name}");
        let count: i64 = sqlx::query(&count_query).fetch_one(&pool).await?.get(0);
        counts.insert(format!("{schema}.{table}"), count.to_string());
    }

    Ok(counts)
}

fn filter_excluded_counts(
    db_name: &str,
    exclude_table_data: &[String],
    counts: &mut BTreeMap<String, String>,
) {
    counts.retain(|full_table_name, _| {
        let Some((schema, table)) = full_table_name.split_once('.') else {
            return true;
        };

        !is_excluded_table(db_name, schema, table, exclude_table_data)
    });
}

fn is_excluded_table(
    db_name: &str,
    schema: &str,
    table: &str,
    exclude_table_data: &[String],
) -> bool {
    let db_prefix = format!("{db_name}.");

    exclude_table_data.iter().any(|exclude| {
        let Some(table_pattern) = exclude.strip_prefix(&db_prefix) else {
            return false;
        };

        wildcard_matches(table_pattern, table)
            || wildcard_matches(table_pattern, &format!("{schema}.{table}"))
    })
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    wildcard_matches_inner(pattern.as_bytes(), value.as_bytes())
}

fn wildcard_matches_inner(pattern: &[u8], value: &[u8]) -> bool {
    match (pattern, value) {
        ([], []) => true,
        ([], _) | ([_, ..], []) => false,
        ([b'*', remaining_pattern @ ..], [_, remaining_value @ ..]) => {
            wildcard_matches_inner(remaining_pattern, value)
                || wildcard_matches_inner(pattern, remaining_value)
        }
        ([b'?', remaining_pattern @ ..], [_, remaining_value @ ..]) => {
            wildcard_matches_inner(remaining_pattern, remaining_value)
        }
        ([pattern_byte, remaining_pattern @ ..], [value_byte, remaining_value @ ..]) => {
            pattern_byte == value_byte && wildcard_matches_inner(remaining_pattern, remaining_value)
        }
    }
}

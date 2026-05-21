use crate::Config;
use crate::db::DbArgs;
use crate::tui::render_verification_report;
use crate::verify_dir;
use anyhow::Result;
use futures::stream::{self, StreamExt};
use log::{info, warn};
use sqlx::Row;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

pub fn verify_marker(db_name: &str) -> PathBuf {
    verify_dir().join(format!("{db_name}.verify"))
}

pub fn delayed_verify_marker(db_name: &str) -> PathBuf {
    verify_dir().join(format!("{db_name}.delayed_verify"))
}

#[must_use]
pub fn src_counts_path(db_name: &str, fast: bool) -> PathBuf {
    let suffix = if fast {
        "src_counts.fast"
    } else {
        "src_counts"
    };
    verify_dir().join(format!("{db_name}.{suffix}.json"))
}

#[must_use]
pub fn dst_counts_path(db_name: &str, fast: bool) -> PathBuf {
    let suffix = if fast {
        "dst_counts.fast"
    } else {
        "dst_counts"
    };
    verify_dir().join(format!("{db_name}.{suffix}.json"))
}

#[allow(dead_code)]
pub async fn verify_all(
    config: &Config,
    db_names: &[String],
    cancel: CancellationToken,
) -> Result<()> {
    for db_name in db_names {
        if verify_marker(db_name).exists() {
            continue;
        }
        verify_db(config, db_name, false, cancel.clone()).await?;
    }
    Ok(())
}

pub async fn verify_db(
    config: &Config,
    db_name: &str,
    include_delayed: bool,
    cancel: CancellationToken,
) -> Result<()> {
    let src_counts_path = if include_delayed {
        let suffix = if config.fast_verify {
            "src_counts.delayed.fast"
        } else {
            "src_counts.delayed"
        };
        verify_dir().join(format!("{db_name}.{suffix}.json"))
    } else {
        src_counts_path(db_name, config.fast_verify)
    };
    let dst_counts_path = if include_delayed {
        let suffix = if config.fast_verify {
            "dst_counts.delayed.fast"
        } else {
            "dst_counts.delayed"
        };
        verify_dir().join(format!("{db_name}.{suffix}.json"))
    } else {
        dst_counts_path(db_name, config.fast_verify)
    };

    let mut src_map: BTreeMap<String, String> = if src_counts_path.exists() {
        let content = fs::read_to_string(&src_counts_path)?;
        serde_json::from_str(&content)?
    } else {
        let counts = stat_counts(
            config,
            &config.source,
            db_name,
            &config.delay_table_data,
            include_delayed,
            cancel.clone(),
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
            config,
            &config.destination,
            db_name,
            &config.delay_table_data,
            include_delayed,
            cancel.clone(),
        )
        .await?;
        let content = serde_json::to_string(&counts)?;
        fs::write(&dst_counts_path, content)?;
        counts
    };

    if !include_delayed {
        filter_delayed_counts(db_name, &config.delay_table_data, &mut src_map);
        filter_delayed_counts(db_name, &config.delay_table_data, &mut dst_map);
    }

    let (output, mismatch) = render_verification_report(db_name, &src_map, &dst_map);

    if mismatch {
        if config.fast_verify {
            let delayed_mismatch = include_delayed
                && delayed_count_mismatch(db_name, &config.delay_table_data, &src_map, &dst_map);
            info!("{output}");
            if delayed_mismatch {
                anyhow::bail!(
                    "Verification failed for {db_name}: delayed-table row counts mismatch"
                );
            }
            warn!(
                "Fast verification mismatch for {db_name} (non-delayed estimates may differ; \
                 ANALYZE both sides for closer values)"
            );
        } else {
            info!("{output}");
            anyhow::bail!("Verification failed for {db_name}: tables or row counts mismatch");
        }
    } else {
        info!("{output}");
    }

    info!(
        "Verified {db_name} (include_delayed={include_delayed}, fast={}): {} tables",
        config.fast_verify,
        src_map.len()
    );
    if include_delayed {
        fs::write(delayed_verify_marker(db_name), "")?;
    } else {
        fs::write(verify_marker(db_name), "")?;
    }
    Ok(())
}

fn delayed_count_mismatch(
    db_name: &str,
    delay_table_data: &[String],
    src_map: &BTreeMap<String, String>,
    dst_map: &BTreeMap<String, String>,
) -> bool {
    let mut keys: HashSet<&String> = src_map.keys().collect();
    keys.extend(dst_map.keys());
    keys.iter().any(|k| {
        let Some((schema, table)) = k.split_once('.') else {
            return false;
        };
        if !is_delayed_table(db_name, schema, table, delay_table_data) {
            return false;
        }
        src_map.get(*k) != dst_map.get(*k)
    })
}

pub async fn stat_counts(
    config: &Config,
    args: &DbArgs,
    db_name: &str,
    delay_table_data: &[String],
    include_delayed: bool,
    cancel: CancellationToken,
) -> Result<BTreeMap<String, String>> {
    let pool = config.pool_cache.get(args, db_name).await?;

    let tables = tokio::select! {
        res = sqlx::query("SELECT schemaname, relname FROM pg_stat_user_tables ORDER BY 1, 2").fetch_all(&pool) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during table discovery for {db_name}"),
    };

    let entries: Vec<(String, String, bool)> = tables
        .into_iter()
        .filter_map(|row| {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            let delayed = is_delayed_table(db_name, &schema, &table, delay_table_data);
            if !include_delayed && delayed {
                return None;
            }
            Some((schema, table, delayed))
        })
        .collect();

    if config.fast_verify {
        return fast_stat_counts(config, &pool, &entries, cancel).await;
    }

    let concurrency = config.verify_concurrency.max(1);
    let cancel_for_stream = cancel.clone();
    let verify_sem = config.verify_sem.clone();

    let results: Vec<Result<(String, String)>> = stream::iter(entries)
        .map(|(schema, table, _)| {
            let pool = pool.clone();
            let cancel = cancel_for_stream.clone();
            let verify_sem = verify_sem.clone();
            async move {
                let _permit = tokio::select! {
                    res = verify_sem.acquire_owned() => res?,
                    () = cancel.cancelled() => anyhow::bail!("cancelled while waiting for verify slot"),
                };
                let full_name = format!("\"{schema}\".\"{table}\"");
                let count_query = format!("SELECT count(*) FROM {full_name}");
                let count: i64 = tokio::select! {
                    res = sqlx::query(&count_query).fetch_one(&pool) => res?.get(0),
                    () = cancel.cancelled() => anyhow::bail!("cancelled during row count of {schema}.{table}"),
                };
                Ok((format!("{schema}.{table}"), count.to_string()))
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut counts = BTreeMap::new();
    for r in results {
        let (k, v) = r?;
        counts.insert(k, v);
    }

    Ok(counts)
}

async fn fast_stat_counts(
    config: &Config,
    pool: &sqlx::PgPool,
    entries: &[(String, String, bool)],
    cancel: CancellationToken,
) -> Result<BTreeMap<String, String>> {
    let _permit = tokio::select! {
        res = config.verify_sem.clone().acquire_owned() => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled while waiting for verify slot"),
    };
    let allowed: HashSet<(String, String)> = entries
        .iter()
        .map(|(s, t, _)| (s.clone(), t.clone()))
        .collect();

    let rows = tokio::select! {
        res = sqlx::query(
            "SELECT n.nspname, c.relname, c.reltuples::bigint \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r' \
               AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')"
        )
        .fetch_all(pool) => res?,
        () = cancel.cancelled() => anyhow::bail!("cancelled during reltuples query"),
    };

    let mut estimates: BTreeMap<(String, String), i64> = BTreeMap::new();
    for row in rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        if !allowed.contains(&(schema.clone(), table.clone())) {
            continue;
        }
        let est: i64 = row.get(2);
        estimates.insert((schema, table), est.max(0));
    }

    let mut counts: BTreeMap<String, String> = BTreeMap::new();

    for (schema, table, delayed) in entries {
        let key = (schema.clone(), table.clone());
        if *delayed {
            let full_name = format!("\"{schema}\".\"{table}\"");
            let count_query = format!("SELECT count(*) FROM {full_name}");
            let count: i64 = tokio::select! {
                res = sqlx::query(&count_query).fetch_one(pool) => res?.get(0),
                () = cancel.cancelled() => anyhow::bail!("cancelled during exact count of {schema}.{table}"),
            };
            counts.insert(format!("{schema}.{table}"), count.to_string());
        } else {
            let est = estimates.get(&key).copied().unwrap_or(0);
            counts.insert(format!("{schema}.{table}"), est.to_string());
        }
    }

    Ok(counts)
}

fn filter_delayed_counts(
    db_name: &str,
    delay_table_data: &[String],
    counts: &mut BTreeMap<String, String>,
) {
    counts.retain(|full_table_name, _| {
        let Some((schema, table)) = full_table_name.split_once('.') else {
            return true;
        };

        !is_delayed_table(db_name, schema, table, delay_table_data)
    });
}

pub fn is_delayed_table(
    db_name: &str,
    schema: &str,
    table: &str,
    delay_table_data: &[String],
) -> bool {
    let db_prefix = format!("{db_name}.");

    delay_table_data.iter().any(|delay| {
        let Some(table_pattern) = delay.strip_prefix(&db_prefix) else {
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
        ([b'*', remaining_pattern @ ..], []) => wildcard_matches_inner(remaining_pattern, &[]),
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
        _ => false,
    }
}

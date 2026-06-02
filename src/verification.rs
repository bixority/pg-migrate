use crate::Config;
use crate::db::DbArgs;
use crate::error::{Error, Result};
use crate::tui::render_verification_report;
use crate::verify_dir;
use futures::stream::{self, StreamExt};
use log::{info, warn};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

pub fn verify_marker(db_name: &str) -> Result<PathBuf> {
    Ok(verify_dir()?.join(format!("{db_name}.verify")))
}

pub fn delayed_verify_marker(db_name: &str) -> Result<PathBuf> {
    Ok(verify_dir()?.join(format!("{db_name}.delayed_verify")))
}

pub fn src_counts_path(db_name: &str, fast: bool, include_delayed: bool) -> Result<PathBuf> {
    let suffix = match (fast, include_delayed) {
        (true, true) => "src_counts.delayed.fast",
        (false, true) => "src_counts.delayed",
        (true, false) => "src_counts.fast",
        (false, false) => "src_counts",
    };
    Ok(verify_dir()?.join(format!("{db_name}.{suffix}.json")))
}

pub fn dst_counts_path(db_name: &str, fast: bool, include_delayed: bool) -> Result<PathBuf> {
    let suffix = match (fast, include_delayed) {
        (true, true) => "dst_counts.delayed.fast",
        (false, true) => "dst_counts.delayed",
        (true, false) => "dst_counts.fast",
        (false, false) => "dst_counts",
    };
    Ok(verify_dir()?.join(format!("{db_name}.{suffix}.json")))
}

#[allow(dead_code)]
pub async fn verify_all(
    config: &Config,
    db_names: &[String],
    cancel: CancellationToken,
) -> Result<()> {
    for db_name in db_names {
        if verify_marker(db_name)?.exists() {
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
    let (mut src_map, mut dst_map) = tokio::try_join!(
        get_or_compute_counts(
            config,
            &config.source,
            db_name,
            include_delayed,
            true,
            cancel.clone(),
        ),
        get_or_compute_counts(
            config,
            &config.destination,
            db_name,
            include_delayed,
            false,
            cancel.clone(),
        )
    )?;

    if !include_delayed {
        filter_delayed_counts(db_name, &config.delay_table_data, &mut src_map);
        filter_delayed_counts(db_name, &config.delay_table_data, &mut dst_map);
    }

    let (output, mismatch) = render_verification_report(db_name, &src_map, &dst_map);

    info!("{output}");

    if mismatch {
        let delayed_mismatch = include_delayed
            && delayed_count_mismatch(db_name, &config.delay_table_data, &src_map, &dst_map);
        let non_delayed_mismatch =
            non_delayed_count_mismatch(db_name, &config.delay_table_data, &src_map, &dst_map);

        if config.fast_verify {
            if delayed_mismatch {
                warn!("Delayed-table row counts mismatch for {db_name}");
            }
            if non_delayed_mismatch {
                warn!(
                    "Fast verification mismatch for {db_name} (non-delayed estimates may differ; \
                     ANALYZE both sides for closer values)"
                );
            }
        } else {
            if non_delayed_mismatch {
                return Err(Error::VerificationFailed {
                    database: db_name.to_string(),
                    details: "tables or row counts mismatch".to_string(),
                });
            }
            if delayed_mismatch {
                warn!("Delayed-table row counts mismatch for {db_name}");
            }
        }
    }

    info!(
        "Verified {db_name} (include_delayed={include_delayed}, fast={}): {} tables",
        config.fast_verify,
        src_map.len()
    );
    if include_delayed {
        fs::write(delayed_verify_marker(db_name)?, "")?;
    } else {
        fs::write(verify_marker(db_name)?, "")?;
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

fn non_delayed_count_mismatch(
    db_name: &str,
    delay_table_data: &[String],
    src_map: &BTreeMap<String, String>,
    dst_map: &BTreeMap<String, String>,
) -> bool {
    let mut keys: HashSet<&String> = src_map.keys().collect();
    keys.extend(dst_map.keys());
    keys.iter().any(|k| {
        let Some((schema, table)) = k.split_once('.') else {
            return src_map.get(*k) != dst_map.get(*k);
        };
        if is_delayed_table(db_name, schema, table, delay_table_data) {
            return false;
        }
        src_map.get(*k) != dst_map.get(*k)
    })
}

pub async fn get_or_compute_counts(
    config: &Config,
    args: &DbArgs,
    db_name: &str,
    include_delayed: bool,
    is_source: bool,
    cancel: CancellationToken,
) -> Result<BTreeMap<String, String>> {
    let path = if is_source {
        src_counts_path(db_name, config.fast_verify, include_delayed)?
    } else {
        dst_counts_path(db_name, config.fast_verify, include_delayed)?
    };

    if path.exists() {
        let content = fs::read_to_string(&path)?;
        Ok(serde_json::from_str(&content)?)
    } else {
        let counts = stat_counts(
            config,
            args,
            db_name,
            &config.delay_table_data,
            include_delayed,
            cancel,
        )
        .await?;
        let content = serde_json::to_string(&counts)?;
        fs::write(&path, content)?;
        Ok(counts)
    }
}

pub async fn stat_counts(
    config: &Config,
    args: &DbArgs,
    db_name: &str,
    delay_table_data: &[String],
    include_delayed: bool,
    cancel: CancellationToken,
) -> Result<BTreeMap<String, String>> {
    let pool = tokio::select! {
        res = config.pool_cache.get(args, db_name) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled(format!("verification connection for {db_name}"))),
    };

    let tables = tokio::select! {
        res = pool.query("SELECT schemaname, relname FROM pg_stat_user_tables ORDER BY 1, 2", &[]) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled(format!("table discovery for {db_name}"))),
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
                    res = verify_sem.clone().acquire_owned() => res?,
                    () = cancel.cancelled() => return Err(Error::Cancelled("waiting for verify slot".to_string())),
                };
                let full_name = format!("\"{schema}\".\"{table}\"");
                let count_query = format!("SELECT count(*) FROM {full_name}");
                let count: i64 = tokio::select! {
                    res = pool.query_one(&count_query, &[]) => res?.get(0),
                    () = cancel.cancelled() => return Err(Error::Cancelled(format!("row count of {schema}.{table}"))),
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
    pool: &tokio_postgres::Client,
    entries: &[(String, String, bool)],
    cancel: CancellationToken,
) -> Result<BTreeMap<String, String>> {
    let _permit = tokio::select! {
        res = config.verify_sem.clone().acquire_owned() => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("waiting for verify slot".to_string())),
    };
    let allowed: HashSet<(String, String)> = entries
        .iter()
        .map(|(s, t, _)| (s.clone(), t.clone()))
        .collect();

    let rows = tokio::select! {
        res = pool.query(
            "SELECT n.nspname, c.relname, c.reltuples::bigint \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r' \
               AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')",
            &[]
        ) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("reltuples query".to_string())),
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
                res = pool.query_one(&count_query, &[]) => res?.get(0),
                () = cancel.cancelled() => return Err(Error::Cancelled(format!("exact count of {schema}.{table}"))),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_is_delayed_table() {
        let delay_table_data = vec!["db1.public.logs".to_string(), "db1.audit.*".to_string()];

        assert!(is_delayed_table("db1", "public", "logs", &delay_table_data));
        assert!(is_delayed_table(
            "db1",
            "audit",
            "actions",
            &delay_table_data
        ));
        assert!(!is_delayed_table(
            "db1",
            "public",
            "users",
            &delay_table_data
        ));
        assert!(!is_delayed_table(
            "db2",
            "public",
            "logs",
            &delay_table_data
        ));
    }

    #[test]
    fn test_mismatch_logic() {
        let db_name = "db1";
        let delay_table_data = vec!["db1.public.logs".to_string()];

        let mut src_map = BTreeMap::new();
        src_map.insert("public.users".to_string(), "100".to_string());
        src_map.insert("public.logs".to_string(), "500".to_string());

        let mut dst_map = BTreeMap::new();
        dst_map.insert("public.users".to_string(), "100".to_string());
        dst_map.insert("public.logs".to_string(), "500".to_string());

        // No mismatch
        assert!(!delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));
        assert!(!non_delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));

        // Delayed mismatch
        dst_map.insert("public.logs".to_string(), "501".to_string());
        assert!(delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));
        assert!(!non_delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));

        // Non-delayed mismatch
        dst_map.insert("public.logs".to_string(), "500".to_string()); // reset
        dst_map.insert("public.users".to_string(), "101".to_string());
        assert!(!delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));
        assert!(non_delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));

        // Both mismatch
        dst_map.insert("public.logs".to_string(), "501".to_string());
        assert!(delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));
        assert!(non_delayed_count_mismatch(
            db_name,
            &delay_table_data,
            &src_map,
            &dst_map
        ));
    }
}

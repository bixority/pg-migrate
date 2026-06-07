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
use wildmatch::WildMatch;

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
    // Delayed-data and copy-engine tables are excluded from count computation
    // entirely (see `stat_counts`), so `src_map`/`dst_map` only ever contain
    // regular tables. There is no separate delayed comparison to make here.
    let (src_map, dst_map) = tokio::try_join!(
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

    let (output, mismatch) = render_verification_report(db_name, &src_map, &dst_map);

    info!("{output}");

    if mismatch {
        if config.fast_verify {
            warn!(
                "Fast verification mismatch for {db_name} (row-count estimates may differ; \
                 ANALYZE both sides for closer values)"
            );
        } else {
            return Err(Error::VerificationFailed {
                database: db_name.to_string(),
                details: "tables or row counts mismatch".to_string(),
            });
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
            &config.deferred_table_patterns(),
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

    // Delayed-data and copy-engine tables are migrated out-of-band (delayed
    // pipeline / copy engine), so their source and destination row counts are
    // not meaningful to compare. Skip them on both sides, in every pass.
    let entries: Vec<(String, String)> = tables
        .into_iter()
        .filter_map(|row| {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            if is_delayed_table(db_name, &schema, &table, delay_table_data) {
                return None;
            }
            Some((schema, table))
        })
        .collect();

    if config.fast_verify {
        return fast_stat_counts(config, &pool, &entries, cancel).await;
    }

    let concurrency = config.verify_concurrency.max(1);
    let cancel_for_stream = cancel.clone();
    let verify_sem = config.verify_sem.clone();

    let results: Vec<Result<(String, String)>> = stream::iter(entries)
        .map(|(schema, table)| {
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
    entries: &[(String, String)],
    cancel: CancellationToken,
) -> Result<BTreeMap<String, String>> {
    let _permit = tokio::select! {
        res = config.verify_sem.clone().acquire_owned() => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("waiting for verify slot".to_string())),
    };
    let allowed: HashSet<(String, String)> = entries.iter().cloned().collect();

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

    for (schema, table) in entries {
        let key = (schema.clone(), table.clone());
        let est = estimates.get(&key).copied().unwrap_or(0);
        counts.insert(format!("{schema}.{table}"), est.to_string());
    }

    Ok(counts)
}

/// Returns whether a table is deferred out of the regular pass.
///
/// `delay_table_data` here is the full deferred set (delay patterns plus
/// copy-engine tables — see [`crate::config::Config::deferred_table_patterns`]).
/// Every entry is fully qualified as `DATABASE.SCHEMA.TABLE`; the `DATABASE.`
/// prefix must equal `db_name`, and the remaining `SCHEMA.TABLE` pattern (which
/// may use `pg_dump`'s `*`/`?` wildcards) is matched with [`WildMatch`] against
/// the relation's `schema.table`.
pub fn is_delayed_table(
    db_name: &str,
    schema: &str,
    table: &str,
    delay_table_data: &[String],
) -> bool {
    let db_prefix = format!("{db_name}.");
    let qualified = format!("{schema}.{table}");

    delay_table_data.iter().any(|delay| {
        let Some(schema_table_pattern) = delay.strip_prefix(&db_prefix) else {
            return false;
        };

        WildMatch::new(schema_table_pattern).matches(&qualified)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_classifies_tables() {
        // The exact (schema-qualified) patterns from the shipped example config.
        let delay = vec![
            "pdb1.public.table3".to_string(),
            "pdb2.public.table*".to_string(),
        ];
        let copy = vec![
            "pdb1.public.table3".to_string(),
            "pdb2.public.table3".to_string(),
        ];

        // Matching is per-database: it only fires when the entry's "DATABASE."
        // prefix equals the actual database name being planned.

        // pdb1.public.table3 is matched by its copy rule (copy wins over delayed).
        assert!(is_delayed_table("pdb1", "public", "table3", &copy));
        // pdb2.public.table3 is matched by its copy rule.
        assert!(is_delayed_table("pdb2", "public", "table3", &copy));
        // pdb2.public.table5 is not a copy table, but the "pdb2.public.table*"
        // delay pattern matches it, so it goes to the delayed pass.
        assert!(!is_delayed_table("pdb2", "public", "table5", &copy));
        assert!(is_delayed_table("pdb2", "public", "table5", &delay));
        // The wildcard is anchored to the public schema: the same table name in
        // another schema is not matched.
        assert!(!is_delayed_table("pdb2", "audit", "table5", &delay));
        // A different table in pdb1 is neither copy nor delayed → regular.
        assert!(!is_delayed_table("pdb1", "public", "users", &copy));
        assert!(!is_delayed_table("pdb1", "public", "users", &delay));

        // The crux: if the real database is NOT named pdb1/pdb2, nothing matches.
        assert!(!is_delayed_table("mydb", "public", "table3", &copy));
        assert!(!is_delayed_table("mydb", "public", "table3", &delay));
    }

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
        // The "db1.audit.*" pattern is schema-anchored: a table of the same name
        // in another schema is regular, not delayed.
        assert!(!is_delayed_table(
            "db1",
            "public",
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
}

use crate::copy_engine::Splitter;
use crate::{Config, Error, Result};
use indicatif::HumanBytes;
use log::{info, warn};
use std::sync::Arc;
use tokio::select;
use tokio_util::sync::CancellationToken;
use wildmatch::WildMatch;

#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub databases: Vec<DatabasePlan>,
}

#[derive(Debug, Clone)]
pub struct DatabasePlan {
    pub name: String,
    pub size: u64,
    pub regular_data_excludes: Vec<String>,
    pub full_excludes: Vec<String>,
    pub delayed_tables: Vec<String>,
    pub copy_rules: Vec<CopyRulePlan>,

    /// Resolved `schema.table` names taking each migration path, populated from
    /// the live source catalog. These are informational (for the plan printout
    /// and diagnostics); the migration mechanics drive off the fields above.
    pub regular_table_names: Vec<String>,
    pub delayed_table_names: Vec<String>,
    pub copy_table_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CopyRulePlan {
    pub table: String, // Schema-qualified `schema.table` (DB prefix stripped)
    pub column: String,
    pub method: String,
    pub from: Option<String>,
    pub till: Option<String>,
    pub partitions: usize,
    pub rule_hash: u64,
}

pub async fn create_plan(
    config: Arc<Config>,
    dbs_with_sizes: &[(String, u64)],
    cancel: CancellationToken,
) -> Result<MigrationPlan> {
    let mut db_plans = Vec::new();

    for (db_name, size) in dbs_with_sizes {
        if config.is_db_excluded(db_name) {
            info!("Skipping entirely excluded database: {db_name}");
            continue;
        }
        db_plans.push(plan_database(&config, db_name, *size, &cancel).await?);

        if cancel.is_cancelled() {
            return Err(Error::Cancelled("planning interrupted".into()));
        }
    }

    // A delay/copy entry only produces delayed work (and therefore a delayed TUI
    // row) when its "DATABASE." prefix matches a migrated database. Warn about
    // entries that match nothing, since that silently yields no delayed rows.
    for entry in &config.delay_table_data {
        warn_unmatched(entry, dbs_with_sizes, "delay_table_data");
    }
    for rule in &config.copy_rules {
        warn_unmatched(&rule.table, dbs_with_sizes, "copy_rules");
    }

    for entry in &config.exclude {
        warn_unmatched(entry, dbs_with_sizes, "exclude");
    }

    Ok(MigrationPlan {
        databases: db_plans,
    })
}

/// Builds the plan for a single database: copy-engine rules (with resolved
/// ranges and partitions), delayed-dump patterns, and the resolved per-table
/// classification used by the plan printout.
async fn plan_database(
    config: &Config,
    db_name: &str,
    size: u64,
    cancel: &CancellationToken,
) -> Result<DatabasePlan> {
    let mut db_plan = DatabasePlan {
        name: db_name.to_string(),
        size,
        regular_data_excludes: Vec::new(),
        full_excludes: Vec::new(),
        delayed_tables: Vec::new(),
        copy_rules: Vec::new(),
        regular_table_names: Vec::new(),
        delayed_table_names: Vec::new(),
        copy_table_names: Vec::new(),
    };

    let db_prefix = format!("{db_name}.");

    // 0. Identify fully excluded tables/patterns.
    for pattern in &config.exclude {
        if pattern == db_name {
            // Entire database is excluded; this is primarily handled in create_plan,
            // but we can add a catch-all for completeness if passed to pg_dump.
            db_plan.full_excludes.push("*.*".to_string());
            continue;
        }
        if let Some(rest) = pattern.strip_prefix(&db_prefix) {
            let translated = if rest.contains('.') {
                rest.to_string()
            } else {
                format!("{rest}.*")
            };
            db_plan.full_excludes.push(translated);
        }
    }

    // 1. Identify copy rules and their partitions.
    let mut copy_excludes = std::collections::HashSet::new();
    for rule in &config.copy_rules {
        if let Some(table_name) = rule.table.strip_prefix(&db_prefix) {
            if db_plan
                .full_excludes
                .iter()
                .any(|p| WildMatch::new(p).matches(table_name))
            {
                continue;
            }
            copy_excludes.insert(table_name.to_string());

            let (from, till) = resolve_range(config, db_name, table_name, rule, cancel).await?;
            let partitions = Splitter::split(
                &rule.split_by_column,
                from.as_deref(),
                till.as_deref(),
                rule.method.as_deref(),
                config.max_parallel,
            )?;

            db_plan.copy_rules.push(CopyRulePlan {
                table: table_name.to_string(),
                column: rule.split_by_column.clone(),
                method: rule.method.clone().unwrap_or_else(|| "time".to_string()),
                from,
                till,
                partitions: partitions.len(),
                rule_hash: rule.rule_hash(),
            });

            db_plan.regular_data_excludes.push(table_name.to_string());
        }
    }

    // 2. Identify delayed tables.
    for delay in &config.delay_table_data {
        let table_pattern = if delay == db_name {
            Some("*.*".to_string())
        } else {
            delay.strip_prefix(&db_prefix).map(|rest| {
                if rest.contains('.') {
                    rest.to_string()
                } else {
                    format!("{rest}.*")
                }
            })
        };

        if let Some(table_pattern) = table_pattern {
            if db_plan
                .full_excludes
                .iter()
                .any(|p| WildMatch::new(p).matches(&table_pattern))
            {
                continue;
            }
            db_plan.regular_data_excludes.push(table_pattern.clone());
            if !copy_excludes.contains(&table_pattern) {
                db_plan.delayed_tables.push(table_pattern);
            }
        }
    }

    // 3. Resolve every table in the database against the rules so the plan
    //    reflects exactly what will be migrated and how. Copy-engine tables win
    //    over delayed (they are excluded from the delayed pg_dump), and delayed
    //    wins over regular.
    for (schema, table) in list_user_tables(config, db_name, cancel).await? {
        if config.is_table_excluded(db_name, &schema, &table) {
            continue;
        }

        let full = format!("{schema}.{table}");
        if config
            .copy_rule_patterns
            .iter()
            .any(|p| p.matches(db_name, &schema, &table))
        {
            db_plan.copy_table_names.push(full);
        } else if config.is_delayed_table(db_name, &schema, &table) {
            db_plan.delayed_table_names.push(full);
        } else {
            db_plan.regular_table_names.push(full);
        }
    }

    Ok(db_plan)
}

/// Resolves the `from`/`till` bounds for a `time`-method copy rule.
///
/// Only the `from` (lower) bound is discovered from the source. The upper bound is never
/// queried: the copy engine's final partition is always open-ended (`column >= x`), so the
/// upper endpoint is only a splitting hint that the splitter synthesizes from the current
/// time. Skipping the `max()` scan avoids a full read of the source on huge tables and
/// captures rows inserted after planning for free.
async fn resolve_range(
    config: &Config,
    db_name: &str,
    table_name: &str,
    rule: &crate::config::CopyRule,
    cancel: &CancellationToken,
) -> Result<(Option<String>, Option<String>)> {
    let from = rule.from.clone();
    let till = rule.till.clone();

    if rule.method.as_deref().unwrap_or("time") != "time" || from.is_some() {
        return Ok((from, till));
    }

    info!(
        "Discovering 'from' bound for {db_name}.{table_name}.{}...",
        rule.split_by_column
    );
    let pool = select! {
        res = config.pool_cache.get(&config.source, db_name) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("planning interrupted".into())),
    };

    let query = format!(
        "SELECT min({})::text FROM {}",
        rule.split_by_column, table_name
    );
    let from: Option<String> = pool.query_one(&query, &[]).await?.get(0);
    if let Some(ref f) = from {
        info!("Discovered 'from' bound for {table_name}: {f}");
    }

    Ok((from, till))
}

/// Warns when a `DATABASE.SCHEMA.TABLE` config entry targets a database that is
/// not in the migrated set, so it contributes no delayed/copy-engine work.
fn warn_unmatched(entry: &str, dbs_with_sizes: &[(String, u64)], source: &str) {
    let db = entry.split_once('.').map_or(entry, |(db, _)| db);
    if !dbs_with_sizes.iter().any(|(name, _)| name.as_str() == db) {
        warn!(
            "Config {source} entry '{entry}' targets database '{db}', which is not among \
             the migrated databases — it produces no delayed/copy-engine work (no '(delayed)' row)"
        );
    }
}

/// Lists the ordinary and partitioned user tables of a database as
/// `(schema, table)` pairs, used to resolve the per-table migration plan.
async fn list_user_tables(
    config: &Config,
    db_name: &str,
    cancel: &CancellationToken,
) -> Result<Vec<(String, String)>> {
    let pool = select! {
        res = config.pool_cache.get(&config.source, db_name) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("planning interrupted".into())),
    };

    let rows = select! {
        res = pool.query(
            "SELECT n.nspname, c.relname \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
             ORDER BY 1, 2",
            &[],
        ) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("planning interrupted".into())),
    };

    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

impl MigrationPlan {
    pub fn print(&self) {
        info!("=== Migration Plan ===");
        for db in &self.databases {
            info!("Database: {} ({})", db.name, HumanBytes(db.size));

            info!(
                "  Copy engine: {} rule(s), {} table(s) matched",
                db.copy_rules.len(),
                db.copy_table_names.len()
            );
            for rule in &db.copy_rules {
                let range = match (rule.from.as_deref(), rule.till.as_deref()) {
                    (Some(from), Some(till)) => format!("{from} .. {till}"),
                    (Some(from), None) => format!("{from} .."),
                    (None, Some(till)) => format!(".. {till}"),
                    (None, None) => "full range".to_string(),
                };
                info!(
                    "    - {} [{} split on {}, {}, {} partition(s)]",
                    rule.table, rule.method, rule.column, range, rule.partitions
                );
            }
            print_tables(&db.copy_table_names, usize::MAX);

            info!(
                "  Delayed (deferred pg_dump --data-only): {} table(s)",
                db.delayed_table_names.len()
            );
            print_tables(&db.delayed_table_names, usize::MAX);

            info!(
                "  Regular (pg_dump / pg_restore): {} table(s)",
                db.regular_table_names.len()
            );
            print_tables(&db.regular_table_names, 25);
        }
        info!("======================");
    }
}

/// Logs up to `max` table names, then a `... and N more` summary line.
fn print_tables(tables: &[String], max: usize) {
    for table in tables.iter().take(max) {
        info!("    - {table}");
    }
    if tables.len() > max {
        info!("    ... and {} more", tables.len() - max);
    }
}

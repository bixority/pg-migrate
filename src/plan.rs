use crate::copy_engine::Splitter;
use crate::{Config, Error, Result};
use indicatif::HumanBytes;
use log::info;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub databases: Vec<DatabasePlan>,
}

#[derive(Debug, Clone)]
pub struct DatabasePlan {
    pub name: String,
    pub size: u64,
    pub regular_data_excludes: Vec<String>,
    pub delayed_tables: Vec<String>,
    pub copy_rules: Vec<CopyRulePlan>,
}

#[derive(Debug, Clone)]
pub struct CopyRulePlan {
    pub table: String, // Just the table name without DB prefix
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
        let mut db_plan = DatabasePlan {
            name: db_name.clone(),
            size: *size,
            regular_data_excludes: Vec::new(),
            delayed_tables: Vec::new(),
            copy_rules: Vec::new(),
        };

        let db_prefix = format!("{db_name}.");

        // 1. Identify copy rules and their partitions
        let mut copy_excludes = std::collections::HashSet::new();
        for rule in &config.copy_rules {
            if let Some(table_name) = rule.table.strip_prefix(&db_prefix) {
                copy_excludes.insert(table_name.to_string());

                let mut actual_from = rule.from.clone();
                let mut actual_till = rule.till.clone();

                if rule.method.as_deref().unwrap_or("time") == "time"
                    && (actual_from.is_none() || actual_till.is_none())
                {
                    info!(
                        "Discovering range for {db_name}.{table_name}.{}...",
                        rule.split_by_column
                    );
                    let pool = config.pool_cache.get(&config.source, db_name).await?;

                    if actual_from.is_none() {
                        let query = format!(
                            "SELECT min({})::text FROM {}",
                            rule.split_by_column, table_name
                        );
                        let row = pool.query_one(&query, &[]).await?;
                        actual_from = row.get(0);
                        if let Some(ref f) = actual_from {
                            info!("Discovered 'from' bound for {table_name}: {f}");
                        }
                    }
                    if actual_till.is_none() {
                        let query = format!(
                            "SELECT max({})::text FROM {}",
                            rule.split_by_column, table_name
                        );
                        let row = pool.query_one(&query, &[]).await?;
                        actual_till = row.get(0);
                        if let Some(ref t) = actual_till {
                            info!("Discovered 'till' bound for {table_name}: {t}");
                        }
                    }
                }

                let partitions = Splitter::split(
                    &rule.split_by_column,
                    actual_from.as_deref(),
                    actual_till.as_deref(),
                    rule.method.as_deref(),
                    config.max_parallel,
                )?;

                db_plan.copy_rules.push(CopyRulePlan {
                    table: table_name.to_string(),
                    column: rule.split_by_column.clone(),
                    method: rule.method.clone().unwrap_or_else(|| "time".to_string()),
                    from: actual_from,
                    till: actual_till,
                    partitions: partitions.len(),
                    rule_hash: rule.rule_hash(),
                });

                db_plan.regular_data_excludes.push(table_name.to_string());
            }
        }

        // 2. Identify delayed tables
        for delay in &config.delay_table_data {
            if let Some(table_pattern) = delay.strip_prefix(&db_prefix) {
                db_plan
                    .regular_data_excludes
                    .push(table_pattern.to_string());
                if !copy_excludes.contains(table_pattern) {
                    db_plan.delayed_tables.push(table_pattern.to_string());
                }
            }
        }

        db_plans.push(db_plan);

        if cancel.is_cancelled() {
            return Err(Error::Cancelled("planning interrupted".to_string()));
        }
    }

    Ok(MigrationPlan {
        databases: db_plans,
    })
}

impl MigrationPlan {
    pub fn print(&self) {
        info!("=== Migration Plan ===");
        for db in &self.databases {
            info!("Database: {}", db.name);
            info!("  Size: {}", HumanBytes(db.size));

            if !db.regular_data_excludes.is_empty() {
                info!(
                    "  Excluded from regular data dump: {:?}",
                    db.regular_data_excludes
                );
            }

            if !db.delayed_tables.is_empty() {
                info!(
                    "  Delayed tables (regular pg_dump): {:?}",
                    db.delayed_tables
                );
            }

            for rule in &db.copy_rules {
                info!("  Copy engine table: {}", rule.table);
                info!("    Column: {}", rule.column);
                info!("    Method: {}", rule.method);
                if let Some(from) = &rule.from {
                    info!("    From:   {from}");
                }
                if let Some(till) = &rule.till {
                    info!("    Till:   {till}");
                }
                info!("    Partitions: {}", rule.partitions);
            }
        }
        info!("======================");
    }
}

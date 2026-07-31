use crate::db;
use clap::Parser;
use serde::Deserialize;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;
use wildmatch::WildMatch;

fn default_split_by_column() -> String {
    "created_at".to_string()
}

#[derive(Debug, Deserialize, Clone, Hash)]
pub struct CopyRule {
    pub table: String,
    #[serde(default = "default_split_by_column")]
    pub split_by_column: String,
    pub from: Option<String>,
    pub till: Option<String>,
    pub method: Option<String>,
}

impl CopyRule {
    #[must_use]
    pub fn rule_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TablePattern {
    Db(String),
    DbSchema(String, WildMatch),
    DbSchemaTable(String, WildMatch, WildMatch),
}

impl TablePattern {
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        match parts.as_slice() {
            [db] if !db.is_empty() => Some(Self::Db((*db).to_string())),
            [db, schema] if !db.is_empty() && !schema.is_empty() => {
                Some(Self::DbSchema((*db).to_string(), WildMatch::new(schema)))
            }
            [db, schema, table] if !db.is_empty() && !schema.is_empty() && !table.is_empty() => {
                Some(Self::DbSchemaTable(
                    (*db).to_string(),
                    WildMatch::new(schema),
                    WildMatch::new(table),
                ))
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn matches(&self, db_name: &str, schema: &str, table: &str) -> bool {
        match self {
            Self::Db(p_db) => p_db == db_name,
            Self::DbSchema(p_db, p_schema) => p_db == db_name && p_schema.matches(schema),
            Self::DbSchemaTable(p_db, p_schema, p_table) => {
                p_db == db_name && p_schema.matches(schema) && p_table.matches(table)
            }
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
pub struct Config {
    pub source: db::DbArgs,
    pub source_db: String,

    pub destination: db::DbArgs,
    pub destination_db: String,

    pub dump_root: PathBuf,
    pub dump_parallel: usize,
    pub dump_jobs: usize,
    pub zstd_level: u8,

    pub restore_jobs: usize,
    pub restore_parallel: usize,
    pub restore_single_transaction: bool,

    pub max_parallel: usize,
    pub migrate_globals: bool,
    pub delay_table_data: Vec<String>,
    pub exclude: Vec<String>,

    pub verify_sem: Arc<Semaphore>,
    pub fast_verify: bool,
    pub verify_concurrency: usize,

    pub copy_buffer_size: u64,
    pub copy_report_interval: u64,

    pub pool_cache: db::PoolCache,

    pub ssl_mode: String,

    pub copy_rules: Vec<CopyRule>,

    pub confirm_delayed: bool,

    pub(crate) exclude_patterns: Vec<TablePattern>,
    pub(crate) deferred_patterns: Vec<TablePattern>,
    pub(crate) copy_rule_patterns: Vec<TablePattern>,
}

impl Config {
    #[must_use]
    pub fn deferred_table_patterns(&self) -> Vec<String> {
        deferred_table_patterns_iter(&self.delay_table_data, &self.copy_rules)
            .map(String::from)
            .collect()
    }

    #[must_use]
    pub fn is_db_excluded(&self, db_name: &str) -> bool {
        self.exclude_patterns.iter().any(|p| match p {
            TablePattern::Db(p_db) => p_db == db_name,
            TablePattern::DbSchema(p_db, p_schema) if p_db == db_name => p_schema.matches("*"),
            TablePattern::DbSchemaTable(p_db, p_schema, p_table) if p_db == db_name => {
                p_schema.matches("*") && p_table.matches("*")
            }
            _ => false,
        })
    }

    #[must_use]
    pub fn is_table_excluded(&self, db_name: &str, schema: &str, table: &str) -> bool {
        self.exclude_patterns
            .iter()
            .any(|p| p.matches(db_name, schema, table))
    }

    #[must_use]
    pub fn is_delayed_table(&self, db_name: &str, schema: &str, table: &str) -> bool {
        self.deferred_patterns
            .iter()
            .any(|p| p.matches(db_name, schema, table))
    }
}

pub fn deferred_table_patterns_iter<'a>(
    delay_table_data: &'a [String],
    copy_rules: &'a [CopyRule],
) -> impl Iterator<Item = &'a str> {
    delay_table_data
        .iter()
        .map(String::as_str)
        .chain(copy_rules.iter().map(|rule| rule.table.as_str()))
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct TomlConfig {
    pub dump_jobs: usize,
    pub restore_jobs: usize,
    pub max_parallel: usize,
    pub dump_parallel: Option<usize>,
    pub restore_parallel: Option<usize>,
    pub dump_root: String,
    pub migrate_globals: bool,
    pub delay_table_data: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub fast_verify: bool,
    pub verify_concurrency: usize,
    pub zstd_level: u8,
    pub sslmode: String,
    pub copy_rules: Option<Vec<CopyRule>>,
    pub restore_single_transaction: bool,
    pub copy_buffer_size_mb: Option<usize>,
}

impl Default for TomlConfig {
    fn default() -> Self {
        Self {
            dump_jobs: 24,
            restore_jobs: 12,
            max_parallel: 6,
            dump_parallel: None,
            restore_parallel: None,
            dump_root: "pg_dumps".to_string(),
            migrate_globals: true,
            delay_table_data: None,
            exclude: None,
            fast_verify: false,
            verify_concurrency: 16,
            zstd_level: 5,
            sslmode: "prefer".to_string(),
            copy_rules: None,
            restore_single_transaction: true,
            copy_buffer_size_mb: None,
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    #[arg(long, default_value = "localhost")]
    pub from_host: String,
    #[arg(long, default_value_t = 5432)]
    pub from_port: u16,
    #[arg(long, default_value = "postgres")]
    pub from_user: String,
    #[arg(long, default_value = "oldpass")]
    pub from_pass: String,
    #[arg(long, default_value = "postgres")]
    pub from_db: String,

    #[arg(long, default_value = "localhost")]
    pub to_host: String,
    #[arg(long, default_value_t = 5432)]
    pub to_port: u16,
    #[arg(long, default_value = "postgres")]
    pub to_user: String,
    #[arg(long, default_value = "newpass")]
    pub to_pass: String,
    #[arg(long, default_value = "postgres")]
    pub to_db: String,

    #[arg(long)]
    pub sslmode: Option<String>,

    #[arg(long)]
    pub confirm_delayed: bool,
}

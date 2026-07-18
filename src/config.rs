//! Configuration: CLI arguments, the TOML config file, and the resolved
//! [`Config`] consumed by the rest of the migrator, plus the filesystem paths
//! used for state and verification markers.

use crate::db;
use crate::error::{Error, Result};
use crate::tls;
use clap::Parser;
use log::{info, warn};
use serde::Deserialize;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{env, fs};
use tokio::sync::Semaphore;
use wildmatch::WildMatch;

/// Default config file looked up (relative to the working directory) when
/// `--config` is not given.
const DEFAULT_CONFIG_PATH: &str = "config.toml";

fn default_split_by_column() -> String {
    "created_at".to_string()
}

/// A copy-engine rule: a table to migrate via streaming `COPY` and how to split
/// it into parallel partitions.
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
    /// Stable identity of this rule, used to name its completion marker so a
    /// resumed run can skip rules that already finished.
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

/// Fully resolved configuration shared across the migration.
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

    pub pool_cache: db::PoolCache,

    /// Normalised `sslmode` (`disable`/`prefer`/`require`) applied to all
    /// native `tokio-postgres` connections, including the copy engine.
    pub ssl_mode: String,

    pub copy_rules: Vec<CopyRule>,

    /// Pre-compiled patterns for efficient matching.
    pub(crate) exclude_patterns: Vec<TablePattern>,
    pub(crate) deferred_patterns: Vec<TablePattern>,
    pub(crate) copy_rule_patterns: Vec<TablePattern>,
}

impl Config {
    /// Table patterns whose data is deferred out of the regular
    /// dump/restore/verify pass.
    ///
    /// This is the union of the configured `delay_table_data` patterns and
    /// every copy-engine rule's table: copy-engine tables are migrated
    /// separately (and excluded from the delayed `pg_dump`), so verification
    /// must also treat them as deferred — otherwise the regular pass would
    /// compare their row counts before the copy engine has run.
    #[must_use]
    pub fn deferred_table_patterns(&self) -> Vec<String> {
        deferred_table_patterns_iter(&self.delay_table_data, &self.copy_rules)
            .map(String::from)
            .collect()
    }

    /// Returns whether a database is entirely excluded from migration.
    /// A database is excluded if there is a pattern `DB`, `DB.*`, or `DB.*.*`
    /// in the `exclude` list.
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

    /// Returns whether a table is excluded from migration.
    #[must_use]
    pub fn is_table_excluded(&self, db_name: &str, schema: &str, table: &str) -> bool {
        self.exclude_patterns
            .iter()
            .any(|p| p.matches(db_name, schema, table))
    }

    /// Returns whether a table is deferred out of the regular pass.
    #[must_use]
    pub fn is_delayed_table(&self, db_name: &str, schema: &str, table: &str) -> bool {
        self.deferred_patterns
            .iter()
            .any(|p| p.matches(db_name, schema, table))
    }
}

fn deferred_table_patterns_iter<'a>(
    delay_table_data: &'a [String],
    copy_rules: &'a [CopyRule],
) -> impl Iterator<Item = &'a str> {
    delay_table_data
        .iter()
        .map(String::as_str)
        .chain(copy_rules.iter().map(|rule| rule.table.as_str()))
}

/// Returns the user's home directory.
///
/// # Errors
///
/// Returns an error if the `HOME` environment variable is not set.
pub fn home() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| Error::Env("HOME environment variable not set".into()))
}

/// Returns the directory used for state markers.
///
/// # Errors
///
/// See [`home`].
pub fn state_dir() -> Result<PathBuf> {
    Ok(home()?.join("pg_migrate_state"))
}

/// Returns the directory used for verification markers.
///
/// # Errors
///
/// See [`home`].
pub fn verify_dir() -> Result<PathBuf> {
    Ok(home()?.join("pg_verify_state"))
}

/// Raw config file contents, deserialized straight from TOML. Optional fields
/// distinguish "absent" from an explicit empty value; defaults are applied in
/// [`build_config`].
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
        }
    }
}

/// Command-line arguments. Connection settings come from flags; everything else
/// comes from the TOML config file.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to the TOML config file. Defaults to `config.toml` in the working
    /// directory; if that default is absent, built-in defaults are used. When
    /// this flag is given explicitly, the file must exist and parse.
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[arg(long, default_value = "localhost")]
    from_host: String,
    #[arg(long, default_value_t = 5432)]
    from_port: u16,
    #[arg(long, default_value = "postgres")]
    from_user: String,
    #[arg(long, default_value = "oldpass")]
    from_pass: String,
    #[arg(long, default_value = "postgres")]
    from_db: String,

    #[arg(long, default_value = "localhost")]
    to_host: String,
    #[arg(long, default_value_t = 5432)]
    to_port: u16,
    #[arg(long, default_value = "postgres")]
    to_user: String,
    #[arg(long, default_value = "newpass")]
    to_pass: String,
    #[arg(long, default_value = "postgres")]
    to_db: String,

    /// TLS mode for native connections: disable, prefer, or require.
    /// Overrides the `sslmode` value from the config file when set.
    #[arg(long)]
    sslmode: Option<String>,
}

/// Loads the TOML config.
///
/// - `Some(path)`: the user asked for a specific file, so it must exist and
///   parse — otherwise this is a hard error. This stops a typo like
///   `--config config.yaml` from silently falling back to defaults (which have
///   no `copy_rules`/`delay_table_data`, sending every table down the regular
///   path).
/// - `None`: try `config.toml` in the working directory; if absent, use the
///   built-in defaults.
///
/// # Errors
///
/// Returns an error if an explicitly requested file cannot be read, or if a
/// config file is present but is not valid TOML.
fn load_toml_config(path: Option<&Path>) -> Result<TomlConfig> {
    if let Some(path) = path {
        let content = fs::read_to_string(path).map_err(|e| {
            Error::Config(format!("failed to read config file '{}': {e}", path.display()).into())
        })?;
        return Ok(toml::from_str(&content)?);
    }

    let default = Path::new(DEFAULT_CONFIG_PATH);
    if default.exists() {
        Ok(toml::from_str(&fs::read_to_string(default)?)?)
    } else {
        info!("No {DEFAULT_CONFIG_PATH} found, using built-in defaults");
        Ok(TomlConfig::default())
    }
}

/// Resolves [`Args`] and the TOML config into the shared [`Config`].
///
/// # Errors
///
/// Returns an error if the config file cannot be loaded (see
/// [`load_toml_config`]) or if a copy rule is malformed (see
/// [`validate_copy_rules`]).
pub fn build_config(args: Args) -> Result<Arc<Config>> {
    let toml_config = load_toml_config(args.config.as_deref())?;

    let verify_concurrency = toml_config.verify_concurrency.max(1);
    let dump_parallel = toml_config
        .dump_parallel
        .unwrap_or(toml_config.max_parallel)
        .max(1);
    let restore_parallel = toml_config
        .restore_parallel
        .unwrap_or(toml_config.max_parallel)
        .max(1);

    let zstd_level = if (1..=22).contains(&toml_config.zstd_level) {
        toml_config.zstd_level
    } else {
        warn!(
            "Invalid zstd_level: {}, must be between 1 and 22. Using default: 5",
            toml_config.zstd_level
        );
        5
    };

    let sslmode_raw = args.sslmode.unwrap_or(toml_config.sslmode);
    let ssl_mode = tls::parse_ssl_mode(&sslmode_raw);
    let ssl_mode_label = tls::ssl_mode_str(ssl_mode);
    info!("Using sslmode={ssl_mode_label} for native connections");

    let copy_rules = toml_config.copy_rules.unwrap_or_default();
    validate_copy_rules(&copy_rules)?;

    let delay_table_data = toml_config.delay_table_data.unwrap_or_default();
    validate_delay_table_data(&delay_table_data)?;

    let exclude = toml_config.exclude.unwrap_or_default();
    validate_exclude_patterns(&exclude)?;

    let exclude_patterns = exclude
        .iter()
        .filter_map(|s| TablePattern::parse(s))
        .collect();

    let deferred_patterns = deferred_table_patterns_iter(&delay_table_data, &copy_rules)
        .filter_map(TablePattern::parse)
        .collect();

    let copy_rule_patterns = copy_rules
        .iter()
        .filter_map(|r| TablePattern::parse(&r.table))
        .collect();

    Ok(Arc::new(Config {
        source: db::DbArgs {
            host: args.from_host.into(),
            port: args.from_port,
            user: args.from_user.into(),
            pass: args.from_pass.into(),
        },
        source_db: args.from_db,
        destination: db::DbArgs {
            host: args.to_host.into(),
            port: args.to_port,
            user: args.to_user.into(),
            pass: args.to_pass.into(),
        },
        destination_db: args.to_db,
        dump_jobs: toml_config.dump_jobs,
        restore_jobs: toml_config.restore_jobs,
        restore_single_transaction: toml_config.restore_single_transaction,
        max_parallel: toml_config.max_parallel,
        dump_parallel,
        restore_parallel,
        dump_root: toml_config.dump_root.into(),
        migrate_globals: toml_config.migrate_globals,
        delay_table_data,
        exclude,
        fast_verify: toml_config.fast_verify,
        verify_concurrency,
        pool_cache: db::PoolCache::new(ssl_mode),
        verify_sem: Arc::new(Semaphore::new(verify_concurrency)),
        zstd_level,
        ssl_mode: ssl_mode_label.to_string(),
        copy_rules,
        exclude_patterns,
        deferred_patterns,
        copy_rule_patterns,
    }))
}

/// Splits a fully-qualified `DATABASE.SCHEMA.TABLE` entry into its three parts.
///
/// Returns `None` unless the entry has exactly three dot-separated, non-empty
/// components.
fn parse_fully_qualified(entry: &str) -> Option<(&str, &str, &str)> {
    let mut parts = entry.split('.');
    let (db, schema, table) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || db.is_empty() || schema.is_empty() || table.is_empty() {
        return None;
    }
    Some((db, schema, table))
}

/// Returns true if the string is a valid pattern (1, 2, or 3 dot-separated,
/// non-empty parts).
fn is_valid_pattern(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.is_empty() || parts.len() > 3 {
        return false;
    }
    parts.iter().all(|p| !p.is_empty())
}

/// Validates that every copy rule targets a fully-qualified
/// `DATABASE.SCHEMA.TABLE`.
///
/// Schema qualification is mandatory: a bare or schema-less name resolves
/// against the destination role's `search_path`, which can omit `public` and
/// make an existing table look missing (and it also never matches any database
/// during planning, silently skipping the rule). Requiring all three components
/// turns those silent or environment-dependent failures into an explicit error.
///
/// # Errors
///
/// Returns [`Error::InvalidCopyRule`] when a rule's table is not in
/// `DATABASE.SCHEMA.TABLE` form.
fn validate_copy_rules(rules: &[CopyRule]) -> Result<()> {
    for rule in rules {
        if parse_fully_qualified(&rule.table).is_none() {
            return Err(Error::InvalidCopyRule {
                table: rule.table.clone(),
                reason: "expected 'DATABASE.SCHEMA.TABLE' format with all parts non-empty".into(),
            });
        }
    }
    Ok(())
}

/// Validates that every `delay_table_data` entry is a fully-qualified
/// `DATABASE.SCHEMA.TABLE` pattern.
///
/// As with copy rules, the schema must be explicit so the deferred `pg_dump`
/// `--table`/`--exclude-table` patterns and the verification matcher are
/// unambiguous. The `SCHEMA` and `TABLE` parts may use `pg_dump` wildcards
/// (e.g. `mydb.public.events_*` or `mydb.audit.*`).
///
/// # Errors
///
/// Returns [`Error::InvalidCopyRule`] when an entry is not in
/// `DATABASE.SCHEMA.TABLE` form.
pub fn validate_delay_table_data(patterns: &[String]) -> Result<()> {
    for pattern in patterns {
        if !is_valid_pattern(pattern) {
            return Err(Error::InvalidCopyRule {
                table: pattern.clone(),
                reason: "delay_table_data entry must be 'DB', 'DB.SCHEMA', or \
                         'DB.SCHEMA.TABLE' with all parts non-empty"
                    .into(),
            });
        }
    }
    Ok(())
}

/// Validates that every `exclude` entry is a fully-qualified
/// `DATABASE.SCHEMA.TABLE` pattern.
///
/// # Errors
///
/// Returns [`Error::InvalidCopyRule`] when an entry is not in
/// `DATABASE.SCHEMA.TABLE` form.
pub fn validate_exclude_patterns(patterns: &[String]) -> Result<()> {
    for pattern in patterns {
        if !is_valid_pattern(pattern) {
            return Err(Error::InvalidCopyRule {
                table: pattern.clone(),
                reason: "exclude entry must be 'DB', 'DB.SCHEMA', or 'DB.SCHEMA.TABLE' with \
                         all parts non-empty"
                    .into(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copy_rule(table: &str) -> CopyRule {
        CopyRule {
            table: table.to_string(),
            split_by_column: default_split_by_column(),
            from: None,
            till: None,
            method: None,
        }
    }

    #[test]
    fn toml_parsing_missing_delay_table_data() -> Result<()> {
        let config: TomlConfig = toml::from_str("dump_root = \"/tmp\"")?;
        assert!(config.delay_table_data.is_none());
        Ok(())
    }

    #[test]
    fn toml_parsing_empty_string() -> Result<()> {
        let config: TomlConfig = toml::from_str("")?;
        assert!(config.delay_table_data.is_none());
        Ok(())
    }

    #[test]
    fn toml_parsing_empty_list_delay_table_data() -> Result<()> {
        let config: TomlConfig = toml::from_str("delay_table_data = []")?;
        assert!(
            config
                .delay_table_data
                .as_ref()
                .ok_or_else(|| Error::Config("delay_table_data should be Some".into()))?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn toml_parsing_copy_rules() -> Result<()> {
        let toml = "
[[copy_rules]]
table = \"mydb.public.large_table\"
split_by_column = \"created_at\"
from = \"2023-01-01\"
till = \"2024-01-01\"
";
        let config: TomlConfig = toml::from_str(toml)?;
        let rules = config
            .copy_rules
            .ok_or_else(|| Error::Config("copy_rules should be Some".into()))?;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].table, "mydb.public.large_table");
        assert_eq!(rules[0].split_by_column, "created_at");
        assert_eq!(rules[0].from.as_deref(), Some("2023-01-01"));
        assert_eq!(rules[0].till.as_deref(), Some("2024-01-01"));
        assert!(rules[0].method.is_none());
        Ok(())
    }

    #[test]
    fn toml_parsing_copy_rules_hash_method() -> Result<()> {
        let toml = "
[[copy_rules]]
table = \"mydb.public.skewed_table\"
method = \"hash\"
";
        let config: TomlConfig = toml::from_str(toml)?;
        let rules = config
            .copy_rules
            .ok_or_else(|| Error::Config("copy_rules should be Some".into()))?;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].method.as_deref(), Some("hash"));
        Ok(())
    }

    #[test]
    fn toml_parsing_multiple_copy_rules_same_table() -> Result<()> {
        let toml = "
[[copy_rules]]
table = \"mydb.public.table1\"
from = \"2023-01-01\"
till = \"2023-02-01\"

[[copy_rules]]
table = \"mydb.public.table1\"
from = \"2023-02-01\"
till = \"2023-03-01\"
";
        let config: TomlConfig = toml::from_str(toml)?;
        let rules = config
            .copy_rules
            .ok_or_else(|| Error::Config("copy_rules should be Some".into()))?;
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].table, "mydb.public.table1");
        assert_eq!(rules[1].table, "mydb.public.table1");
        assert_ne!(rules[0].rule_hash(), rules[1].rule_hash());
        Ok(())
    }

    #[test]
    fn explicit_missing_config_is_an_error() {
        // An explicitly requested file that doesn't exist must fail loudly
        // rather than silently falling back to defaults (e.g. `--config
        // config.yaml` when only `config.toml` exists).
        let missing = Path::new("/nonexistent/pg-migrate/config.toml");
        assert!(matches!(
            load_toml_config(Some(missing)),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn validate_copy_rules_accepts_schema_qualified_tables() {
        let rules = [
            copy_rule("mydb.public.table1"),
            copy_rule("mydb.audit.table2"),
        ];
        assert!(validate_copy_rules(&rules).is_ok());
    }

    #[test]
    fn validate_copy_rules_rejects_bare_table() {
        let rules = [copy_rule("table1")];
        assert!(matches!(
            validate_copy_rules(&rules),
            Err(Error::InvalidCopyRule { .. })
        ));
    }

    #[test]
    fn validate_copy_rules_rejects_schema_less_table() {
        // Two parts (DATABASE.TABLE, no schema) is no longer accepted: the
        // schema must be explicit.
        let rules = [copy_rule("mydb.table1")];
        assert!(matches!(
            validate_copy_rules(&rules),
            Err(Error::InvalidCopyRule { .. })
        ));
    }

    #[test]
    fn validate_copy_rules_rejects_empty_parts() {
        for bad in [
            ".public.table1",
            "mydb..table1",
            "mydb.public.",
            "mydb.public.table.extra",
        ] {
            assert!(
                matches!(
                    validate_copy_rules(&[copy_rule(bad)]),
                    Err(Error::InvalidCopyRule { .. })
                ),
                "expected '{bad}' to be rejected"
            );
        }
    }

    #[test]
    fn validate_delay_table_data_accepts_flexible_patterns() {
        let patterns = vec![
            "mydb".to_string(),
            "mydb.public".to_string(),
            "mydb.public.events_*".to_string(),
            "mydb.audit.*".to_string(),
        ];
        assert!(validate_delay_table_data(&patterns).is_ok());
    }

    #[test]
    fn deferred_patterns_include_copy_rule_tables() {
        let delay = vec!["pdb1.public.table3".to_string()];
        let rules = vec![
            copy_rule("pdb2.public.events"),
            copy_rule("pdb1.public.audit"),
        ];

        let deferred: Vec<String> = deferred_table_patterns_iter(&delay, &rules)
            .map(String::from)
            .collect();

        assert_eq!(
            deferred,
            vec![
                "pdb1.public.table3".to_string(),
                "pdb2.public.events".to_string(),
                "pdb1.public.audit".to_string(),
            ]
        );

        let toml = TomlConfig {
            delay_table_data: Some(delay),
            copy_rules: Some(rules),
            ..Default::default()
        };
        let config = build_config_with_toml(toml);

        // A copy-engine table not covered by any delay pattern must still be
        // recognised as deferred, so the regular verification pass skips it.
        assert!(config.is_delayed_table("pdb2", "public", "events"));
    }

    #[test]
    fn test_is_db_excluded() {
        let toml = TomlConfig {
            exclude: Some(vec![
                "mydb.*.*".to_string(),
                "db1".to_string(),
                "db2.*".to_string(),
            ]),
            ..Default::default()
        };
        let config = build_config_with_toml(toml);

        assert!(config.is_db_excluded("mydb"));
        assert!(config.is_db_excluded("db1"));
        assert!(config.is_db_excluded("db2"));
        assert!(!config.is_db_excluded("otherdb"));
    }

    #[test]
    fn test_is_table_excluded() {
        let toml = TomlConfig {
            exclude: Some(vec![
                "mydb.public.secret".to_string(),
                "mydb.internal.*".to_string(),
                "otherdb.*.temp_*".to_string(),
                "db3".to_string(),
                "db4.audit".to_string(),
            ]),
            ..Default::default()
        };
        let config = build_config_with_toml(toml);

        assert!(config.is_table_excluded("mydb", "public", "secret"));
        assert!(config.is_table_excluded("mydb", "internal", "anything"));
        assert!(config.is_table_excluded("otherdb", "any", "temp_123"));
        assert!(config.is_table_excluded("db3", "any", "any"));
        assert!(config.is_table_excluded("db4", "audit", "any"));
        assert!(!config.is_table_excluded("db4", "public", "any"));
        assert!(!config.is_table_excluded("mydb", "public", "other"));
        assert!(!config.is_table_excluded("another", "public", "secret"));
    }

    fn build_config_with_toml(toml_config: TomlConfig) -> Arc<Config> {
        let verify_concurrency = toml_config.verify_concurrency.max(1);
        let dump_parallel = toml_config
            .dump_parallel
            .unwrap_or(toml_config.max_parallel)
            .max(1);
        let restore_parallel = toml_config
            .restore_parallel
            .unwrap_or(toml_config.max_parallel)
            .max(1);

        let ssl_mode = tokio_postgres::config::SslMode::Prefer;
        let ssl_mode_label = "prefer";

        let copy_rules = toml_config.copy_rules.unwrap_or_default();
        let delay_table_data = toml_config.delay_table_data.unwrap_or_default();
        let exclude = toml_config.exclude.unwrap_or_default();

        let exclude_patterns = exclude
            .iter()
            .filter_map(|s| TablePattern::parse(s))
            .collect();
        let deferred_patterns = deferred_table_patterns_iter(&delay_table_data, &copy_rules)
            .filter_map(TablePattern::parse)
            .collect();
        let copy_rule_patterns = copy_rules
            .iter()
            .filter_map(|r| TablePattern::parse(&r.table))
            .collect();

        Arc::new(Config {
            source: db::DbArgs {
                host: "localhost".into(),
                port: 5432,
                user: "postgres".into(),
                pass: "pass".into(),
            },
            source_db: "postgres".to_string(),
            destination: db::DbArgs {
                host: "localhost".into(),
                port: 5432,
                user: "postgres".into(),
                pass: "pass".into(),
            },
            destination_db: "postgres".to_string(),
            dump_jobs: toml_config.dump_jobs,
            restore_jobs: toml_config.restore_jobs,
            restore_single_transaction: toml_config.restore_single_transaction,
            max_parallel: toml_config.max_parallel,
            dump_parallel,
            restore_parallel,
            dump_root: toml_config.dump_root.into(),
            migrate_globals: toml_config.migrate_globals,
            delay_table_data,
            exclude,
            fast_verify: toml_config.fast_verify,
            verify_concurrency,
            pool_cache: db::PoolCache::new(ssl_mode),
            verify_sem: Arc::new(Semaphore::new(verify_concurrency)),
            zstd_level: 5,
            ssl_mode: ssl_mode_label.to_string(),
            copy_rules,
            exclude_patterns,
            deferred_patterns,
            copy_rule_patterns,
        })
    }
}

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

    pub max_parallel: usize,
    pub migrate_globals: bool,
    pub delay_table_data: Vec<String>,

    pub verify_sem: Arc<Semaphore>,
    pub fast_verify: bool,
    pub verify_concurrency: usize,

    pub pool_cache: db::PoolCache,

    /// Normalised `sslmode` (`disable`/`prefer`/`require`) applied to all
    /// native `tokio-postgres` connections, including the copy engine.
    pub ssl_mode: String,

    pub copy_rules: Vec<CopyRule>,
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
        deferred_table_patterns(&self.delay_table_data, &self.copy_rules)
    }
}

/// Builds the deferred-table pattern list from the raw config pieces.
///
/// Copy-engine rule tables are already in `DATABASE.SCHEMA.TABLE` form,
/// identical to `delay_table_data` entries, so they slot straight into the same
/// matching logic used by verification.
fn deferred_table_patterns(delay_table_data: &[String], copy_rules: &[CopyRule]) -> Vec<String> {
    delay_table_data
        .iter()
        .cloned()
        .chain(copy_rules.iter().map(|rule| rule.table.clone()))
        .collect()
}

/// Returns the user's home directory.
///
/// # Errors
///
/// Returns an error if the `HOME` environment variable is not set.
pub fn home() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| Error::Env("HOME environment variable not set".to_string()))
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
    pub fast_verify: bool,
    pub verify_concurrency: usize,
    pub zstd_level: u8,
    pub sslmode: String,
    pub copy_rules: Option<Vec<CopyRule>>,
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
            fast_verify: false,
            verify_concurrency: 16,
            zstd_level: 5,
            sslmode: "prefer".to_string(),
            copy_rules: None,
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
            Error::Config(format!(
                "failed to read config file '{}': {e}",
                path.display()
            ))
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

    Ok(Arc::new(Config {
        source: db::DbArgs {
            host: args.from_host,
            port: args.from_port,
            user: args.from_user,
            pass: args.from_pass,
        },
        source_db: args.from_db,
        destination: db::DbArgs {
            host: args.to_host,
            port: args.to_port,
            user: args.to_user,
            pass: args.to_pass,
        },
        destination_db: args.to_db,
        dump_jobs: toml_config.dump_jobs,
        restore_jobs: toml_config.restore_jobs,
        max_parallel: toml_config.max_parallel,
        dump_parallel,
        restore_parallel,
        dump_root: toml_config.dump_root.into(),
        migrate_globals: toml_config.migrate_globals,
        delay_table_data,
        fast_verify: toml_config.fast_verify,
        verify_concurrency,
        pool_cache: db::PoolCache::new(ssl_mode),
        verify_sem: Arc::new(Semaphore::new(verify_concurrency)),
        zstd_level,
        ssl_mode: ssl_mode_label.to_string(),
        copy_rules,
    }))
}

/// Splits a fully-qualified `DATABASE.SCHEMA.TABLE` entry into its three parts.
///
/// Returns `None` unless the entry has exactly three dot-separated, non-empty
/// components. The `SCHEMA` and `TABLE` components may carry `pg_dump` wildcards
/// (`*`/`?`) for `delay_table_data` patterns; the database component is always a
/// literal name. Table or schema identifiers containing a literal `.` are not
/// supported (they would parse as extra components).
fn parse_qualified(entry: &str) -> Option<(&str, &str, &str)> {
    let mut parts = entry.split('.');
    let (db, schema, table) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || db.is_empty() || schema.is_empty() || table.is_empty() {
        return None;
    }
    Some((db, schema, table))
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
        if parse_qualified(&rule.table).is_none() {
            return Err(Error::InvalidCopyRule {
                table: rule.table.clone(),
                reason: "expected 'DATABASE.SCHEMA.TABLE' format with all parts non-empty"
                    .to_string(),
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
fn validate_delay_table_data(patterns: &[String]) -> Result<()> {
    for pattern in patterns {
        if parse_qualified(pattern).is_none() {
            return Err(Error::InvalidCopyRule {
                table: pattern.clone(),
                reason: "delay_table_data entry must be 'DATABASE.SCHEMA.TABLE' with all parts \
                         non-empty"
                    .to_string(),
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
    fn validate_delay_table_data_accepts_schema_qualified_patterns() {
        let patterns = vec![
            "mydb.public.events_*".to_string(),
            "mydb.audit.*".to_string(),
        ];
        assert!(validate_delay_table_data(&patterns).is_ok());
    }

    #[test]
    fn validate_delay_table_data_rejects_schema_less_pattern() {
        let patterns = vec!["mydb.events_*".to_string()];
        assert!(matches!(
            validate_delay_table_data(&patterns),
            Err(Error::InvalidCopyRule { .. })
        ));
    }

    #[test]
    fn deferred_patterns_include_copy_rule_tables() {
        let delay = vec!["pdb1.public.table3".to_string()];
        let rules = [
            copy_rule("pdb2.public.events"),
            copy_rule("pdb1.public.audit"),
        ];

        let deferred = deferred_table_patterns(&delay, &rules);

        assert_eq!(
            deferred,
            vec![
                "pdb1.public.table3".to_string(),
                "pdb2.public.events".to_string(),
                "pdb1.public.audit".to_string(),
            ]
        );

        // A copy-engine table not covered by any delay pattern must still be
        // recognised as deferred, so the regular verification pass skips it.
        assert!(crate::verification::is_delayed_table(
            "pdb2", "public", "events", &deferred
        ));
    }
}

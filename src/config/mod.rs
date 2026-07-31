use crate::db;
use crate::error::{Error, Result};
use crate::tls;
use log::{info, warn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{env, fs};
use tokio::sync::Semaphore;

mod types;
mod validation;

pub use types::{Args, Config, CopyRule, TablePattern, TomlConfig};
pub use validation::{validate_copy_rules, validate_delay_table_data, validate_exclude_patterns};

const DEFAULT_CONFIG_PATH: &str = "config.toml";

pub fn home() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| Error::Env("HOME environment variable not set".into()))
}

pub fn state_dir() -> Result<PathBuf> {
    Ok(home()?.join("pg_migrate_state"))
}

pub fn verify_dir() -> Result<PathBuf> {
    Ok(home()?.join("pg_verify_state"))
}

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

    let deferred_patterns = types::deferred_table_patterns_iter(&delay_table_data, &copy_rules)
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
        confirm_delayed: args.confirm_delayed,

        copy_buffer_size: toml_config.copy_buffer_size_mb.unwrap_or(32).max(1) as u64 * 1024 * 1024,
        copy_report_interval: 10 * 1024 * 1024,

        exclude_patterns,
        deferred_patterns,
        copy_rule_patterns,
    }))
}

#[must_use]
pub fn get_test_config(toml_config: TomlConfig) -> Arc<Config> {
    build_config_with_toml(toml_config)
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
    let deferred_patterns = types::deferred_table_patterns_iter(&delay_table_data, &copy_rules)
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
        confirm_delayed: false,
        copy_buffer_size: toml_config.copy_buffer_size_mb.unwrap_or(32).max(1) as u64 * 1024 * 1024,
        copy_report_interval: 10 * 1024 * 1024,
        exclude_patterns,
        deferred_patterns,
        copy_rule_patterns,
    })
}

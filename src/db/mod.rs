mod client;
pub mod dump_restore;
mod globals;
mod types;

pub use client::{create_dbs, discover_databases};
pub use dump_restore::{
    copy_rule_done_marker, dump_db, dump_delayed_data, restore_db, restore_delayed_data,
};
pub use globals::{filter_globals_sql, migrate_globals};
pub use types::{DbArgs, MigrationState, PoolCache, PoolKey};

/// Quotes a `PostgreSQL` identifier (table name, column name, etc.) by wrapping
/// it in double quotes and escaping any existing double quotes.
#[must_use]
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quotes a schema-qualified table name (e.g. `schema.table` into `"schema"."table"`).
#[must_use]
pub fn quote_table_name(name: &str) -> String {
    name.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

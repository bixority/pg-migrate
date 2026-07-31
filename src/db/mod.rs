mod types;
mod client;
pub mod dump_restore;
mod globals;

pub use types::{DbArgs, MigrationState, PoolCache, PoolKey};
pub use client::{discover_databases, create_dbs};
pub use dump_restore::{dump_db, restore_db, dump_delayed_data, restore_delayed_data, copy_rule_done_marker};
pub use globals::{migrate_globals, filter_globals_sql};

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

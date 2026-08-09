use pg_migrate::db::{filter_globals_sql, quote_ident, quote_literal, quote_table_name, DiscoveredDb};

#[test]
fn test_filter_globals_sql() {
    let sql = "\
CREATE ROLE postgres;
ALTER ROLE postgres WITH PASSWORD 'abc';
CREATE ROLE other;
ALTER ROLE other WITH PASSWORD 'xyz';
";
    let filtered = filter_globals_sql(sql, "postgres");
    assert!(!filtered.contains("CREATE ROLE postgres;"));
    assert!(!filtered.contains("ALTER ROLE postgres WITH PASSWORD"));
    assert!(filtered.contains("CREATE ROLE other;"));
    assert!(filtered.contains("ALTER ROLE other WITH PASSWORD"));
}

#[test]
fn test_filter_globals_sql_with_spaces() {
    let sql = "CREATE ROLE postgres WITH LOGIN;";
    let filtered = filter_globals_sql(sql, "postgres");
    assert_eq!(filtered, "");
}

#[test]
fn test_filter_globals_sql_psql_meta() {
    let sql = "\
-- comment
\\restrict token
SET x = y;
\\unrestrict token
";
    let filtered = filter_globals_sql(sql, "postgres");
    assert!(filtered.contains("-- comment"));
    assert!(!filtered.contains("\\restrict token"));
    assert!(!filtered.contains("\\unrestrict token"));
    assert!(filtered.contains("SET x = y;"));
}

#[test]
fn test_filter_globals_sql_quoted_role() {
    let sql = "CREATE ROLE \"postgres\" WITH LOGIN;";
    let filtered = filter_globals_sql(sql, "postgres");
    assert_eq!(filtered, "");
}

#[test]
fn test_quote_ident() {
    assert_eq!(quote_ident("users"), "\"users\"");
    assert_eq!(quote_ident("user\"s"), "\"user\"\"s\"");
}

#[test]
fn test_quote_table_name() {
    assert_eq!(quote_table_name("users"), "\"users\"");
    assert_eq!(quote_table_name("public.users"), "\"public\".\"users\"");
    assert_eq!(
        quote_table_name("my schema.my table"),
        "\"my schema\".\"my table\""
    );
}

#[test]
fn test_quote_literal() {
    assert_eq!(quote_literal("UTF8"), "'UTF8'");
    assert_eq!(quote_literal("en_US.UTF-8"), "'en_US.UTF-8'");
    assert_eq!(quote_literal("O'Reilly"), "'O''Reilly'");
}

#[test]
fn test_discovered_db_struct() {
    let db = DiscoveredDb {
        name: "mydb".to_string(),
        size: 1024,
        encoding: "UTF8".to_string(),
        datcollate: "en_US.UTF-8".to_string(),
        datctype: "en_US.UTF-8".to_string(),
        owner: "dev".to_string(),
    };
    assert_eq!(db.name, "mydb");
    assert_eq!(db.size, 1024);
    assert_eq!(db.encoding, "UTF8");
    assert_eq!(db.datcollate, "en_US.UTF-8");
    assert_eq!(db.datctype, "en_US.UTF-8");
    assert_eq!(db.owner, "dev");
}

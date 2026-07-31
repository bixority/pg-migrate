use pg_migrate::config::{CopyRule, TablePattern, TomlConfig, get_test_config, validate_copy_rules, validate_delay_table_data};
use pg_migrate::error::{Error, Result};
use wildmatch::WildMatch;

fn copy_rule(table: &str) -> CopyRule {
    CopyRule {
        table: table.to_string(),
        split_by_column: "created_at".to_string(),
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
fn test_is_db_excluded() {
    let toml = TomlConfig {
        exclude: Some(vec![
            "mydb.*.*".to_string(),
            "db1".to_string(),
            "db2.*".to_string(),
        ]),
        ..Default::default()
    };
    let config = get_test_config(toml);

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
    let config = get_test_config(toml);

    assert!(config.is_table_excluded("mydb", "public", "secret"));
    assert!(config.is_table_excluded("mydb", "internal", "anything"));
    assert!(config.is_table_excluded("otherdb", "any", "temp_123"));
    assert!(config.is_table_excluded("db3", "any", "any"));
    assert!(config.is_table_excluded("db4", "audit", "any"));
    assert!(!config.is_table_excluded("db4", "public", "any"));
    assert!(!config.is_table_excluded("mydb", "public", "other"));
    assert!(!config.is_table_excluded("another", "public", "secret"));
}

#[test]
fn test_table_pattern_parse() {
    assert_eq!(
        TablePattern::parse("db.schema.table").expect("valid pattern"),
        TablePattern::DbSchemaTable(
            "db".to_string(),
            WildMatch::new("schema"),
            WildMatch::new("table")
        )
    );

    assert_eq!(
        TablePattern::parse("db.table").expect("valid pattern"),
        TablePattern::DbSchema("db".to_string(), WildMatch::new("table"))
    );

    assert_eq!(
        TablePattern::parse("db").expect("valid pattern"),
        TablePattern::Db("db".to_string())
    );

    assert!(TablePattern::parse("").is_none());
}

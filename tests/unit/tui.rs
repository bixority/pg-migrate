use pg_migrate::plan::{CopyRulePlan, DatabasePlan, MigrationPlan};
use pg_migrate::tui::{MigrationStates, render_verification_report};
use std::collections::BTreeMap;
use std::sync::Arc;

fn db_plan(name: &str, delayed: Vec<String>, copy: Vec<CopyRulePlan>) -> DatabasePlan {
    DatabasePlan {
        name: name.to_string(),
        size: 4096,
        regular_data_excludes: Vec::new(),
        full_excludes: Vec::new(),
        delayed_tables: delayed,
        copy_rules: copy,
        regular_table_names: Vec::new(),
        delayed_table_names: Vec::new(),
        copy_table_names: Vec::new(),
    }
}

fn copy_rule_plan(table: &str) -> CopyRulePlan {
    CopyRulePlan {
        table: table.to_string(),
        column: "created_at".to_string(),
        method: "time".to_string(),
        from: None,
        till: None,
        partitions: 1,
        rule_hash: 0,
    }
}

#[test]
fn delayed_row_created_for_delayed_tables() {
    let plan = MigrationPlan {
        databases: vec![Arc::new(db_plan(
            "pdb1",
            vec!["public.bigtable".to_string()],
            vec![],
        ))],
    };
    let table = MigrationStates::new(&plan).render_table();
    assert!(table.contains("pdb1 (delayed)"), "table was:\n{table}");
}

#[test]
fn delayed_row_created_for_copy_rules_only() {
    let plan = MigrationPlan {
        databases: vec![Arc::new(db_plan(
            "pdb2",
            vec![],
            vec![copy_rule_plan("public.events")],
        ))],
    };
    let table = MigrationStates::new(&plan).render_table();
    assert!(table.contains("pdb2 (delayed)"), "table was:\n{table}");
}

#[test]
fn no_delayed_row_without_delayed_work() {
    let plan = MigrationPlan {
        databases: vec![Arc::new(db_plan("plain", vec![], vec![]))],
    };
    let table = MigrationStates::new(&plan).render_table();
    assert!(table.contains("plain"));
    assert!(!table.contains("(delayed)"), "table was:\n{table}");
}

#[test]
fn test_render_verification_report() {
    let mut src = BTreeMap::new();
    src.insert("public.users".to_string(), "100".to_string());
    src.insert("public.posts".to_string(), "50".to_string());

    let mut dst = BTreeMap::new();
    dst.insert("public.users".to_string(), "100".to_string());
    dst.insert("public.posts".to_string(), "40".to_string());
    dst.insert("public.comments".to_string(), "10".to_string());

    let (report, mismatch) = render_verification_report("mydb", &src, &dst);
    assert!(mismatch);
    assert!(report.contains("mydb"));
    assert!(report.contains("public.users"));
    assert!(report.contains("100"));
    assert!(report.contains("OK"));
    assert!(report.contains("public.posts"));
    assert!(report.contains("MISMATCH"));
    assert!(report.contains("public.comments"));
    assert!(report.contains("MISSING"));
}

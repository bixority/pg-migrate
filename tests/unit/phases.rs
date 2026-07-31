use pg_migrate::error::{Error, MigrationPhase};
use pg_migrate::phases::{wait_for_delayed_tasks, PipelineArgs};
use pg_migrate::phases::delayed::run_delayed_pipeline_steps_internal;
use pg_migrate::phases::copy::migrate_copy_rules_internal;
use pg_migrate::plan::{DatabasePlan, MigrationPlan, CopyRulePlan};
use pg_migrate::config::{TomlConfig, get_test_config};
use pg_migrate::tui;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

fn mock_db_plan(name: &str) -> DatabasePlan {
    DatabasePlan {
        name: name.to_string(),
        size: 100,
        regular_data_excludes: vec![],
        full_excludes: vec![],
        delayed_tables: vec![],
        copy_rules: vec![],
        regular_table_names: vec![],
        delayed_table_names: vec![],
        copy_table_names: vec![],
    }
}

fn mock_copy_rule(table: &str) -> CopyRulePlan {
    CopyRulePlan {
        table: table.to_string(),
        column: "id".into(),
        method: "time".into(),
        from: None,
        till: None,
        partitions: 4,
        rule_hash: 12345,
    }
}

#[tokio::test]
async fn test_wait_for_delayed_tasks_aborts_all_on_failure() {
    let mut set = JoinSet::new();
    let cancel = CancellationToken::new();

    set.spawn(async { 
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(()) 
    });
    set.spawn(async { 
        Err(Error::Config("worker failed".into())) 
    });

    let res = wait_for_delayed_tasks(&mut set, &cancel).await;
    
    assert!(res.is_err());
    assert!(cancel.is_cancelled());
}

#[tokio::test]
async fn test_migrate_copy_rules_internal_failure() {
    let mut db_plan_raw = mock_db_plan("test_db");
    db_plan_raw.copy_rules.push(mock_copy_rule("table1"));
    db_plan_raw.copy_rules.push(mock_copy_rule("table2"));

    let db_plan = Arc::new(db_plan_raw);
    let plan = MigrationPlan {
        databases: vec![db_plan.clone()],
    };
    let states = tui::shared_migration_states(&plan);
    let cancel = CancellationToken::new();
    
    let config = get_test_config(TomlConfig {
        max_parallel: 2,
        ..Default::default()
    });

    let res = migrate_copy_rules_internal(
        config,
        &plan.databases[0],
        "test_db (delayed)",
        &states,
        &cancel,
        |_config, db_name, _target, _sem, _cancel, _progress| {
            let db_name = db_name.to_string();
            Box::pin(async move {
                if db_name == "test_db" {
                    Err(Error::Config("worker failure".into()))
                } else {
                    Ok(())
                }
            })
        }
    ).await;

    assert!(res.is_err());
}

#[tokio::test]
async fn test_run_delayed_pipeline_ui_update_on_failure() {
    let mut db_plan_raw = mock_db_plan("test_db");
    db_plan_raw.copy_rules.push(mock_copy_rule("table1"));
    let db_plan = Arc::new(db_plan_raw);
    
    let plan = MigrationPlan {
        databases: vec![db_plan.clone()],
    };
    let states = tui::shared_migration_states(&plan);
    let cancel = CancellationToken::new();
    
    let args = PipelineArgs {
        config: get_test_config(TomlConfig::default()),
        db_plan,
        states: states.clone(),
        cancel: cancel.clone(),
        dump_sem: Arc::new(Semaphore::new(1)),
        restore_sem: Arc::new(Semaphore::new(1)),
    };

    let (_proceed_tx, proceed_rx) = watch::channel(true);

    let res = run_delayed_pipeline_steps_internal(
        args,
        proceed_rx,
        |_args, _proceed| async {
            Err(Error::Config("something failed".into()))
        }
    ).await;

    assert!(res.is_err());
    
    let (phase, _step) = states.lock().expect("lock poisoned").get_state("test_db (delayed)").expect("row should exist");
    assert_eq!(phase, MigrationPhase::Failed);
}

#[cfg(not(test))]
use crate::db;
use crate::error::{Error, MigrationPhase, Result};
use crate::plan::DatabasePlan;
use crate::tui::SharedMigrationStates;
use crate::{Config, copy_engine};
use crate::{CopyTarget, run_copy_engine};
use indicatif::HumanBytes;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn migrate_copy_rules(
    config: Arc<Config>,
    db_plan: &DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    cancel: &CancellationToken,
) -> Result<()> {
    migrate_copy_rules_internal(
        config,
        db_plan,
        delayed_name,
        states,
        cancel,
        run_copy_engine_callback,
    )
    .await
}

fn run_copy_engine_callback(
    config: Arc<Config>,
    db_name: &str,
    target: CopyTarget<'_>,
    sem: Arc<Semaphore>,
    cancel: CancellationToken,
    on_progress: Box<dyn FnMut(copy_engine::CopyProgress) + Send + 'static>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    let db_name = db_name.to_string();
    let target = target.to_owned();
    Box::pin(async move {
        run_copy_engine(
            &config,
            &db_name,
            target.as_target(),
            sem,
            cancel,
            on_progress,
        )
        .await
    })
}

pub async fn migrate_copy_rules_internal<F>(
    config: Arc<Config>,
    db_plan: &DatabasePlan,
    delayed_name: &str,
    states: &SharedMigrationStates,
    cancel: &CancellationToken,
    copy_engine: F,
) -> Result<()>
where
    F: Fn(
            Arc<Config>,
            &str,
            CopyTarget<'_>,
            Arc<Semaphore>,
            CancellationToken,
            Box<dyn FnMut(copy_engine::CopyProgress) + Send + 'static>,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync
        + Clone
        + 'static,
{
    if db_plan.copy_rules.is_empty() {
        return Ok(());
    }

    let mut tasks = JoinSet::new();
    let sem = Arc::new(Semaphore::new(config.max_parallel));
    let progress = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));

    for rule in &db_plan.copy_rules {
        #[cfg(not(test))]
        {
            let marker = db::copy_rule_done_marker(&db_plan.name, &rule.table, rule.rule_hash)?;
            if marker.exists() {
                continue;
            }
        }

        let rule = rule.clone();
        let config = config.clone();
        let db_name = db_plan.name.clone();
        let delayed_name = delayed_name.to_string();
        let states = states.clone();
        let cancel = cancel.clone();
        let sem = sem.clone();
        let progress = progress.clone();
        let rule_key = format!("{}:{}", rule.table, rule.rule_hash);
        let copy_engine = copy_engine.clone();

        tasks.spawn(async move {
            let table_name = rule.table.clone();
            let column = rule.column.clone();
            let from = rule.from.clone();
            let till = rule.till.clone();
            let method = rule.method.clone();
            let sem_inner = sem.clone();
            let cancel_inner = cancel.clone();

            copy_engine(
                config.clone(),
                &db_name,
                CopyTarget {
                    table: &table_name,
                    column: &column,
                    from: from.as_deref(),
                    till: till.as_deref(),
                    method: Some(&method),
                },
                sem_inner,
                cancel_inner,
                Box::new(move |p| {
                    if let Ok(mut lock) = progress.lock() {
                        lock.insert(rule_key.clone(), p);
                        let total_p: usize = lock.values().map(|v| v.total_partitions).sum();
                        let comp_p: usize = lock.values().map(|v| v.completed_partitions).sum();
                        let bytes: u64 = lock.values().map(|v| v.total_bytes).sum();
                        let table_count = lock.len();

                        if let Ok(mut ui_lock) = states.lock() {
                            ui_lock.update(
                                &delayed_name,
                                MigrationPhase::DelayedRestoring,
                                3,
                                format!(
                                    "copying {table_count} tables via copy engine ({comp_p}/{total_p} partitions, {})",
                                    HumanBytes(bytes)
                                ),
                            );
                        }
                    }
                }),
            )
            .await?;

            #[cfg(not(test))]
            {
                let marker = db::copy_rule_done_marker(&db_name, &table_name, rule.rule_hash)?;
                std::fs::write(marker, "")?;
            }

            Ok::<(), Error>(())
        });
    }

    while let Some(res) = tasks.join_next().await {
        res??;
    }

    Ok(())
}

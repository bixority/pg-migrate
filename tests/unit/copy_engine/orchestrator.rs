use pg_migrate::copy_engine::{Orchestrator, CopySettings, Result};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_orchestrator_new() {
    let sem = Arc::new(Semaphore::new(1));
    let cancel = CancellationToken::new();
    let orch = Orchestrator::new(
        "src",
        "dest",
        "table",
        CopySettings {
            worker_count: 4,
            buffer_size: 32 * 1024 * 1024,
            report_interval: 10 * 1024 * 1024,
        },
        sem,
        cancel,
    );
    assert_eq!(&*orch.source_config, "src");
    assert_eq!(&*orch.dest_config, "dest");
    assert_eq!(&*orch.table_name, "table");
    assert_eq!(orch.worker_count, 4);
}

#[tokio::test]
async fn test_orchestrator_empty_partitions() -> Result<()> {
    let sem = Arc::new(Semaphore::new(1));
    let cancel = CancellationToken::new();
    let orch = Orchestrator::new(
        "src",
        "dest",
        "table",
        CopySettings {
            worker_count: 4,
            buffer_size: 32 * 1024 * 1024,
            report_interval: 10 * 1024 * 1024,
        },
        sem,
        cancel,
    );
    let result = orch.run(vec![], |_| {}).await?;
    assert_eq!(result, 0);
    Ok(())
}

use pg_migrate::copy_engine::worker::Worker;
use std::sync::Arc;

#[test]
fn test_worker_new() {
    let worker = Worker::new(
        1,
        Arc::from("src"),
        Arc::from("dest"),
        Arc::from("table"),
        32 * 1024 * 1024,
        10 * 1024 * 1024,
    );
    assert_eq!(worker.id, 1);
    assert_eq!(&*worker.source_config, "src");
    assert_eq!(&*worker.dest_config, "dest");
    assert_eq!(&*worker.table_name, "table");
}

use pg_migrate::copy_engine::Result;
use pg_migrate::copy_engine::splitter::Partition;
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

#[test]
fn test_build_copy_queries_hash_with_nulls() -> Result<()> {
    let worker = Worker::new(
        1,
        Arc::from("src"),
        Arc::from("dest"),
        Arc::from("public.table"),
        1024,
        1024,
    );
    let partition = Partition {
        column: "val".into(),
        from: Some("0".to_string()),
        till: Some("2".to_string()),
        method: "hash".into(),
        include_nulls: true,
    };
    let (source_query, _) = worker.build_copy_queries(&partition)?;
    assert_eq!(
        source_query,
        "COPY (SELECT * FROM \"public\".\"table\" WHERE (abs(hashtext(\"val\"::text)::bigint) % 2 = 0 OR \"val\" IS NULL)) TO STDOUT"
    );
    Ok(())
}

#[test]
fn test_build_copy_queries_hash_no_nulls() -> Result<()> {
    let worker = Worker::new(
        1,
        Arc::from("src"),
        Arc::from("dest"),
        Arc::from("table"),
        1024,
        1024,
    );
    let partition = Partition {
        column: "val".into(),
        from: Some("1".to_string()),
        till: Some("2".to_string()),
        method: "hash".into(),
        include_nulls: false,
    };
    let (source_query, _) = worker.build_copy_queries(&partition)?;
    assert_eq!(
        source_query,
        "COPY (SELECT * FROM \"table\" WHERE abs(hashtext(\"val\"::text)::bigint) % 2 = 1) TO STDOUT"
    );
    Ok(())
}

#[test]
fn test_build_copy_queries_range_with_nulls() -> Result<()> {
    let worker = Worker::new(
        1,
        Arc::from("src"),
        Arc::from("dest"),
        Arc::from("table"),
        1024,
        1024,
    );
    let partition = Partition {
        column: "val".into(),
        from: None,
        till: Some("2024-01-01".to_string()),
        method: "time".into(),
        include_nulls: true,
    };
    let (source_query, _) = worker.build_copy_queries(&partition)?;
    assert_eq!(
        source_query,
        "COPY (SELECT * FROM \"table\" WHERE (\"val\" < '2024-01-01' OR \"val\" IS NULL)) TO STDOUT"
    );
    Ok(())
}

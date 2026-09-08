use pg_migrate::copy_engine::error::CopyEngineError;
use std::io::Error as IoError;

#[test]
fn test_error_display() {
    let err = CopyEngineError::Configuration("invalid config".into());
    assert_eq!(format!("{err}"), "Configuration error: invalid config");

    let err = CopyEngineError::Io(IoError::other("disk full"));
    assert_eq!(format!("{err}"), "IO error: disk full");

    let worker_err = CopyEngineError::WorkerFailed {
        partition: "part1".into(),
        source: Box::new(CopyEngineError::Configuration("failed".into())),
    };
    assert_eq!(
        format!("{worker_err}"),
        "Worker failure in partition part1: Configuration error: failed"
    );
}

#[test]
fn test_error_retryable() {
    let io_err = CopyEngineError::Io(IoError::other("reset by peer"));
    assert!(io_err.is_retryable());

    let config_err = CopyEngineError::Configuration("bad setting".into());
    assert!(!config_err.is_retryable());

    let table_err = CopyEngineError::TableNotFound {
        side: "destination",
        table: "public.ord_log".into(),
        search_path: "public".into(),
    };
    assert!(!table_err.is_retryable());

    let worker_io_err = CopyEngineError::WorkerFailed {
        partition: "part1".into(),
        source: Box::new(CopyEngineError::Io(IoError::other("broken pipe"))),
    };
    assert!(worker_io_err.is_retryable());

    let worker_config_err = CopyEngineError::WorkerFailed {
        partition: "part1".into(),
        source: Box::new(CopyEngineError::Configuration("bad setting".into())),
    };
    assert!(!worker_config_err.is_retryable());
}

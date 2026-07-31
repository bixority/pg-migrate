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

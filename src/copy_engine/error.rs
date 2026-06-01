use std::io::Error as IoError;
use thiserror::Error;
use tokio::sync::AcquireError;
use tokio_postgres::Error as PgError;

#[derive(Debug, Error)]
pub enum CopyEngineError {
    #[error("Connection error: {0}")]
    Connection(#[from] PgError),

    #[error("IO error: {0}")]
    Io(#[from] IoError),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Worker failure in partition {partition}: {source}")]
    WorkerFailed {
        partition: String,
        source: Box<Self>,
    },

    #[error("Semaphore acquire failed: {0}")]
    Semaphore(#[from] AcquireError),

    #[error("Splitter error: {0}")]
    Splitter(String),

    #[error("Join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, CopyEngineError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Error as IoError;

    #[test]
    fn test_error_display() {
        let err = CopyEngineError::Configuration("invalid config".to_string());
        assert_eq!(format!("{err}"), "Configuration error: invalid config");

        let err = CopyEngineError::Io(IoError::other("disk full"));
        assert_eq!(format!("{err}"), "IO error: disk full");

        let worker_err = CopyEngineError::WorkerFailed {
            partition: "part1".to_string(),
            source: Box::new(CopyEngineError::Configuration("failed".to_string())),
        };
        assert_eq!(
            format!("{worker_err}"),
            "Worker failure in partition part1: Configuration error: failed"
        );
    }
}

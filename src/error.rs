use std::fmt;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationPhase {
    Pending,
    Dumping,
    SourceCounts,
    Restoring,
    DestinationCounts,
    Verifying,
    DelayedDumping,
    DelayedRestoring,
    DelayedVerifying,
    Complete,
    Failed,
}

impl MigrationPhase {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Dumping => "dumping",
            Self::SourceCounts => "source counts",
            Self::Restoring => "restoring",
            Self::DestinationCounts => "dest counts",
            Self::Verifying => "verifying",
            Self::DelayedDumping => "delayed dumping",
            Self::DelayedRestoring => "delayed restoring",
            Self::DelayedVerifying => "delayed verifying",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Migration cancelled: {0}")]
    Cancelled(String),

    #[error("Process failed: {command}\nError: {stderr}")]
    ProcessFailed { command: String, stderr: String },

    #[error("Failed to spawn {command}: {source}")]
    SpawnFailed {
        command: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Verification failed for {database}: {details}")]
    VerificationFailed { database: String, details: String },

    #[error("Semaphore acquire failed: {0}")]
    Semaphore(#[from] tokio::sync::AcquireError),

    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),

    #[error("Copy engine error: {0}")]
    CopyEngine(#[from] crate::copy_engine::error::CopyEngineError),

    #[error("Environment error: {0}")]
    Env(String),

    #[error("Connection timeout: {0}")]
    Timeout(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database '{database}' not found")]
    DatabaseNotFound { database: String },

    #[error("Dump not found for {database} at {path}")]
    DumpNotFound { database: String, path: String },

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Invalid copy rule for table '{table}': {reason}")]
    InvalidCopyRule { table: String, reason: String },

    #[error("Error in database '{database}', phase {phase}, step {step}: {source}")]
    WithContext {
        database: String,
        phase: MigrationPhase,
        step: u8,
        #[source]
        source: Box<Self>,
    },

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn with_context(
        self,
        database: impl Into<String>,
        phase: MigrationPhase,
        step: u8,
    ) -> Self {
        Self::WithContext {
            database: database.into(),
            phase,
            step,
            source: Box::new(self),
        }
    }
}

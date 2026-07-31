use crate::copy_engine;
use std::borrow::Cow;
use std::fmt;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationPhase {
    Pending,
    Dumping,
    Restoring,
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
            Self::Restoring => "restoring",
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
    Cancelled(Cow<'static, str>),

    #[error("Process failed: {command}\nError: {stderr}")]
    ProcessFailed {
        command: Cow<'static, str>,
        stderr: Cow<'static, str>,
    },

    #[error("Failed to spawn {command}: {source}")]
    SpawnFailed {
        command: Cow<'static, str>,
        #[source]
        source: std::io::Error,
    },

    #[error("Verification failed for {database}: {details}")]
    VerificationFailed {
        database: Cow<'static, str>,
        details: Cow<'static, str>,
    },

    #[error("Semaphore acquire failed: {0}")]
    Semaphore(#[from] tokio::sync::AcquireError),

    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Lock poisoned: {0}")]
    LockPoisoned(Cow<'static, str>),

    #[error("Copy engine error: {0}")]
    CopyEngine(#[from] copy_engine::error::CopyEngineError),

    #[error("Environment error: {0}")]
    Env(Cow<'static, str>),

    #[error("Connection timeout: {0}")]
    Timeout(Cow<'static, str>),

    #[error("Configuration error: {0}")]
    Config(Cow<'static, str>),

    #[error("Database '{database}' not found")]
    DatabaseNotFound { database: Cow<'static, str> },

    #[error("Dump not found for {database} at {path}")]
    DumpNotFound {
        database: Cow<'static, str>,
        path: Cow<'static, str>,
    },

    #[error("Invalid path: {0}")]
    InvalidPath(Cow<'static, str>),

    #[error("Invalid copy rule for table '{table}': {reason}")]
    InvalidCopyRule {
        table: Cow<'static, str>,
        reason: Cow<'static, str>,
    },

    #[error("Error in database '{database}', phase {phase}, step {step}: {source}")]
    WithContext {
        database: Cow<'static, str>,
        phase: MigrationPhase,
        step: u8,
        #[source]
        source: Box<Self>,
    },

    #[error("{0}")]
    Other(Cow<'static, str>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn with_context(
        self,
        database: impl Into<Cow<'static, str>>,
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

use std::borrow::Cow;
use std::io::Error as IoError;
use thiserror::Error;
use tokio::sync::AcquireError;
use tokio_postgres::Error as PgError;
use tokio_postgres::error::SqlState;

#[derive(Debug, Error)]
pub enum CopyEngineError {
    #[error("Connection error: {0}")]
    Connection(#[from] PgError),

    #[error("IO error: {0}")]
    Io(#[from] IoError),

    #[error("Configuration error: {0}")]
    Configuration(Cow<'static, str>),

    /// A `COPY` (or its connection) failed against a specific side of the
    /// migration. Boxed so this rich-context variant does not bloat the size of
    /// every `Result` in the crate (see clippy `result_large_err`).
    #[error(transparent)]
    CopyFailed(Box<CopyFailure>),

    /// A preflight probe found the copy-rule table is not visible to the copy
    /// connection on one side, before any partition work began. The message
    /// surfaces the resolved `search_path` and the most likely causes so the
    /// operator can act without decoding a raw `COPY` failure.
    #[error(
        "table \"{table}\" is not visible to the copy connection on the {side} database.\n  \
         search_path: {search_path}\n  \
         likely causes:\n    \
         - the copy rule names the table as SCHEMA.TABLE, so the SCHEMA must exist on the \
         {side} and the name must be spelled correctly — confirm the schema is present and the \
         table lives in it;\n    \
         - identifier case/quoting: an unquoted name folds to lower case, so a table created \
         as \"MyTable\" must be referenced with its exact case;\n    \
         - (destination only) the regular schema restore was skipped, e.g. a stale \
         \"<db>.done\" marker in $HOME/pg_migrate_state/ left over from a previous run against \
         a now-recreated destination — remove the marker and re-run to recreate the schema."
    )]
    TableNotFound {
        side: &'static str,
        table: Cow<'static, str>,
        search_path: Cow<'static, str>,
    },

    #[error("Worker failure in partition {partition}: {source}")]
    WorkerFailed {
        partition: Cow<'static, str>,
        source: Box<Self>,
    },

    #[error("Semaphore acquire failed: {0}")]
    Semaphore(#[from] AcquireError),

    #[error("Splitter error: {0}")]
    Splitter(Cow<'static, str>),

    #[error("Join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Full diagnostic context for a failed `COPY` operation.
///
/// Carries which `stage` failed, on the `source` or `destination` database, for
/// which `table` and `partition`, the SQL/`detail` involved, the underlying
/// Postgres error, and an optional `hint` (e.g. a `search_path` tip when the
/// server reports the relation as missing even though it exists).
#[derive(Debug, Error)]
#[error(
    "{stage} failed for table \"{table}\" on the {side} database \
     (partition: {partition})\n  cause: {source}\n  detail: {detail}{hint}"
)]
pub struct CopyFailure {
    pub stage: &'static str,
    pub side: &'static str,
    pub table: Cow<'static, str>,
    pub partition: Cow<'static, str>,
    pub detail: Cow<'static, str>,
    pub hint: Cow<'static, str>,
    #[source]
    pub source: PgError,
}

impl CopyEngineError {
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connection(e) => Self::is_pg_retryable(e),
            Self::Io(_) => true,
            Self::CopyFailed(failure) => Self::is_pg_retryable(&failure.source),
            Self::WorkerFailed { source, .. } => source.is_retryable(),
            Self::Configuration(_)
            | Self::TableNotFound { .. }
            | Self::Semaphore(_)
            | Self::Splitter(_)
            | Self::Join(_) => false,
        }
    }

    fn is_pg_retryable(err: &PgError) -> bool {
        if err.is_closed() {
            return true;
        }
        err.as_db_error().is_none_or(|db_err| {
            let code = db_err.code();
            !matches!(
                *code,
                SqlState::UNDEFINED_TABLE
                    | SqlState::UNDEFINED_COLUMN
                    | SqlState::SYNTAX_ERROR
                    | SqlState::INVALID_CATALOG_NAME
                    | SqlState::INVALID_SCHEMA_NAME
                    | SqlState::INSUFFICIENT_PRIVILEGE
                    | SqlState::INVALID_PASSWORD
            )
        })
    }
}

pub type Result<T> = std::result::Result<T, CopyEngineError>;

pub mod error;
pub mod orchestrator;
pub mod splitter;
pub mod worker;

pub use error::{CopyEngineError, CopyFailure, Result};
pub use orchestrator::{CopyProgress, CopySettings, Orchestrator};
pub use splitter::{Partition, Splitter};
pub use worker::Worker;

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub(crate) async fn acquire(
    sem: &Arc<Semaphore>,
    cancel: &CancellationToken,
) -> Result<OwnedSemaphorePermit> {
    tokio::select! {
        res = sem.clone().acquire_owned() => Ok(res.map_err(|_| CopyEngineError::Splitter("semaphore closed".to_string()))?),
        () = cancel.cancelled() => Err(CopyEngineError::Splitter("interrupted while waiting for semaphore".into())),
    }
}

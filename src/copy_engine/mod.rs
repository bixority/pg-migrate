pub mod error;
pub mod orchestrator;
pub mod splitter;
pub mod worker;

pub use error::{CopyEngineError, Result};
pub use orchestrator::{CopyProgress, CopySettings, Orchestrator};
pub use splitter::Splitter;

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub(crate) async fn acquire(
    sem: &Arc<Semaphore>,
    cancel: &CancellationToken,
) -> Result<OwnedSemaphorePermit> {
    tokio::select! {
        res = sem.clone().acquire_owned() => Ok(res.map_err(|_| CopyEngineError::Splitter("semaphore closed".into()))?),
        () = cancel.cancelled() => Err(CopyEngineError::Splitter("interrupted while waiting for semaphore".into())),
    }
}

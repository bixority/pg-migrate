use crate::copy_engine::error::{CopyEngineError, Result};
use crate::copy_engine::splitter::Partition;
use crate::copy_engine::worker::Worker;
use log::info;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Progress snapshot emitted by [`Orchestrator::run`] after each partition
/// completes, so callers can surface copy-engine progress in their UI.
#[derive(Clone, Copy, Debug)]
pub struct CopyProgress {
    pub completed_partitions: usize,
    pub total_partitions: usize,
    pub total_bytes: u64,
}

pub struct Orchestrator {
    source_config: String,
    dest_config: String,
    table_name: String,
    worker_count: usize,
}

impl Orchestrator {
    #[must_use]
    pub const fn new(
        source_config: String,
        dest_config: String,
        table_name: String,
        worker_count: usize,
    ) -> Self {
        Self {
            source_config,
            dest_config,
            table_name,
            worker_count,
        }
    }

    /// Runs the migration for the given partitions.
    ///
    /// `on_progress` is invoked once before any work starts and again after
    /// each partition finishes, allowing the caller to surface live progress.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection to the source or destination database fails.
    /// - The `COPY` operation fails.
    /// - A worker fails to process its partition.
    pub async fn run(
        &self,
        partitions: Vec<Partition>,
        mut on_progress: impl FnMut(CopyProgress),
    ) -> Result<u64> {
        let total_partitions = partitions.len();
        info!(
            "Starting migration for table {} with {} workers and {total_partitions} partitions",
            self.table_name, self.worker_count,
        );

        let semaphore = Arc::new(Semaphore::new(self.worker_count));
        let mut join_set = JoinSet::new();

        for (id, partition) in partitions.into_iter().enumerate() {
            let permit = semaphore.clone().acquire_owned().await?;
            let worker = Worker::new(
                id,
                self.source_config.clone(),
                self.dest_config.clone(),
                self.table_name.clone(),
            );

            join_set.spawn(async move {
                let _permit = permit;
                worker
                    .run(partition.clone())
                    .await
                    .map_err(|e| CopyEngineError::WorkerFailed {
                        partition: partition.to_string(),
                        source: Box::new(e),
                    })
            });
        }

        let mut total_bytes = 0;
        let mut completed = 0;
        on_progress(CopyProgress {
            completed_partitions: 0,
            total_partitions,
            total_bytes: 0,
        });
        while let Some(res) = join_set.join_next().await {
            let bytes = res??;
            total_bytes += bytes;
            completed += 1;
            on_progress(CopyProgress {
                completed_partitions: completed,
                total_partitions,
                total_bytes,
            });
        }

        info!(
            "Migration for table {} complete. Total bytes: {}",
            self.table_name, total_bytes
        );

        Ok(total_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_orchestrator_new() {
        let orch = Orchestrator::new(
            "src".to_string(),
            "dest".to_string(),
            "table".to_string(),
            4,
        );
        assert_eq!(orch.source_config, "src");
        assert_eq!(orch.dest_config, "dest");
        assert_eq!(orch.table_name, "table");
        assert_eq!(orch.worker_count, 4);
    }

    #[tokio::test]
    async fn test_orchestrator_empty_partitions() -> Result<()> {
        let orch = Orchestrator::new(
            "src".to_string(),
            "dest".to_string(),
            "table".to_string(),
            4,
        );
        let result = orch.run(vec![], |_| {}).await?;
        assert_eq!(result, 0);
        Ok(())
    }
}

use crate::copy_engine::error::{CopyEngineError, Result};
use crate::copy_engine::splitter::Partition;
use crate::copy_engine::worker::Worker;
use crate::{copy_engine, db, tls};
use log::{error, info};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_postgres::error::SqlState;
use tokio_util::sync::CancellationToken;

/// Progress snapshot emitted by [`Orchestrator::run`] after each partition
/// completes, so callers can surface copy-engine progress in their UI.
#[derive(Clone, Copy, Debug)]
pub struct CopyProgress {
    pub completed_partitions: usize,
    pub total_partitions: usize,
    pub total_bytes: u64,
}

pub enum ProgressEvent {
    Bytes(u64),
    PartitionComplete,
}

#[derive(Clone, Copy, Debug)]
pub struct CopySettings {
    pub worker_count: usize,
    pub buffer_size: u64,
    pub report_interval: u64,
}

pub struct Orchestrator {
    pub source_config: Arc<str>,
    pub dest_config: Arc<str>,
    pub table_name: Arc<str>,
    pub worker_count: usize,
    pub buffer_size: u64,
    pub report_interval: u64,
    pub semaphore: Arc<Semaphore>,
    pub cancel: CancellationToken,
}

impl Orchestrator {
    #[must_use]
    pub fn new(
        source_config: impl Into<Arc<str>>,
        dest_config: impl Into<Arc<str>>,
        table_name: impl Into<Arc<str>>,
        settings: CopySettings,
        semaphore: Arc<Semaphore>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            source_config: source_config.into(),
            dest_config: dest_config.into(),
            table_name: table_name.into(),
            worker_count: settings.worker_count,
            buffer_size: settings.buffer_size,
            report_interval: settings.report_interval,
            semaphore,
            cancel,
        }
    }

    /// Probes whether `self.table_name` is visible to a copy connection on one
    /// `side` ("source" or "destination"), using the same unqualified name
    /// resolution (and therefore the same `search_path`) the `COPY` will use.
    ///
    /// A `SELECT ... LIMIT 0` resolves the relation exactly as `COPY` does but
    /// reads no rows, so a missing table produces Postgres' `undefined_table`
    /// (SQLSTATE 42P01) — which is translated into a [`CopyEngineError::TableNotFound`]
    /// carrying the side and the resolved `search_path`. Any other error (e.g.
    /// connectivity, privileges) is surfaced verbatim.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails, the probe fails for a reason
    /// other than a missing table, or the table is not visible.
    async fn ensure_table_visible(&self, config: &str, side: &'static str) -> Result<()> {
        let _permit = copy_engine::acquire(&self.semaphore, &self.cancel).await?;
        let (client, connection) = tokio_postgres::connect(config, tls::make_tls()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("{side} preflight connection error: {e}");
            }
        });

        let search_path: String = client
            .query_one("SELECT array_to_string(current_schemas(true), ', ')", &[])
            .await?
            .get(0);

        let quoted_table = db::quote_table_name(&self.table_name);
        let probe = format!("SELECT 1 FROM {quoted_table} LIMIT 0");
        if let Err(e) = client.simple_query(&probe).await {
            if e.as_db_error().map(tokio_postgres::error::DbError::code)
                == Some(&SqlState::UNDEFINED_TABLE)
            {
                return Err(CopyEngineError::TableNotFound {
                    side,
                    table: (*self.table_name).to_string().into(),
                    search_path: search_path.into(),
                });
            }
            return Err(CopyEngineError::Connection(e));
        }
        Ok(())
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

        if total_partitions == 0 {
            return Ok(0);
        }

        // Fail fast with a precise, side-attributed message if the table is not
        // visible to the copy connection. Without this, a missing table only
        // surfaces as an opaque per-partition `COPY` error that does not say
        // which side, or why.
        self.ensure_table_visible(&self.source_config, "source")
            .await?;
        self.ensure_table_visible(&self.dest_config, "destination")
            .await?;

        let (partition_tx, partition_rx) = tokio::sync::mpsc::channel(total_partitions + 1);
        for p in partitions {
            let _ = partition_tx.send(p).await;
        }
        drop(partition_tx);

        let shared_rx = Arc::new(tokio::sync::Mutex::new(partition_rx));
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(self.worker_count * 1024);
        let mut join_set = JoinSet::new();

        for id in 0..self.worker_count {
            let worker = Worker::new(
                id,
                self.source_config.clone(),
                self.dest_config.clone(),
                self.table_name.clone(),
                self.buffer_size,
                self.report_interval,
            );
            let shared_rx = shared_rx.clone();
            let progress_tx = progress_tx.clone();
            let semaphore = self.semaphore.clone();
            let cancel = self.cancel.clone();
            join_set
                .spawn(async move { worker.run(shared_rx, progress_tx, semaphore, cancel).await });
        }

        // Drop our sender so progress_rx finishes once all workers are done.
        drop(progress_tx);

        let mut total_bytes = 0;
        let mut completed = 0;
        on_progress(CopyProgress {
            completed_partitions: 0,
            total_partitions,
            total_bytes: 0,
        });

        loop {
            tokio::select! {
                Some(event) = progress_rx.recv() => {
                    match event {
                        ProgressEvent::Bytes(bytes) => total_bytes += bytes,
                        ProgressEvent::PartitionComplete => completed += 1,
                    }
                    on_progress(CopyProgress {
                        completed_partitions: completed,
                        total_partitions,
                        total_bytes,
                    });
                }
                res = join_set.join_next() => {
                    if let Some(r) = res {
                        r??;
                    } else {
                        // All workers finished. Drain any remaining progress updates.
                        while let Some(event) = progress_rx.recv().await {
                            match event {
                                ProgressEvent::Bytes(bytes) => total_bytes += bytes,
                                ProgressEvent::PartitionComplete => completed += 1,
                            }
                            on_progress(CopyProgress {
                                completed_partitions: completed,
                                total_partitions,
                                total_bytes,
                            });
                        }
                        break;
                    }
                }
            }
        }

        info!(
            "Migration for table {} complete. Total bytes: {}",
            self.table_name, total_bytes
        );

        Ok(total_bytes)
    }
}

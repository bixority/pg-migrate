use crate::copy_engine::error::{CopyEngineError, CopyFailure, Result};
use crate::copy_engine::orchestrator::ProgressEvent;
use crate::copy_engine::splitter::Partition;
use crate::{copy_engine, db, tls};
use futures_util::{SinkExt, StreamExt, pin_mut};
use log::{error, info};
use std::borrow::Cow;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio_postgres::error::SqlState;
use tokio_postgres::Error as PgError;
use tokio_util::sync::CancellationToken;

pub struct Worker {
    pub id: usize,
    pub source_config: Arc<str>,
    pub dest_config: Arc<str>,
    pub table_name: Arc<str>,
    pub columns: Arc<[Box<str>]>,
    pub buffer_size: u64,
    pub report_interval: u64,
}

impl Worker {
    #[must_use]
    pub const fn new(
        id: usize,
        source_config: Arc<str>,
        dest_config: Arc<str>,
        table_name: Arc<str>,
        columns: Arc<[Box<str>]>,
        buffer_size: u64,
        report_interval: u64,
    ) -> Self {
        Self {
            id,
            source_config,
            dest_config,
            table_name,
            columns,
            buffer_size,
            report_interval,
        }
    }

    /// Builds a [`CopyEngineError::CopyFailed`] carrying full context for a
    /// failed operation: which `stage` failed, on which `side` ("source" or
    /// "destination"), the SQL/`detail` involved, the `partition`, and — when
    /// the server reports the relation as missing — a hint about schema
    /// qualification and `search_path`.
    fn copy_failed(
        &self,
        stage: &'static str,
        side: &'static str,
        partition: Option<&Partition>,
        detail: impl Into<Cow<'static, str>>,
        source: PgError,
    ) -> CopyEngineError {
        let hint = self.missing_relation_hint(&source, side);
        CopyEngineError::CopyFailed(Box::new(CopyFailure {
            stage,
            side,
            table: (*self.table_name).to_string().into(),
            partition: partition
                .map_or_else(|| "none".to_string(), ToString::to_string)
                .into(),
            detail: detail.into(),
            hint: hint.into(),
            source,
        }))
    }

    /// Returns a diagnostic hint when `err` is Postgres' `undefined_table`
    /// (SQLSTATE 42P01), otherwise an empty string. A bare table name in a copy
    /// rule is resolved against the connection's `search_path`; a table in a
    /// non-public schema is reported as missing even though it exists, so point
    /// the user at schema-qualifying the rule.
    fn missing_relation_hint(&self, err: &PgError, side: &str) -> String {
        let Some(db_err) = err.as_db_error() else {
            return String::new();
        };
        if *db_err.code() != SqlState::UNDEFINED_TABLE {
            return String::new();
        }
        if self.table_name.contains('.') {
            format!(
                "\n  hint: the {side} database reports schema-qualified table \
                 \"{0}\" as missing. Confirm the schema name is spelled correctly \
                 and that the table exists on the {side} before the copy phase runs.",
                self.table_name
            )
        } else {
            format!(
                "\n  hint: the {side} database reports table \"{0}\" as missing. A bare \
                 table name is resolved against the connection's search_path, so a table \
                 in a non-public schema looks missing even when it exists. Qualify it in \
                 the copy rule as DATABASE.SCHEMA.TABLE (e.g. mydb.myschema.{0}).",
                self.table_name
            )
        }
    }

    /// Opens a connection to one `side` ("source" or "destination") and spawns
    /// its connection driver task. Failures are wrapped with full copy context.
    async fn connect_side(
        &self,
        config: &str,
        side: &'static str,
    ) -> Result<tokio_postgres::Client> {
        let (client, connection) = tokio_postgres::connect(config, tls::make_tls())
            .await
            .map_err(|e| {
                self.copy_failed(
                    "Connection",
                    side,
                    None,
                    format!("establishing {side} connection"),
                    e,
                )
            })?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("{side} connection error: {e}");
            }
        });
        Ok(client)
    }

    /// Runs the worker loop, pulling partitions from the channel and processing
    /// them with automatic retry and subdivision.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection to the source or destination database fails permanently.
    /// - Any partition copy fails after retries and cannot be subdivided.
    pub async fn run(
        &self,
        rx: Arc<Mutex<tokio::sync::mpsc::Receiver<Partition>>>,
        progress_tx: tokio::sync::mpsc::Sender<ProgressEvent>,
        semaphore: Arc<Semaphore>,
        cancel: CancellationToken,
    ) -> Result<u64> {
        let first_partition = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };

        let Some(mut partition) = first_partition else {
            return Ok(0);
        };

        let _permit = copy_engine::acquire(&semaphore, &cancel).await?;

        let mut total_bytes = 0;
        loop {
            let bytes = self
                .process_partition(&partition, &progress_tx, &cancel, 4)
                .await?;

            total_bytes += bytes;
            let _ = progress_tx.send(ProgressEvent::PartitionComplete).await;

            let next_partition = {
                let mut guard = rx.lock().await;
                guard.recv().await
            };
            if let Some(p) = next_partition {
                partition = p;
            } else {
                break;
            }
        }

        Ok(total_bytes)
    }

    /// Processes a partition with retries and exponential backoff. If transient failures
    /// persist after retries, attempts to dynamically subdivide the partition into smaller chunks.
    async fn process_partition(
        &self,
        partition: &Partition,
        progress_tx: &tokio::sync::mpsc::Sender<ProgressEvent>,
        cancel: &CancellationToken,
        max_depth: usize,
    ) -> Result<u64> {
        const MAX_RETRIES: usize = 3;
        let mut attempt = 0;

        loop {
            if cancel.is_cancelled() {
                return Err(CopyEngineError::Configuration("cancelled".into()));
            }

            let mut reported_bytes = 0;
            match self
                .do_copy_partition(partition, progress_tx, &mut reported_bytes)
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    if reported_bytes > 0 {
                        let _ = progress_tx.send(ProgressEvent::RevertBytes(reported_bytes)).await;
                    }

                    attempt += 1;
                    if !err.is_retryable() || cancel.is_cancelled() {
                        return Err(err);
                    }

                    if attempt > MAX_RETRIES
                        && max_depth > 0
                        && let Some((p1, p2)) = partition.split_in_half()
                    {
                        log::warn!(
                            "Worker {} partition {} failed after {MAX_RETRIES} attempts ({err}); dynamically splitting into smaller partitions:\n  sub 1: {p1}\n  sub 2: {p2}",
                            self.id, partition
                        );
                        let _ = progress_tx.send(ProgressEvent::PartitionSplit).await;
                        let b1 = Box::pin(self.process_partition(
                            &p1,
                            progress_tx,
                            cancel,
                            max_depth - 1,
                        ))
                        .await?;
                        let _ = progress_tx.send(ProgressEvent::PartitionComplete).await;

                        let b2 = Box::pin(self.process_partition(
                            &p2,
                            progress_tx,
                            cancel,
                            max_depth - 1,
                        ))
                        .await?;
                        return Ok(b1 + b2);
                    }

                    let backoff = std::time::Duration::from_secs(2 * (attempt as u64));
                    log::warn!(
                        "Worker {} partition {} failed (attempt {attempt}/{MAX_RETRIES}): {err}. Retrying in {backoff:?}...",
                        self.id, partition
                    );
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {},
                        () = cancel.cancelled() => {
                            return Err(CopyEngineError::Configuration("cancelled".into()));
                        }
                    }
                }
            }
        }
    }

    /// Performs the streaming COPY from source to destination for a single partition.
    #[allow(clippy::too_many_lines)]
    async fn do_copy_partition(
        &self,
        partition: &Partition,
        progress_tx: &tokio::sync::mpsc::Sender<ProgressEvent>,
        reported_bytes: &mut u64,
    ) -> Result<u64> {
        info!("Worker {} starting partition: {}", self.id, partition);

        let (source_query, dest_query) = self.build_copy_queries(partition)?;

        let mut client_src = self.connect_side(&self.source_config, "source").await?;
        let tx_src = client_src.transaction().await.map_err(|e| {
            self.copy_failed(
                "Transaction start",
                "source",
                Some(partition),
                "beginning source transaction",
                e,
            )
        })?;

        let stream = tx_src.copy_out(&source_query).await.map_err(|e| {
            self.copy_failed(
                "COPY OUT",
                "source",
                Some(partition),
                source_query.clone(),
                e,
            )
        })?;

        let mut client_dest = self.connect_side(&self.dest_config, "destination").await?;
        let tx_dest = client_dest.transaction().await.map_err(|e| {
            self.copy_failed(
                "Transaction start",
                "destination",
                Some(partition),
                "beginning destination transaction",
                e,
            )
        })?;

        let sink = tx_dest.copy_in(&dest_query).await.map_err(|e| {
            self.copy_failed(
                "COPY IN",
                "destination",
                Some(partition),
                dest_query.clone(),
                e,
            )
        })?;

        pin_mut!(stream);
        pin_mut!(sink);

        let mut total_bytes = 0;
        let mut last_reported_bytes = 0;
        let mut last_flushed_bytes = 0;
        let mut last_flushed_time = tokio::time::Instant::now();
        let flush_interval = tokio::time::Duration::from_secs(5);

        while let Some(row_data) = stream.next().await {
            let data = row_data.map_err(|e| {
                self.copy_failed(
                    "COPY OUT (streaming)",
                    "source",
                    Some(partition),
                    source_query.clone(),
                    e,
                )
            })?;
            let len = data.len() as u64;
            total_bytes += len;

            sink.feed(data).await.map_err(|e| {
                self.copy_failed(
                    "COPY IN (streaming)",
                    "destination",
                    Some(partition),
                    dest_query.clone(),
                    e,
                )
            })?;

            if total_bytes - last_reported_bytes >= self.report_interval {
                let delta = total_bytes - last_reported_bytes;
                let _ = progress_tx.send(ProgressEvent::Bytes(delta)).await;
                last_reported_bytes = total_bytes;
                *reported_bytes = total_bytes;
            }

            let now = tokio::time::Instant::now();
            if total_bytes - last_flushed_bytes >= self.buffer_size
                || (total_bytes > last_flushed_bytes
                    && now.duration_since(last_flushed_time) >= flush_interval)
            {
                sink.flush().await.map_err(|e| {
                    self.copy_failed(
                        "COPY IN (flush)",
                        "destination",
                        Some(partition),
                        dest_query.clone(),
                        e,
                    )
                })?;
                last_flushed_bytes = total_bytes;
                last_flushed_time = now;
            }
        }

        // Final flush to ensure all buffered data is sent before closing.
        sink.flush().await.map_err(|e| {
            self.copy_failed(
                "COPY IN (final flush)",
                "destination",
                Some(partition),
                dest_query.clone(),
                e,
            )
        })?;

        sink.close().await.map_err(|e| {
            self.copy_failed(
                "COPY IN (finalize)",
                "destination",
                Some(partition),
                dest_query.clone(),
                e,
            )
        })?;

        tx_dest.commit().await.map_err(|e| {
            self.copy_failed(
                "Transaction commit",
                "destination",
                Some(partition),
                "committing destination transaction",
                e,
            )
        })?;

        tx_src.commit().await.map_err(|e| {
            self.copy_failed(
                "Transaction commit",
                "source",
                Some(partition),
                "committing source transaction",
                e,
            )
        })?;

        // Report any remaining bytes.
        if total_bytes > last_reported_bytes {
            let delta = total_bytes - last_reported_bytes;
            let _ = progress_tx.send(ProgressEvent::Bytes(delta)).await;
            *reported_bytes = total_bytes;
        }

        drop(client_src);
        drop(client_dest);

        info!(
            "Worker {} finished partition: {}. Total bytes: {}",
            self.id, partition, total_bytes
        );

        Ok(total_bytes)
    }

    /// Builds the `COPY` queries for the source and destination databases based
    /// on the partition's method and range/index.
    pub fn build_copy_queries(&self, partition: &Partition) -> Result<(String, String)> {
        let quoted_column = db::quote_ident(&partition.column);
        let conditions: Vec<String> =
            if &*partition.method == "hash" {
                let i = partition.from.as_ref().ok_or_else(|| {
                    CopyEngineError::Splitter("Hash partition missing index".into())
                })?;
                let n = partition.till.as_ref().ok_or_else(|| {
                    CopyEngineError::Splitter("Hash partition missing count".into())
                })?;
                vec![format!(
                    "abs(hashtext({quoted_column}::text)::bigint) % {n} = {i}"
                )]
            } else {
                [
                    partition
                        .from
                        .as_ref()
                        .map(|f| format!("{quoted_column} >= '{f}'")),
                    partition
                        .till
                        .as_ref()
                        .map(|t| format!("{quoted_column} < '{t}'")),
                ]
                .into_iter()
                .flatten()
                .collect()
            };

        let mut where_body = if conditions.is_empty() {
            String::new()
        } else {
            conditions.join(" AND ")
        };

        if partition.include_nulls {
            let null_cond = format!("{quoted_column} IS NULL");
            where_body = if where_body.is_empty() {
                null_cond
            } else {
                format!("(({where_body}) OR {null_cond})")
            };
        }

        let where_clause = if where_body.is_empty() {
            String::new()
        } else {
            format!(" WHERE {where_body}")
        };

        let quoted_table = db::quote_table_name(&self.table_name);
        let quoted_columns = self
            .columns
            .iter()
            .map(|c| db::quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let source_query =
            format!("COPY (SELECT {quoted_columns} FROM {quoted_table}{where_clause}) TO STDOUT");
        let dest_query = format!("COPY {quoted_table} ({quoted_columns}) FROM STDIN");

        Ok((source_query, dest_query))
    }
}

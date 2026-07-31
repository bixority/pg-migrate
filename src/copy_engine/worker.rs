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
use tokio_postgres::{Error as PgError, Transaction};
use tokio_util::sync::CancellationToken;

pub struct Worker {
    pub id: usize,
    pub source_config: Arc<str>,
    pub dest_config: Arc<str>,
    pub table_name: Arc<str>,
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
        buffer_size: u64,
        report_interval: u64,
    ) -> Self {
        Self {
            id,
            source_config,
            dest_config,
            table_name,
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
    /// them over a single pair of connections and a single pair of transactions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection to the source or destination database fails.
    /// - A transaction cannot be started.
    /// - Any partition copy fails.
    /// - A transaction cannot be committed.
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

        let mut client_src = self.connect_side(&self.source_config, "source").await?;
        let mut client_dest = self.connect_side(&self.dest_config, "destination").await?;

        let tx_src = client_src.transaction().await.map_err(|e| {
            self.copy_failed(
                "Transaction start",
                "source",
                None,
                "beginning source transaction",
                e,
            )
        })?;
        let tx_dest = client_dest.transaction().await.map_err(|e| {
            self.copy_failed(
                "Transaction start",
                "destination",
                None,
                "beginning destination transaction",
                e,
            )
        })?;

        let mut total_bytes = 0;
        loop {
            let bytes = self
                .copy_partition(&tx_src, &tx_dest, &partition, &progress_tx)
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

        tx_src.commit().await.map_err(|e| {
            self.copy_failed(
                "Transaction commit",
                "source",
                None,
                "committing source transaction",
                e,
            )
        })?;
        tx_dest.commit().await.map_err(|e| {
            self.copy_failed(
                "Transaction commit",
                "destination",
                None,
                "committing destination transaction",
                e,
            )
        })?;

        Ok(total_bytes)
    }

    /// Copies a single partition using the provided transactions.
    async fn copy_partition(
        &self,
        tx_src: &Transaction<'_>,
        tx_dest: &Transaction<'_>,
        partition: &Partition,
        progress_tx: &tokio::sync::mpsc::Sender<ProgressEvent>,
    ) -> Result<u64> {
        info!("Worker {} starting partition: {}", self.id, partition);

        let (source_query, dest_query) = self.build_copy_queries(partition)?;

        let stream = tx_src.copy_out(&source_query).await.map_err(|e| {
            self.copy_failed(
                "COPY OUT",
                "source",
                Some(partition),
                source_query.clone(),
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

            // Use `feed` to buffer the data instead of `send` which flushes after every chunk.
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
            }

            if total_bytes - last_flushed_bytes >= self.buffer_size {
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

        // Report any remaining bytes.
        if total_bytes > last_reported_bytes {
            let delta = total_bytes - last_reported_bytes;
            let _ = progress_tx.send(ProgressEvent::Bytes(delta)).await;
        }

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
        let mut conditions: Vec<String> =
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

        if partition.include_nulls
            && let Some(last) = conditions.pop()
        {
            conditions.push(format!("({last} OR {quoted_column} IS NULL)"));
        }
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let quoted_table = db::quote_table_name(&self.table_name);
        let source_query = format!("COPY (SELECT * FROM {quoted_table}{where_clause}) TO STDOUT");
        let dest_query = format!("COPY {quoted_table} FROM STDIN");

        Ok((source_query, dest_query))
    }
}

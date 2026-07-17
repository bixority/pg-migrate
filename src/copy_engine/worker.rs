use crate::copy_engine::error::{CopyEngineError, CopyFailure, Result};
use crate::copy_engine::splitter::Partition;
use futures_util::{SinkExt, StreamExt, pin_mut};
use log::{error, info};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Error as PgError, Transaction};

pub struct Worker {
    id: usize,
    source_config: Arc<str>,
    dest_config: Arc<str>,
    table_name: Arc<str>,
}

impl Worker {
    #[must_use]
    pub const fn new(
        id: usize,
        source_config: Arc<str>,
        dest_config: Arc<str>,
        table_name: Arc<str>,
    ) -> Self {
        Self {
            id,
            source_config,
            dest_config,
            table_name,
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
        detail: String,
        source: PgError,
    ) -> CopyEngineError {
        let hint = self.missing_relation_hint(&source, side);
        CopyEngineError::CopyFailed(Box::new(CopyFailure {
            stage,
            side,
            table: self.table_name.to_string(),
            partition: partition.map_or_else(|| "none".to_string(), ToString::to_string),
            detail,
            hint,
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
        let (client, connection) = tokio_postgres::connect(config, crate::tls::make_tls())
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
        progress_tx: tokio::sync::mpsc::Sender<u64>,
    ) -> Result<u64> {
        let mut client_src = self.connect_side(&self.source_config, "source").await?;
        let mut client_dest = self.connect_side(&self.dest_config, "destination").await?;

        let tx_src = client_src.transaction().await.map_err(|e| {
            self.copy_failed(
                "Transaction start",
                "source",
                None,
                "beginning source transaction".into(),
                e,
            )
        })?;
        let tx_dest = client_dest.transaction().await.map_err(|e| {
            self.copy_failed(
                "Transaction start",
                "destination",
                None,
                "beginning destination transaction".into(),
                e,
            )
        })?;

        let mut total_bytes = 0;
        loop {
            let partition = {
                let mut guard = rx.lock().await;
                guard.recv().await
            };

            let Some(partition) = partition else {
                break;
            };

            let bytes = self.copy_partition(&tx_src, &tx_dest, &partition).await?;
            total_bytes += bytes;
            let _ = progress_tx.send(bytes).await;
        }

        tx_src.commit().await.map_err(|e| {
            self.copy_failed(
                "Transaction commit",
                "source",
                None,
                "committing source transaction".into(),
                e,
            )
        })?;
        tx_dest.commit().await.map_err(|e| {
            self.copy_failed(
                "Transaction commit",
                "destination",
                None,
                "committing destination transaction".into(),
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
    ) -> Result<u64> {
        info!("Worker {} starting partition: {}", self.id, partition);

        let conditions: Vec<String> = if partition.method == "hash" {
            let i = partition.from.as_ref().ok_or_else(|| {
                CopyEngineError::Splitter("Hash partition missing index".to_string())
            })?;
            let n = partition.till.as_ref().ok_or_else(|| {
                CopyEngineError::Splitter("Hash partition missing count".to_string())
            })?;
            vec![format!(
                "abs(hashtext({}::text)::bigint) % {} = {}",
                partition.column, n, i
            )]
        } else {
            [
                partition
                    .from
                    .as_ref()
                    .map(|f| format!("{} >= '{}'", partition.column, f)),
                partition
                    .till
                    .as_ref()
                    .map(|t| format!("{} < '{}'", partition.column, t)),
            ]
            .into_iter()
            .flatten()
            .collect()
        };
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let source_query = format!(
            "COPY (SELECT * FROM {}{}) TO STDOUT",
            self.table_name, where_clause
        );

        let dest_query = format!("COPY {} FROM STDIN", self.table_name);

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
            total_bytes += data.len() as u64;
            sink.send(data).await.map_err(|e| {
                self.copy_failed(
                    "COPY IN (streaming)",
                    "destination",
                    Some(partition),
                    dest_query.clone(),
                    e,
                )
            })?;
        }

        sink.close().await.map_err(|e| {
            self.copy_failed(
                "COPY IN (finalize)",
                "destination",
                Some(partition),
                dest_query.clone(),
                e,
            )
        })?;

        info!(
            "Worker {} finished partition: {}. Total bytes: {}",
            self.id, partition, total_bytes
        );

        Ok(total_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_new() {
        let worker = Worker::new(1, Arc::from("src"), Arc::from("dest"), Arc::from("table"));
        assert_eq!(worker.id, 1);
        assert_eq!(&*worker.source_config, "src");
        assert_eq!(&*worker.dest_config, "dest");
        assert_eq!(&*worker.table_name, "table");
    }
}

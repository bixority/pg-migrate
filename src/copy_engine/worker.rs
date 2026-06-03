use crate::copy_engine::error::{CopyEngineError, Result};
use crate::copy_engine::splitter::Partition;
use futures_util::{SinkExt, StreamExt, pin_mut};
use log::{error, info};

pub struct Worker {
    id: usize,
    source_config: String,
    dest_config: String,
    table_name: String,
}

impl Worker {
    #[must_use]
    pub const fn new(
        id: usize,
        source_config: String,
        dest_config: String,
        table_name: String,
    ) -> Self {
        Self {
            id,
            source_config,
            dest_config,
            table_name,
        }
    }

    /// Runs the worker for a single partition.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection to the source or destination database fails.
    /// - The `COPY` operation fails.
    pub async fn run(&self, partition: Partition) -> Result<u64> {
        info!("Worker {} starting partition: {}", self.id, partition);

        let (client_src, connection_src) =
            tokio_postgres::connect(&self.source_config, crate::tls::make_tls()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection_src.await {
                error!("Source connection error: {e}");
            }
        });

        let (client_dest, connection_dest) =
            tokio_postgres::connect(&self.dest_config, crate::tls::make_tls()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection_dest.await {
                error!("Destination connection error: {e}");
            }
        });

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

        let stream = client_src
            .copy_out(&source_query)
            .await
            .map_err(CopyEngineError::Connection)?;
        let sink = client_dest
            .copy_in(&dest_query)
            .await
            .map_err(CopyEngineError::Connection)?;

        pin_mut!(stream);
        pin_mut!(sink);

        let mut total_bytes = 0;

        while let Some(row_data) = stream.next().await {
            let data = row_data.map_err(CopyEngineError::Connection)?;
            total_bytes += data.len() as u64;
            sink.send(data).await.map_err(CopyEngineError::Connection)?;
        }

        sink.close().await.map_err(CopyEngineError::Connection)?;

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
        let worker = Worker::new(
            1,
            "src".to_string(),
            "dest".to_string(),
            "table".to_string(),
        );
        assert_eq!(worker.id, 1);
        assert_eq!(worker.source_config, "src");
        assert_eq!(worker.dest_config, "dest");
        assert_eq!(worker.table_name, "table");
    }
}

# High-Performance PostgreSQL Migration Engine

The `copy_engine` is a modular, high-performance migration tool designed to move large-scale PostgreSQL tables 
(hundreds of GB to multi-TB) between databases with maximum throughput and minimal resource overhead.

## Core Principles

- **Exclusively COPY Protocol**: Uses binary-safe streaming `COPY` to transfer data.
  - Source: `COPY (SELECT ...) TO STDOUT`
  - Destination: `COPY table FROM STDIN`
- **Zero Row-by-Row Processing**: Data flows as a continuous byte stream without being parsed into rows or ORM objects.
- **Async Streaming**: Built on `tokio` and `tokio-postgres`, ensuring non-blocking I/O and natural backpressure propagation.
- **Parallel Execution**: Orchestrates multiple independent workers handling disjoint data partitions.

## Architecture

The engine is composed of four primary modules:

### 1. Orchestrator (`src/copy_engine/orchestrator.rs`)
The central coordinator that:
- Manages the lifecycle of migration tasks.
- Limits concurrency using semaphores to prevent saturating disk I/O or WAL (Write-Ahead Log).
- Collects and aggregates metrics (e.g., total bytes transferred).

### 2. Worker (`src/copy_engine/worker.rs`)
The execution unit for a single data partition:
- Establishes independent source and destination connections.
- Pipelines the `COPY OUT` stream directly into the `COPY IN` sink.
- Maintains bounded buffers to ensure constant memory usage regardless of table size.

### 3. Splitter (`src/copy_engine/splitter.rs`)
Handles data partitioning logic to enable parallelism:
- **Time-Range Partitioning**: Splits data based on a timestamp column (e.g., `created_at`).
- **Hash-Based Partitioning**: Used for skewed data or when time-based ranges are not applicable.
- Ensures partitions are deterministic and non-overlapping.

### 4. Error Handling (`src/copy_engine/error.rs`)
Uses `thiserror` for structured, type-safe error propagation:
- Distinguishes between connection issues, I/O errors, and worker-specific failures.
- Ensures partial failures in one worker are cleanly reported to the orchestrator without corrupting other partitions.

## Performance and Reliability

- **Throughput**: Designed to be disk-bound or network-bound, reaching hundreds of MB/s depending on hardware.
- **Backpressure**: If the destination database slows down (e.g., due to WAL pressure), the async sink will naturally slow down the source's `COPY OUT` stream.
- **Fault Tolerance**: Workers operate independently. The orchestrator tracks completion, allowing for conceptual restartability at the partition level.

## Usage

The engine is integrated into the `pg-migrate` CLI and can be triggered using the following arguments:

Configure copy rules in `config.toml`. You can specify multiple rules for the same table (e.g., to migrate different time periods in sequence):

```toml
[[copy_rules]]
table = "mydb.large_table"
split_by_column = "created_at"
from = "2023-01-01"
till = "2023-06-01"

[[copy_rules]]
table = "mydb.large_table"
split_by_column = "created_at"
from = "2023-06-01"
till = "2024-01-01"
```

- `table`: Fully-qualified table name (`database.table`).
- `split_by_column`: Column used for the `WHERE` condition and partitioning (default: `created_at`).
- `method`: Partitioning method. `time` (default) splits a time range. `hash` uses modulus-like partitioning (useful for large tables without a clear time range).
- `from`: Inclusive lower bound for `time` method — generates `split_by_column >= 'from'`. If omitted, the minimum value is automatically discovered from the database to enable parallel splitting.
- `till`: Exclusive upper bound for `time` method — generates `split_by_column < 'till'`. If omitted, the maximum value is automatically discovered from the database to enable parallel splitting.

When `method` is `time`, the range is split into parallel sub-partitions (automatically discovering missing bounds if necessary).
When `method` is `hash`, the table is split into `num_partitions` based on the hash of the column values.
- `--max-parallel`: Number of concurrent workers (default matches global parallelism settings).

## Integration with the migration pipeline

A copy-engine table is treated as **deferred** throughout the pipeline, the same
way `delay_table_data` tables are — it does not need to appear in
`delay_table_data`:

- **Regular `pg_dump`**: the table's data is excluded (`--exclude-table-data`);
  only its schema is dumped/restored, so the destination table exists (empty)
  before the copy engine runs.
- **Delayed `pg_dump`**: the table is excluded (`--exclude-table`), so its data
  is never dumped by `pg_dump` even if it also matches a `delay_table_data`
  pattern. The copy engine is the single owner of that data.
- **Copy engine**: runs during the delayed phase and `COPY`s the data directly.
- **Verification**: the table is row-count-verified only in the *delayed*
  verification pass (after the copy completes), never in the regular pass —
  otherwise it would be compared while still empty and fail the migration.

## Technical Requirements

- **Rust**: 1.96 (2024 edition)
- **Runtime**: `tokio`
- **Database Driver**: `tokio-postgres` (COPY protocol support)

### PostgreSQL Migration Tool

This tool automates migrating every user database (and global objects) between two PostgreSQL instances. It drives `pg_dump`/`pg_restore` in parallel, optionally defers the bulk data of nominated tables to a separate pass that runs after all regular databases finish, can stream the very largest tables with a custom binary `COPY` engine, and verifies row counts on both sides.

### Purpose

The tool migrates all user databases from a source server to a target server. It also migrates global objects like roles while carefully avoiding overwriting the migration user's credentials on the target.

For very large tables, two deferral mechanisms keep them out of the critical path:

- **Delayed data** — the table's schema, constraints, indexes, sequences, and triggers are created during the regular restore (so the table is fully usable), but its bulk data is dumped `--data-only` and restored in a separate **delayed pass** that starts as soon as its own regular pipeline (schema/small tables) has finished. This lets all the schema/structure work and the small tables complete first, while large tables continue in the background.
- **Copy engine** — for tables too large even for the delayed `pg_dump` path, a built-in streaming engine partitions the table by a time/date/hash column and pipelines raw binary `COPY OUT → COPY IN` across many parallel workers. See [COPY_ENGINE.md](COPY_ENGINE.md).
- **Exclusion** — the `exclude` parameter lets you skip entire databases, schemas, or specific tables. These entities are not created on the target and are omitted from all migration and verification phases.

> **Note:** This tool does **not** drop/recreate indexes around the bulk load, and it does **not** alter destination server settings (`fsync`, `maintenance_work_mem`, etc.). If you want a "fast restore" configuration on the target, set it yourself before running (e.g. `postgres -c fsync=off -c synchronous_commit=off`) and revert it afterwards.

### Building

To build a static binary for the current architecture, use the provided Makefile. It requires Rust and the musl target.

- `make build` — release binary at `target/<arch>-unknown-linux-musl/release/pg-migrate`.
- `make compress` — strip and compress the binary with UPX (brute).
- `make release` — `build` + `compress` in one step.
- `make clean` — `cargo clean`.

Alternatively, `cargo build --release` produces a non-static binary at `target/release/pg-migrate`.

### Running Locally

Ensure PostgreSQL client utilities `pg_dump`, `pg_dumpall`, and `pg_restore` are available on `$PATH`. They must be from a version compatible with the source server (typically the destination's major version or newer).

#### Usage

```bash
pg-migrate \
  --config config.toml \
  --from-host source-db.example.com --from-port 5432 \
  --from-user postgres --from-pass secret123 \
  --to-host   target-db.example.com --to-port 5433 \
  --to-user   postgres --to-pass newsecret456
```

The tool discovers databases on the source, dumps them to `dump_root` (specified in config), restores them to the target, and verifies row counts. State files in `$HOME/pg_migrate_state` and `$HOME/pg_verify_state` let it resume after interruption (Ctrl-C cancels gracefully and kills child `pg_dump`/`pg_restore` processes).

> **All tuning is done in `config.toml`, not on the command line.** The only command-line flags are the source/target connection settings, `--config`, and `--sslmode` (see [Configuration](#configuration)). Parallelism, deferred tables, copy rules, verification mode, etc. are TOML keys.

### Process Flow

```
                       +--------------------------------+
                       |        Startup / Prep          |
                       +--------------------------------+
                       | discover_databases  (source)   |
                       |   pg_database, size ASC        |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | migrate_globals (if enabled)   |
                       |   pg_dumpall --globals-only,    |
                       |   filter destination superuser  |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | create_dbs  (CREATE DATABASE)  |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | build migration plan (source   |
                       | catalog): classify each table  |
                       | as regular / delayed / copy    |
                       +---------------+----------------+
                                       v
              +--------------------------------------------+
              | Spawn, per DB, a REGULAR pipeline (always) |
              | and a DELAYED pipeline (only if the DB has |
              | delayed tables or copy rules). Two         |
              | independent semaphores throttle the        |
              | source-side (dump_sem) and destination-    |
              | side (restore_sem) stages so the next dump |
              | overlaps the previous restore.             |
              +-----------------------+--------------------+

   ===================== REGULAR PIPELINE =====================
   === acquire dump_sem (dump_parallel slots, source-side) ====
                                      v
                       +--------------------------------+
                       | 1. dumping                     |
                       |    pg_dump -Fd -j dump_jobs    |
                       |             -Z zstd:<level>    |
                       |    delayed/copy tables get     |
                       |       --exclude-table-data     |
                       +---------------+----------------+
                                       v
   ===================== release dump_sem =====================
   === acquire restore_sem (restore_parallel, dest-side) ======
                                      v
                       +--------------------------------+
                       | 2. restoring                   |
                       |    pg_restore -j restore_jobs  |
                       |               --disable-       |
                       |               triggers         |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 3. source counts               |
                       |    concurrent count(*),        |
                       |    GLOBALLY capped by          |
                       |    verify_concurrency (one     |
                       |    semaphore shared by every   |
                       |    DB and both servers).       |
                       |    fast_verify => single       |
                       |    pg_class.reltuples query    |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 4. dest counts (same logic)    |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 5. verifying  (compare maps;   |
                       |    deferred tables excluded)   |
                       |    strict => mismatch fails;   |
                       |    fast_verify => warning      |
                       +---------------+----------------+
                                       v
   ===================== release restore_sem ==================
                                       v
                                   Complete

   ===================== DELAYED PIPELINE =====================
   (only for DBs with delayed_table_data and/or copy_rules)
                                      v
   === acquire dump_sem ======================================
                       +--------------------------------+
                       | A. delayed dumping             |
                       |    pg_dump -Fd --data-only     |
                       |       --table=<delayed pattern>|
                       |       --exclude-table=<copy>   |
                       +---------------+----------------+
   === release dump_sem ======================================
                                      v
                  wait until THIS database's regular
                  pipeline is done (guarantees schema
                  already exists on the target)
                                     v
   === acquire restore_sem ===================================
                       +--------------------------------+
                       | B. delayed restoring           |
                       |    pg_restore --data-only      |
                       |                --disable-      |
                       |                triggers        |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | C. copy engine                 |
                       |    for each copy_rule: split   |
                       |    into partitions, stream     |
                       |    binary COPY OUT -> COPY IN  |
                       |    (max_parallel workers)      |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | D. delayed verifying           |
                       |    re-run count comparison of  |
                       |    the REGULAR tables (delayed |
                       |    and copy tables are never   |
                       |    row-count compared)         |
                       +---------------+----------------+
   === release restore_sem ===================================
                                      v
                                   Complete

   After every DB reaches Complete:
       render summary table; print regular vs. total durations.
```

Pipeline overlap (illustrative timeline with `dump_parallel = restore_parallel = 2`):

```
  time -->

  DB A : [ dump ][ restore ][ src ][ dst ][ verify ] -> Complete
  DB B :        [ dump ][ restore ][ src ][ dst ][ verify ] -> Complete
  DB C :               [ dump ][ restore ][ src ][ dst ][ verify ] -> ...
                ^             ^
                |             |
                |             +-- DB C grabs dump_sem the moment B vacates it,
                |                 even while A and B are still restoring.
                +-- DB B grabs dump_sem as soon as A finishes dumping;
                    A keeps holding restore_sem through restore + counts + verify.

  Legend:  src/dst = source/dest count(*) (or reltuples in fast_verify),
                     run under restore_sem after the restore completes.
```

### Migration Workflow

Per-server, before any database:

1. **Preparation** — create `$HOME/pg_migrate_state` and `$HOME/pg_verify_state`.
2. **Discovery** — list user databases on the source via `pg_database`, ordered by size ascending.
3. **Globals** (when `migrate_globals = true`) — `pg_dumpall --globals-only`, filter out `CREATE/ALTER ROLE` lines that would overwrite the destination superuser, and execute the rest. Existing-object errors (and deprecated-MD5-password warnings) are tolerated.
4. **Database creation** — `CREATE DATABASE` for every discovered database on the target (already-exists errors are warned and ignored).
5. **Planning** — query the source catalog and classify every table as **regular**, **delayed**, or **copy-engine** (copy wins over delayed, delayed wins over regular). The plan is printed before migration starts.

Then, in parallel (bounded by the concurrency knobs), each database runs through a **regular phase**:

| Step | Phase           | What it does                                                                                                                                                                                                                                                                                                                                  |
|------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1    | `dumping`       | `pg_dump -Fd -j <dump_jobs> -Z zstd:<zstd_level>`. Tables matching any `delay_table_data` pattern or any `copy_rules` table are emitted with `--exclude-table-data` (schema kept, data skipped).                                                                                                                                              |
| 2    | `restoring`     | `pg_restore -j <restore_jobs> --disable-triggers`. Restores schema, indexes, PKs, FKs, sequences, triggers, and the data of regular tables. Deferred tables exist but are empty.                                                                                                                                                              |
| 3    | `source counts` | `SELECT count(*)` per table on the source, cached to `$HOME/pg_verify_state/<db>.src_counts.json`. Queries run concurrently, globally bounded by `verify_concurrency`. With `fast_verify`, a single `pg_class.reltuples` query replaces per-table counts (cached to `<db>.src_counts.fast.json`). Deferred (delayed/copy) tables are skipped. |
| 4    | `dest counts`   | Same as step 3, against the destination.                                                                                                                                                                                                                                                                                                      |
| 5    | `verifying`     | Compare source vs. destination counts. Mismatches fail the migration. With `fast_verify`, mismatches are logged as warnings rather than failures (since `reltuples` is an estimate).                                                                                                                                                          |

If a database has no delayed tables and no copy rules, it reaches `complete` here.

Databases with delayed tables and/or copy rules also run a **delayed phase**. Its delayed *dump* starts as soon as `dump_sem` is free, but its *restore* waits until its own regular pipeline has completed:

| Step | Phase               | What it does                                                                                                                                                                                                   |
|------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| A    | `delayed dumping`   | `pg_dump -Fd --data-only --table=<pattern>` for delayed tables, with `--exclude-table` for any copy-engine tables. (Skipped if the DB has only copy rules and no delayed tables.)                              |
| B    | `delayed restoring` | `pg_restore --data-only --disable-triggers` COPYs the delayed data into the already-built tables.                                                                                                              |
| C    | `copy engine`       | For each `copy_rules` entry, partition the table and stream it with the binary COPY engine. Each rule writes a completion marker so a resumed run skips finished rules.                                        |
| D    | `delayed verifying` | Re-run the row-count comparison of the **regular** tables (using a separate cache namespace). Delayed-data and copy-engine tables are migrated out-of-band and are **not** row-count compared in either phase. |

Finally a summary table is printed with the regular-phase and total durations.

### Connection Budget

PostgreSQL connections are bound to a specific database at handshake, so the tool keeps a small cached pool per `(server, database)` (idle connections drain in ~10s). All tuning below is set in `config.toml`. Approximate peak client-side connections to either server:

```
peak_source_conns  ≈  dump_parallel × (1 + dump_jobs)    # active pg_dump leaders + workers
                     + verify_concurrency                  # global cap, all DBs combined
                     + max_parallel                        # copy-engine source workers (delayed phase)

peak_dest_conns    ≈  restore_parallel × (1 + restore_jobs) # active pg_restore leaders + workers
                     + verify_concurrency                    # global cap
                     + max_parallel                          # copy-engine dest workers (delayed phase)
```

With the example defaults (`max_parallel = 6`, `dump_jobs = 24`, `restore_jobs = 12`, `verify_concurrency = 16`) the source needs roughly `6 × 25 + 16 + 6 ≈ 172` slots and the destination roughly `6 × 13 + 16 + 6 ≈ 100`. PostgreSQL's default `max_connections = 100` is therefore **too low** for the defaults — raise it (e.g. `postgres -c max_connections=300`) or lower the parallelism knobs in `config.toml`.

If you can't change server settings, throttle the source side independently with `dump_parallel` (which defaults to `max_parallel`):

```toml
# Source has max_connections=100, room for ~3 × 25 = 75 pg_dump conns + overhead.
# Destination has room for more parallel restores.
max_parallel = 6
dump_parallel = 3
restore_parallel = 6
```

### State and Resumability

Markers under `$HOME/pg_migrate_state/`:

- `globals.done`
- `<db>.dumped`, `<db>.done` — regular dump/restore complete
- `<db>.delayed_dumped`, `<db>.delayed_done` — delayed dump/restore complete
- `<db>.<schema.table>.copy.<hash>.done` — a copy-engine rule finished (hash identifies the specific rule, so multiple rules on one table track independently)

Markers under `$HOME/pg_verify_state/`:

- `<db>.src_counts.json`, `<db>.dst_counts.json` — cached counts (regular phase, strict verify)
- `<db>.src_counts.fast.json`, `<db>.dst_counts.fast.json` — cached counts when `fast_verify` is used (separate files so modes don't collide)
- `<db>.src_counts.delayed.json`, `<db>.dst_counts.delayed.json` (plus `.fast` variants) — cached counts for the delayed-verify pass
- `<db>.verify`, `<db>.delayed_verify` — verification complete

Re-running the tool resumes from wherever it stopped (it also skips any dump whose target directory already contains a `toc.dat`). Delete the relevant markers (and the `dump_root/<db>` directory if you want a fresh dump) to redo a step.

### Launching with Podman Compose

The bundled `compose.yml` brings up a Postgres source, a Postgres target, and a one-shot migration container that depends on both being healthy.

```bash
podman-compose up --build
```

> **Heads-up:** deferred tables and copy rules are configured in `config.toml`, which the `Containerfile` does **not** copy into the runtime image — only the binary is. To exercise `delay_table_data`/`copy_rules` in a container you must mount or `COPY` a `config.toml` into the image and pass `--config` to it; otherwise the container runs with built-in defaults (every table on the regular path). The connection flags in `compose.yml`'s `command:` are passed through to the binary.

### Configuration

#### CLI Arguments

**Global**
- `-c`, `--config` — path to the TOML configuration file. When omitted, `config.toml` in the working directory is used if present, otherwise built-in defaults apply. When given explicitly, the file **must** exist and parse — a missing or invalid path is a hard error (so a typo like `--config config.yaml` fails loudly instead of silently running with defaults, which would send every table down the regular path).

**Source connection**
- `--from-host` — source host (default: `localhost`)
- `--from-port` — source port (default: `5432`)
- `--from-user` — source user (default: `postgres`)
- `--from-pass` — source password (default: `oldpass`)
- `--from-db` — initial database used for discovery (default: `postgres`)

**Target connection**
- `--to-host` — target host (default: `localhost`)
- `--to-port` — target port (default: `5432`)
- `--to-user` — target user (default: `postgres`)
- `--to-pass` — target password (default: `newpass`)
- `--to-db` — initial database used for globals (default: `postgres`)

**TLS**
- `--sslmode` — TLS mode for native connections: `disable`, `prefer`, or `require`. Overrides the `sslmode` value from the config file when set.

There are no command-line flags for parallelism, deferred tables, copy rules, globals, or verification mode — those live in the TOML file below.

#### TOML Configuration

The configuration file (default `config.toml`) is shown below; the repository ships a working `config.toml` you can copy. All parameters are optional and use their default values if omitted.

```toml
# Number of parallel jobs (-j) for pg_dump per database (default: 24)
dump_jobs = 24

# Number of parallel jobs (-j) for pg_restore per database (default: 12).
# Automatically disabled if restore_single_transaction is true.
restore_jobs = 12

# (Optional) Use --single-transaction for pg_restore (default: false).
# Reduces "Xact committed" count but disables parallel restore jobs (-j).
# restore_single_transaction = false

# Maximum number of databases being migrated concurrently (default: 6).
# Also defines the size of the worker pool for copy-engine migration.
max_parallel = 6

# (Optional) Independent override for source-side concurrency (defaults to max_parallel)
# dump_parallel = 6

# (Optional) Independent override for destination-side concurrency (defaults to max_parallel)
# restore_parallel = 6

# Local directory where database dumps are stored (default: "pg_dumps")
dump_root = "pg_dumps"

# Whether to migrate global objects like roles and groups (default: true)
migrate_globals = true

# List of patterns whose bulk data is deferred to a separate pass that runs after
# every regular pipeline finishes. The schema and all structure are restored
# normally during the regular phase; only the table DATA is loaded later.
# Patterns can be "DB", "DB.SCHEMA", or "DB.SCHEMA.TABLE". SCHEMA and
# TABLE_PATTERN may use pg_dump wildcards (* = any sequence, ? = one character),
# so entire schemas can be targeted (e.g. "mydb.audit").
# delay_table_data = [
#   "mydb.public.large_table",
#   "mydb.public.events_*",
# ]

# List of patterns to entirely exclude from the migration. Matched entities are
# not created on the target and are skipped by both the regular and delayed
# passes. Patterns can be "DB", "DB.SCHEMA", or "DB.SCHEMA.TABLE". Use "mydb"
# to exclude a whole database or "mydb.internal" to exclude a whole schema.
# exclude = [
#   "mydb.internal.*",
#   "test_db.*.*",
# ]

# If true, uses pg_class.reltuples estimates instead of count(*) (default: false).
# Mismatches become warnings rather than hard failures, since reltuples is an
# estimate; ANALYZE both sides for closer values.
fast_verify = false

# Global cap on concurrent row-count/verification queries
# across all databases and both servers (default: 16).
verify_concurrency = 16

# Zstd compression level for database dumps (1-22, default: 5)
zstd_level = 5

# TLS mode for native (tokio-postgres) connections: disable, prefer, or require
# (default: "prefer"). Mirrors libpq's sslmode: "prefer" negotiates TLS when the
# server offers it and falls back to plaintext, so it works against both TLS and
# non-TLS servers. The server certificate is not verified (matching
# sslmode=prefer/require). Overridable via --sslmode.
sslmode = "prefer"

# (Optional) Tables to migrate using the high-performance Copy Engine.
# See COPY_ENGINE.md for details. A copy-engine table is treated as deferred
# automatically: it is excluded from both the regular and delayed pg_dump,
# migrated by the copy engine, and skipped by row-count verification — so it does
# NOT need to be listed in delay_table_data. Each `table` must be a fully-
# qualified "DATABASE.SCHEMA.TABLE"; anything else is a hard error. Multiple rules
# may target the same table (e.g. to split it into separate time ranges).
[[copy_rules]]
table = "DATABASE.SCHEMA.TABLE"   # The table to migrate (must be DATABASE.SCHEMA.TABLE)
split_by_column = "created_at"    # Column used for WHERE / partitioning (default: created_at)
method = "time"                   # Partitioning method: "time" (default), "date"/"day" (one partition per UTC day), or "hash"
from = "2023-01-01"               # Inclusive lower bound (optional; auto-discovered via min() for 'time' split when omitted)
till = "2024-01-01"               # Exclusive upper bound (optional; the last partition is always open-ended)
```

#### Environment Variables

- `HOME` — base directory for state markers, verification reports, and the default `pg_dumps/` root.
- `RUST_LOG` — log level (default `info`; e.g. `debug`, `warn`).
- `PGPASSWORD` — set automatically per child process from `--from-pass`/`--to-pass`; you don't need to export it.

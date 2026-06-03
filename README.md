### PostgreSQL Migration Tool

This tool automates migrating every user database (and global objects) between two PostgreSQL instances. It drives `pg_dump`/`pg_restore` in parallel, optionally defers bulk data for nominated tables until secondary indexes have been dropped (so the COPY runs without index overhead), and verifies row counts on both sides.

### Purpose

The tool migrates all user databases from a source server to a target server. It also migrates global objects like roles while carefully avoiding overwriting the migration user's credentials on the target. It optimizes the target server settings for fast restoration and reverts them after completion.

For very large tables, it can additionally defer the data load: the table schema, PK/UNIQUE constraints, FKs, sequences, and triggers are restored as part of the regular phase (so clients can already INSERT into the table using its sequences), but the table's secondary indexes are dropped before the bulk COPY and rebuilt afterwards, avoiding per-row index maintenance on millions of inserts.

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
                       | enable_fast_restore  (dest)    |
                       |   fsync/sync_commit=off,       |
                       |   maintenance_work_mem=2GB,    |
                       |   ALTER SYSTEM + reload        |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | migrate_globals                |
                       |   pg_dumpall --globals-only,   |
                       |   filter destination superuser |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | create_dbs  (CREATE DATABASE)  |
                       +---------------+----------------+
                                       v
              +--------------------------------------------+
              | Spawn one task per DB. Each task flows     |
              | through the per-DB pipeline below.         |
              | Two independent semaphores throttle the    |
              | source-side and destination-side stages so |
              | the next dump overlaps the previous restore|
              +-----------------------+--------------------+
                                      v
   ============================================================
   === acquire dump_sem  (max_parallel slots, source-side) ====
   ============================================================
                                      v
                       +--------------------------------+
                       | 1. dumping                     |
                       |    pg_dump -Fd -j dump-jobs    |
                       |             -Z zstd:5          |
                       |    delayed tables get          |
                       |       --exclude-table-data     |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 2. source counts               |
                       |    concurrent count(*),        |
                       |    GLOBALLY capped by          |
                       |    verify_concurrency (one     |
                       |    semaphore shared by every   |
                       |    DB and both servers).       |
                       |    --fast-verify => single     |
                       |    pg_class.reltuples query    |
                       +---------------+----------------+
                                       v
   ===================== release dump_sem =====================
                                       v
   ============================================================
   === acquire restore_sem (max_parallel, destination-side) ===
   ============================================================
                                       v
                       +--------------------------------+
                       | 3. restoring                   |
                       |    pg_restore -j restore-jobs  |
                       |               --disable-       |
                       |               triggers         |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 4. dest counts  (same logic as |
                       |    source counts)              |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 5. verifying  (compare maps)   |
                       |    fast-verify: non-delayed    |
                       |    mismatches => warning;      |
                       |    strict mode => failure      |
                       +---------------+----------------+
                                       v
                          mark_regular_done
                                       v
                       no delayed tables for this DB?
                       --------- yes ---------> Complete
                                  | no
                                  v
   ==================== release restore_sem ===================
                                       v
   ============================================================
   === acquire dump_sem again ================================
   ============================================================
                                       v
                       +--------------------------------+
                       | 7. delayed dumping             |
                       |    pg_dump --data-only         |
                       |             --table=<pattern>  |
                       +---------------+----------------+
                                       v
   ===================== release dump_sem =====================
                                       v
   ============================================================
   === acquire restore_sem again =============================
   ============================================================
                                       v
                       +--------------------------------+
                       | 8. drop indexes (destination)  |
                       |    DROP non-constraint indexes |
                       |    on delayed tables           |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 9. delayed restoring           |
                       |    pg_restore --data-only      |
                       |                --disable-      |
                       |                triggers        |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 10. recreate indexes           |
                       |     JoinSet + Semaphore(       |
                       |        restore-jobs);          |
                       |     CREATE INDEX in parallel   |
                       +---------------+----------------+
                                       v
                       +--------------------------------+
                       | 11. delayed verifying          |
                       |     exact count(*) on delayed; |
                       |     reltuples on the rest in   |
                       |     --fast-verify              |
                       +---------------+----------------+
                                       v
   ==================== release restore_sem ===================
                                       v
                                   Complete

   After every DB reaches Complete:
       restore_safe_settings  (ALTER SYSTEM RESET + reload)
       render summary table; print regular vs. total durations.
```

Pipeline overlap (illustrative timeline with `--max-parallel 2`):

```
  time -->

  DB A : [ dump ][ src ][ restore           ][ dst ][ verify ] -> Complete
  DB B :        [ dump ][ src ][ restore           ][ dst ][ verify ] -> Complete
  DB C :               [ dump ][ src ][ restore        ][ dst ][ verify ] -> ...
                ^             ^
                |             |
                |             +-- DB C grabs dump_sem the moment B vacates it,
                |                 even while A and B are still restoring.
                +-- DB B grabs dump_sem as soon as A finishes dumping;
                    A keeps holding restore_sem.

  Legend:  src/dst = source/dest count(*) (or reltuples in --fast-verify)
```

### Migration Workflow

Per-server, before any database:

1. **Preparation** — create `$HOME/pg_migrate_state` and `$HOME/pg_verify_state`.
2. **Discovery** — list user databases on the source via `pg_database`, ordered by size ascending.
3. **Destination optimization** (unless `--disable-dst-optimizations`) — `ALTER SYSTEM SET` to turn off `fsync`, `synchronous_commit`, `full_page_writes`, raise `maintenance_work_mem` to 2GB, set `checkpoint_completion_target=0.9`, then `pg_reload_conf()`.
4. **Globals** (unless `--migrate-globals=false`) — `pg_dumpall --globals-only`, filter out `CREATE/ALTER ROLE` lines that would overwrite the destination superuser, and execute the rest. Existing-object errors are tolerated.
5. **Database creation** — `CREATE DATABASE` for every discovered database on the target.

Then, in parallel across up to `--max-parallel` databases, each database runs through a **regular phase**:

| Step | Phase | What it does |
|------|-------|--------------|
| 1 | `dumping` | `pg_dump -Fd -j <dump-jobs> -Z zstd:5`. If any `--delay-table-data` patterns match this DB, the matching tables are emitted with `--exclude-table-data` (schema kept, data skipped). |
| 2 | `source counts` | `SELECT count(*)` per table on the source, cached to `$HOME/pg_verify_state/<db>.src_counts.json`. Queries run concurrently and the total in-flight queries across all DBs are bounded by `--verify-concurrency`. With `--fast-verify`, a single `pg_class.reltuples` query replaces per-table counts; cached to `<db>.src_counts.fast.json`. |
| 3 | `restoring` | `pg_restore -j <restore-jobs> --disable-triggers`. Restores schema, indexes, PKs, FKs, sequences, triggers, and the data of non-delayed tables. Delayed tables exist but are empty. |
| 4 | `dest counts` | Same as step 2, against the destination. |
| 5 | `verifying` | Compare source vs. destination counts (delayed tables excluded). Mismatches fail the migration. With `--fast-verify`, non-delayed mismatches are logged as warnings rather than failures (since `reltuples` is an estimate); delayed-table mismatches still fail. |

If a database has no matching `--delay-table-data` patterns, it transitions to `complete` (step 6) here.

Databases with delayed tables continue into a **delayed phase**:

| Step | Phase | What it does |
|------|-------|--------------|
| 7 | `delayed dumping` | `pg_dump -Fd --data-only --table=<pattern>` for the delayed tables. |
| 8 | `dropping indexes` | Query the source for each delayed table's non-constraint indexes (PK/UNIQUE/EXCLUDE indexes are kept). `DROP INDEX IF EXISTS` for each on the destination. |
| 9 | `delayed restoring` | `pg_restore --data-only --disable-triggers` COPYs delayed data into the now-index-less tables. |
| 10 | `recreating indexes` | Re-run each saved `CREATE INDEX` DDL on the destination, in parallel bounded by `--restore-jobs` (skipping any that already exist, for resumability). |
| 11 | `delayed verifying` | Re-count every table on both sides (including delayed) and compare. With `--fast-verify`, delayed tables still use exact `count(*)` (and must match); non-delayed tables use `reltuples`. |

Finally, the destination optimizations are reverted (`ALTER SYSTEM RESET` + reload) and a summary table is printed.

### Connection Budget

PostgreSQL connections are bound to a specific database at handshake, so the tool keeps a small cached pool per `(server, database)` (idle connections drain in ~10s). Total client-side connections to either server at peak are bounded by:

```
peak_source_conns  ≈  dump_parallel    × (1 + dump_jobs)     # active pg_dump workers
                     + verify_concurrency                     # global cap, all DBs combined
                     + small idle-pool residue                # drains in ~2s

peak_dest_conns    ≈  restore_parallel × (1 + restore_jobs)  # active pg_restore workers
                     + verify_concurrency                     # global cap
                     + restore_parallel × restore_jobs        # parallel CREATE INDEX (delayed phase only)
                     + small idle-pool residue
```

With defaults (`--max-parallel 6 --dump-jobs 24 --restore-jobs 12 --verify-concurrency 16`) the source needs roughly `6 × 25 + 16 ≈ 166` connection slots; the destination needs roughly `6 × 13 + 16 ≈ 94` during the regular phase and up to `6 × 12 + 16 ≈ 88` during delayed-index recreate. Both PostgreSQL's `max_connections` default of 100 is therefore **too low** for the defaults — bump it (e.g. `postgres -c max_connections=300`) or back off the parallelism knobs.

If you can't change server settings, throttle the source-side concurrency independently with `--dump-parallel`:

```bash
# Source has max_connections=100, can fit ~3 × 25 = 75 pg_dump conns + overhead.
# Destination has room for more parallel restores.
pg-migrate --max-parallel 6 --dump-parallel 3 --restore-parallel 6 ...
```

### State and Resumability

Markers under `$HOME/pg_migrate_state/`:

- `globals.done`
- `<db>.dumped`, `<db>.done` — regular dump/restore complete
- `<db>.delayed_dumped`, `<db>.delayed_done` — delayed dump/restore complete
- `<db>.delayed_indexes_dropped`, `<db>.delayed_indexes_recreated`

Markers under `$HOME/pg_verify_state/`:

- `<db>.src_counts.json`, `<db>.dst_counts.json` — cached counts (regular phase, strict verify)
- `<db>.src_counts.fast.json`, `<db>.dst_counts.fast.json` — cached counts when `--fast-verify` is used (separate files so modes don't collide)
- `<db>.src_counts.delayed.json`, `<db>.dst_counts.delayed.json` (plus `.fast` variants) — cached counts for the delayed-verify phase
- `<db>.verify`, `<db>.delayed_verify` — verification complete

Re-running the tool resumes from wherever it stopped. Delete the relevant markers (and the `--dump-root/<db>` directory if you want a fresh dump) to redo a step.

### Launching with Podman Compose

The bundled `compose.yml` brings up a Postgres 9.5 source, a Postgres 18 target, and a one-shot migration container that depends on both being healthy. Edit the `command:` in `compose.yml` to change flags (the bundled example uses `--delay-table-data pdb1.table3` and `pdb2.table*`).

```bash
podman-compose up --build
```

### Configuration

#### CLI Arguments

**Global**
- `-c`, `--config` — path to the TOML configuration file. When omitted, `config.toml` in the working directory is used if present, otherwise built-in defaults apply. When given explicitly, the file **must** exist and parse — a missing or invalid path is a hard error (so a typo like `--config config.yaml` fails loudly instead of silently running with defaults).

**Source connection**
- `--from-host` — source host (default: `localhost`)
- `--from-port` — source port (default: `5432`)
- `--from-user` — source user (default: `postgres`)
- `--from-pass` — source password (default: `oldpass`)
- `--from-db` — initial database for discovery (default: `postgres`)

**Target connection**
- `--to-host` — target host (default: `localhost`)
- `--to-port` — target port (default: `5432`)
- `--to-user` — target user (default: `postgres`)
- `--to-pass` — target password (default: `newpass`)
- `--to-db` — initial database for ALTER SYSTEM / globals (default: `postgres`)

**TLS**
- `--sslmode` — TLS mode for native connections: `disable`, `prefer`, or `require`. Overrides the `sslmode` value from the config file when set.

#### TOML Configuration

The configuration file (default `config.toml`) is shown below; the repository ships a working `config.toml` you can copy. All parameters are optional and will use their default values if omitted.

```toml
# Number of parallel jobs (-j) for pg_dump per database (default: 24)
dump_jobs = 24

# Number of parallel jobs (-j) for pg_restore per database (default: 12).
# Also bounds parallel CREATE INDEX during the delayed phase.
restore_jobs = 12

# Maximum number of databases being migrated concurrently (default: 6)
max_parallel = 6

# (Optional) Independent override for source-side concurrency (defaults to max_parallel)
# dump_parallel = 6

# (Optional) Independent override for destination-side concurrency (defaults to max_parallel)
# restore_parallel = 6

# Local directory where database dumps are stored (default: "pg_dumps")
dump_root = "pg_dumps"

# Whether to migrate global objects like roles and groups (default: true)
migrate_globals = true

# List of "DATABASE.TABLE_PATTERN" patterns whose data is deferred to a
# separate pass after the regular tables. Schema is restored normally, but bulk
# data is loaded after indexes are dropped to speed up restoration, and these
# tables are only row-count-verified once the delayed pass completes.
# TABLE_PATTERN uses pg_dump wildcards (* = any sequence, ? = one character).
# delay_table_data = [
#   "mydb.large_table",
#   "mydb.events_*",
# ]

# If true, uses pg_class.reltuples estimates instead of count(*)
# for regular tables (default: false).
fast_verify = false

# Global cap on concurrent row-count/verification queries
# across all databases (default: 16).
verify_concurrency = 16

# Zstd compression level for database dumps (1-22, default: 5)
zstd_level = 5

# TLS mode for native (tokio-postgres) connections: disable, prefer, or require
# (default: "prefer"). Mirrors libpq's sslmode: "prefer" negotiates TLS when the
# server offers it and falls back to plaintext, so it works against both TLS and
# non-TLS servers. The server certificate is not verified (matching
# sslmode=prefer/require). Overridable via --sslmode.
sslmode = "prefer"

# (Optional) List of tables to migrate using the high-performance Copy Engine.
# See COPY_ENGINE.md for details. A copy-engine table is treated as deferred
# automatically: it is excluded from both the regular and delayed pg_dump,
# migrated by the copy engine, and verified after the copy completes — so it
# does NOT need to be listed in delay_table_data. Each `table` must be a
# fully-qualified "DATABASE.TABLE" (an unqualified name is a hard error).
# Multiple rules can be specified for the same table.
[[copy_rules]]
table = "DATABASE.TABLE"          # The table to migrate (must be DATABASE.TABLE)
split_by_column = "created_at"    # Column used for WHERE / partitioning (default: created_at)
method = "time"                   # Partitioning method: "time" (default), "date"/"day" (one partition per UTC day), or "hash"
from = "2023-01-01"               # Inclusive lower bound (optional; auto-discovered for parallel 'time' split)
till = "2024-01-01"               # Exclusive upper bound (optional; auto-discovered for parallel 'time' split)
```

#### Environment Variables

- `HOME` — base directory for state markers, verification reports, and the default `pg_dumps/` root.
- `RUST_LOG` — log level (default `info`; e.g. `debug`, `warn`).
- `PGPASSWORD` — set automatically per child process from `--from-pass`/`--to-pass`; you don't need to export it.

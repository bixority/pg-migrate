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
  --from-host source-db.example.com --from-port 5432 \
  --from-user postgres --from-pass secret123 \
  --to-host   target-db.example.com --to-port 5433 \
  --to-user   postgres --to-pass newsecret456 \
  --max-parallel 6 \
  --dump-jobs 24 --restore-jobs 12 \
  --delay-table-data analytics.events \
  --delay-table-data warehouse.fact_*
```

The tool discovers databases on the source, dumps them to `--dump-root`, restores them to the target, and verifies row counts. State files in `$HOME/pg_migrate_state` and `$HOME/pg_verify_state` let it resume after interruption (Ctrl-C cancels gracefully and kills child `pg_dump`/`pg_restore` processes).

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

**Parallelism**
- `--dump-jobs` — `pg_dump -j` per database (default: `24`)
- `--restore-jobs` — `pg_restore -j` per database; also bounds parallel `CREATE INDEX` during delayed-index recreate (default: `12`)
- `-p`, `--max-parallel` — number of databases dumped/restored concurrently. Internally split into two semaphores (source-side and destination-side) so the next dump can overlap the previous restore (default: `6`)
- `--dump-parallel` — override the source-side semaphore independently of `--max-parallel`. Use this when your source's `max_connections` can't accommodate `max_parallel × (1 + dump_jobs)` connections (default: same as `--max-parallel`)
- `--restore-parallel` — override the destination-side semaphore independently (default: same as `--max-parallel`)
- `--verify-concurrency` — **global** cap on concurrent `count(*)` queries across all DBs and both servers during verification (default: `16`). Lower this if your source/destination has a tight `max_connections` budget.

**Behavior**
- `--dump-root` — local dump directory (default: `pg_dumps`)
- `--migrate-globals` — migrate roles/globals (default: `true`)
- `--disable-dst-optimizations` — skip the `ALTER SYSTEM` fast-restore knobs on the destination (default: `false`)
- `--fast-verify` — replace per-table `count(*)` with `pg_class.reltuples` (single query per side). Delayed tables still use exact counts. Run `ANALYZE` on both sides beforehand for meaningful estimates. Non-delayed mismatches become warnings, not failures (default: `false`)
- `--delay-table-data <DATABASE.TABLE_PATTERN>` — repeatable. Defers data load and secondary-index rebuild for the matching tables. `*` and `?` glob wildcards are supported in the table portion. Examples:
  - `mydb.events` — single table
  - `mydb.fact_*` — all tables in `mydb` whose name starts with `fact_`
  - `mydb.public.events_2024_*` — schema-qualified pattern

#### Environment Variables

- `HOME` — base directory for state markers, verification reports, and the default `pg_dumps/` root.
- `RUST_LOG` — log level (default `info`; e.g. `debug`, `warn`).
- `PGPASSWORD` — set automatically per child process from `--from-pass`/`--to-pass`; you don't need to export it.

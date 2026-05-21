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
| 2 | `source counts` | `SELECT count(*)` per table on the source; cached to `$HOME/pg_verify_state/<db>.src_counts.json`. |
| 3 | `restoring` | `pg_restore -j <restore-jobs> --disable-triggers`. Restores schema, indexes, PKs, FKs, sequences, triggers, and the data of non-delayed tables. Delayed tables exist but are empty. |
| 4 | `dest counts` | `SELECT count(*)` per non-delayed table on the destination. |
| 5 | `verifying` | Compare source vs. destination counts (delayed tables excluded). Mismatches fail the migration. |

If a database has no matching `--delay-table-data` patterns, it transitions to `complete` (step 6) here.

Databases with delayed tables continue into a **delayed phase**:

| Step | Phase | What it does |
|------|-------|--------------|
| 7 | `delayed dumping` | `pg_dump -Fd --data-only --table=<pattern>` for the delayed tables. |
| 8 | `dropping indexes` | Query the source for each delayed table's non-constraint indexes (PK/UNIQUE/EXCLUDE indexes are kept). `DROP INDEX IF EXISTS` for each on the destination. |
| 9 | `delayed restoring` | `pg_restore --data-only --disable-triggers` COPYs delayed data into the now-index-less tables. |
| 10 | `recreating indexes` | Re-run each saved `CREATE INDEX` DDL on the destination (skipping any that already exist, for resumability). |
| 11 | `delayed verifying` | Re-count every table on both sides (including delayed) and compare. |

Finally, the destination optimizations are reverted (`ALTER SYSTEM RESET` + reload) and a summary table is printed.

### State and Resumability

Markers under `$HOME/pg_migrate_state/`:

- `globals.done`
- `<db>.dumped`, `<db>.done` — regular dump/restore complete
- `<db>.delayed_dumped`, `<db>.delayed_done` — delayed dump/restore complete
- `<db>.delayed_indexes_dropped`, `<db>.delayed_indexes_recreated`

Markers under `$HOME/pg_verify_state/`:

- `<db>.src_counts.json`, `<db>.dst_counts.json` — cached counts
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
- `--restore-jobs` — `pg_restore -j` per database (default: `12`)
- `-p`, `--max-parallel` — number of databases dumped/restored concurrently (default: `6`)

**Behavior**
- `--dump-root` — local dump directory (default: `pg_dumps`)
- `--migrate-globals` — migrate roles/globals (default: `true`)
- `--disable-dst-optimizations` — skip the `ALTER SYSTEM` fast-restore knobs on the destination (default: `false`)
- `--delay-table-data <DATABASE.TABLE_PATTERN>` — repeatable. Defers data load and secondary-index rebuild for the matching tables. `*` and `?` glob wildcards are supported in the table portion. Examples:
  - `mydb.events` — single table
  - `mydb.fact_*` — all tables in `mydb` whose name starts with `fact_`
  - `mydb.public.events_2024_*` — schema-qualified pattern

#### Environment Variables

- `HOME` — base directory for state markers, verification reports, and the default `pg_dumps/` root.
- `RUST_LOG` — log level (default `info`; e.g. `debug`, `warn`).
- `PGPASSWORD` — set automatically per child process from `--from-pass`/`--to-pass`; you don't need to export it.

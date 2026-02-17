### PostgreSQL Migration Tool

This tool automates the migration of multiple databases and global objects between two PostgreSQL instances. It handles database discovery, parallel schema and data transfer using native PostgreSQL utilities, and verification of row counts.

### Purpose

The tool migrates all user databases from a source server to a target server. It also migrates global objects like roles while carefully avoiding overwriting the migration user's credentials on the target. It optimizes the target server settings for fast restoration and reverts them after completion.

### Building

To build a static binary for the current architecture, use the provided Makefile. It requires Rust and the musl target.

- Use the build target for a release binary.
- Use the compress target to further reduce the binary size using UPX.
- Use the release target to build and compress in one step.

Alternatively, use standard Cargo commands to build the project.

### Running Locally

Ensure PostgreSQL client utilities such as psql, pg_dump, and pg_restore are available in your system path.

#### Usage

```bash
pg-migrate \
  --from-host "source-db.example.com" \
  --from-user "postgres" \
  --from-pass "secret123" \
  --to-host "target-db.example.com" \
  --to-user "postgres" \
  --to-pass "newsecret456" \
  --jobs 4 \
  --max-parallel 2
```

Launch the compiled binary directly. It will discover databases, dump them to a local directory, and restore them to the target. Use the command line arguments to specify connection details for both source and target servers.

The tool uses state markers in the home directory to track progress, allowing it to resume if interrupted.

### Migration Workflow

The migration process follows these steps:

1.  **Preparation**: Creates local state and verification directories.
2.  **Discovery**: Queries the source server to list all available user databases.
3.  **Optimization**: Sets the target server to a "fast restore" mode (disabling fsync, etc.) to improve performance.
4.  **Globals**: Dumps and restores global objects like roles, filtering out the migration user to prevent credential overwrites.
5.  **Initialization**: Creates the required databases on the target server.
6.  **Migration**: Dumps each database from the source and restores it to the target in parallel. Uses directory-format dumps with compression.
7.  **Verification**: Compares table lists and row counts between source and target for every migrated database.
8.  **Cleanup**: Reverts the target server settings to their safe, original state.

### Launching with Podman Compose

The project includes a configuration for running the migration and testing databases in a containerized environment.

- Use podman-compose up to build the migration image and start the entire stack.
- This will launch a source database, a target database, and the migration tool as a one-shot service.
- The migration tool container depends on both databases being healthy before starting.

### Configuration

#### CLI Arguments

Source Connection:
- `--from-host`: Source database host (default: `localhost`).
- `--from-port`: Source database port (default: `5432`).
- `--from-user`: Username for source connection (default: `postgres`).
- `--from-pass`: Password for source connection (default: `oldpass`).
- `--from-db`: Initial database to connect to for discovery (default: `postgres`).

Target Connection:
- `--to-host`: Target database host (default: `localhost`).
- `--to-port`: Target database port (default: `5432`).
- `--to-user`: Username for target connection (default: `postgres`).
- `--to-pass`: Password for target connection (default: `newpass`).
- `--to-db`: Initial database to connect to for configuration (default: `postgres`).

Migration Options:
- `-j`, `--jobs`: Number of parallel jobs for a single database dump or restore (default: `4`).
- `-m`, `--max-parallel`: Number of databases to migrate concurrently (default: `4`).
- `--dump-root`: Local directory path for temporary dump files (default: `pg_dumps`).
- `--migrate-globals`: Boolean flag to enable or disable global objects migration (default: `true`).

#### Environment Variables

- HOME: Used to determine the default location for dump files, state markers, and verification reports.

use crate::error::{Error, Result};
use crate::Config;
use tokio::select;
use tokio_util::sync::CancellationToken;

pub async fn discover_databases(
    config: &Config,
    cancel: CancellationToken,
) -> Result<Vec<(String, u64)>> {
    let pool = select! {
        res = config.pool_cache.get(&config.source, &config.source_db) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database connection".into())),
    };

    let rows = select! {
        res = pool.query(
            "SELECT datname, pg_database_size(datname) AS size \
             FROM pg_database \
             WHERE datname NOT IN ('postgres','template0','template1') \
             AND datallowconn IS TRUE \
             ORDER BY pg_database_size(datname) ASC;",
             &[]
        ) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database discovery".into())),
    };

    let mut dbs = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let size: i64 = row.get(1);
        dbs.push((name, size.max(0).try_into().unwrap_or(0)));
    }

    dbs.sort_by_key(|&(_, size)| size);

    Ok(dbs)
}

pub async fn create_dbs(config: &Config, dbs: &[String], cancel: CancellationToken) -> Result<()> {
    let pool = select! {
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database connection".into())),
    };

    for db in dbs {
        let sql = format!("CREATE DATABASE \"{db}\"");
        select! {
            res = pool.execute(&sql, &[]) => {
                if let Err(e) = res {
                    if let Some(db_err) = e.as_db_error() && db_err.code() == &tokio_postgres::error::SqlState::DUPLICATE_DATABASE {
                            continue;
                    }
                    return Err(Error::ProcessFailed {
                        command: format!("CREATE DATABASE \"{db}\"").into(),
                        stderr: e.to_string().into(),
                    });
                }
            }
            () = cancel.cancelled() => return Err(Error::Cancelled(format!("database creation of {db}").into())),
        }
    }
    Ok(())
}

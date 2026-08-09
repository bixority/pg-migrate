use crate::Config;
use crate::error::{Error, Result};
use tokio::select;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredDb {
    pub name: String,
    pub size: u64,
    pub encoding: String,
    pub datcollate: String,
    pub datctype: String,
    pub owner: String,
}

pub async fn discover_databases(
    config: &Config,
    cancel: CancellationToken,
) -> Result<Vec<DiscoveredDb>> {
    let pool = select! {
        res = config.pool_cache.get(&config.source, &config.source_db) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database connection".into())),
    };

    let rows = select! {
        res = pool.query(
            "SELECT datname, pg_database_size(datname) AS size, \
             pg_encoding_to_char(encoding) AS encoding, \
             datcollate, datctype, \
             pg_get_userbyid(datdba) AS owner \
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
        let encoding: String = row.get(2);
        let datcollate: String = row.get(3);
        let datctype: String = row.get(4);
        let owner: String = row.get(5);

        dbs.push(DiscoveredDb {
            name,
            size: size.max(0).try_into().unwrap_or(0),
            encoding,
            datcollate,
            datctype,
            owner,
        });
    }

    dbs.sort_by_key(|db| db.size);

    Ok(dbs)
}

pub async fn create_dbs(
    config: &Config,
    dbs: &[DiscoveredDb],
    cancel: CancellationToken,
) -> Result<()> {
    let pool = select! {
        res = config.pool_cache.get(&config.destination, &config.destination_db) => res?,
        () = cancel.cancelled() => return Err(Error::Cancelled("database connection".into())),
    };

    for db in dbs {
        let quoted_name = super::quote_ident(&db.name);
        let quoted_owner = super::quote_ident(&db.owner);
        let quoted_encoding = super::quote_literal(&db.encoding);
        let quoted_collate = super::quote_literal(&db.datcollate);
        let quoted_ctype = super::quote_literal(&db.datctype);

        let sql = format!(
            "CREATE DATABASE {quoted_name} WITH OWNER = {quoted_owner} ENCODING = {quoted_encoding} LC_COLLATE = {quoted_collate} LC_CTYPE = {quoted_ctype}"
        );

        select! {
            res = pool.execute(&sql, &[]) => {
                if let Err(e) = res {
                    if let Some(db_err) = e.as_db_error() && db_err.code() == &tokio_postgres::error::SqlState::DUPLICATE_DATABASE {
                        let alter_owner = format!("ALTER DATABASE {quoted_name} OWNER TO {quoted_owner}");
                        let _ = pool.execute(&alter_owner, &[]).await;
                        continue;
                    }

                    let sql_no_owner = format!(
                        "CREATE DATABASE {quoted_name} WITH ENCODING = {quoted_encoding} LC_COLLATE = {quoted_collate} LC_CTYPE = {quoted_ctype}"
                    );
                    if pool.execute(&sql_no_owner, &[]).await.is_ok() {
                        let alter_owner = format!("ALTER DATABASE {quoted_name} OWNER TO {quoted_owner}");
                        let _ = pool.execute(&alter_owner, &[]).await;
                        continue;
                    }

                    return Err(Error::ProcessFailed {
                        command: sql.into(),
                        stderr: e.to_string().into(),
                    });
                }
            }
            () = cancel.cancelled() => return Err(Error::Cancelled(format!("database creation of {}", db.name).into())),
        }
    }
    Ok(())
}

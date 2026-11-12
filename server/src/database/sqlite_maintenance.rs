//! SQLite query planner statistics maintenance.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sea_orm::sqlx::{Executor, SqliteConnection, sqlite::SqlitePoolOptions};
use sea_orm::{ConnectOptions, DatabaseBackend, DatabaseConnection};

const INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

pub(crate) fn connect_options(url: &str) -> ConnectOptions {
    let mut options = ConnectOptions::new(url);
    options.map_sqlx_sqlite_pool_opts(|pool| pool_options(pool, INTERVAL));
    options
}

fn pool_options(pool: SqlitePoolOptions, interval: Duration) -> SqlitePoolOptions {
    let last_maintenance = Arc::new(Mutex::new(Instant::now()));
    let connected = last_maintenance.clone();

    // SeaORM already defaults to one SQLite connection. Keep that invariant so
    // the maintenance deadline and the connection-local statistics stay together.
    pool.max_connections(1)
        .after_connect(move |conn, _| {
            *connected.lock().unwrap() = Instant::now();
            Box::pin(async move {
                optimize(conn).await;
                Ok(())
            })
        })
        .before_acquire(move |conn, _| {
            let due = {
                let mut last = last_maintenance.lock().unwrap();
                if last.elapsed() >= interval {
                    // Also rate-limit failed attempts, rather than retrying on
                    // every request when the database is busy or read-only.
                    *last = Instant::now();
                    true
                } else {
                    false
                }
            };
            Box::pin(async move {
                if due {
                    optimize(conn).await;
                }
                Ok(true)
            })
        })
}

async fn optimize(conn: &mut SqliteConnection) {
    // Consider all tables, including ones not queried by this connection yet.
    // SQLite bounds the analysis work performed by PRAGMA optimize.
    // https://www.sqlite.org/pragma.html#pragma_optimize
    if let Err(error) = conn.execute("PRAGMA optimize=0x10002;").await {
        tracing::warn!(%error, "Failed to optimize SQLite planner statistics");
    }

    // Another process (for example the garbage collector) may have updated the
    // statistics. Reload them even when optimize did not need to analyze anything.
    // This also expires prepared statements using old query plans.
    if let Err(error) = conn.execute("ANALYZE sqlite_schema;").await {
        tracing::warn!(%error, "Failed to reload SQLite planner statistics");
    }
}

pub(crate) async fn after_migrations(db: &DatabaseConnection) {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return;
    }

    match db.get_sqlite_connection_pool().acquire().await {
        Ok(mut conn) => optimize(&mut conn).await,
        Err(error) => {
            tracing::warn!(%error, "Failed to acquire SQLite connection for optimization")
        }
    }
}

#[cfg(test)]
mod tests;

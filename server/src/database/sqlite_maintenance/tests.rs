use std::io::{self, Write};
use std::path::PathBuf;

use sea_orm::sqlx::{self, AssertSqlSafe, Row};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::database::migration::{Migrator, MigratorTrait};
use crate::database::{AtticDatabase, build_find_and_lock_chunk_query};
use crate::narinfo::Compression;
use crate::{StateInner, config::Config, run_api_server, run_migrations};
use attic::hash::Hash;

// Keep the state distribution skewed, but not constant: SQLite marks a
// single-valued index as low-quality and retains that flag across ANALYZE reloads.
const POPULATE_CHUNKS: &str = r#"
            WITH RECURSIVE numbers(n) AS (
                SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 8192
            )
            INSERT INTO chunk (state, chunk_hash, chunk_size, compression,
                remote_file, remote_file_id, holders_count, created_at)
            SELECT CASE WHEN n % 64 = 0 THEN 'P' ELSE 'V' END, 'sha256:' || printf('%064x', n), 1, 'zstd',
                '{"Local":{"name":"fixture"}}', printf('%d', n), 0,
                '2026-01-01T00:00:00Z' FROM numbers;
            "#;

struct Fixture {
    directory: PathBuf,
    url: String,
}

impl Fixture {
    fn empty() -> Self {
        let directory = std::env::temp_dir().join(format!("attic-sqlite-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let url = format!("sqlite://{}/db?mode=rwc", directory.display());
        Self { directory, url }
    }

    async fn new(steps: Option<u32>) -> Self {
        let fixture = Self::empty();
        let db = Database::connect(&fixture.url).await.unwrap();
        Migrator::up(&db, steps).await.unwrap();
        Self::populate(&db).await;
        db.close().await.unwrap();
        fixture
    }

    async fn populate(db: &DatabaseConnection) {
        db.execute_unprepared(POPULATE_CHUNKS).await.unwrap();
    }

    fn config(&self) -> Config {
        toml::from_str(&format!(
            r#"
            [database]
            url = "{}"
            heartbeat = false
            [storage]
            type = "local"
            path = "{}"
            [chunking]
            nar-size-threshold = 0
            min-size = 1024
            avg-size = 2048
            max-size = 4096
            [jwt.signing]
            token-hs256-secret-base64 = "dGVzdC1vbmx5"
            "#,
            self.url,
            self.directory.display()
        ))
        .unwrap()
    }

    async fn connect(&self, interval: Duration) -> DatabaseConnection {
        let mut options = ConnectOptions::new(&self.url);
        options.map_sqlx_sqlite_pool_opts(move |pool| pool_options(pool, interval));
        Database::connect(options).await.unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

async fn chunk_plan(conn: &mut SqliteConnection) -> String {
    // Use the actual UPDATE-with-subquery, not just an approximation of its SELECT.
    let statement = DatabaseBackend::Sqlite.build(&build_find_and_lock_chunk_query(
        &Hash::Sha256([0; 32]),
        Compression::Zstd,
    ));
    let mut query = sqlx::query(AssertSqlSafe(format!(
        "EXPLAIN QUERY PLAN {}",
        statement.sql
    )));
    for value in statement.values.unwrap().0 {
        use sea_orm::sea_query::Value;
        query = match value {
            Value::Unsigned(value) => query.bind(value.map(i64::from)),
            Value::BigUnsigned(value) => query.bind(value.map(|n| i64::try_from(n).unwrap())),
            Value::String(value) => query.bind(value),
            value => panic!("Unexpected chunk query parameter: {value:?}"),
        };
    }
    query
        .fetch_all(conn)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn assert_index(conn: &mut SqliteConnection, index: &str) {
    let plan = chunk_plan(conn).await;
    assert!(plan.contains(index), "Expected {index}, got:\n{plan}");
}

#[tokio::test]
async fn startup_fixes_the_actual_chunk_lookup_without_statistics() {
    let fixture = Fixture::new(None).await;
    let baseline = Database::connect(&fixture.url).await.unwrap();
    let mut conn = baseline
        .get_sqlite_connection_pool()
        .acquire()
        .await
        .unwrap();
    let stat_table: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(stat_table, 0);
    assert_index(&mut conn, "idx-chunk-state-holders").await;
    drop(conn);
    baseline.close().await.unwrap();

    let state = StateInner::new(fixture.config()).await;
    let db = state.database().await.unwrap();
    let pool = db.get_sqlite_connection_pool();
    assert_eq!(pool.options().get_max_connections(), 1);
    let mut conn = pool.acquire().await.unwrap();
    assert_index(&mut conn, "idx-chunk-chunk-hash").await;
    drop(conn);
    assert!(
        db.find_and_lock_chunk(&Hash::Sha256([0; 32]), Compression::Zstd)
            .await
            .unwrap()
            .is_none()
    );
    let hash = Hash::from_typed(&format!("sha256:{:064x}", 1)).unwrap();
    let guard = db
        .find_and_lock_chunk(&hash, Compression::Zstd)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(guard.chunk_hash, hash.to_typed_base16());
    assert_eq!(guard.holders_count, 1);
    drop(guard);
    // The guard unlock is asynchronous. Wait for it before closing the pool.
    timeout(Duration::from_secs(5), async {
        loop {
            let holders: i32 = sqlx::query_scalar("SELECT holders_count FROM chunk WHERE id = 1")
                .fetch_one(pool)
                .await
                .unwrap();
            if holders == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    db.clone().close().await.unwrap();
}

#[tokio::test]
async fn periodic_maintenance_repairs_an_already_open_connection() {
    let fixture = Fixture::empty();
    let db = fixture.connect(Duration::ZERO).await;
    Migrator::up(&db, None).await.unwrap();
    let pool = db.get_sqlite_connection_pool();
    let mut conn = pool.acquire().await.unwrap();
    conn.execute("CREATE TEMP TABLE connection_marker (id INTEGER);")
        .await
        .unwrap();
    conn.execute(POPULATE_CHUNKS).await.unwrap();
    assert_index(&mut conn, "idx-chunk-state-holders").await;
    drop(conn);

    let mut conn = pool.acquire().await.unwrap();
    conn.execute("SELECT * FROM connection_marker;")
        .await
        .unwrap();
    assert_index(&mut conn, "idx-chunk-chunk-hash").await;
    drop(conn);
    db.close().await.unwrap();
}

#[tokio::test]
async fn maintenance_is_not_run_on_every_acquisition() {
    let fixture = Fixture::empty();
    let db = fixture.connect(INTERVAL).await;
    Migrator::up(&db, None).await.unwrap();
    Fixture::populate(&db).await;
    let mut conn = db.get_sqlite_connection_pool().acquire().await.unwrap();
    assert_index(&mut conn, "idx-chunk-state-holders").await;
    drop(conn);
    for _ in 0..3 {
        let mut conn = db.get_sqlite_connection_pool().acquire().await.unwrap();
        assert_index(&mut conn, "idx-chunk-state-holders").await;
    }
    after_migrations(&db).await;
    let mut conn = db.get_sqlite_connection_pool().acquire().await.unwrap();
    assert_index(&mut conn, "idx-chunk-chunk-hash").await;
    drop(conn);
    db.close().await.unwrap();
}

#[tokio::test]
async fn recycled_connections_optimize_before_the_next_deadline() {
    let fixture = Fixture::empty();
    let db = fixture.connect(INTERVAL).await;
    Migrator::up(&db, None).await.unwrap();
    Fixture::populate(&db).await;
    let pool = db.get_sqlite_connection_pool();
    let mut conn = pool.acquire().await.unwrap();
    conn.execute("CREATE TEMP TABLE connection_marker (id INTEGER);")
        .await
        .unwrap();
    assert_index(&mut conn, "idx-chunk-state-holders").await;
    conn.close().await.unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let markers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_temp_schema WHERE name='connection_marker'",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert_eq!(markers, 0);
    assert_index(&mut conn, "idx-chunk-chunk-hash").await;
    drop(conn);
    db.close().await.unwrap();
}

#[tokio::test]
async fn periodic_maintenance_reloads_external_statistics_and_cached_plans() {
    let fixture = Fixture::new(None).await;
    let db = fixture.connect(Duration::ZERO).await;
    let pool = db.get_sqlite_connection_pool();
    let mut conn = pool.acquire().await.unwrap();
    let has_stat4: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name='sqlite_stat4'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    // Histograms must not override the deliberately stale STAT1 estimates.
    if has_stat4 != 0 {
        conn.execute("DELETE FROM sqlite_stat4;").await.unwrap();
    }
    conn.execute(
        "UPDATE sqlite_stat1 SET stat='8192 1 1' WHERE idx='idx-chunk-state-holders';
         UPDATE sqlite_stat1 SET stat='8192 8' WHERE idx='idx-chunk-chunk-hash';
         ANALYZE sqlite_schema;",
    )
    .await
    .unwrap();
    assert_index(&mut conn, "idx-chunk-state-holders").await;

    let external = Database::connect(&fixture.url).await.unwrap();
    external.execute_unprepared("ANALYZE;").await.unwrap();
    external.close().await.unwrap();
    // The on-disk statistics changed, but this connection still has the old plan.
    assert_index(&mut conn, "idx-chunk-state-holders").await;
    drop(conn);

    let mut conn = pool.acquire().await.unwrap();
    assert_index(&mut conn, "idx-chunk-chunk-hash").await;
    drop(conn);
    db.close().await.unwrap();
}

#[tokio::test]
async fn migrations_optimize_new_indexes_after_upgrading() {
    // The state/holders index does not exist until migration 13.
    let fixture = Fixture::new(Some(12)).await;
    run_migrations(fixture.config()).await.unwrap();
    let db = Database::connect(&fixture.url).await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_stat1 WHERE idx='idx-chunk-state-holders'")
            .fetch_one(db.get_sqlite_connection_pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    let mut conn = db.get_sqlite_connection_pool().acquire().await.unwrap();
    assert_index(&mut conn, "idx-chunk-chunk-hash").await;
    drop(conn);
    db.close().await.unwrap();
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Logs {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

#[tokio::test]
async fn read_only_maintenance_warns_without_breaking_reads_or_retrying_each_one() {
    let fixture = Fixture::new(None).await;
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let url = fixture.url.replace("mode=rwc", "mode=ro");
    let db = Database::connect(connect_options(&url)).await.unwrap();
    let before = logs.text();
    assert!(
        before.contains("Failed to optimize SQLite planner statistics"),
        "{before}"
    );
    for _ in 0..3 {
        assert!(
            db.find_and_lock_chunk(&Hash::Sha256([0; 32]), Compression::Zstd)
                .await
                .is_err()
        );
        // Read-only mode rejects the application's UPDATE, but reads remain usable.
        let mut conn = db.get_sqlite_connection_pool().acquire().await.unwrap();
        assert_index(&mut conn, "idx-chunk-state-holders").await;
    }
    assert_eq!(logs.text(), before);
    db.close().await.unwrap();
}

#[tokio::test]
async fn closed_pool_maintenance_warns_and_returns() {
    let fixture = Fixture::new(None).await;
    let db = fixture.connect(INTERVAL).await;
    let logs = Logs::default();
    let writer = logs.clone();
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );
    db.get_sqlite_connection_pool().close().await;
    timeout(Duration::from_secs(5), after_migrations(&db))
        .await
        .unwrap();
    assert!(
        logs.text()
            .contains("Failed to acquire SQLite connection for optimization")
    );
}

#[tokio::test]
async fn sqlite_heartbeat_runs_when_disabled_and_shuts_down_with_the_server() {
    let fixture = Fixture::new(None).await;
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(run_api_server(
        Some("127.0.0.1:0".parse().unwrap()),
        fixture.config(),
        shutdown.clone(),
    ));
    let db = Database::connect(&fixture.url).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name='sqlite_stat1'")
                    .fetch_one(db.get_sqlite_connection_pool())
                    .await
                    .unwrap();
            if count != 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn busy_maintenance_warns_and_requests_continue_after_the_writer_releases() {
    let fixture = Fixture::new(None).await;
    let writer = Database::connect(&fixture.url).await.unwrap();
    let mut locked = writer.get_sqlite_connection_pool().acquire().await.unwrap();
    locked.execute("BEGIN IMMEDIATE;").await.unwrap();

    let logs = Logs::default();
    let output = logs.clone();
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(move || output.clone())
            .finish(),
    );
    let mut options = connect_options(&fixture.url);
    options.map_sqlx_sqlite_opts(|options| options.busy_timeout(Duration::from_millis(10)));
    let db = Database::connect(options).await.unwrap();
    let before = logs.text();
    assert!(
        before.contains("Failed to optimize SQLite planner statistics"),
        "{before}"
    );
    locked.execute("ROLLBACK;").await.unwrap();
    drop(locked);
    writer.close().await.unwrap();

    for _ in 0..3 {
        assert!(
            db.find_and_lock_chunk(&Hash::Sha256([0; 32]), Compression::Zstd)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(logs.text(), before);
    db.close().await.unwrap();
}

#[tokio::test]
async fn migrations_initialize_an_empty_database() {
    let fixture = Fixture::empty();
    run_migrations(fixture.config()).await.unwrap();
    let state = StateInner::new(fixture.config()).await;
    let db = state.database().await.unwrap();
    assert!(
        Migrator::get_pending_migrations(db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        db.find_and_lock_chunk(&Hash::Sha256([0; 32]), Compression::Zstd)
            .await
            .unwrap()
            .is_none()
    );
    db.clone().close().await.unwrap();
}

#[tokio::test]
async fn postgres_pool_options_and_post_migration_behavior_are_unchanged() {
    // Lazy pools allow testing backend selection without a PostgreSQL server.
    let url = "postgres://localhost/unused";
    let mut plain = ConnectOptions::new(url);
    plain.connect_lazy(true);
    let plain = Database::connect(plain).await.unwrap();
    let mut maintained = connect_options(url);
    maintained.connect_lazy(true);
    let maintained = Database::connect(maintained).await.unwrap();
    assert_eq!(maintained.get_database_backend(), DatabaseBackend::Postgres);
    assert_eq!(
        maintained
            .get_postgres_connection_pool()
            .options()
            .get_max_connections(),
        plain
            .get_postgres_connection_pool()
            .options()
            .get_max_connections()
    );
    after_migrations(&maintained).await;
    assert_eq!(maintained.get_postgres_connection_pool().size(), 0);
    maintained.close().await.unwrap();
    plain.close().await.unwrap();
}

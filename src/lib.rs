//! `skip_locked` — a Postgres job queue in one table, built on
//! `FOR NO KEY UPDATE SKIP LOCKED`.
//!
//! This crate is the runnable companion to two essays:
//!
//! - "PostgreSQL as a queue"
//!   <https://czeresniowski.dev/writing/postgresql-as-a-queue>
//! - "Migrating a Django queue to Rust"
//!   <https://czeresniowski.dev/writing/migrating-a-django-queue-to-rust>
//!
//! Every scar in those essays — SKIP LOCKED claims, per-tenant fairness, the
//! naive-reaper footgun, heartbeat leases, idempotent handlers, the PgBouncer
//! prepared-statement trap, backwards pool sizing, transactional enqueue — has
//! a function here and a `examples/` demo or test that exercises it.
//!
//! Everything uses RUNTIME sqlx (`sqlx::query`, `sqlx::query_as`,
//! `#[derive(sqlx::FromRow)]`), never the compile-time macros, so the crate
//! compiles with no database present. Migrations are embedded with
//! [`MIGRATOR`] and applied at runtime.

use sqlx::postgres::{PgPool, Postgres};
use sqlx::Transaction;

pub mod claim;
pub mod lease;
pub mod pool;
pub mod retry;
pub mod worker;

pub use claim::{claim_batch, fair_claim};
pub use lease::{heartbeat, reap};
pub use pool::{connect, connect_pgbouncer};
pub use retry::{classify, dead_letter, requeue, Class, HandlerError};
pub use worker::{run_worker, WorkerConfig};

/// Migrations embedded at compile time. Apply with `MIGRATOR.run(pool)`.
/// Embedding needs no database, so the crate still compiles offline.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// A claimed job. Exactly the shape the essays use: `id`, `kind`, `payload`,
/// `attempts`. Runtime `FromRow`, no compile-time query macro.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Job {
    pub id: i64,
    pub kind: String,
    pub payload: serde_json::Value,
    pub attempts: i32,
}

/// Transactional enqueue — the whole point of the essays. The job row and the
/// row that caused it commit in the SAME transaction. Both land or neither
/// does, so there is no dual-write seam and no outbox to reconcile.
///
/// The caller owns the transaction, inserts whatever business row caused the
/// job in the same `tx`, then commits. A NOTIFY is *not* sent here: it must
/// fire at commit time, which the caller controls. Use [`notify_ready`] just
/// before commit, or use [`enqueue_pool`] for the fire-and-forget convenience
/// path that does its own transaction and NOTIFY.
pub async fn enqueue(
    tx: &mut Transaction<'_, Postgres>,
    kind: &str,
    payload: &serde_json::Value,
) -> sqlx::Result<i64> {
    let row: (i64,) = sqlx::query_as(
        r#"
        INSERT INTO jobs (kind, payload)
        VALUES ($1, $2)
        RETURNING id
        "#,
    )
    .bind(kind)
    .bind(payload)
    .fetch_one(&mut **tx)
    .await?;
    Ok(row.0)
}

/// Enqueue at a future time (delayed job). Same transactional guarantee as
/// [`enqueue`]; sets `run_at` so the claim's `run_at <= now()` predicate hides
/// the row until it is due.
pub async fn enqueue_at(
    tx: &mut Transaction<'_, Postgres>,
    kind: &str,
    payload: &serde_json::Value,
    run_at: chrono::DateTime<chrono::Utc>,
) -> sqlx::Result<i64> {
    let row: (i64,) = sqlx::query_as(
        r#"
        INSERT INTO jobs (kind, payload, run_at)
        VALUES ($1, $2, $3)
        RETURNING id
        "#,
    )
    .bind(kind)
    .bind(payload)
    .bind(run_at)
    .fetch_one(&mut **tx)
    .await?;
    Ok(row.0)
}

/// Fire `NOTIFY jobs_ready`. Call this inside the enqueue transaction, after
/// the INSERT, BEFORE commit. NOTIFY fires at commit time, so a notify in a
/// transaction that rolls back is never delivered — which is what you want.
/// It is a latency optimization, never a correctness mechanism: it is
/// best-effort in-memory delivery, not durable and not queued for offline
/// listeners. The worker's poll loop is the backstop.
pub async fn notify_ready(tx: &mut Transaction<'_, Postgres>) -> sqlx::Result<()> {
    sqlx::query("SELECT pg_notify('jobs_ready', '')")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Convenience enqueue that opens its own transaction, inserts the job, fires
/// `NOTIFY jobs_ready`, and commits. Use this only when the job is NOT being
/// produced alongside other business writes — when it is, use [`enqueue`] and
/// commit it with that work so you keep the transactional-enqueue property.
pub async fn enqueue_pool(
    pool: &PgPool,
    kind: &str,
    payload: &serde_json::Value,
) -> sqlx::Result<i64> {
    let mut tx = pool.begin().await?;
    let id = enqueue(&mut tx, kind, payload).await?;
    notify_ready(&mut tx).await?;
    tx.commit().await?;
    Ok(id)
}

/// Mark a claimed job done. The completion transaction is where success and
/// failure handling live; on success the row leaves the hot partial index.
pub async fn mark_done(pool: &PgPool, id: i64) -> sqlx::Result<()> {
    sqlx::query("UPDATE jobs SET state = 'done', locked_at = NULL, locked_by = NULL WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

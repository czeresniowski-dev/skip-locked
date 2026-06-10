//! "Why the naive reaper is a footgun" + "Heartbeat lease plus idempotent
//! handlers".
//!
//! Three scenes:
//!   1. Naive reaper, no heartbeat: a still-running job runs past its lease, the
//!      reaper resurrects it, a second worker claims it -> DOUBLE EXECUTION.
//!   2. Heartbeat lease: the live worker keeps bumping locked_at, so the reaper
//!      does NOT resurrect its job (the locked_by guard holds).
//!   3. The same reaper still recovers a GENUINELY dead worker's job (no
//!      heartbeat coming) -> correct recovery.
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --example demo_reaper

use skip_locked::{claim_batch, enqueue, heartbeat, reap, MIGRATOR};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/skip_locked".into());
    let pool = PgPoolOptions::new().max_connections(8).connect(&url).await?;
    MIGRATOR.run(&pool).await?;

    // A 1-second lease keeps the demo fast; production sets it from the
    // per-kind p99.9 duration. We age locked_at directly so we don't have to
    // sleep through it.
    let lease = Duration::from_secs(1);

    // -------- Scene 1: naive reaper, no heartbeat, slow job --------
    reset(&pool).await?;
    let id = seed_one(&pool, "A", "evt-1").await?;
    let claimed = claim_batch(&pool, &["webhook.normalize"], 1, "worker-slow").await?;
    assert_eq!(claimed[0].id, id);
    println!("scene 1: worker-slow claimed job {id} and is STILL RUNNING it");

    // The job is slow: it has not finished, but its lease has aged out (a
    // tail-latency spike). Simulate that by aging locked_at past the lease.
    age_lock(&pool, id, 5).await?;
    let resurrected = reap(&pool, lease).await?;
    println!("scene 1: naive reaper resurrected {resurrected} row(s) — the live job is now 'ready' again");

    // A second worker claims the SAME job. The slow worker is still running it.
    let second = claim_batch(&pool, &["webhook.normalize"], 1, "worker-2").await?;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, id);
    println!("scene 1: worker-2 ALSO claimed job {id} — DOUBLE EXECUTION, the footgun\n");

    // -------- Scene 2: heartbeat lease protects the live job --------
    reset(&pool).await?;
    let id = seed_one(&pool, "B", "evt-2").await?;
    let _ = claim_batch(&pool, &["webhook.normalize"], 1, "worker-live").await?;
    println!("scene 2: worker-live claimed job {id}");

    // Time passes, but worker-live is alive and heartbeats, bumping locked_at.
    age_lock(&pool, id, 5).await?;
    let beat = heartbeat(&pool, &[id], "worker-live").await?;
    println!("scene 2: worker-live heartbeat bumped {beat} row (locked_by guard matched)");

    // Now the reaper runs. Because the heartbeat just refreshed locked_at, the
    // lease has NOT expired, so the reaper leaves the live job alone.
    let resurrected = reap(&pool, lease).await?;
    println!("scene 2: reaper resurrected {resurrected} row(s) — the live job was protected");
    assert_eq!(resurrected, 0, "heartbeat must keep the live job out of the reaper's reach");

    // And the locked_by guard: if the row HAD been stolen, a stale worker's
    // heartbeat would touch zero rows and it would know to abort.
    let stale_beat = heartbeat(&pool, &[id], "worker-ghost").await?;
    assert_eq!(stale_beat, 0, "a non-owner heartbeat must update zero rows");
    println!("scene 2: a non-owner (worker-ghost) heartbeat touched {stale_beat} rows — it would abort\n");

    // -------- Scene 3: same reaper recovers a genuinely dead worker --------
    reset(&pool).await?;
    let id = seed_one(&pool, "C", "evt-3").await?;
    let _ = claim_batch(&pool, &["webhook.normalize"], 1, "worker-dead").await?;
    println!("scene 3: worker-dead claimed job {id}, then the box panicked (no more heartbeats)");

    // No heartbeat ever comes. The lease ages out for real.
    age_lock(&pool, id, 5).await?;
    let resurrected = reap(&pool, lease).await?;
    println!("scene 3: reaper resurrected {resurrected} row(s) — the dead worker's job is recoverable");
    assert_eq!(resurrected, 1, "reaper must recover a genuinely dead worker's job");

    let st = state_of(&pool, id).await?;
    assert_eq!(st, "ready");
    println!("scene 3: job {id} is '{st}' again, ready for a healthy worker\n");

    println!("PASS: naive reaper double-executes; heartbeat protects live jobs and still recovers dead ones");
    Ok(())
}

async fn reset(pool: &sqlx::PgPool) -> sqlx::Result<()> {
    sqlx::query("TRUNCATE jobs, jobs_dead, processed_events RESTART IDENTITY")
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_one(pool: &sqlx::PgPool, carrier: &str, evt: &str) -> sqlx::Result<i64> {
    let mut tx = pool.begin().await?;
    let p = serde_json::json!({"carrier_id": carrier, "event_id": evt});
    let id = enqueue(&mut tx, "webhook.normalize", &p).await?;
    tx.commit().await?;
    Ok(id)
}

/// Push locked_at into the past so the lease has aged out, without sleeping.
async fn age_lock(pool: &sqlx::PgPool, id: i64, secs: i64) -> sqlx::Result<()> {
    sqlx::query("UPDATE jobs SET locked_at = now() - make_interval(secs => $2) WHERE id = $1")
        .bind(id)
        .bind(secs)
        .execute(pool)
        .await?;
    Ok(())
}

async fn state_of(pool: &sqlx::PgPool, id: i64) -> sqlx::Result<String> {
    let row: (String,) = sqlx::query_as("SELECT state FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

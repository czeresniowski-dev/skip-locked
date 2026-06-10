//! Assertion versions of every scar. Run serially against a real Postgres:
//!
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo test -- --test-threads=1
//!
//! Each test runs the embedded migrations and TRUNCATEs the queue tables at
//! the start, so the suite is idempotent and order-independent.

use skip_locked::{
    claim_batch, classify, dead_letter, enqueue, fair_claim, heartbeat, reap, requeue, Class,
    HandlerError, MIGRATOR,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashSet;
use std::time::Duration;

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/skip_locked".into());
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect to Postgres (is DATABASE_URL set and docker compose up?)");
    MIGRATOR.run(&pool).await.expect("run migrations");
    sqlx::query("TRUNCATE jobs, jobs_dead, processed_events RESTART IDENTITY")
        .execute(&pool)
        .await
        .expect("truncate");
    pool
}

async fn seed(pool: &PgPool, carrier: &str, n: i32, kind: &str) {
    let mut tx = pool.begin().await.unwrap();
    for i in 0..n {
        let p = serde_json::json!({"carrier_id": carrier, "event_id": format!("{carrier}{i}")});
        enqueue(&mut tx, kind, &p).await.unwrap();
    }
    tx.commit().await.unwrap();
}

/// Seed a fairness scenario the way it happens in production: a noisy carrier
/// dumps a large backlog while a few other carriers' webhooks arrive
/// interleaved in time. We set run_at explicitly so the other carriers' rows
/// are NOT all stranded behind the entire backlog (which would put them outside
/// any finite candidate window). This is the multi-tenant burst the per-carrier
/// cap is built for.
async fn seed_fairness(pool: &PgPool) {
    use skip_locked::enqueue_at;
    let base = chrono::Utc::now() - chrono::Duration::seconds(600);
    let mut tx = pool.begin().await.unwrap();
    // 500 carrier-A jobs spread across a 10-minute backfill window.
    for i in 0..500i32 {
        let p = serde_json::json!({"carrier_id": "A", "event_id": format!("a{i}")});
        let run_at = base + chrono::Duration::milliseconds((i as i64) * 1000);
        enqueue_at(&mut tx, "webhook.normalize", &p, run_at).await.unwrap();
    }
    // 5 each of B and C, arriving interleaved within the same window.
    for c in ["B", "C"] {
        for i in 0..5i32 {
            let p = serde_json::json!({"carrier_id": c, "event_id": format!("{c}{i}")});
            let run_at = base + chrono::Duration::seconds((i as i64) * 20);
            enqueue_at(&mut tx, "webhook.normalize", &p, run_at).await.unwrap();
        }
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn skip_locked_gives_disjoint_claims() {
    let pool = pool().await;
    seed(&pool, "A", 100, "webhook.normalize").await;

    let p1 = pool.clone();
    let p2 = pool.clone();
    let h1 =
        tokio::spawn(async move { claim_batch(&p1, &["webhook.normalize"], 30, "c1").await });
    let h2 =
        tokio::spawn(async move { claim_batch(&p2, &["webhook.normalize"], 30, "c2").await });
    let b1: HashSet<i64> = h1.await.unwrap().unwrap().iter().map(|j| j.id).collect();
    let b2: HashSet<i64> = h2.await.unwrap().unwrap().iter().map(|j| j.id).collect();

    assert!(!b1.is_empty() && !b2.is_empty());
    assert!(
        b1.is_disjoint(&b2),
        "SKIP LOCKED must give disjoint batches; overlap: {:?}",
        b1.intersection(&b2).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn fair_claim_caps_a_noisy_carrier() {
    let pool = pool().await;
    seed_fairness(&pool).await;

    let batch = fair_claim(&pool, &["webhook.normalize"], 64, 8, "fair")
        .await
        .unwrap();

    let mut a = 0;
    let mut bc = 0;
    for j in &batch {
        match j.payload["carrier_id"].as_str() {
            Some("A") => a += 1,
            Some("B") | Some("C") => bc += 1,
            _ => {}
        }
    }
    assert!(a <= 8, "carrier A must be capped at the per-tenant cap, got {a}");
    assert!(bc > 0, "B and C must be served, got {bc}");
}

#[tokio::test]
async fn plain_fifo_starves_other_carriers() {
    // The "before" of the fairness fix: strict FIFO drains the noisy carrier
    // far more than the per-carrier cap would, crowding out the rest.
    let pool = pool().await;
    seed_fairness(&pool).await;

    let batch = claim_batch(&pool, &["webhook.normalize"], 64, "fifo")
        .await
        .unwrap();
    let a = batch
        .iter()
        .filter(|j| j.payload["carrier_id"].as_str() == Some("A"))
        .count();
    // Strict FIFO lets the noisy carrier take the lion's share of the batch;
    // far more than the 8-row cap fair_claim would impose.
    assert!(a > 8, "strict FIFO should let carrier A exceed the fair cap, got {a}");
}

#[tokio::test]
async fn heartbeat_prevents_double_exec_and_reaper_recovers_dead() {
    let pool = pool().await;
    let lease = Duration::from_secs(1);

    // Live worker, heartbeat keeps the lease fresh: reaper must NOT resurrect.
    seed(&pool, "L", 1, "webhook.normalize").await;
    let claimed = claim_batch(&pool, &["webhook.normalize"], 1, "worker-live")
        .await
        .unwrap();
    let id = claimed[0].id;
    age_lock(&pool, id, 5).await;
    let beat = heartbeat(&pool, &[id], "worker-live").await.unwrap();
    assert_eq!(beat, 1, "owner heartbeat updates its row");
    let resurrected = reap(&pool, lease).await.unwrap();
    assert_eq!(resurrected, 0, "heartbeat must protect the live job from the reaper");

    // Non-owner heartbeat updates zero rows (the locked_by guard).
    let stale = heartbeat(&pool, &[id], "worker-ghost").await.unwrap();
    assert_eq!(stale, 0, "non-owner heartbeat must update zero rows");

    // Dead worker, no heartbeat: reaper recovers it.
    sqlx::query("TRUNCATE jobs RESTART IDENTITY").execute(&pool).await.unwrap();
    seed(&pool, "D", 1, "webhook.normalize").await;
    let claimed = claim_batch(&pool, &["webhook.normalize"], 1, "worker-dead")
        .await
        .unwrap();
    let dead_id = claimed[0].id;
    age_lock(&pool, dead_id, 5).await;
    let resurrected = reap(&pool, lease).await.unwrap();
    assert_eq!(resurrected, 1, "reaper must recover a genuinely dead worker's job");
    let st: (String,) = sqlx::query_as("SELECT state FROM jobs WHERE id = $1")
        .bind(dead_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(st.0, "ready");
}

#[tokio::test]
async fn naive_reaper_double_claims_a_live_job() {
    // The footgun itself: with no heartbeat, a slow-but-live job gets claimed
    // twice.
    let pool = pool().await;
    let lease = Duration::from_secs(1);
    seed(&pool, "S", 1, "webhook.normalize").await;
    let first = claim_batch(&pool, &["webhook.normalize"], 1, "worker-slow")
        .await
        .unwrap();
    let id = first[0].id;
    age_lock(&pool, id, 5).await; // lease aged out, but the worker is still running it
    let resurrected = reap(&pool, lease).await.unwrap();
    assert_eq!(resurrected, 1, "naive reaper resurrects the still-running job");
    let second = claim_batch(&pool, &["webhook.normalize"], 1, "worker-2")
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, id, "the same job is now claimed by two workers");
}

#[tokio::test]
async fn idempotent_dedupe_applies_once() {
    let pool = pool().await;

    async fn process(pool: &PgPool, carrier_id: &str, event_id: &str) -> bool {
        let mut tx = pool.begin().await.unwrap();
        let key = format!("{carrier_id}:{event_id}");
        let dedupe =
            sqlx::query("INSERT INTO processed_events (key) VALUES ($1) ON CONFLICT DO NOTHING")
                .bind(&key)
                .execute(&mut *tx)
                .await
                .unwrap();
        if dedupe.rows_affected() == 0 {
            tx.rollback().await.unwrap();
            return false; // duplicate
        }
        tx.commit().await.unwrap();
        true // applied
    }

    assert!(process(&pool, "A", "evt-1").await, "first run applies");
    assert!(!process(&pool, "A", "evt-1").await, "second run is a no-op");

    let count: (i64,) = sqlx::query_as("SELECT count(*) FROM processed_events WHERE key = $1")
        .bind("A:evt-1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1, "exactly one ledger row despite two runs");
}

#[tokio::test]
async fn transactional_enqueue_rollback_leaves_no_job() {
    // The whole point: if the producing transaction rolls back, no job row
    // survives. No dual-write seam, no orphan.
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();
    let p = serde_json::json!({"carrier_id": "A", "event_id": "rolled-back"});
    let _id = enqueue(&mut tx, "webhook.normalize", &p).await.unwrap();
    tx.rollback().await.unwrap();

    let count: (i64,) = sqlx::query_as("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0, "a rolled-back enqueue must leave no job");
}

#[tokio::test]
async fn classify_and_backoff_and_dead_letter() {
    let pool = pool().await;

    // Classification.
    assert_eq!(
        classify(&HandlerError::Transient("db timeout".into())),
        Class::Transient
    );
    assert_eq!(
        classify(&HandlerError::Permanent("bad schema".into())),
        Class::Permanent
    );

    // Transient: requeue pushes run_at into the future with capped backoff.
    seed(&pool, "A", 1, "webhook.normalize").await;
    let claimed = claim_batch(&pool, &["webhook.normalize"], 1, "w")
        .await
        .unwrap();
    let id = claimed[0].id; // attempts is now 1
    let n = requeue(&pool, id, "carrier 503").await.unwrap();
    assert_eq!(n, 1);
    let row: (String, chrono::DateTime<chrono::Utc>, Option<String>) =
        sqlx::query_as("SELECT state, run_at, last_error FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "ready");
    assert!(row.1 > chrono::Utc::now(), "backoff must schedule run_at in the future");
    assert_eq!(row.2.as_deref(), Some("carrier 503"));

    // Permanent: dead_letter moves the row out of jobs into jobs_dead.
    let n = dead_letter(&pool, id, "permanent: unknown event type")
        .await
        .unwrap();
    assert_eq!(n, 1);
    let in_jobs: (i64,) = sqlx::query_as("SELECT count(*) FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let in_dead: (i64,) = sqlx::query_as("SELECT count(*) FROM jobs_dead WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(in_jobs.0, 0, "row must leave the hot table");
    assert_eq!(in_dead.0, 1, "row must land in jobs_dead");
}

#[tokio::test]
async fn backoff_is_capped_at_one_hour() {
    // least(power(2, attempts), 3600): a high attempt count must not back off
    // past one hour.
    let pool = pool().await;
    seed(&pool, "A", 1, "webhook.normalize").await;
    let id: (i64,) = sqlx::query_as("SELECT id FROM jobs LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    // Force a large attempts so power(2, attempts) would blow past 3600s.
    sqlx::query("UPDATE jobs SET attempts = 20 WHERE id = $1")
        .bind(id.0)
        .execute(&pool)
        .await
        .unwrap();
    requeue(&pool, id.0, "still failing").await.unwrap();
    let secs: (f64,) =
        sqlx::query_as("SELECT EXTRACT(EPOCH FROM (run_at - now()))::float8 FROM jobs WHERE id = $1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        secs.0 <= 3601.0,
        "backoff must be capped at 3600s, got {}",
        secs.0
    );
}

async fn age_lock(pool: &PgPool, id: i64, secs: i64) {
    sqlx::query("UPDATE jobs SET locked_at = now() - make_interval(secs => $2) WHERE id = $1")
        .bind(id)
        .bind(secs)
        .execute(pool)
        .await
        .unwrap();
}

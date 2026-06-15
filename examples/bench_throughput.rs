//! Throughput benchmark for the claim path the essays describe.
//!
//! This is the runnable backing for the essays' throughput claim ("low
//! thousands to roughly 10k claims/s on a single primary before WAL fsync and
//! autovacuum become the ceiling"). It measures three rates against a real
//! Postgres, using the SAME public functions the demos and worker use — no
//! special-cased fast path:
//!
//!   1. enqueue/s        — single-row transactional enqueue (`enqueue_pool`)
//!   2. claims/s         — the FOR NO KEY UPDATE SKIP LOCKED hot path
//!                         (`claim_batch`), W workers draining a seeded backlog
//!   3. process/s        — claim + `mark_done` end to end
//!
//! Numbers depend on hardware, batch size, and worker count; the point is that
//! they are reproducible, not that they hit a specific figure. Run:
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --release --example bench_throughput
//!
//! Tune with env vars: BENCH_JOBS (default 50000), BENCH_WORKERS (8),
//! BENCH_BATCH (100), BENCH_ENQUEUE (5000).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use skip_locked::{claim_batch, enqueue_pool, mark_done, MIGRATOR};
use sqlx::postgres::PgPoolOptions;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn seed(pool: &sqlx::PgPool, n: i64) -> sqlx::Result<()> {
    sqlx::query("TRUNCATE jobs RESTART IDENTITY").execute(pool).await?;
    // Bulk insert is just test setup, not part of any measured rate. Spread
    // carriers so the rows look like the essays' multi-tenant backlog.
    sqlx::query(
        "INSERT INTO jobs (kind, payload)
         SELECT 'bench', jsonb_build_object('carrier_id', (i % 8)::text)
         FROM generate_series(1, $1) AS i",
    )
    .bind(n)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .expect("set DATABASE_URL, e.g. postgres://postgres:postgres@localhost:5432/skip_locked");
    let jobs = env_usize("BENCH_JOBS", 50_000) as i64;
    let workers = env_usize("BENCH_WORKERS", 8);
    let batch = env_usize("BENCH_BATCH", 100) as i64;
    let enq_sample = env_usize("BENCH_ENQUEUE", 5_000) as i64;

    let pool = PgPoolOptions::new()
        .max_connections(workers as u32 + 4)
        .connect(&url)
        .await?;
    MIGRATOR.run(&pool).await?;

    println!(
        "config: jobs={jobs} workers={workers} batch={batch} enqueue_sample={enq_sample}\n\
         postgres: {}",
        sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(&pool)
            .await?
    );

    // ── 1. enqueue/s — single-row transactional enqueue (tx + NOTIFY + commit)
    sqlx::query("TRUNCATE jobs RESTART IDENTITY").execute(&pool).await?;
    let payload = serde_json::json!({ "carrier_id": "0" });
    let t = Instant::now();
    for _ in 0..enq_sample {
        enqueue_pool(&pool, "bench", &payload).await?;
    }
    let dt = t.elapsed().as_secs_f64();
    println!("\n[1] enqueue   : {enq_sample} jobs in {dt:.2}s -> {:.0} enqueue/s", enq_sample as f64 / dt);

    // ── 2. claims/s — pure SKIP LOCKED claim path, W workers draining a backlog
    seed(&pool, jobs).await?;
    let claimed = Arc::new(AtomicI64::new(0));
    let t = Instant::now();
    let mut handles = Vec::new();
    for w in 0..workers {
        let pool = pool.clone();
        let claimed = claimed.clone();
        let wid = format!("claim-{w}");
        handles.push(tokio::spawn(async move {
            loop {
                let rows = claim_batch(&pool, &["bench"], batch, &wid).await.unwrap();
                if rows.is_empty() {
                    break;
                }
                claimed.fetch_add(rows.len() as i64, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.await?;
    }
    let dt = t.elapsed().as_secs_f64();
    let total = claimed.load(Ordering::Relaxed);
    println!("[2] claim     : {total} jobs in {dt:.2}s -> {:.0} claims/s", total as f64 / dt);

    // ── 3. process/s — claim + mark_done end to end
    seed(&pool, jobs).await?;
    let done = Arc::new(AtomicI64::new(0));
    let t = Instant::now();
    let mut handles = Vec::new();
    for w in 0..workers {
        let pool = pool.clone();
        let done = done.clone();
        let wid = format!("proc-{w}");
        handles.push(tokio::spawn(async move {
            loop {
                let rows = claim_batch(&pool, &["bench"], batch, &wid).await.unwrap();
                if rows.is_empty() {
                    break;
                }
                for j in &rows {
                    mark_done(&pool, j.id).await.unwrap();
                }
                done.fetch_add(rows.len() as i64, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.await?;
    }
    let dt = t.elapsed().as_secs_f64();
    let total = done.load(Ordering::Relaxed);
    println!("[3] process   : {total} jobs in {dt:.2}s -> {:.0} process/s (claim + mark_done)", total as f64 / dt);

    pool.close().await;
    Ok(())
}

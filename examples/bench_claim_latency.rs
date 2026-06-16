//! Claim-latency benchmark — the runnable backing for the essays' "claim latency
//! p99 under a few milliseconds" claim.
//!
//! `bench_throughput` measures aggregate *rate*; this measures the *latency* of a
//! single claim. One worker drains a seeded backlog one job at a time
//! (`claim_batch(.., 1, ..)` + `mark_done`), timing every individual claim call,
//! then reports the latency distribution. Same public function the worker uses —
//! no special-cased fast path.
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --release --example bench_claim_latency
//!
//! Tune with BENCH_JOBS (default 20000).

use std::time::Instant;

use skip_locked::{claim_batch, mark_done, MIGRATOR};
use sqlx::postgres::PgPoolOptions;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn seed(pool: &sqlx::PgPool, n: i64) -> sqlx::Result<()> {
    sqlx::query("TRUNCATE jobs RESTART IDENTITY").execute(pool).await?;
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

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let mut i = (p / 100.0 * sorted.len() as f64) as usize;
    if i >= sorted.len() {
        i = sorted.len() - 1;
    }
    sorted[i]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .expect("set DATABASE_URL, e.g. postgres://postgres:postgres@localhost:5432/skip_locked");
    let jobs = env_usize("BENCH_JOBS", 20_000) as i64;

    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await?;
    MIGRATOR.run(&pool).await?;

    println!(
        "config: jobs={jobs}\npostgres: {}",
        sqlx::query_scalar::<_, String>("SHOW server_version").fetch_one(&pool).await?
    );

    seed(&pool, jobs).await?;

    // One worker, one job per claim, timing each individual claim call.
    let mut lat_ms: Vec<f64> = Vec::with_capacity(jobs as usize);
    loop {
        let t = Instant::now();
        let rows = claim_batch(&pool, &["bench"], 1, "lat").await?;
        let dt = t.elapsed().as_secs_f64() * 1000.0;
        if rows.is_empty() {
            break;
        }
        lat_ms.push(dt);
        mark_done(&pool, rows[0].id).await?;
    }

    lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "[claim-latency] n={} p50={:.3}ms p95={:.3}ms p99={:.3}ms p99.9={:.3}ms max={:.3}ms",
        lat_ms.len(),
        pct(&lat_ms, 50.0),
        pct(&lat_ms, 95.0),
        pct(&lat_ms, 99.0),
        pct(&lat_ms, 99.9),
        lat_ms.last().copied().unwrap_or(0.0),
    );

    pool.close().await;
    Ok(())
}

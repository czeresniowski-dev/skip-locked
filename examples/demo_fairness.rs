//! "The day one carrier backed everyone up" / "One noisy carrier should not eat
//! the queue".
//!
//! Seed ~500 ready jobs for carrier "A" plus a handful for "B" and "C". Plain
//! `claim_batch` is strict FIFO, so it drains almost all of "A" first and
//! starves "B"/"C". `fair_claim` caps any one carrier's share of a batch so
//! "B" and "C" get served even while "A" is backlogged.
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --example demo_fairness

use skip_locked::{claim_batch, fair_claim, MIGRATOR};
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/skip_locked".into());
    let pool = PgPoolOptions::new().max_connections(8).connect(&url).await?;
    MIGRATOR.run(&pool).await?;

    println!("=== plain claim_batch (strict FIFO) ===");
    seed(&pool).await?;
    let batch = claim_batch(&pool, &["webhook.normalize"], 64, "fifo").await?;
    let fifo = tally(&batch);
    print_tally("claim_batch took", &fifo, batch.len());
    println!(
        "  -> carrier A took {} of {} rows; B/C nearly starved",
        fifo.get("A").copied().unwrap_or(0),
        batch.len()
    );

    println!();
    println!("=== fair_claim (per-carrier cap) ===");
    seed(&pool).await?;
    // batch=64, per_tenant_cap=8: A can use spare capacity but cannot take the
    // whole batch, so B and C are served.
    let batch = fair_claim(&pool, &["webhook.normalize"], 64, 8, "fair").await?;
    let fair = tally(&batch);
    print_tally("fair_claim took", &fair, batch.len());
    println!(
        "  -> carrier A capped at {} rows; B={} C={} served",
        fair.get("A").copied().unwrap_or(0),
        fair.get("B").copied().unwrap_or(0),
        fair.get("C").copied().unwrap_or(0),
    );

    // Prove the fix: FIFO starved B/C, fair_claim did not.
    let fifo_bc = fifo.get("B").copied().unwrap_or(0) + fifo.get("C").copied().unwrap_or(0);
    let fair_bc = fair.get("B").copied().unwrap_or(0) + fair.get("C").copied().unwrap_or(0);
    println!();
    println!("FIFO served B+C = {fifo_bc}; fair_claim served B+C = {fair_bc}");
    assert!(
        fair_bc > fifo_bc,
        "fair_claim must serve more of B+C than strict FIFO"
    );
    assert!(
        fair.get("A").copied().unwrap_or(0) <= 8,
        "fair_claim must cap carrier A at the per-tenant cap"
    );
    println!("PASS: fair_claim capped the noisy carrier and unstarved B/C");
    Ok(())
}

async fn seed(pool: &sqlx::PgPool) -> sqlx::Result<()> {
    use skip_locked::enqueue_at;
    sqlx::query("TRUNCATE jobs, jobs_dead, processed_events RESTART IDENTITY")
        .execute(pool)
        .await?;
    // The production-shaped burst: carrier A dumps a 500-job backfill across a
    // 10-minute window while B and C webhooks arrive interleaved in time. The
    // noisy carrier dominates the FIFO front; the fair cap is what lets B and C
    // through anyway.
    let base = chrono::Utc::now() - chrono::Duration::seconds(600);
    let mut tx = pool.begin().await?;
    for i in 0..500i32 {
        let p = serde_json::json!({"carrier_id": "A", "event_id": format!("a{i}")});
        let run_at = base + chrono::Duration::milliseconds((i as i64) * 1000);
        enqueue_at(&mut tx, "webhook.normalize", &p, run_at).await?;
    }
    for c in ["B", "C"] {
        for i in 0..5i32 {
            let p = serde_json::json!({"carrier_id": c, "event_id": format!("{c}{i}")});
            let run_at = base + chrono::Duration::seconds((i as i64) * 20);
            enqueue_at(&mut tx, "webhook.normalize", &p, run_at).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

fn tally(batch: &[skip_locked::Job]) -> BTreeMap<String, usize> {
    let mut m = BTreeMap::new();
    for j in batch {
        let carrier = j.payload["carrier_id"].as_str().unwrap_or("?").to_string();
        *m.entry(carrier).or_insert(0) += 1;
    }
    m
}

fn print_tally(label: &str, m: &BTreeMap<String, usize>, total: usize) {
    let parts: Vec<String> = m.iter().map(|(k, v)| format!("{k}={v}")).collect();
    println!("{label} {total} rows: {}", parts.join(" "));
}

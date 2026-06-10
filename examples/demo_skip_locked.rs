//! "The claim: SKIP LOCKED" — two concurrent claimers walk away with DISJOINT
//! batches. No double-claim, no coordination, no advisory locks.
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --example demo_skip_locked

use skip_locked::{claim_batch, enqueue, notify_ready, MIGRATOR};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/skip_locked".into());
    let pool = PgPoolOptions::new().max_connections(8).connect(&url).await?;
    MIGRATOR.run(&pool).await?;

    // Idempotent: reset our tables so re-runs are clean.
    sqlx::query("TRUNCATE jobs, jobs_dead, processed_events RESTART IDENTITY")
        .execute(&pool)
        .await?;

    // Seed 100 ready jobs of one kind.
    let mut tx = pool.begin().await?;
    for i in 0..100 {
        let payload = serde_json::json!({"carrier_id": "A", "event_id": format!("e{i}")});
        enqueue(&mut tx, "webhook.normalize", &payload).await?;
    }
    notify_ready(&mut tx).await?;
    tx.commit().await?;

    // Two claimers run concurrently against the same table.
    let p1 = pool.clone();
    let p2 = pool.clone();
    let h1 = tokio::spawn(async move {
        claim_batch(&p1, &["webhook.normalize"], 30, "claimer-1").await
    });
    let h2 = tokio::spawn(async move {
        claim_batch(&p2, &["webhook.normalize"], 30, "claimer-2").await
    });
    let batch1 = h1.await??;
    let batch2 = h2.await??;

    let ids1: HashSet<i64> = batch1.iter().map(|j| j.id).collect();
    let ids2: HashSet<i64> = batch2.iter().map(|j| j.id).collect();
    let overlap: Vec<i64> = ids1.intersection(&ids2).copied().collect();

    println!("claimer-1 took {} ids: {:?}", ids1.len(), sorted(&ids1));
    println!("claimer-2 took {} ids: {:?}", ids2.len(), sorted(&ids2));
    println!("overlap: {overlap:?}");

    assert!(overlap.is_empty(), "SKIP LOCKED guarantees disjoint batches");
    println!("PASS: batches are disjoint — SKIP LOCKED gave no double-claim");
    Ok(())
}

fn sorted(s: &HashSet<i64>) -> Vec<i64> {
    let mut v: Vec<i64> = s.iter().copied().collect();
    v.sort_unstable();
    v
}

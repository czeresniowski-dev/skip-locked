//! Enqueue N jobs across several carriers, transactionally, then NOTIFY.
//!
//! Demonstrates "Transactional enqueue deletes a class of bugs": the job rows
//! and (here, stand-in) business writes commit in ONE transaction, and the
//! NOTIFY fires only at commit. Run a `worker` example alongside this to watch
//! them drain.
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --example producer

use skip_locked::{enqueue, notify_ready, MIGRATOR};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/skip_locked".into());
    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await?;
    MIGRATOR.run(&pool).await?;

    let carriers = ["A", "B", "C", "D"];
    let n: usize = std::env::var("N").ok().and_then(|s| s.parse().ok()).unwrap_or(40);

    // One transaction for the whole burst. Both the jobs and the NOTIFY land or
    // neither does. In real code the shipment row that caused each job would be
    // inserted in this same tx.
    let mut tx = pool.begin().await?;
    for i in 0..n {
        let carrier = carriers[i % carriers.len()];
        let payload = serde_json::json!({
            "carrier_id": carrier,
            "event_id": format!("evt-{i}"),
            "status": "in_transit",
        });
        enqueue(&mut tx, "webhook.normalize", &payload).await?;
    }
    notify_ready(&mut tx).await?;
    tx.commit().await?;

    println!("enqueued {n} jobs across {} carriers, committed, NOTIFY jobs_ready fired", carriers.len());
    Ok(())
}

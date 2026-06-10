//! "Idempotency, because both could run a job".
//!
//! Process the same (carrier_id, event_id) twice. The dedupe insert into
//! `processed_events` is in the SAME transaction as the side effect, and we
//! branch on `rows_affected()`: the first run applies, the second is a no-op.
//! At-least-once delivery is the only honest guarantee, so the handler must be
//! idempotent.
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --example demo_idempotency

use skip_locked::MIGRATOR;
use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Debug, PartialEq)]
enum Outcome {
    Applied,
    Duplicate,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/skip_locked".into());
    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await?;
    MIGRATOR.run(&pool).await?;

    sqlx::query("TRUNCATE jobs, jobs_dead, processed_events RESTART IDENTITY")
        .execute(&pool)
        .await?;
    // A side-effect counter standing in for "notification sent / state advanced".
    sqlx::query("CREATE TABLE IF NOT EXISTS notifications (id bigserial primary key, key text)")
        .execute(&pool)
        .await?;
    sqlx::query("TRUNCATE notifications RESTART IDENTITY")
        .execute(&pool)
        .await?;

    let carrier_id = "A";
    let event_id = "evt-42";

    let first = process(&pool, carrier_id, event_id).await?;
    println!("first run:  {first:?}");
    let second = process(&pool, carrier_id, event_id).await?;
    println!("second run: {second:?} (no-op)");

    let effects: (i64,) = sqlx::query_as("SELECT count(*) FROM notifications WHERE key = $1")
        .bind(format!("{carrier_id}:{event_id}"))
        .fetch_one(&pool)
        .await?;
    println!("side effects applied for {carrier_id}:{event_id}: {}", effects.0);

    assert_eq!(first, Outcome::Applied);
    assert_eq!(second, Outcome::Duplicate);
    assert_eq!(effects.0, 1, "exactly one effect despite two runs");
    println!("PASS: applied once, duplicate was a no-op — exactly one effect");
    Ok(())
}

/// The handler, exactly the shape from the essay: dedupe insert and side effect
/// in one transaction, branch on rows_affected().
async fn process(pool: &PgPool, carrier_id: &str, event_id: &str) -> sqlx::Result<Outcome> {
    let mut tx = pool.begin().await?;

    let key = format!("{carrier_id}:{event_id}");
    let dedupe = sqlx::query("INSERT INTO processed_events (key) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(&key)
        .execute(&mut *tx)
        .await?;

    if dedupe.rows_affected() == 0 {
        tx.rollback().await?; // already processed; nothing to do
        return Ok(Outcome::Duplicate);
    }

    // The real side effect. Commits or rolls back WITH the dedupe row, so there
    // is no window where the event is marked processed but the effect did not
    // land, or the reverse.
    sqlx::query("INSERT INTO notifications (key) VALUES ($1)")
        .bind(&key)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Outcome::Applied)
}

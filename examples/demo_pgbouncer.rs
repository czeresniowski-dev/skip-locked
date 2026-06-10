//! "The PgBouncer prepared-statement trap" — OPTIONAL, env-guarded.
//!
//! If `PGBOUNCER_URL` is set (pointing at PgBouncer in TRANSACTION pooling
//! mode), this shows:
//!   1. A normal cached/named-statement pool throws a prepared-statement error
//!      under transaction pooling, because the backend moves between
//!      transactions and sqlx's per-connection named statements (`sqlx_s_N`)
//!      land on a backend that either never saw the PREPARE (`... does not
//!      exist`) or already holds that name from a prior client (`... already
//!      exists`). Both are the same root cause the essay describes.
//!   2. The statement_cache_capacity(0) pool (via `connect_pgbouncer`) PLUS
//!      `.persistent(false)` on each query does NOT, because sqlx then uses an
//!      unnamed statement that cannot survive — or collide — across
//!      transactions. (statement_cache_capacity(0) alone is not enough in sqlx
//!      0.8; see the connect_pgbouncer doc-comment.)
//!
//! If `PGBOUNCER_URL` is unset, it prints a skip line and exits 0, so the
//! default `cargo run --example demo_pgbouncer` never breaks.
//!
//!   docker compose up -d
//!   export PGBOUNCER_URL=postgres://postgres:postgres@localhost:6432/skip_locked
//!   cargo run --example demo_pgbouncer

use skip_locked::{connect_pgbouncer, MIGRATOR};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bouncer = match std::env::var("PGBOUNCER_URL") {
        Ok(u) => u,
        Err(_) => {
            println!("skipped (set PGBOUNCER_URL to reproduce)");
            return Ok(());
        }
    };

    // Apply migrations via a direct connection if available, so the table
    // exists regardless of pooling mode. Fall back to the bouncer URL.
    let migrate_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| bouncer.clone());
    if let Ok(p) = PgPoolOptions::new().max_connections(1).connect(&migrate_url).await {
        let _ = MIGRATOR.run(&p).await;
    }

    println!("=== cached-statement pool behind transaction-mode PgBouncer ===");
    // Default sqlx pool: keeps the per-connection prepared-statement cache. The
    // trap needs two things: (1) sqlx reuses a cached server-side statement
    // across transactions, and (2) PgBouncer hands the next transaction a
    // DIFFERENT backend that never saw the PREPARE. We reuse the SAME query
    // string every iteration so sqlx caches `sqlx_s_N` and reuses it, and we
    // run across a multi-connection pool so transactions rotate over the
    // bouncer's backends.
    let opts = PgConnectOptions::from_str(&bouncer)?;
    let cached = PgPoolOptions::new().max_connections(4).connect_with(opts).await?;

    let mut trap_hit = false;
    let mut last_err = String::new();
    // One stable query string -> one cached prepared statement that sqlx will
    // try to reuse on whatever backend the next transaction lands on.
    let q = "SELECT $1::int AS n";
    for i in 0..500 {
        match sqlx::query_as::<_, (i32,)>(q)
            .bind(i)
            .fetch_one(&cached)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                last_err = e.to_string();
                if last_err.contains("does not exist")
                    || last_err.to_lowercase().contains("prepared statement")
                {
                    trap_hit = true;
                    break;
                }
            }
        }
    }
    if trap_hit {
        println!("cached pool FAILED as the essay predicts: {last_err}");
    } else {
        println!(
            "cached pool did not throw in this run (last error: {}).",
            if last_err.is_empty() { "none".into() } else { last_err.clone() }
        );
        println!("note: some PgBouncer builds rewrite prepared statements; set MAX_PREPARED_STATEMENTS=0 to see the trap.");
    }

    println!();
    println!("=== statement_cache_capacity(0) + .persistent(false) (connect_pgbouncer) ===");
    let fixed = connect_pgbouncer(&bouncer).await?;
    let mut ok = 0u32;
    for i in 0..500 {
        // Unnamed statements (persistent(false)) cannot collide or go missing
        // across the bouncer's per-transaction backend swaps.
        sqlx::query_as::<_, (i32,)>("SELECT $1::int AS n")
            .bind(i)
            .persistent(false)
            .fetch_one(&fixed)
            .await?;
        ok += 1;
    }
    println!("statement-cache-off + non-persistent pool ran {ok}/500 probes with no prepared-statement error");

    Ok(())
}

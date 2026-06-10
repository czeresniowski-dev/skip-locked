//! Run a worker with a real SIGTERM drain via tokio::signal::unix.
//!
//! Demonstrates "Draining on SIGTERM", "LISTEN/NOTIFY with a poll backstop",
//! and the "Fast enough to break the downstream" backpressure semaphore (inside
//! `run_worker`). On SIGTERM (or Ctrl-C) the worker stops claiming, lets
//! in-flight work finish, and bounds the drain to 30 s.
//!
//!   docker compose up -d postgres
//!   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked
//!   cargo run --example producer      # seed some work
//!   cargo run --example worker        # drains it, then Ctrl-C / SIGTERM

use skip_locked::{connect, mark_done, run_worker, WorkerConfig, MIGRATOR};
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/skip_locked".into());

    // "Pool sizing is backwards": max_connections(8), 5 s acquire_timeout.
    let pool = connect(&url).await?;
    MIGRATOR.run(&pool).await?;

    let token = CancellationToken::new();

    // Wire BOTH SIGTERM (what the orchestrator sends on deploy) and SIGINT
    // (Ctrl-C) to cancellation. The orchestrator's termination grace period
    // must be longer than our drain_grace or we get SIGKILLed mid-drain.
    let signal_token = token.clone();
    tokio::spawn(async move {
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = term.recv() => tracing::info!("SIGTERM received, draining"),
            _ = int.recv()  => tracing::info!("SIGINT received, draining"),
        }
        signal_token.cancel();
    });

    let cfg = WorkerConfig {
        worker_id: format!("worker-{}", std::process::id()),
        kinds: vec!["webhook.normalize".to_string(), "tracking.refresh".to_string()],
        batch: 64,
        fanout_concurrency: 32,
        poll_interval: Duration::from_secs(1),
        drain_grace: Duration::from_secs(30),
    };

    tracing::info!(worker_id = %cfg.worker_id, "worker up, LISTEN jobs_ready + poll backstop");

    run_worker(pool.clone(), cfg, token, move |pool, job| async move {
        // Stand-in handler: in real code this normalizes the payload and
        // advances the shipment state machine, idempotently.
        tracing::info!(job_id = job.id, kind = %job.kind, attempts = job.attempts, "processing");
        if let Err(e) = mark_done(&pool, job.id).await {
            tracing::warn!(job_id = job.id, error = %e, "mark_done failed");
        }
    })
    .await?;

    tracing::info!("drained, exiting cleanly");
    Ok(())
}

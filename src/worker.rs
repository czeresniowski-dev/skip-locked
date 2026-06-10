//! The worker run loop. Reproduces "LISTEN/NOTIFY with a poll backstop",
//! "Draining on SIGTERM", and the backpressure semaphore from "Fast enough to
//! break the downstream".

use crate::claim::claim_batch;
use crate::Job;
use sqlx::postgres::{PgListener, PgPool};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Worker tuning. Defaults mirror the essays: a batch `LIMIT` of 64, a
/// fan-out concurrency cap of 32 (the throttle the slow worker used to be),
/// a 1 s poll backstop, and a 30 s drain grace.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub kinds: Vec<String>,
    pub batch: i64,
    /// Cap on concurrent fan-out tasks. "Fast enough to break the downstream":
    /// a `Semaphore` is the throttle the slow worker used to be, so the worker
    /// cannot push more in-flight calls than the downstream can absorb.
    pub fanout_concurrency: usize,
    /// Poll backstop. NOTIFY is best-effort, so we never wait longer than this
    /// for work even if a notification is lost.
    pub poll_interval: Duration,
    /// Bounded grace for in-flight fan-out tasks to settle on shutdown.
    pub drain_grace: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
            kinds: vec!["webhook.normalize".to_string()],
            batch: 64,
            fanout_concurrency: 32,
            poll_interval: Duration::from_secs(1),
            drain_grace: Duration::from_secs(30),
        }
    }
}

/// Run the worker loop until `token` is cancelled, then drain.
///
/// Control flow, faithful to the essays:
///
/// - **LISTEN/NOTIFY with a poll backstop.** A [`PgListener`] does
///   `LISTEN jobs_ready` so an idle worker wakes the moment a producer commits.
///   The loop waits for a notification OR the `poll_interval` timeout,
///   whichever comes first. NOTIFY removes the latency floor; the poll loop
///   bounds the worst case if a notification is ever lost. NOTIFY is a latency
///   optimization, never a correctness mechanism.
///
/// - **Graceful drain on SIGTERM.** `tokio::select!` with `biased` checks the
///   cancellation arm FIRST, so once the token fires we stop claiming on the
///   next loop turn instead of grabbing one more batch. In-flight work finishes.
///
/// - **Backpressure.** Each fan-out task acquires a permit from a bounded
///   [`Semaphore`]; the permit releases on drop when the task finishes. This is
///   what stops a fast worker from overwhelming a downstream sized against the
///   old slow one.
///
/// `handler` is your business logic. It is given the pool and the job and runs
/// under a semaphore permit. Keep it idempotent: at-least-once is the only
/// guarantee.
pub async fn run_worker<H, Fut>(
    pool: PgPool,
    cfg: WorkerConfig,
    token: CancellationToken,
    handler: H,
) -> sqlx::Result<()>
where
    H: Fn(PgPool, Job) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handler = Arc::new(handler);
    let permits = Arc::new(Semaphore::new(cfg.fanout_concurrency));
    let kinds: Vec<&str> = cfg.kinds.iter().map(|s| s.as_str()).collect();

    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen("jobs_ready").await?;

    loop {
        // Stop claiming the instant the token fires; biased makes the select
        // check cancellation before it even tries to claim.
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            batch = claim_batch(&pool, &kinds, cfg.batch, &cfg.worker_id) => {
                let batch = batch?;
                if batch.is_empty() {
                    // Nothing ready. Wait for a NOTIFY or the poll backstop,
                    // whichever comes first, then loop and claim again.
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        _ = listener.recv() => {}
                        _ = tokio::time::sleep(cfg.poll_interval) => {}
                    }
                    continue;
                }
                for job in batch {
                    let permit = permits.clone().acquire_owned().await.expect("semaphore open");
                    let pool = pool.clone();
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let _permit = permit; // released on drop, when the task finishes
                        handler(pool, job).await;
                    });
                }
            }
        }
    }

    // Bounded grace period for spawned fan-out tasks to settle. We drain by
    // acquiring every permit: once we hold them all, no task is in flight.
    let total = cfg.fanout_concurrency as u32;
    let _ = tokio::time::timeout(cfg.drain_grace, permits.acquire_many(total)).await;
    Ok(())
}

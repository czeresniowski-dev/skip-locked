//! Heartbeat lease and reaper. Reproduces "Why the naive reaper is a footgun"
//! and "Heartbeat lease plus idempotent handlers".

use sqlx::postgres::PgPool;
use std::time::Duration;

/// Bump the lease on jobs this worker still holds. Reproduces the heartbeat
/// UPDATE.
///
/// The worker periodically bumps `locked_at` while it works (e.g. every 30 s
/// for a 5-minute lease) so the lease expires only when the worker has
/// genuinely stopped, not because a job is merely slow. The heartbeat touches
/// `locked_at` only, which is un-indexed, so the update stays HOT and is
/// nearly free.
///
/// The `locked_by = $2` guard is the safety. If the reaper already resurrected
/// a row and another worker re-claimed it, `locked_by` no longer matches and
/// this updates zero rows. The original worker should treat a zero-row
/// heartbeat as "I have been preempted" and abort, rather than racing the new
/// owner. The returned count is the number of rows the worker still legitimately
/// holds.
pub async fn heartbeat(pool: &PgPool, ids: &[i64], worker_id: &str) -> sqlx::Result<u64> {
    let res = sqlx::query(
        r#"
        UPDATE jobs SET locked_at = now()
        WHERE id = ANY($1) AND locked_by = $2 AND state = 'running'
        "#,
    )
    .bind(ids)
    .bind(worker_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Resurrect rows whose lease has expired. Reproduces the reaper UPDATE.
///
/// Returns the number of rows put back to `ready`.
///
/// # The double-execution footgun
///
/// This reaper CANNOT tell a dead worker from a slow one. If a legitimate job
/// runs past the lease — a tail-latency spike, a slow downstream, a GC pause —
/// the reaper resurrects a job that is still executing, a second worker claims
/// it, and the same job runs twice concurrently. The reaper does not fix
/// double execution; configured carelessly it CAUSES it.
///
/// The two correct mitigations, and you want both:
///
/// 1. A heartbeat lease ([`heartbeat`]) so the lease expires only when the
///    worker has actually stopped. Set `lease` from the per-kind job-duration
///    distribution (p99.9, a couple of multiples above), not a round number.
/// 2. Idempotent handlers, unconditionally. Even with a heartbeat a worker can
///    complete a job, then crash in the microsecond before it commits
///    `state = 'done'`, and the job reruns. At-least-once is the only honest
///    guarantee. The heartbeat plus idempotency is the correctness boundary;
///    the lease is only a backstop.
pub async fn reap(pool: &PgPool, lease: Duration) -> sqlx::Result<u64> {
    // Express the lease as seconds for the interval arithmetic.
    let secs = lease.as_secs() as i64;
    let res = sqlx::query(
        r#"
        UPDATE jobs
        SET state = 'ready', locked_at = NULL, locked_by = NULL
        WHERE state = 'running'
          AND locked_at < now() - make_interval(secs => $1)
          AND attempts < max_attempts
        "#,
    )
    .bind(secs)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

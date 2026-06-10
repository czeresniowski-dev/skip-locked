//! The claim query. Reproduces "The claim: SKIP LOCKED" and the per-tenant cap
//! from "Per-kind routing and per-tenant caps" / "One noisy carrier should not
//! eat the queue".

use crate::Job;
use sqlx::postgres::PgPool;

/// Claim up to `n` ready jobs whose `kind` is in `kinds`.
///
/// The shape is mandatory: an inner `SELECT ... FOR NO KEY UPDATE SKIP LOCKED
/// LIMIT n` wrapped in an outer `UPDATE ... RETURNING`. You cannot attach
/// `SKIP LOCKED` to an `UPDATE`, so you select IDs with skip semantics, update
/// by id, and `RETURNING` hands the rows back in one round trip.
///
/// `SKIP LOCKED` makes this a queue instead of a thundering herd: any row
/// another worker holds in its in-flight transaction is silently skipped, not
/// blocked on. Twenty workers running this concurrently each walk away with a
/// disjoint batch.
///
/// The lock strength is `FOR NO KEY UPDATE`, deliberately, not `FOR UPDATE`.
/// `FOR UPDATE` is the strongest row lock and blocks the key-share lock that
/// FK validation takes, needlessly serializing the claim against audit/result
/// rows that reference `jobs.id`. `FOR NO KEY UPDATE` locks against concurrent
/// updates and deletes but not the key-share lock, and it matches the lock an
/// ordinary `UPDATE` of non-key columns already takes.
///
/// `SKIP LOCKED` changes lock semantics, not visibility: the claim commits
/// immediately and the row is now `running`. A worker that dies after
/// committing the claim leaves a stale `running` row the database will never
/// recover for you — that is the reaper's and the heartbeat's job
/// ([`crate::lease`]).
///
/// Tune `n` against job duration: short jobs want larger batches to amortize
/// the round trip; long jobs want `LIMIT 1` so one slow worker does not hoard
/// a batch.
pub async fn claim_batch(
    pool: &PgPool,
    kinds: &[&str],
    n: i64,
    worker_id: &str,
) -> sqlx::Result<Vec<Job>> {
    sqlx::query_as::<_, Job>(
        r#"
        UPDATE jobs
        SET state     = 'running',
            locked_at = now(),
            locked_by = $1,
            attempts  = attempts + 1
        WHERE id IN (
            SELECT id FROM jobs
            WHERE state = 'ready' AND run_at <= now() AND kind = ANY($2)
            ORDER BY run_at
            FOR NO KEY UPDATE SKIP LOCKED
            LIMIT $3
        )
        RETURNING id, kind, payload, attempts
        "#,
    )
    .bind(worker_id)
    .bind(kinds)
    .bind(n)
    .fetch_all(pool)
    .await
}

/// Weighted claim: no single carrier eats more than `per_tenant_cap` rows of a
/// batch. Reproduces "Per-kind routing and per-tenant caps".
///
/// FIFO (`ORDER BY run_at`) is fair to rows and unfair to tenants: a burst from
/// one carrier sorts to the front and starves everything behind it. This caps
/// each carrier's share so one hot tenant can use spare capacity but cannot
/// monopolize a pool.
///
/// The inner select pulls a wider candidate window (`batch * 4`) under
/// `SKIP LOCKED`, ranks candidates within each `payload->>'carrier_id'` by
/// `run_at` with `row_number()`, and the outer `UPDATE` keeps only rows whose
/// per-carrier rank is `<= per_tenant_cap`. It is not strict weighted fair
/// queuing and does not try to be — strict WFQ on a table is more machinery
/// than it is worth. It is a cap, and a cap is what defends an SLO.
pub async fn fair_claim(
    pool: &PgPool,
    kinds: &[&str],
    batch: i64,
    per_tenant_cap: i64,
    worker_id: &str,
) -> sqlx::Result<Vec<Job>> {
    // Postgres forbids FOR NO KEY UPDATE in the same SELECT as a window
    // function ("FOR NO KEY UPDATE is not allowed with window functions"), so
    // the row-locking SKIP LOCKED select is a subquery and row_number() ranks
    // its locked candidate set in the outer CTE. The semantics are identical to
    // the essay's CTE: lock a `batch * 4` candidate window with SKIP LOCKED,
    // rank within each carrier by run_at, keep the first `per_tenant_cap` per
    // carrier.
    sqlx::query_as::<_, Job>(
        r#"
        WITH locked AS (
            SELECT id, run_at, payload->>'carrier_id' AS carrier_id
            FROM jobs
            WHERE state = 'ready' AND run_at <= now() AND kind = ANY($1)
            ORDER BY run_at
            FOR NO KEY UPDATE SKIP LOCKED
            LIMIT $2 * 4
        ),
        ranked AS (
            SELECT id,
                   row_number() OVER (PARTITION BY carrier_id
                                      ORDER BY run_at) AS rn
            FROM locked
        )
        UPDATE jobs
        SET state = 'running', locked_at = now(), locked_by = $3, attempts = attempts + 1
        WHERE id IN (SELECT id FROM ranked WHERE rn <= $4)
        RETURNING id, kind, payload, attempts
        "#,
    )
    .bind(kinds)
    .bind(batch)
    .bind(worker_id)
    .bind(per_tenant_cap)
    .fetch_all(pool)
    .await
}

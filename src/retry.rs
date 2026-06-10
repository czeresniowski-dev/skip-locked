//! Retry classification, capped backoff, and dead-lettering. Reproduces
//! "Retries, backoff, and the dead-letter table" and "Retry or dead-letter".

use sqlx::postgres::PgPool;

/// How a failure should be handled. Getting this wrong in either direction is
/// expensive: dead-letter a retryable timeout and you drop work; retry a schema
/// violation forever and you build a hot loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Carrier 5xx, DB timeout, connection reset: back off and requeue.
    Transient,
    /// Schema validation, unknown event type: a human decides.
    Permanent,
}

/// A handler error carrying its own classification. Real handlers would match
/// on concrete error types; this enum keeps the demo's `classify` honest
/// without inventing a carrier client.
#[derive(Debug)]
pub enum HandlerError {
    /// Transient by nature (timeout, 5xx, reset).
    Transient(String),
    /// Permanent by nature (bad schema, unknown event).
    Permanent(String),
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerError::Transient(s) => write!(f, "transient: {s}"),
            HandlerError::Permanent(s) => write!(f, "permanent: {s}"),
        }
    }
}

impl std::error::Error for HandlerError {}

/// Classify a handler error. The classification is a `match`; the cost of
/// getting it wrong is why the dead-letter table's *growth* is an alert, not
/// the table's existence. We shipped this subtly wrong at first and
/// dead-lettered some retryable timeouts; the table growing when it shouldn't
/// have is how we caught it.
pub fn classify(err: &HandlerError) -> Class {
    match err {
        HandlerError::Transient(_) => Class::Transient,
        HandlerError::Permanent(_) => Class::Permanent,
    }
}

/// Requeue a transient failure with capped exponential backoff.
///
/// `run_at = now() + least(power(2, attempts), 3600) seconds`. Capped at one
/// hour so a long-failing job does not back off into next week; jitter is added
/// upstream. Clears the lock so the row is claimable again once it is due, and
/// only while `attempts < max_attempts` (an exhausted row dead-letters
/// instead). Records `last_error` for triage.
pub async fn requeue(pool: &PgPool, id: i64, last_error: &str) -> sqlx::Result<u64> {
    let res = sqlx::query(
        r#"
        UPDATE jobs
        SET state = 'ready',
            run_at = now() + (least(power(2, attempts), 3600) * interval '1 second'),
            locked_at = NULL, locked_by = NULL, last_error = $2
        WHERE id = $1 AND attempts < max_attempts
        "#,
    )
    .bind(id)
    .bind(last_error)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Move an exhausted (or permanently-failed) job out of the hot table entirely.
///
/// The `DELETE ... RETURNING` / `INSERT` CTE atomically removes the row from
/// `jobs` and lands it in `jobs_dead`, so it stops consuming claim capacity and
/// stops being dead weight on every vacuum pass of the live table. The partial
/// index only sees `ready` rows, but the heap still has to be vacuumed, and a
/// pile of permanently-failed rows is dead weight on every pass.
///
/// Unlike the transient path this does not gate on `attempts >= max_attempts`,
/// because a `Permanent` classification dead-letters immediately regardless of
/// attempts. The transient exhaustion path is expressed by [`requeue`] no-op'ing
/// (zero rows) once `attempts >= max_attempts`, after which the caller
/// dead-letters.
pub async fn dead_letter(pool: &PgPool, id: i64, err: &str) -> sqlx::Result<u64> {
    let res = sqlx::query(
        r#"
        WITH dead AS (
            DELETE FROM jobs WHERE id = $1
            RETURNING id, kind, payload, state, run_at, locked_at, locked_by,
                      attempts, max_attempts
        )
        INSERT INTO jobs_dead
            (id, kind, payload, state, run_at, locked_at, locked_by,
             attempts, max_attempts, last_error)
        SELECT id, kind, payload, 'dead', run_at, locked_at, locked_by,
               attempts, max_attempts, $2
        FROM dead
        "#,
    )
    .bind(id)
    .bind(err)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

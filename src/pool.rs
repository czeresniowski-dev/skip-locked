//! Connection-pool construction. Reproduces "Pool sizing is backwards" and the
//! two fixes from "The PgBouncer prepared-statement trap".

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use std::str::FromStr;
use std::time::Duration;

/// Direct-to-Postgres pool (or a PgBouncer in *session* pooling mode).
///
/// `max_connections(8)` with a 5 s `acquire_timeout` is the essay's
/// counterintuitive sizing. `max_connections` in Postgres is a hard,
/// shared, cluster-wide ceiling: every connection costs a backend process
/// and a slice of `work_mem`, and the queue worker competes with the web app
/// for the same budget. The *faster* worker needs *fewer* connections, held
/// for shorter durations, because a connection is only busy for the
/// milliseconds a query is in flight. A small bounded pool sustains far more
/// throughput than a slow worker's large one. The 5 s `acquire_timeout` makes
/// a pathological backlog surface as a timeout instead of an unbounded queue
/// of waiters.
///
/// This pool KEEPS the prepared-statement cache, so the hot claim loop reuses
/// server-side plans. That is correct against a direct connection. It is a
/// correctness bug behind a transaction-mode bouncer; for that case use
/// [`connect_pgbouncer`].
pub async fn connect(url: &str) -> sqlx::Result<PgPool> {
    let opts = PgConnectOptions::from_str(url)?;
    PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(opts)
        .await
}

/// Pool for use behind PgBouncer in **transaction** pooling mode.
///
/// The runbook line that came out of an afternoon lost to this:
///
/// > "`prepared statement ... does not exist` means a pooler is moving
/// > connections between transactions. Check whether the worker is behind
/// > PgBouncer in transaction mode before you touch connection limits."
///
/// Under transaction pooling a backend is handed to a client for one
/// transaction and then returned, so the next transaction lands on a different
/// backend. `sqlx` prepares statements on the server and caches them per
/// connection, so it issues a `PREPARE` once and then references
/// `sqlx_s_3` on a backend that no longer holds it: `prepared statement
/// "sqlx_s_3" does not exist`, intermittently, under load, in exactly the way
/// that does not reproduce on a cold laptop where you are the only client.
///
/// The fix here is the first of the two the essay describes: keep PgBouncer in
/// transaction mode and set `statement_cache_capacity(0)` so `sqlx` does not
/// assume a statement survives across transactions. The cost is a parse/plan
/// round-trip per execution, which for a hot claim loop is real overhead. The
/// essay *shipped* the other fix (point the worker straight at Postgres with a
/// small bounded pool, see [`connect`]) because a worker fleet is a handful of
/// processes, not the hundreds of web handlers transaction pooling exists to
/// fan in.
///
/// One sharp edge the essay's prose glosses over and this crate verifies
/// against a real transaction-mode bouncer: in sqlx 0.8, disabling the cache
/// is necessary but not sufficient. A query is still "persistent" by default,
/// so sqlx assigns it a *named* server-side statement (`sqlx_s_N`) even with
/// caching off, and behind a transaction bouncer those names collide on a
/// reused backend (`prepared statement "sqlx_s_1" already exists`). The
/// complete fix is to ALSO mark each query non-persistent so sqlx uses an
/// unnamed statement that cannot collide:
///
/// ```ignore
/// sqlx::query("...").persistent(false).execute(&pool).await?;
/// ```
///
/// This is exactly why the essay's authors shipped [`connect`] instead: a
/// direct connection keeps named prepared statements and plan reuse, with no
/// `.persistent(false)` sprinkled through the hot path.
pub async fn connect_pgbouncer(url: &str) -> sqlx::Result<PgPool> {
    let opts = PgConnectOptions::from_str(url)?.statement_cache_capacity(0);
    PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(opts)
        .await
}

# skip-locked

A Postgres job queue in one table, built on `FOR NO KEY UPDATE SKIP LOCKED`,
with the recovery and fairness machinery that makes it safe in production. I
ran a queue shaped exactly like this for about four years on a parcel-tracking
system, and later strangled a Celery fleet onto a Rust worker over the same
table. This crate is the runnable companion to both write-ups: every claim in
them is backed here by an example or a test you can run against a real Postgres
and watch hold.

This is a clean-room reference implementation built for the portfolio — not
extracted from any production system.

It backs two articles:

- [PostgreSQL as a queue](https://czeresniowski.dev/writing/postgresql-as-a-queue)
- [Migrating a Django queue to Rust](https://czeresniowski.dev/writing/migrating-a-django-queue-to-rust)

Everything uses runtime `sqlx` (`sqlx::query`, `sqlx::query_as`,
`#[derive(sqlx::FromRow)]`), never the compile-time macros, so the crate builds
with no database present. Migrations are embedded and applied at runtime.

## Quickstart

```sh
# 1. Bring up the exact Postgres the articles assume (postgres:16).
docker compose up -d postgres

# 2. Point the demos at it.
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/skip_locked

# 3. Run any demo. They are idempotent: each resets its own tables first.
cargo run --example demo_skip_locked
```

Expected output:

```
claimer-1 took 30 ids: [31, 32, ... 60]
claimer-2 took 30 ids: [1, 2, ... 30]
overlap: []
PASS: batches are disjoint — SKIP LOCKED gave no double-claim
```

The producer/worker pair runs end to end:

```sh
cargo run --example producer        # enqueues N jobs transactionally, then NOTIFY
RUST_LOG=info cargo run --example worker   # drains them; Ctrl-C / SIGTERM drains gracefully
```

Run the whole suite against the database (the DB tests are serial):

```sh
cargo test -- --test-threads=1
```

## What each demo proves

Each demo reproduces a specific section of one of the articles and asserts the
outcome that section predicts. If a demo prints `PASS`, the claim held.

| Demo / test | Article section | What it proves |
| --- | --- | --- |
| `examples/demo_skip_locked.rs` | [The claim: SKIP LOCKED](https://czeresniowski.dev/writing/postgresql-as-a-queue) | Two concurrent claimers walk away with disjoint batches. No double-claim, no advisory locks. |
| `examples/demo_fairness.rs` | [The day one carrier backed everyone up](https://czeresniowski.dev/writing/postgresql-as-a-queue) | Strict FIFO `claim_batch` lets one noisy carrier take 58 of 64 rows; `fair_claim` caps it at 8 and unstarves the rest. |
| `examples/demo_reaper.rs` | [Why the naive reaper is a footgun](https://czeresniowski.dev/writing/postgresql-as-a-queue) | A heartbeat-less reaper resurrects a still-running job (double execution). |
| `examples/demo_reaper.rs` | [Heartbeat lease plus idempotent handlers](https://czeresniowski.dev/writing/postgresql-as-a-queue) | A heartbeat (with the `locked_by` guard) protects a live job from the reaper, while the same reaper still recovers a genuinely dead worker's job. |
| `examples/demo_idempotency.rs` | [Idempotency, because both could run a job](https://czeresniowski.dev/writing/migrating-a-django-queue-to-rust) | Processing the same `(carrier_id, event_id)` twice applies exactly one effect; the second run is a no-op. |
| `examples/demo_pgbouncer.rs` | [The PgBouncer prepared-statement trap](https://czeresniowski.dev/writing/migrating-a-django-queue-to-rust) | Behind a transaction-mode bouncer, a cached/named-statement pool throws a prepared-statement error; the cache-off + non-persistent pool runs clean. Optional, env-guarded. |
| `examples/worker.rs` | [Draining on SIGTERM](https://czeresniowski.dev/writing/migrating-a-django-queue-to-rust) | On SIGTERM the worker stops claiming, drains in-flight work, and bounds the wait to 30 s. |
| `src/worker.rs` (semaphore) | [Fast enough to break the downstream](https://czeresniowski.dev/writing/migrating-a-django-queue-to-rust) | Fan-out concurrency is bounded by a `tokio::sync::Semaphore` so a fast worker cannot overwhelm a downstream sized against the old slow one. |
| `src/pool.rs::connect` | [Pool sizing is backwards](https://czeresniowski.dev/writing/migrating-a-django-queue-to-rust) | The faster worker needs fewer connections: `max_connections(8)`, `acquire_timeout(5s)`. |
| `examples/producer.rs`, `transactional_enqueue_rollback_leaves_no_job` test | [Transactional enqueue deletes a class of bugs](https://czeresniowski.dev/writing/postgresql-as-a-queue) | The job row commits in the same transaction as its cause; a rolled-back producer leaves no job and no orphan. |

The integration tests in `tests/integration.rs` are assertion versions of the
same scars: disjoint claims, the fairness cap, heartbeat-prevents-double-exec
plus reaper-recovers-dead, idempotent dedupe, capped backoff, dead-lettering,
and transactional-enqueue rollback.

## Benchmarks

`cargo run --release --example bench_throughput` measures the claim path against
a real Postgres, through the same public functions the worker uses — no
special-cased fast path. On an Apple Silicon laptop against `postgres:16` in
Docker (8 workers, batch 100, 50k jobs):

| path | rate |
| --- | --- |
| `claim_batch` — `FOR NO KEY UPDATE SKIP LOCKED` | ~38,000 claims/s |
| claim + `mark_done`, end to end | ~4,700 jobs/s |
| single-row transactional `enqueue_pool` (serial) | ~650/s |

The numbers are hardware- and config-dependent; the point is that they are
reproducible. The claim hot path clears the "low thousands to roughly 10k
claims/s" the essays describe — that ceiling is about *sustained* WAL fsync and
autovacuum under churn on a single primary, not a raw claim burst. ~4,700
jobs/s end to end is ~400M/day, comfortably above the tens of millions a day
the queue actually carried. Tune with `BENCH_JOBS`, `BENCH_WORKERS`,
`BENCH_BATCH`.

### Claim latency

`cargo run --release --example bench_claim_latency` measures the latency of a
*single* claim (one worker, `claim_batch(.., 1, ..)` + `mark_done`, timing every
call) rather than aggregate rate. On the same setup (20k jobs):

```
[claim-latency] n=20000 p50=4.9ms p95=6.9ms p99=7.6ms p99.9=10.9ms
```

That backs the essays' "claim latency in the single-digit milliseconds" — each
claim is one `FOR NO KEY UPDATE SKIP LOCKED` round trip plus a commit, so the
floor is a Postgres transaction's fsync, not the lock itself.

### Migration compute (Python → Rust normalize)

The migration essay says ~70% of the old worker's wall time was in *parse and
validate*. `bash bench/run_migration.sh` runs the `webhook.normalize` hot path —
parse a carrier tracking webhook, validate, normalize — in Rust and in Python
(`bench/celery_normalize.py`) over the **identical** payloads, no database, with
peak RSS measured via `getrusage`. On the same machine (200k payloads):

| runtime | p50 | p99 | parse+validate share |
| --- | --- | --- | --- |
| Rust   | ~1.6 µs | ~1.8 µs  | ~84% |
| Python | ~5.9 µs | ~13 µs   | ~73% |

Python spends **~73% of the time in parse+validate** (the essay's "~70%"), and
the Rust hot path is several times faster on the same work. This is a *compute*
comparison only: the essay's `900ms → 34ms` p99 and `280MB → 40MB` are
production-tail figures from a loaded Django/Celery prefork fleet under a retry
storm — not something a single-process microbench reproduces. What's reproducible
here is the parse/validate share and the direction and rough size of the compute
win.

## How it maps to the article

- **`migrations/0001_jobs.sql`** — the `jobs` table exactly as in *The table:
  fillfactor and HOT updates*: `fillfactor = 80` so recovery updates stay HOT,
  `autovacuum_vacuum_scale_factor = 0.02` and `autovacuum_vacuum_cost_delay = 0`
  so vacuum keeps pace under burst. The partial index is the per-kind form
  `(kind, run_at) WHERE state = 'ready'` from *Per-kind routing and per-tenant
  caps*; the single-column variant the article starts from is noted in a
  comment. Also ships `jobs_dead` (same columns) and `processed_events(key)`.

- **`migrations/0002_partitioning.sql`** — illustrative range-partitioning of
  completed jobs by completion time and the `DROP PARTITION` example from
  *Retention, and who owns the dead letters* / *Where the ceiling actually is*.
  Kept separable from the hot path on purpose: the core demos use the plain
  table, and retention is a `DROP TABLE` of a whole partition, not a sweeper
  fighting autovacuum.

- **`src/lib.rs`** — the `Job` struct (`id`, `kind`, `payload`, `attempts`) via
  `#[derive(sqlx::FromRow)]`, `enqueue(&mut Transaction, ...)` for the
  transactional enqueue that is the whole point, and `enqueue_pool(...)` which
  does its own transaction plus `NOTIFY jobs_ready`.

- **`src/claim.rs`** — `claim_batch` is the inner `SELECT ... FOR NO KEY UPDATE
  SKIP LOCKED LIMIT n` wrapped in an outer `UPDATE ... RETURNING`, the
  inner-select/outer-update shape both articles use. The lock strength is the one
  *PostgreSQL as a queue* argues for in "FOR NO KEY UPDATE, not FOR UPDATE"
  (`FOR NO KEY UPDATE`, not the plain `FOR UPDATE` the migration article's
  snippet shows). `fair_claim` is the weighted CTE with
  `row_number() OVER (PARTITION BY payload->>'carrier_id' ORDER BY run_at)` and
  a per-carrier cap.

- **`src/lease.rs`** — `heartbeat` is the `UPDATE ... WHERE id = ANY($1) AND
  locked_by = $2 AND state = 'running'` with the `locked_by` guard; `reap`
  resurrects stale `running` rows. The double-execution footgun is in the
  doc-comment.

- **`src/retry.rs`** — `Class { Transient, Permanent }`, `classify`, `requeue`
  with the capped exponential backoff `least(power(2, attempts), 3600)`, and
  `dead_letter` with the `DELETE ... RETURNING` / `INSERT` CTE.

- **`src/worker.rs`** — the run loop: `LISTEN jobs_ready` plus a poll backstop
  (wait for `NOTIFY` or the poll timeout), graceful drain via a `biased`
  `tokio::select!` against a `CancellationToken` with a bounded 30 s grace, and
  fan-out backpressure via a `tokio::sync::Semaphore`.

- **`src/pool.rs`** — `connect` is the small bounded pool (`max_connections(8)`,
  `acquire_timeout(5s)`) the team shipped; `connect_pgbouncer` sets
  `statement_cache_capacity(0)` for the transaction-mode-bouncer case, with the
  runbook line in the doc-comment.

## Where this breaks / when not to use it

These are the articles' own stated limits, plus what I learned wiring the demos
up against a real Postgres and a real PgBouncer.

- **The ceiling is autovacuum and WAL, not `SELECT` throughput.** `SKIP LOCKED`
  claims are cheap. On a single well-provisioned primary the sustainable ceiling
  is in the low thousands to roughly 10k claims/s, bounded by how fast vacuum
  reclaims dead tuples and how fast one primary can fsync WAL. You cannot shard
  that away without losing the transactional-enqueue property that was the whole
  point.

- **Leave for a dedicated broker only at a measured wall.** Real multi-consumer
  fan-out (one event to many independent consumer groups), sustained five-figure
  jobs/s, or strict total ordering across partitions are message-bus problems.
  `SKIP LOCKED` deliberately does not give you cross-partition ordering. Do not
  migrate because a blog post said queues need Kafka; migrate when you measure
  the wall.

- **`fair_claim` is a cap, not strict fairness.** It stops one carrier from
  monopolizing a pool; it does not guarantee proportional service. Strict
  weighted fair queuing on a table is more machinery than it is worth.

- **The fairness cap needs a candidate window wide enough to reach other
  tenants.** `fair_claim` locks `batch * 4` candidates under `SKIP LOCKED` and
  ranks within that window. If one carrier's backlog is so deep that every row
  in the window is theirs, the cap has nothing else to promote. In practice
  other tenants' jobs are interleaved in time (as the demo seeds them), so they
  fall inside the window; a pathological single-tenant flood still wants
  per-kind routing in front of the cap.

- **The article's `fair_claim` SQL is shown with `FOR NO KEY UPDATE` in the
  same SELECT as the window function. Postgres rejects that** (`FOR NO KEY
  UPDATE is not allowed with window functions`). This crate splits it into a
  locking subquery feeding a ranking CTE, which is semantically identical and
  actually runs. See `src/claim.rs`.

- **The PgBouncer fix is subtler than "disable the statement cache."** In sqlx
  0.8, `statement_cache_capacity(0)` is necessary but not sufficient: a query is
  still persistent by default, so sqlx assigns it a *named* statement
  (`sqlx_s_N`) even with caching off, and those names collide on a reused
  backend behind a transaction-mode bouncer (`prepared statement "sqlx_s_1"
  already exists`). The complete fix is `statement_cache_capacity(0)` **plus**
  `.persistent(false)` on each query so sqlx uses an unnamed statement.
  `demo_pgbouncer` reproduces the failure and the complete fix. This is exactly
  why the team shipped the other option — point the worker straight at Postgres
  with a small bounded pool and keep plan reuse, since a worker fleet is a
  handful of processes, not the hundreds of web handlers transaction pooling
  exists to fan in.

- **`NOTIFY` is a latency optimization, never correctness.** It is best-effort,
  in-memory, not durable, and a worker that drops its connection misses every
  notification in the gap. The poll backstop is what makes the queue correct;
  `LISTEN/NOTIFY` only removes the latency floor.

- **At-least-once is the only honest guarantee.** A worker can finish a job and
  crash before it commits `state = 'done'`, so every handler must be idempotent,
  keyed on the job id or a natural idempotency key. The heartbeat plus
  idempotency is the correctness boundary; the lease is a backstop.

## The two services in docker-compose

`docker compose up -d postgres` is all the default demos need. The compose file
also defines a `pgbouncer` service in transaction pooling mode in front of that
Postgres, on port 6432, purely so you can reproduce the prepared-statement trap:

```sh
docker compose up -d                # postgres + pgbouncer
export PGBOUNCER_URL=postgres://postgres:postgres@localhost:6432/skip_locked
cargo run --example demo_pgbouncer
```

With `PGBOUNCER_URL` unset, `demo_pgbouncer` prints `skipped` and exits 0, so it
never breaks the default run.

## Sibling repos

Other pieces of the same Postgres-as-infrastructure toolkit:

- [skip-locked](https://github.com/czeresniowski-dev/skip-locked) — this repo.
- [refund-engine](https://github.com/czeresniowski-dev/refund-engine)
- [pg-outbox](https://github.com/czeresniowski-dev/pg-outbox) — the outbox
  pattern, the seam a broker introduces that a queue inside the database does
  not have.
- [idem-key](https://github.com/czeresniowski-dev/idem-key) — idempotency keys,
  the `processed_events` ledger generalized.

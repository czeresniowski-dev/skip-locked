-- The whole queue fits in one table. Two physical-storage decisions matter as
-- much as the columns: fillfactor=80 (leave 20% of each page free so later
-- UPDATEs stay HOT and touch no index) and an aggressive per-table autovacuum
-- posture (reclaim dead tuples before they bloat the heap and the partial
-- index). This is the table from "The table: fillfactor and HOT updates".

CREATE TABLE IF NOT EXISTS jobs (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL,
    state        text        NOT NULL DEFAULT 'ready',
    run_at       timestamptz NOT NULL DEFAULT now(),
    locked_at    timestamptz,
    locked_by    text,
    attempts     int         NOT NULL DEFAULT 0,
    max_attempts int         NOT NULL DEFAULT 20,
    last_error   text
) WITH (
    fillfactor                     = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay   = 0
);

-- The partial index is the whole game. The hot path only looks at 'ready' rows,
-- so the index stays proportional to backlog depth, not table size, even with
-- tens of millions of 'done' rows in the heap.
--
-- This is the PER-KIND form from "Per-kind routing and per-tenant caps": the
-- index leads with `kind` so a worker pool claiming WHERE kind = ANY($pool)
-- still hits the index cleanly. The single shared-queue variant the essay
-- starts from is the single-column form:
--
--     CREATE INDEX jobs_claimable ON jobs (run_at) WHERE state = 'ready';
--
-- We ship the per-kind index because every demo here routes by kind. The
-- weighted fair claim also benefits from it.
CREATE INDEX IF NOT EXISTS jobs_claimable
    ON jobs (kind, run_at)
    WHERE state = 'ready';

-- Dead-letter table: same columns as `jobs`. Exhausted jobs are moved here so
-- they stop consuming claim capacity and stop being dead weight on every
-- vacuum pass of the live table. ("Retries, backoff, and the dead-letter
-- table".)
CREATE TABLE IF NOT EXISTS jobs_dead (
    id           bigint      NOT NULL,
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL,
    state        text        NOT NULL,
    run_at       timestamptz NOT NULL,
    locked_at    timestamptz,
    locked_by    text,
    attempts     int         NOT NULL,
    max_attempts int         NOT NULL,
    last_error   text,
    died_at      timestamptz NOT NULL DEFAULT now()
);

-- Idempotency ledger. The dedupe key is derived from the real-world event
-- (carrier_id + event_id), unique per status change regardless of how many
-- times the queue delivers it. The PRIMARY KEY is the unique constraint
-- ON CONFLICT fires on. ("Idempotency, because both could run a job".)
CREATE TABLE IF NOT EXISTS processed_events (
    key text PRIMARY KEY
);

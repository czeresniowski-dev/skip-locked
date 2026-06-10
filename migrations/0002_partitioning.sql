-- ILLUSTRATIVE ONLY. This migration is deliberately separable from the hot
-- path: the core demos and tests use the plain `jobs` table from 0001. This
-- file shows the retention shape the essay describes in "Retention, and who
-- owns the dead letters" and "Where the ceiling actually is": you do NOT
-- DELETE completed rows one at a time (millions of dead tuples autovacuum then
-- chases). You range-partition completed jobs by completion time and DROP
-- whole partitions on a schedule. A partition DROP is an instant catalog
-- operation that produces zero dead tuples, and "partition dropping bought the
-- most headroom by far".
--
-- We keep this as a parallel `jobs_done` archive rather than re-parenting the
-- live `jobs` table, so the claim path in 0001 stays simple. In production you
-- would either make `jobs` itself partitioned from the start, or (as the essay
-- did) move completed rows into a partitioned archive. The mechanics of the
-- DROP are identical either way, and that is the part worth showing.

CREATE TABLE IF NOT EXISTS jobs_done (
    id            bigint      NOT NULL,
    kind          text        NOT NULL,
    payload       jsonb       NOT NULL,
    attempts      int         NOT NULL,
    completed_at  timestamptz NOT NULL
) PARTITION BY RANGE (completed_at);

-- One partition per day. A scheduled job creates tomorrow's partition ahead of
-- time and drops partitions older than the retention window.
CREATE TABLE IF NOT EXISTS jobs_done_2026_06_08
    PARTITION OF jobs_done
    FOR VALUES FROM ('2026-06-08') TO ('2026-06-09');

CREATE TABLE IF NOT EXISTS jobs_done_2026_06_09
    PARTITION OF jobs_done
    FOR VALUES FROM ('2026-06-09') TO ('2026-06-10');

CREATE TABLE IF NOT EXISTS jobs_done_2026_06_10
    PARTITION OF jobs_done
    FOR VALUES FROM ('2026-06-10') TO ('2026-06-11');

-- Retention is a DROP PARTITION on a schedule, not a sweeper fighting
-- autovacuum. Dropping the 2026-06-08 partition instantly evicts every
-- completed job from that day with zero row deletes and zero dead tuples:
--
--     DROP TABLE IF EXISTS jobs_done_2026_06_08;
--
-- That single DDL statement is the entire retention story for completed jobs.
-- Recent partitions stay online for debugging and replay; older ones are
-- dropped, or for audit-required kinds copied to cold storage first.

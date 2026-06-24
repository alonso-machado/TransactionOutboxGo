-- DispatchOutbox's FetchPending (internal/adapter/persistence/outbox_repo.go)
-- filters status IN ('NEW','RETRYING') and orders by created_at ASC. Neither
-- existing index (idx_outbox_messages_status, the plain status btree; or
-- idx_outbox_messages_next_retry_at, partial on next_retry_at) covers
-- created_at, so once a large fraction of the table matches that status
-- filter, the planner reverts to a sequential scan followed by a sort of
-- every matching row before applying LIMIT — and once that sort no longer
-- fits in work_mem, it spills to disk. Confirmed via EXPLAIN ANALYZE against
-- a 250k-row NEW backlog: Seq Scan + external merge sort (177MB on disk),
-- ~972ms per call — nearly 5x OUTBOX_DISPATCH_INTERVAL_MS's 200ms default,
-- meaning the poll cycle itself, not the batch/interval config, became the
-- throughput ceiling.
--
-- A partial index on created_at, scoped to the same status filter the
-- partial next_retry_at index already uses, lets FetchPending's query become
-- a forward index scan that's already in created_at order — next_retry_at is
-- still checked, just as a cheap filter during the scan instead of a
-- sort key. now() can't appear in a partial index predicate (not
-- IMMUTABLE), so next_retry_at <= now() stays a runtime filter, same as
-- before; only the ORDER BY/sort cost goes away.
CREATE INDEX IF NOT EXISTS idx_outbox_messages_pending_created_at
    ON outbox_messages (created_at)
    WHERE status IN ('NEW', 'RETRYING');

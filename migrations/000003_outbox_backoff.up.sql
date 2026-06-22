-- Phase 5 Track 2.A: retry backoff. NULL means "eligible immediately" (NEW
-- rows); RETRYING rows get next_retry_at = now() + backoff(retry_count) so
-- FetchPending stops hot-looping on a struggling broker.
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS next_retry_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_outbox_messages_next_retry_at
    ON outbox_messages (next_retry_at)
    WHERE status IN ('NEW', 'RETRYING');

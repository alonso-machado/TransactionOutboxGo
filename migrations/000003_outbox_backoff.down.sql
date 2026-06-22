DROP INDEX IF EXISTS idx_outbox_messages_next_retry_at;

ALTER TABLE outbox_messages DROP COLUMN IF EXISTS next_retry_at;

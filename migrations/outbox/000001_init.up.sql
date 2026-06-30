-- The Transactional Outbox table — the only table ingestion-api writes to and
-- the only one outbox-worker reads. This single migration is the squashed form
-- of what were four separate migrations (init + retry-backoff column + the
-- LISTEN/NOTIFY trigger + the dispatch index); they were merged because this is
-- a small project always migrated against a fresh database (compose `make
-- down -v`, CI, and the integration suite all start clean).
CREATE TABLE IF NOT EXISTS outbox_messages (
    id              uuid        NOT NULL PRIMARY KEY,
    idempotency_key text        NOT NULL,
    aggregate_type  text,
    http_method     text,
    route           text,
    payload         jsonb,
    headers         jsonb,
    status          text        DEFAULT 'NEW',
    retry_count     integer     DEFAULT 0,
    last_error      text,
    created_at      timestamptz,
    published_at    timestamptz,
    -- Retry backoff: NULL means "eligible immediately" (NEW rows); RETRYING
    -- rows get next_retry_at = now() + backoff(retry_count) so FetchPending
    -- stops hot-looping on a struggling broker.
    next_retry_at   timestamptz,
    payment_method  text        NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_messages_idempotency_key
    ON outbox_messages (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_status
    ON outbox_messages (status);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_next_retry_at
    ON outbox_messages (next_retry_at)
    WHERE status IN ('NEW', 'RETRYING');

-- Partial index in FetchPending's exact ORDER BY (created_at, id) order, scoped
-- to its status filter, so the dispatch poll is a forward index scan with no
-- sort (id is UUIDv7, time-sortable, and covers the created_at tiebreaker).
-- next_retry_at <= now() stays a runtime filter (now() isn't IMMUTABLE, so it
-- can't sit in a partial-index predicate). Under a 250k-row NEW backlog this
-- turned a ~972ms seq-scan + on-disk sort into a sub-interval index scan.
CREATE INDEX IF NOT EXISTS idx_outbox_messages_pending_created_at
    ON outbox_messages (created_at, id)
    WHERE status IN ('NEW', 'RETRYING');

-- LISTEN/NOTIFY trigger so internal/infrastructure/database.Listener can wake
-- outbox-worker immediately on enqueue instead of waiting out the poll
-- interval. Strictly an optimization — DispatchOutbox.Run's poll ticker is the
-- correctness fallback if the LISTEN connection is ever down. The NOTIFY fires
-- in the outbox DB on INSERT (from ingestion-api) and is delivered to the
-- outbox-worker process LISTENing on the channel.
CREATE OR REPLACE FUNCTION notify_outbox_new() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('outbox_new', '');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS outbox_messages_notify ON outbox_messages;

CREATE TRIGGER outbox_messages_notify
  AFTER INSERT ON outbox_messages
  FOR EACH ROW
  EXECUTE FUNCTION notify_outbox_new();

-- The two Transactional Outbox tables — the only tables ingestion-api writes
-- to and the only ones outbox-worker reads. Two separate tables, not one,
-- because they carry two independent flows that must never block each other:
-- order_outbox (order intake -> order-worker) and payment_event_outbox
-- (payment-gateway webhook confirmations -> fulfillment-worker). Both share
-- the identical state machine (NEW -> PUBLISHED, NEW -> RETRYING, RETRYING ->
-- PUBLISHED/DEAD_LETTER) and are relayed by the same generalized DispatchOutbox
-- use-case, run as two independent goroutines inside outbox-worker.
--
-- event_type/event_subtype are denormalized onto every row (rather than
-- requiring a payload parse) since they're what RabbitMQ routing
-- (internal/infrastructure/rabbitmq) keys on for both tables.

CREATE TABLE IF NOT EXISTS order_outbox (
    id              uuid        NOT NULL PRIMARY KEY,
    idempotency_key text        NOT NULL,
    aggregate_type  text        NOT NULL DEFAULT 'order',
    http_method     text,
    route           text,
    payload         jsonb,
    headers         jsonb,
    status          text        DEFAULT 'NEW',
    retry_count     integer     DEFAULT 0,
    last_error      text,
    created_at      timestamptz,
    published_at    timestamptz,
    next_retry_at   timestamptz,
    event_type      text        NOT NULL,
    event_subtype   text        NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_outbox_idempotency_key
    ON order_outbox (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_order_outbox_status
    ON order_outbox (status);

-- Partial index in FetchPending's exact ORDER BY (created_at, id) order,
-- scoped to its status filter, so the dispatch poll is a forward index scan
-- with no sort (id is UUIDv7, time-sortable, and covers the created_at
-- tiebreaker). next_retry_at <= now() stays a runtime filter (now() isn't
-- IMMUTABLE, so it can't sit in a partial-index predicate).
CREATE INDEX IF NOT EXISTS idx_order_outbox_pending_created_at
    ON order_outbox (created_at, id)
    WHERE status IN ('NEW', 'RETRYING');

-- LISTEN/NOTIFY trigger so internal/infrastructure/database.Listener can wake
-- outbox-worker's order dispatcher immediately on enqueue instead of waiting
-- out the poll interval. Strictly an optimization — the poll ticker is the
-- correctness fallback if the LISTEN connection is ever down.
CREATE OR REPLACE FUNCTION notify_order_outbox_new() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('order_outbox_new', '');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS order_outbox_notify ON order_outbox;

CREATE TRIGGER order_outbox_notify
  AFTER INSERT ON order_outbox
  FOR EACH ROW
  EXECUTE FUNCTION notify_order_outbox_new();

-- payment_event_outbox lands verified payment-gateway webhook confirmations
-- (POST /api/v1/webhooks/payments/{provider}). Lower volume than orders — the
-- dispatcher for this table is poll-only, no LISTEN/NOTIFY.
CREATE TABLE IF NOT EXISTS payment_event_outbox (
    id              uuid        NOT NULL PRIMARY KEY,
    idempotency_key text        NOT NULL,
    aggregate_type  text        NOT NULL DEFAULT 'payment_event',
    http_method     text,
    route           text,
    payload         jsonb,
    headers         jsonb,
    status          text        DEFAULT 'NEW',
    retry_count     integer     DEFAULT 0,
    last_error      text,
    created_at      timestamptz,
    published_at    timestamptz,
    next_retry_at   timestamptz,
    event_type      text        NOT NULL,
    event_subtype   text        NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_event_outbox_idempotency_key
    ON payment_event_outbox (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_payment_event_outbox_status
    ON payment_event_outbox (status);

CREATE INDEX IF NOT EXISTS idx_payment_event_outbox_pending_created_at
    ON payment_event_outbox (created_at, id)
    WHERE status IN ('NEW', 'RETRYING');

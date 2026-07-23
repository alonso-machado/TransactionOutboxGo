-- Mirrors migrations/outbox/000002_add_ticket_notification_outbox.up.sql's
-- schema for revertibility.
CREATE TABLE IF NOT EXISTS ticket_notification_outbox (
    id              uuid        NOT NULL PRIMARY KEY,
    idempotency_key text        NOT NULL,
    aggregate_type  text        NOT NULL DEFAULT 'ticket_notification',
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_ticket_notification_outbox_idempotency_key
    ON ticket_notification_outbox (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_ticket_notification_outbox_status
    ON ticket_notification_outbox (status);

CREATE INDEX IF NOT EXISTS idx_ticket_notification_outbox_pending_created_at
    ON ticket_notification_outbox (created_at, id)
    WHERE status IN ('NEW', 'RETRYING');

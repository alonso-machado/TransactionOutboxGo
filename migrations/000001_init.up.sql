-- Lifted from internal/adapter/persistence/migrate.go's former AutoMigrate
-- target (OutboxMessageModel). This is the Transactional Outbox table —
-- the only table ingestion-api writes to directly.
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
    payment_method  text        NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_messages_idempotency_key
    ON outbox_messages (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_status
    ON outbox_messages (status);

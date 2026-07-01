-- ticket_outbox — landing table for POST /api/v1/ticket. A separate outbox
-- from outbox_messages (different aggregate, no per-method routing): ingestion-
-- api only stores the raw "order" object here with status NEW; a future
-- ticket-processing microservice will read and process it. No NOTIFY trigger
-- yet because nothing relays it. Dedup is the UNIQUE idempotency_key, derived
-- from the order's event_id (usecase/ticket).
--
-- Its own migration (not folded into 000001_init) so the ticket feature has an
-- independent, revertible schema change of its own.
CREATE TABLE IF NOT EXISTS ticket_outbox (
    id              uuid        NOT NULL PRIMARY KEY,
    idempotency_key text        NOT NULL,
    event_id        text        NOT NULL,
    payload         jsonb       NOT NULL,
    status          text        DEFAULT 'NEW',
    created_at      timestamptz,
    processed_at    timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ticket_outbox_idempotency_key
    ON ticket_outbox (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_ticket_outbox_status
    ON ticket_outbox (status);

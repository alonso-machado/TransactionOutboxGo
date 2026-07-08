-- Third Transactional Outbox table (Phase 8): lands one row per issued
-- ticket, written inside the same transaction as
-- usecase/fulfillment.IssueTickets.MarkIssued, so a notification row can
-- never exist without its ticket having actually been issued. Relayed by
-- the same generalized DispatchOutbox as order_outbox/payment_event_outbox,
-- run as a third goroutine in outbox-worker — poll-only, no LISTEN/NOTIFY
-- (low volume, same precedent as payment_event_outbox).
--
-- idempotency_key is the ticket's own ID — one notification per issued
-- ticket, a natural dedup key.
--
-- event_type/event_subtype stay denormalized here for consistency with the
-- other two outbox tables' schema, even though notification-consumer-worker
-- itself is not sharded by them (see
-- internal/infrastructure/rabbitmq.NotificationStream's doc comment) —
-- AMQPPublisher.fire still needs some routing-key inputs to compute against,
-- and the columns keep this row meaningful for any future per-genre
-- notification reporting.

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

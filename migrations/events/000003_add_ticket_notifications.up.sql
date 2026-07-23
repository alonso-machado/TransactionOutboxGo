-- Replaces ticket_notification_outbox (see
-- migrations/outbox/000003_drop_ticket_notification_outbox.up.sql): ticket
-- email delivery no longer goes through RabbitMQ, so this table lives in the
-- events DB (alongside tickets) instead of the outbox DB, and is written
-- inside the SAME transaction as usecase/fulfillment.IssueTickets.MarkIssued
-- — a notification row can never exist without its ticket having actually
-- been issued, and vice versa (the old cross-database version could only be
-- best-effort).
--
-- ticket_id is the primary key: one row per issued ticket, a natural dedup
-- key (mirrors the old table's idempotency_key = ticket.ID convention).
-- email_sent_timestamp/email_sent_error track delivery instead of a generic
-- outbox status column — fulfillment-consumer-worker sends the email
-- synchronously right after issuing the ticket; notification-retry-cron (a
-- Kubernetes CronJob, no RabbitMQ) retries any row still missing
-- email_sent_timestamp once next_retry_at has passed.

CREATE TABLE IF NOT EXISTS ticket_notifications (
    ticket_id            uuid        NOT NULL PRIMARY KEY REFERENCES tickets (id),
    attempt_count        integer     NOT NULL DEFAULT 0,
    email_sent_timestamp timestamptz,
    email_sent_error     text,
    next_retry_at        timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ticket_notifications_pending
    ON ticket_notifications (created_at)
    WHERE email_sent_timestamp IS NULL;

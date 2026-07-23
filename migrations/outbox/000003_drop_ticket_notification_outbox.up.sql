-- Ticket email delivery no longer goes through RabbitMQ/outbox-worker — see
-- migrations/events/000003_add_ticket_notifications.up.sql for its
-- replacement (in the events DB, written atomically with MarkIssued).
DROP TABLE IF EXISTS ticket_notification_outbox;

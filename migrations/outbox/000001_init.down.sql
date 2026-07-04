DROP TRIGGER IF EXISTS order_outbox_notify ON order_outbox;
DROP FUNCTION IF EXISTS notify_order_outbox_new();
DROP TABLE IF EXISTS payment_event_outbox;
DROP TABLE IF EXISTS order_outbox;

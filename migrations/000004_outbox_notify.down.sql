DROP TRIGGER IF EXISTS outbox_messages_notify ON outbox_messages;
DROP FUNCTION IF EXISTS notify_outbox_new();

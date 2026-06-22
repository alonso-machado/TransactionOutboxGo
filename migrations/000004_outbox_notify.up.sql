-- Phase 5 Track 3.A: LISTEN/NOTIFY trigger so internal/infrastructure/database.Listener
-- can wake DispatchOutbox immediately on enqueue instead of waiting out the
-- poll interval. Strictly an optimization — the existing poll ticker in
-- DispatchOutbox.Run remains the correctness fallback if the LISTEN
-- connection is ever down.
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

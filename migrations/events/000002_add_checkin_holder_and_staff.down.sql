DROP TABLE IF EXISTS staff_users;
DROP INDEX IF EXISTS idx_tickets_status_checked_in_at;
ALTER TABLE tickets DROP COLUMN IF EXISTS checked_in_at;

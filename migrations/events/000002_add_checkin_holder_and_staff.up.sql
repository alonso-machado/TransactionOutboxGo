-- Phase 8: check-in (a ticket flips VALID -> CHECKED_IN, recording when)
-- and staff authentication (a local roster of who's allowed to perform that
-- check-in, keyed on a Clerk-authenticated identity — see
-- internal/domain/{staff,staffauth}.go). No buyer-age column: the
-- ticket-holder-update endpoint only ever corrects the buyer's name.
--
-- No migration is needed for the CHECKED_IN status value itself — status
-- is bare text with no ENUM type or CHECK constraint on tickets.status.

ALTER TABLE tickets
    ADD COLUMN IF NOT EXISTS checked_in_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_tickets_status_checked_in_at
    ON tickets (status, checked_in_at)
    WHERE status = 'CHECKED_IN';

-- location_id nullable: NULL means the staff member is unscoped (can check
-- in at any venue — an admin/floater role); set means restricted to that
-- one venue (usecase/checkin's WRONG_VENUE outcome).
CREATE TABLE IF NOT EXISTS staff_users (
    id            uuid        NOT NULL PRIMARY KEY,
    clerk_user_id text        NOT NULL,
    name          text        NOT NULL,
    role          text        NOT NULL,
    location_id   uuid        REFERENCES locations (id),
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_staff_users_clerk_user_id
    ON staff_users (clerk_user_id);

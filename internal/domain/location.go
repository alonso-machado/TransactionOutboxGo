package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Location is a venue an Event takes place at, upserted from the order
// payload's "venue" object. SourceVenueID is the dedup/upsert key (the
// venue.id the order carries) — a redelivered order for a venue we already
// know about is a safe no-op, not a duplicate row.
type Location struct {
	ID            uuid.UUID
	Name          string
	City          string
	SourceVenueID string
	CreatedAt     time.Time
}

// LocationRepository is the port for the locations table (events DB).
// UpsertBySourceVenueID inserts on first sight of a venue and returns the
// existing row's ID on every subsequent sighting (ON CONFLICT (source_venue_id)
// DO UPDATE ... RETURNING id), so order-consumer-worker never needs a separate
// find-then-create round trip.
type LocationRepository interface {
	UpsertBySourceVenueID(ctx context.Context, uow UnitOfWork, loc *Location) (uuid.UUID, error)
}

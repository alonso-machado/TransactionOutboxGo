package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Event is a producer-run happening at a Location — a concert, match, or
// show — categorized by EventType/EventSubtype (e.g. "CONCERT"/"ROCK"). That
// pair is the routing key for both RabbitMQ (internal/infrastructure/rabbitmq)
// and, going forward, DB partitioning: every order and ticket carries the
// same (EventType, EventSubtype) as the Event they belong to.
//
// SourceEventID is the dedup/upsert key: the event_id the order payload
// carries. producer_id/event_areas are intentionally not modelled yet — the
// order payload carries no producer or seating-area data to populate them
// from (see migrations/events).
type Event struct {
	ID            uuid.UUID
	EventType     string
	EventSubtype  string
	Name          string
	LocationID    uuid.UUID
	SourceEventID string
	CreatedAt     time.Time
}

// EventRepository is the port for the events table (events DB).
// UpsertBySourceEventID mirrors LocationRepository.UpsertBySourceVenueID:
// insert on first sight, return the existing row's ID otherwise.
type EventRepository interface {
	UpsertBySourceEventID(ctx context.Context, uow UnitOfWork, e *Event) (uuid.UUID, error)
	// FindByID looks up an event by its own UUID — usecase/checkin resolves
	// a ticket's Event.LocationID this way to check the authenticated
	// staff member's venue scoping.
	FindByID(ctx context.Context, id uuid.UUID) (*Event, error)
}

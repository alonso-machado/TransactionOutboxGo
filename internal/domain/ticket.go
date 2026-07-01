package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// TicketOutboxMessage is a ticket-order event durably landed in the
// ticket_outbox table by ingestion-api's POST /api/v1/ticket. It's a separate
// outbox from the payments one (OutboxMessage): a future ticket-processing
// microservice will read and process these, so for now ingestion-api only
// stores the raw order payload with a NEW status — there is no relay yet.
type TicketOutboxMessage struct {
	ID             uuid.UUID
	IdempotencyKey string
	EventID        string
	Payload        []byte // the raw "order" object, stored opaquely as JSONB
	Status         OutboxStatus
	CreatedAt      time.Time
	ProcessedAt    *time.Time
}

// TicketOutboxRepository is the port for the ticket_outbox table. Enqueue
// reports whether the row was newly created (false => a duplicate
// idempotency key already existed), mirroring OutboxRepository.Enqueue so the
// handler can answer "duplicate" instead of "accepted".
type TicketOutboxRepository interface {
	Enqueue(ctx context.Context, uow UnitOfWork, msg *TicketOutboxMessage) (created bool, err error)
}

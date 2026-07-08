// Ticket is the event-ticket domain (post-pivot): a Ticket is reserved when
// an Order is placed and issued (QR + signature) once its Charge is
// confirmed PAID. This file previously held TicketOutboxMessage/
// TicketOutboxRepository for the old POST /api/v1/ticket -> ticket_outbox
// landing table; that table is superseded by the generic order_outbox (see
// OutboxMessage) — a ticket order is now just another outbox row, routed by
// (EventType, EventSubtype) like everything else.
package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type TicketStatus string

const (
	TicketStatusReserved  TicketStatus = "RESERVED"
	TicketStatusValid     TicketStatus = "VALID"
	TicketStatusVoid      TicketStatus = "VOID"
	TicketStatusCheckedIn TicketStatus = "CHECKED_IN"
)

// Ticket is one seat/admission reserved for an Order. QRPNG/QRContent/
// ValidationCode/Signature are populated only once Status moves RESERVED ->
// VALID (see usecase/fulfillment.IssueTickets); a RESERVED ticket that never
// gets paid is marked VOID, releasing the reservation.
type Ticket struct {
	ID             uuid.UUID
	OrderID        uuid.UUID
	EventID        uuid.UUID
	SourceTicketID string
	Section        string
	Row            string
	Seat           string
	Price          int64
	Currency       string
	BuyerName      string
	BuyerEmail     string
	QRPNG          []byte
	QRContent      string
	ValidationCode string
	Signature      string
	Status         TicketStatus
	CreatedAt      time.Time
	// CheckedInAt is nil until check-in (usecase/checkin.CheckIn), the same
	// nilable-timestamp convention OutboxMessage.PublishedAt already uses.
	CheckedInAt *time.Time
}

// TicketRepository is the port for the tickets table (events DB).
type TicketRepository interface {
	// ReserveForOrder inserts one RESERVED row per item, keyed by
	// SourceTicketID (ON CONFLICT DO NOTHING) — idempotent against a
	// redelivered order-intake message.
	ReserveForOrder(ctx context.Context, uow UnitOfWork, tickets []*Ticket) error
	FindByOrderID(ctx context.Context, orderID uuid.UUID) ([]*Ticket, error)
	// FindByID looks up one ticket by its own ID — used by check-in (the
	// QR's embedded ticketID) and ticket-holder update.
	FindByID(ctx context.Context, id uuid.UUID) (*Ticket, error)
	// MarkIssued persists t's QR/validation fields and flips it to VALID.
	MarkIssued(ctx context.Context, uow UnitOfWork, t *Ticket) error
	// MarkVoid flips every RESERVED ticket for orderID to VOID (payment
	// failed — the reservation is released).
	MarkVoid(ctx context.Context, uow UnitOfWork, orderID uuid.UUID) error
	// CheckIn flips id from VALID to CHECKED_IN, recording checkedInAt.
	// alreadyCheckedIn reports whether the row was already CHECKED_IN (a
	// safe no-op) rather than the update actually applying — the caller
	// already has the ticket in hand from FindByID to distinguish that from
	// "wrong state" (RESERVED/VOID never became VALID).
	CheckIn(ctx context.Context, uow UnitOfWork, id uuid.UUID, checkedInAt time.Time) (alreadyCheckedIn bool, err error)
	// UpdateHolderName corrects the buyer name on an already-reserved/issued
	// ticket. The "can this status still be edited" rule lives in
	// usecase/ticketholder, not here.
	UpdateHolderName(ctx context.Context, uow UnitOfWork, id uuid.UUID, name string) error
}

// TicketQR is the port for generating a ticket's QR code + signed
// validation data (internal/adapter/ticketqr).
type TicketQR interface {
	Generate(ticketID uuid.UUID) (qrPNG []byte, qrContent, validationCode, signature string, err error)
}

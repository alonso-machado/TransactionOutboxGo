package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type OrderStatus string

const (
	OrderStatusPending  OrderStatus = "PENDING"
	OrderStatusReserved OrderStatus = "RESERVED"
	OrderStatusPaid     OrderStatus = "PAID"
	OrderStatusFailed   OrderStatus = "FAILED"
)

// OrderItem is one requested ticket line inside an Order — the seating/
// pricing detail order-consumer-worker turns into a reserved Ticket row.
// SourceTicketID is the order payload's own ticket id (e.g. "TKT-...") and
// is the dedup/idempotency key TicketRepository.ReserveForOrder keys on.
type OrderItem struct {
	SourceTicketID string
	Section        string
	Row            string
	Seat           string
	Price          int64 // minor units
	Currency       string
}

// Customer is the buyer's contact info carried on an Order — PII (Email,
// Document) is masked wherever it might be logged (internal/domain/pii).
type Customer struct {
	Name     string
	Email    string
	Document string
}

// Order is a customer's request for tickets to an Event, charged through a
// PaymentGateway. EventType/EventSubtype are denormalized from the Event so
// the outbox row and RabbitMQ routing key don't require a payload parse.
// SourceOrderID is the dedup/idempotency key (the order payload's own
// event_id — an order redelivery carries the same value).
type Order struct {
	ID            uuid.UUID
	SourceOrderID string
	EventType     string
	EventSubtype  string
	SourceEventID string
	SourceVenueID string
	VenueName     string
	VenueCity     string
	Items         []OrderItem
	Customer      Customer
	Amount        int64 // minor units, sum of Items[].Price
	Currency      string
	Status        OrderStatus
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// OrderRepository is the port for the orders table (events DB). Save is
// idempotent (ON CONFLICT (source_order_id) DO NOTHING): a redelivered
// order-intake message is a safe no-op, reported via created=false so
// ProcessOrder can skip re-charging it through the gateway.
type OrderRepository interface {
	Save(ctx context.Context, uow UnitOfWork, o *Order) (created bool, err error)
	FindBySourceOrderID(ctx context.Context, sourceOrderID string) (*Order, error)
	// FindByID looks up an order by its own UUID — tickets-api's GET
	// /orders/{id} receives that UUID from the URL path, not the caller's
	// own SourceOrderID, so FindBySourceOrderID doesn't fit.
	FindByID(ctx context.Context, id uuid.UUID) (*Order, error)
	UpdateStatus(ctx context.Context, uow UnitOfWork, id uuid.UUID, status OrderStatus) error
}

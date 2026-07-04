package persistence

import (
	"time"

	"github.com/google/uuid"
)

// outboxRow is the shared GORM shape for both order_outbox and
// payment_event_outbox — no fixed TableName(): GORMOutboxRepository is
// constructed with the table name it operates on and applies it via
// .Table(name) on every query, so the two outboxes share one repository
// implementation instead of two near-identical copies.
type outboxRow struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey"`
	IdempotencyKey string    `gorm:"uniqueIndex;not null"`
	AggregateType  string
	HTTPMethod     string
	Route          string
	Payload        []byte `gorm:"type:jsonb"`
	Headers        []byte `gorm:"type:jsonb"`
	Status         string `gorm:"index;default:NEW"`
	RetryCount     int    `gorm:"default:0"`
	LastError      string
	CreatedAt      time.Time
	PublishedAt    *time.Time
	EventType      string     `gorm:"column:event_type;not null"`
	EventSubtype   string     `gorm:"column:event_subtype;not null"`
	NextRetryAt    *time.Time `gorm:"column:next_retry_at"`
}

// LocationModel is the locations table (events DB).
type LocationModel struct {
	ID            uuid.UUID `gorm:"type:uuid;primaryKey"`
	Name          string
	City          string
	SourceVenueID string `gorm:"column:source_venue_id;uniqueIndex;not null"`
	CreatedAt     time.Time
}

func (LocationModel) TableName() string { return "locations" }

// EventModel is the events table (events DB).
type EventModel struct {
	ID            uuid.UUID `gorm:"type:uuid;primaryKey"`
	EventType     string    `gorm:"column:event_type;not null"`
	EventSubtype  string    `gorm:"column:event_subtype;not null"`
	Name          string
	LocationID    uuid.UUID `gorm:"type:uuid;column:location_id;not null"`
	SourceEventID string    `gorm:"column:source_event_id;uniqueIndex;not null"`
	CreatedAt     time.Time
}

func (EventModel) TableName() string { return "events" }

// OrderModel is the orders table (events DB). Items is stored as a single
// jsonb column rather than a child table — an order's line items are never
// queried independently of the order itself.
type OrderModel struct {
	ID                uuid.UUID `gorm:"type:uuid;primaryKey"`
	SourceOrderID     string    `gorm:"column:source_order_id;uniqueIndex;not null"`
	EventType         string    `gorm:"column:event_type;not null"`
	EventSubtype      string    `gorm:"column:event_subtype;not null"`
	SourceEventID     string    `gorm:"column:source_event_id;not null"`
	SourceVenueID     string    `gorm:"column:source_venue_id"`
	VenueName         string    `gorm:"column:venue_name"`
	VenueCity         string    `gorm:"column:venue_city"`
	Items             []byte    `gorm:"type:jsonb;not null"`
	CustomerName      string    `gorm:"column:customer_name"`
	CustomerEmail     string    `gorm:"column:customer_email"`
	CustomerDocument  string    `gorm:"column:customer_document"`
	Amount            int64     `gorm:"not null"`
	Currency          string    `gorm:"not null"`
	Status            string    `gorm:"not null;default:PENDING"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (OrderModel) TableName() string { return "orders" }

// TicketModel is the tickets table (events DB).
type TicketModel struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey"`
	OrderID        uuid.UUID `gorm:"type:uuid;column:order_id;not null"`
	EventID        uuid.UUID `gorm:"type:uuid;column:event_id;not null"`
	SourceTicketID string    `gorm:"column:source_ticket_id;uniqueIndex;not null"`
	Section        string
	Row            string
	Seat           string
	Price          int64  `gorm:"not null"`
	Currency       string `gorm:"not null"`
	BuyerName      string `gorm:"column:buyer_name"`
	BuyerEmail     string `gorm:"column:buyer_email"`
	QRPNG          []byte `gorm:"column:qr_png;type:bytea"`
	QRContent      string `gorm:"column:qr_content"`
	ValidationCode string `gorm:"column:validation_code"`
	Signature      string
	Status         string `gorm:"not null;default:RESERVED"`
	CreatedAt      time.Time
}

func (TicketModel) TableName() string { return "tickets" }

// ChargeModel is the charges table (events DB).
type ChargeModel struct {
	ID          uuid.UUID `gorm:"type:uuid;primaryKey"`
	OrderID     uuid.UUID `gorm:"type:uuid;column:order_id;uniqueIndex;not null"`
	Provider    string    `gorm:"not null"`
	ProviderRef string    `gorm:"column:provider_ref;uniqueIndex;not null"`
	CheckoutURL string    `gorm:"column:checkout_url"`
	Amount      int64     `gorm:"not null"`
	Currency    string    `gorm:"not null"`
	Status      string    `gorm:"not null;default:PENDING"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (ChargeModel) TableName() string { return "charges" }

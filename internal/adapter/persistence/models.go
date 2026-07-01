package persistence

import (
	"time"

	"github.com/google/uuid"
)

type OutboxMessageModel struct {
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
	PaymentMethod  string     `gorm:"column:payment_method;not null"`
	NextRetryAt    *time.Time `gorm:"column:next_retry_at"`
}

func (OutboxMessageModel) TableName() string { return "outbox_messages" }

// TicketOutboxModel is the ticket_outbox table — the landing table for POST
// /api/v1/ticket. Lives in the same `outbox` database as OutboxMessageModel
// (ingestion-api's DB); a future ticket-processing microservice reads it.
type TicketOutboxModel struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey"`
	IdempotencyKey string    `gorm:"uniqueIndex;not null"`
	EventID        string    `gorm:"index;not null"`
	Payload        []byte    `gorm:"type:jsonb;not null"`
	Status         string    `gorm:"index;default:NEW"`
	CreatedAt      time.Time
	ProcessedAt    *time.Time `gorm:"column:processed_at"`
}

func (TicketOutboxModel) TableName() string { return "ticket_outbox" }

type PaymentModel struct {
	ID                uuid.UUID `gorm:"type:uuid;primaryKey"`
	SourceMessageID   string    `gorm:"uniqueIndex;not null"`
	EventID           string    `gorm:"index"`
	ProviderName      string
	ProviderPaymentID string
	ExternalPaymentID string
	PayerID           *uuid.UUID `gorm:"type:uuid"`
	RecipientID       *uuid.UUID `gorm:"type:uuid"`
	Amount            int64
	Currency          string
	Method            string `gorm:"index"`
	MethodDetails     []byte `gorm:"type:jsonb"`
	OccurredAt        time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (PaymentModel) TableName() string { return "payments" }

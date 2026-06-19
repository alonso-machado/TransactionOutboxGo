package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Payment is the core business entity for a financial transfer.
//
// Amount is always stored in minor units (e.g. cents for USD: 4250 = $42.50)
// to avoid floating-point precision errors inherent to float64.
//
// ID is a UUID v7: time-ordered, which gives monotonically increasing primary
// keys and significantly better B-tree index performance than random UUID v4.
type Payment struct {
	ID          uuid.UUID
	PayerID     uuid.UUID
	RecipientID uuid.UUID
	Amount      int64  // minor units — never float
	Currency    string // ISO 4217 (e.g. "USD", "BRL")
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewPayment is the only way to construct a Payment — it generates the UUID v7
// primary key so callers never have to think about key generation.
// CreatedAt and UpdatedAt are both set to the same UTC instant on creation.
func NewPayment(payerID, recipientID uuid.UUID, amount int64, currency string) (*Payment, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return &Payment{
		ID:          id,
		PayerID:     payerID,
		RecipientID: recipientID,
		Amount:      amount,
		Currency:    currency,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// Update applies changes coming from a PUT or PATCH request and stamps UpdatedAt.
// Only the fields that are meaningful to update are accepted; ID and CreatedAt
// are immutable after creation.
func (p *Payment) Update(amount int64, currency string) {
	p.Amount = amount
	p.Currency = currency
	p.UpdatedAt = time.Now().UTC()
}

// PaymentRepository is the port (interface) for persisting Payment entities.
// The concrete implementation lives in internal/adapter/persistence and is
// injected at the composition root (cmd/ingestion-api/main.go).
type PaymentRepository interface {
	Save(ctx context.Context, uow UnitOfWork, p *Payment) error
	FindByID(ctx context.Context, id uuid.UUID) (*Payment, error)
}

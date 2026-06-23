package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Payment is the core business entity representing an externally-sourced
// payment event (e.g. a webhook notification from a payment provider such
// as Mercado Pago). Amount is stored in minor units (cents) to avoid
// floating-point precision errors; the wire format's decimal float is
// converted at the HTTP boundary, never inside domain or persistence code.
type Payment struct {
	ID                uuid.UUID
	SourceMessageID   string // dedup key = outbox idempotency key / RabbitMQ MessageId
	EventID           string // provider's webhook event id
	ProviderName      string // e.g. "MERCADO_PAGO"
	ProviderPaymentID string // provider's payment/transaction id
	ExternalPaymentID string // provider-side payment reference (e.g. "pay_123")
	PayerID           *uuid.UUID
	RecipientID       *uuid.UUID
	Amount            int64 // minor units — never float
	Currency          string
	Method            string // e.g. "PIX", "CARD", "BOLETO"
	MethodDetails     []byte // JSONB — opaque, method-specific (e.g. PIX endToEndId/txid)
	OccurredAt        time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// PaymentRepository is the port for persisting Payment entities. The
// concrete implementation lives in internal/adapter/persistence and is
// injected at the composition root (cmd/consumer-worker/main.go). Save is
// idempotent: a duplicate SourceMessageID is silently ignored (ON CONFLICT
// DO NOTHING), so the consumer can safely re-process redelivered messages.
// created reports whether the row was newly inserted (false means a
// redelivery of an already-processed message) — the caller (ProcessMessage)
// surfaces this as a distinct "duplicate" outcome on its trace/metrics
// instead of conflating it with a fresh "saved", the same way
// OutboxRepository.Enqueue's created return lets IngestPayment tell
// "accepted" apart from "duplicate".
type PaymentRepository interface {
	Save(ctx context.Context, uow UnitOfWork, p *Payment) (created bool, err error)
}

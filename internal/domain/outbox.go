package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type OutboxStatus string

const (
	OutboxStatusNew        OutboxStatus = "NEW"
	OutboxStatusPublished  OutboxStatus = "PUBLISHED"
	OutboxStatusRetrying   OutboxStatus = "RETRYING"
	OutboxStatusDeadLetter OutboxStatus = "DEAD_LETTER"
)

type OutboxMessage struct {
	ID             uuid.UUID
	IdempotencyKey string
	AggregateType  string
	HTTPMethod     string
	Route          string
	Payload        []byte
	Headers        map[string]string
	Status         OutboxStatus
	RetryCount     int
	LastError      string
	CreatedAt      time.Time
	PublishedAt    *time.Time
	PaymentMethod  string // e.g. "PIX" — drives the per-method routing key
}

// OutboxRepository is the port for the Outbox table. Enqueue reports
// whether the row was newly created (false means a duplicate idempotency
// key already existed) so the caller can respond "duplicate" instead of
// "accepted".
type OutboxRepository interface {
	Enqueue(ctx context.Context, uow UnitOfWork, msg *OutboxMessage) (created bool, err error)
	FetchPending(ctx context.Context, limit int) ([]*OutboxMessage, error) // status IN (NEW, RETRYING)
	MarkPublished(ctx context.Context, id uuid.UUID, publishedAt time.Time) error
	MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error
	MarkDeadLetter(ctx context.Context, id uuid.UUID, lastError string) error
	DeleteOldPublished(ctx context.Context, olderThan time.Duration) error
}

type Publisher interface {
	Publish(ctx context.Context, msg *OutboxMessage) error
}

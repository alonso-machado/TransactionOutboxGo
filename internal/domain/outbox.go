package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusPublished OutboxStatus = "published"
	OutboxStatusFailed    OutboxStatus = "failed"
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
}

type OutboxRepository interface {
	Enqueue(ctx context.Context, uow UnitOfWork, msg *OutboxMessage) error
	FetchPending(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkPublished(ctx context.Context, id uuid.UUID, publishedAt time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, lastError string) error
	IncrementRetry(ctx context.Context, id uuid.UUID, lastError string) error
	DeleteOldPublished(ctx context.Context, olderThan time.Duration) error
}

type Publisher interface {
	Publish(ctx context.Context, msg *OutboxMessage) error
}

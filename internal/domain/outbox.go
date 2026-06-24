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
	// NextRetryAt gates FetchPending eligibility (Phase 5 Track 2.A): NULL
	// for NEW rows (eligible immediately), set to now()+backoff(RetryCount)
	// by MarkRetrying so RETRYING rows wait out their backoff instead of
	// being re-fetched every dispatch tick.
	NextRetryAt *time.Time
}

// OutboxRepository is the port for the Outbox table. Enqueue reports
// whether the row was newly created (false means a duplicate idempotency
// key already existed) so the caller can respond "duplicate" instead of
// "accepted".
type OutboxRepository interface {
	Enqueue(ctx context.Context, uow UnitOfWork, msg *OutboxMessage) (created bool, err error)
	// FetchPending selects status IN (NEW, RETRYING) AND (next_retry_at IS
	// NULL OR next_retry_at <= now()), FOR UPDATE SKIP LOCKED.
	FetchPending(ctx context.Context, limit int) ([]*OutboxMessage, error)
	// MarkPublished marks every row in ids PUBLISHED in a single statement —
	// the dispatcher calls this once per batch with every successfully
	// published message's ID, not once per message, so a 100-row batch is
	// one UPDATE round trip instead of 100.
	MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error
	MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error
	MarkDeadLetter(ctx context.Context, id uuid.UUID, lastError string) error
	DeleteOldPublished(ctx context.Context, olderThan time.Duration) error
	// CountPending returns the true count of status IN (NEW, RETRYING) rows
	// — Phase 5 Track 2.B fix for the "pending_count" gauge being capped at
	// batchSize (len(msgs)) instead of reflecting the real backlog.
	CountPending(ctx context.Context) (int64, error)
	// CountDeadLetter returns the count of DEAD_LETTER rows, exposed as the
	// dead_letter_count gauge (Track 2.B) — a non-zero value is an operator
	// signal.
	CountDeadLetter(ctx context.Context) (int64, error)
}

// DLQReplayer is the port for outbox-side dead-letter replay (Phase 5
// Track 2.C): resets DEAD_LETTER rows back to NEW so the existing dispatch
// loop picks them up and republishes. Kept as a separate, small interface
// (rather than folded into OutboxRepository) so cmd/outbox-admin depends on
// exactly the operation it needs.
type DLQReplayer interface {
	// ReplayDeadLetters resets up to limit DEAD_LETTER rows for method (or
	// all methods if method is "") back to status=NEW, retry_count=0,
	// next_retry_at=NULL, last_error cleared. Returns the number of rows
	// reset.
	ReplayDeadLetters(ctx context.Context, method string, limit int) (int64, error)
}

type Publisher interface {
	Publish(ctx context.Context, msg *OutboxMessage) error
}

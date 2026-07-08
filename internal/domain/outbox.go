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
	// AggregateType is "order", "payment_event", or "ticket_notification" —
	// which of the three outboxes (order_outbox / payment_event_outbox /
	// ticket_notification_outbox) this row belongs to. The publisher uses it
	// to pick the RabbitMQ stream (queue-name/routing-key prefix) a message
	// routes to (rmq.StreamForAggregateType); EventType/EventSubtype pick
	// the specific queue within that stream — except ticket_notification,
	// whose stream is a single unsharded queue regardless of these two
	// fields (see rmq.NotificationStream's doc comment).
	AggregateType string
	HTTPMethod    string
	Route         string
	Payload       []byte
	Headers       map[string]string
	Status        OutboxStatus
	RetryCount    int
	LastError     string
	CreatedAt     time.Time
	PublishedAt   *time.Time
	EventType     string // e.g. "CONCERT" — drives per-(type,subtype) routing
	EventSubtype  string // e.g. "ROCK"
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
	// ReplayDeadLetters resets up to limit DEAD_LETTER rows for eventType
	// (or every event type if eventType is "") back to status=NEW,
	// retry_count=0, next_retry_at=NULL, last_error cleared. Returns the
	// number of rows reset.
	ReplayDeadLetters(ctx context.Context, eventType string, limit int) (int64, error)
}

type Publisher interface {
	Publish(ctx context.Context, msg *OutboxMessage) error
	// PublishBatch publishes every msg, returning one error per msg (nil on
	// success) in the same order — pipelined: implementations should fire
	// all publishes before waiting on any broker confirm, instead of
	// round-tripping per message, so a batch's total latency is closer to
	// one round trip than len(msgs) of them.
	PublishBatch(ctx context.Context, msgs []*OutboxMessage) []error
}

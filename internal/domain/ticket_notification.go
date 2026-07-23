package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// TicketNotification tracks email delivery for one issued ticket (events
// DB, not the outbox DB — ticket email no longer goes through RabbitMQ, see
// usecase/notification.SendTicketNotification). One row per ticket,
// TicketID is the primary key: created inside the same transaction as
// usecase/fulfillment.IssueTickets.MarkIssued, so a ticket can never end up
// issued without a corresponding notification row (the old
// ticket_notification_outbox version could only make this best-effort,
// since it lived in a different logical database).
type TicketNotification struct {
	TicketID           uuid.UUID
	AttemptCount       int
	EmailSentTimestamp *time.Time
	EmailSentError     string
	NextRetryAt        *time.Time
	CreatedAt          time.Time
}

// TicketNotificationRepository is the port for the ticket_notifications
// table.
type TicketNotificationRepository interface {
	// Create inserts a PENDING row for ticketID. Called from inside the
	// caller's events-DB transaction (uow non-nil) so it commits atomically
	// with MarkIssued.
	Create(ctx context.Context, uow UnitOfWork, ticketID uuid.UUID) error
	// FetchPendingForRetry selects rows WHERE email_sent_timestamp IS NULL
	// AND (next_retry_at IS NULL OR next_retry_at <= now()), FOR UPDATE SKIP
	// LOCKED — used only by notification-retry-cron.
	FetchPendingForRetry(ctx context.Context, limit int) ([]*TicketNotification, error)
	// MarkSent clears email_sent_error and records email_sent_timestamp.
	MarkSent(ctx context.Context, ticketID uuid.UUID, sentAt time.Time) error
	// MarkFailed increments attempt_count and records lastError, computing
	// next_retry_at from domain.Backoff(attempt_count, base, cap) internally
	// — the same convention GORMOutboxRepository.MarkRetrying already uses.
	MarkFailed(ctx context.Context, ticketID uuid.UUID, lastError string) error
}

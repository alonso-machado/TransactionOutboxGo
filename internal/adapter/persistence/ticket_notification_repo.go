package persistence

import (
	"context"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GORMTicketNotificationRepository implements domain.TicketNotificationRepository
// against the ticket_notifications table (events DB).
type GORMTicketNotificationRepository struct {
	db               *gorm.DB
	retryBackoffBase time.Duration
	retryBackoffCap  time.Duration
}

// NewTicketNotificationRepository wires the repository with the
// retry-backoff base/cap used by MarkFailed — falls back to 1s/5m if zero
// values are passed, same convention NewOutboxRepository already follows.
func NewTicketNotificationRepository(db *gorm.DB, retryBackoffBase, retryBackoffCap time.Duration) *GORMTicketNotificationRepository {
	if retryBackoffBase <= 0 {
		retryBackoffBase = time.Second
	}
	if retryBackoffCap <= 0 {
		retryBackoffCap = 5 * time.Minute
	}
	return &GORMTicketNotificationRepository{db: db, retryBackoffBase: retryBackoffBase, retryBackoffCap: retryBackoffCap}
}

func (r *GORMTicketNotificationRepository) Create(ctx context.Context, uow domain.UnitOfWork, ticketID uuid.UUID) error {
	m := TicketNotificationModel{
		TicketID:  ticketID,
		CreatedAt: time.Now().UTC(),
	}
	db := TxFromContext(ctx, r.db)
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&m).Error
}

func (r *GORMTicketNotificationRepository) FetchPendingForRetry(ctx context.Context, limit int) ([]*domain.TicketNotification, error) {
	var rows []TicketNotificationModel
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.
			Where("email_sent_timestamp IS NULL").
			Where("next_retry_at IS NULL OR next_retry_at <= ?", time.Now().UTC()).
			Order("created_at ASC, ticket_id ASC").
			Limit(limit).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Find(&rows).Error
	})
	if err != nil {
		return nil, err
	}
	notifications := make([]*domain.TicketNotification, len(rows))
	for i, m := range rows {
		notifications[i] = toDomainTicketNotification(m)
	}
	return notifications, nil
}

func (r *GORMTicketNotificationRepository) MarkSent(ctx context.Context, ticketID uuid.UUID, sentAt time.Time) error {
	return r.db.WithContext(ctx).Model(&TicketNotificationModel{}).
		Where("ticket_id = ?", ticketID).
		Updates(map[string]any{
			"email_sent_timestamp": sentAt,
			"email_sent_error":     "",
			"next_retry_at":        nil,
		}).Error
}

// MarkFailed bumps attempt_count and sets next_retry_at = now() +
// backoff(attempt_count) — exponential with full jitter — so
// FetchPendingForRetry's predicate makes the row wait out its backoff
// instead of being retried on the cron's very next run.
func (r *GORMTicketNotificationRepository) MarkFailed(ctx context.Context, ticketID uuid.UUID, lastError string) error {
	var m TicketNotificationModel
	if err := r.db.WithContext(ctx).Select("attempt_count").Where("ticket_id = ?", ticketID).First(&m).Error; err != nil {
		return err
	}
	nextRetryAt := time.Now().UTC().Add(domain.Backoff(m.AttemptCount+1, r.retryBackoffBase, r.retryBackoffCap))
	return r.db.WithContext(ctx).Model(&TicketNotificationModel{}).
		Where("ticket_id = ?", ticketID).
		Updates(map[string]any{
			"attempt_count":    gorm.Expr("attempt_count + 1"),
			"email_sent_error": lastError,
			"next_retry_at":    nextRetryAt,
		}).Error
}

func toDomainTicketNotification(m TicketNotificationModel) *domain.TicketNotification {
	return &domain.TicketNotification{
		TicketID:           m.TicketID,
		AttemptCount:       m.AttemptCount,
		EmailSentTimestamp: m.EmailSentTimestamp,
		EmailSentError:     m.EmailSentError,
		NextRetryAt:        m.NextRetryAt,
		CreatedAt:          m.CreatedAt,
	}
}

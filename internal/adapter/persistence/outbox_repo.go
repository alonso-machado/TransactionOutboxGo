package persistence

import (
	"context"
	"encoding/json"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMOutboxRepository struct {
	db               *gorm.DB
	retryBackoffBase time.Duration
	retryBackoffCap  time.Duration
}

// NewOutboxRepository wires the repository with the retry-backoff
// base/cap used by MarkRetrying (Phase 5 Track 2.A) — falls back to the
// config defaults (1s/5m) if zero values are passed so existing callers
// (and tests) that don't care about backoff still get sane behavior.
func NewOutboxRepository(db *gorm.DB, retryBackoffBase, retryBackoffCap time.Duration) *GORMOutboxRepository {
	if retryBackoffBase <= 0 {
		retryBackoffBase = time.Second
	}
	if retryBackoffCap <= 0 {
		retryBackoffCap = 5 * time.Minute
	}
	return &GORMOutboxRepository{db: db, retryBackoffBase: retryBackoffBase, retryBackoffCap: retryBackoffCap}
}

func (r *GORMOutboxRepository) Enqueue(ctx context.Context, uow domain.UnitOfWork, msg *domain.OutboxMessage) (bool, error) {
	headersJSON, _ := json.Marshal(msg.Headers)
	m := OutboxMessageModel{
		ID:             msg.ID,
		IdempotencyKey: msg.IdempotencyKey,
		AggregateType:  msg.AggregateType,
		HTTPMethod:     msg.HTTPMethod,
		Route:          msg.Route,
		Payload:        msg.Payload,
		Headers:        headersJSON,
		Status:         string(msg.Status),
		RetryCount:     msg.RetryCount,
		CreatedAt:      msg.CreatedAt,
		PaymentMethod:  msg.PaymentMethod,
	}
	db := TxFromContext(ctx, r.db)
	tx := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&m)
	if tx.Error != nil {
		return false, tx.Error
	}
	return tx.RowsAffected > 0, nil
}

func (r *GORMOutboxRepository) FetchPending(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	var models []OutboxMessageModel
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.
			Where("status IN ?", []string{string(domain.OutboxStatusNew), string(domain.OutboxStatusRetrying)}).
			Where("next_retry_at IS NULL OR next_retry_at <= ?", time.Now().UTC()).
			Order("created_at ASC, id ASC").
			Limit(limit).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Find(&models).Error
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]*domain.OutboxMessage, len(models))
	for i, m := range models {
		msgs[i] = toDomainOutbox(m)
	}
	return msgs, nil
}

func (r *GORMOutboxRepository) MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("id IN ?", ids).
		Updates(map[string]any{"status": string(domain.OutboxStatusPublished), "published_at": publishedAt}).Error
}

// MarkRetrying bumps retry_count and sets next_retry_at = now() +
// backoff(retry_count) — exponential with full jitter (Phase 5 Track 2.A) —
// so FetchPending's new predicate makes the row wait out its backoff
// instead of being re-fetched on the very next dispatch tick.
func (r *GORMOutboxRepository) MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error {
	var m OutboxMessageModel
	if err := r.db.WithContext(ctx).Select("retry_count").Where("id = ?", id).First(&m).Error; err != nil {
		return err
	}
	nextRetryAt := time.Now().UTC().Add(domain.Backoff(m.RetryCount+1, r.retryBackoffBase, r.retryBackoffCap))
	return r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":        string(domain.OutboxStatusRetrying),
			"retry_count":   gorm.Expr("retry_count + 1"),
			"last_error":    lastError,
			"next_retry_at": nextRetryAt,
		}).Error
}

func (r *GORMOutboxRepository) MarkDeadLetter(ctx context.Context, id uuid.UUID, lastError string) error {
	return r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("id = ?", id).
		Updates(map[string]any{"status": string(domain.OutboxStatusDeadLetter), "last_error": lastError}).Error
}

func (r *GORMOutboxRepository) DeleteOldPublished(ctx context.Context, olderThan time.Duration) error {
	cutoff := time.Now().UTC().Add(-olderThan)
	return r.db.WithContext(ctx).
		Where("status = ? AND published_at < ?", string(domain.OutboxStatusPublished), cutoff).
		Delete(&OutboxMessageModel{}).Error
}

// CountPending returns the true count of NEW/RETRYING rows — Phase 5 Track
// 2.B fix for the pending_count gauge being capped at batchSize.
func (r *GORMOutboxRepository) CountPending(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("status IN ?", []string{string(domain.OutboxStatusNew), string(domain.OutboxStatusRetrying)}).
		Count(&n).Error
	return n, err
}

// CountDeadLetter returns the count of DEAD_LETTER rows (Track 2.B).
func (r *GORMOutboxRepository) CountDeadLetter(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("status = ?", string(domain.OutboxStatusDeadLetter)).
		Count(&n).Error
	return n, err
}

// ReplayDeadLetters implements domain.DLQReplayer (Phase 5 Track 2.C):
// resets DEAD_LETTER rows back to NEW so the existing dispatch loop picks
// them up and republishes. method == "" replays across every method.
func (r *GORMOutboxRepository) ReplayDeadLetters(ctx context.Context, method string, limit int) (int64, error) {
	var ids []uuid.UUID
	q := r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("status = ?", string(domain.OutboxStatusDeadLetter))
	if method != "" {
		q = q.Where("payment_method = ?", method)
	}
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Order("created_at ASC").Pluck("id", &ids).Error; err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	res := r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("id IN ?", ids).
		Updates(map[string]any{
			"status":        string(domain.OutboxStatusNew),
			"retry_count":   0,
			"next_retry_at": nil,
			"last_error":    "",
		})
	return res.RowsAffected, res.Error
}

func toDomainOutbox(m OutboxMessageModel) *domain.OutboxMessage {
	var headers map[string]string
	_ = json.Unmarshal(m.Headers, &headers)
	return &domain.OutboxMessage{
		ID:             m.ID,
		IdempotencyKey: m.IdempotencyKey,
		AggregateType:  m.AggregateType,
		HTTPMethod:     m.HTTPMethod,
		Route:          m.Route,
		Payload:        m.Payload,
		Headers:        headers,
		Status:         domain.OutboxStatus(m.Status),
		RetryCount:     m.RetryCount,
		LastError:      m.LastError,
		CreatedAt:      m.CreatedAt,
		PublishedAt:    m.PublishedAt,
		PaymentMethod:  m.PaymentMethod,
		NextRetryAt:    m.NextRetryAt,
	}
}

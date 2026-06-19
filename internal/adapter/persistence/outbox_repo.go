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
	db *gorm.DB
}

func NewOutboxRepository(db *gorm.DB) *GORMOutboxRepository {
	return &GORMOutboxRepository{db: db}
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
			Order("created_at ASC").
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

func (r *GORMOutboxRepository) MarkPublished(ctx context.Context, id uuid.UUID, publishedAt time.Time) error {
	return r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("id = ?", id).
		Updates(map[string]any{"status": string(domain.OutboxStatusPublished), "published_at": publishedAt}).Error
}

func (r *GORMOutboxRepository) MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error {
	return r.db.WithContext(ctx).Model(&OutboxMessageModel{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":      string(domain.OutboxStatusRetrying),
			"retry_count": gorm.Expr("retry_count + 1"),
			"last_error":  lastError,
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
	}
}

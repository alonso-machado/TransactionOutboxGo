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

// GORMOutboxRepository implements domain.OutboxRepository against a single
// table, given at construction time — order_outbox and payment_event_outbox
// both use this same implementation (see cmd/outbox-worker/main.go), rather
// than two hand-duplicated repositories, since their schema and state
// machine are identical.
type GORMOutboxRepository struct {
	db               *gorm.DB
	table            string
	retryBackoffBase time.Duration
	retryBackoffCap  time.Duration
}

// NewOutboxRepository wires the repository to table (e.g. "order_outbox" or
// "payment_event_outbox") with the retry-backoff base/cap used by
// MarkRetrying — falls back to 1s/5m if zero values are passed so tests that
// don't care about backoff still get sane behavior.
func NewOutboxRepository(db *gorm.DB, table string, retryBackoffBase, retryBackoffCap time.Duration) *GORMOutboxRepository {
	if retryBackoffBase <= 0 {
		retryBackoffBase = time.Second
	}
	if retryBackoffCap <= 0 {
		retryBackoffCap = 5 * time.Minute
	}
	return &GORMOutboxRepository{db: db, table: table, retryBackoffBase: retryBackoffBase, retryBackoffCap: retryBackoffCap}
}

func (r *GORMOutboxRepository) Enqueue(ctx context.Context, uow domain.UnitOfWork, msg *domain.OutboxMessage) (bool, error) {
	headersJSON, _ := json.Marshal(msg.Headers)
	m := outboxRow{
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
		EventType:      msg.EventType,
		EventSubtype:   msg.EventSubtype,
	}
	db := TxFromContext(ctx, r.db)
	tx := db.Table(r.table).Clauses(clause.OnConflict{DoNothing: true}).Create(&m)
	if tx.Error != nil {
		return false, tx.Error
	}
	return tx.RowsAffected > 0, nil
}

func (r *GORMOutboxRepository) FetchPending(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	var rows []outboxRow
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Table(r.table).
			Where("status IN ?", []string{string(domain.OutboxStatusNew), string(domain.OutboxStatusRetrying)}).
			Where("next_retry_at IS NULL OR next_retry_at <= ?", time.Now().UTC()).
			Order("created_at ASC, id ASC").
			Limit(limit).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Find(&rows).Error
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]*domain.OutboxMessage, len(rows))
	for i, m := range rows {
		msgs[i] = toDomainOutbox(m)
	}
	return msgs, nil
}

func (r *GORMOutboxRepository) MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Table(r.table).
		Where("id IN ?", ids).
		Updates(map[string]any{"status": string(domain.OutboxStatusPublished), "published_at": publishedAt}).Error
}

// MarkRetrying bumps retry_count and sets next_retry_at = now() +
// backoff(retry_count) — exponential with full jitter — so FetchPending's
// predicate makes the row wait out its backoff instead of being re-fetched
// on the very next dispatch tick.
func (r *GORMOutboxRepository) MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error {
	var m outboxRow
	if err := r.db.WithContext(ctx).Table(r.table).Select("retry_count").Where("id = ?", id).First(&m).Error; err != nil {
		return err
	}
	nextRetryAt := time.Now().UTC().Add(domain.Backoff(m.RetryCount+1, r.retryBackoffBase, r.retryBackoffCap))
	return r.db.WithContext(ctx).Table(r.table).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":        string(domain.OutboxStatusRetrying),
			"retry_count":   gorm.Expr("retry_count + 1"),
			"last_error":    lastError,
			"next_retry_at": nextRetryAt,
		}).Error
}

func (r *GORMOutboxRepository) MarkDeadLetter(ctx context.Context, id uuid.UUID, lastError string) error {
	return r.db.WithContext(ctx).Table(r.table).
		Where("id = ?", id).
		Updates(map[string]any{"status": string(domain.OutboxStatusDeadLetter), "last_error": lastError}).Error
}

func (r *GORMOutboxRepository) DeleteOldPublished(ctx context.Context, olderThan time.Duration) error {
	cutoff := time.Now().UTC().Add(-olderThan)
	return r.db.WithContext(ctx).Table(r.table).
		Where("status = ? AND published_at < ?", string(domain.OutboxStatusPublished), cutoff).
		Delete(&outboxRow{}).Error
}

// CountPending returns the true count of NEW/RETRYING rows.
func (r *GORMOutboxRepository) CountPending(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Table(r.table).
		Where("status IN ?", []string{string(domain.OutboxStatusNew), string(domain.OutboxStatusRetrying)}).
		Count(&n).Error
	return n, err
}

// CountDeadLetter returns the count of DEAD_LETTER rows.
func (r *GORMOutboxRepository) CountDeadLetter(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Table(r.table).
		Where("status = ?", string(domain.OutboxStatusDeadLetter)).
		Count(&n).Error
	return n, err
}

// ReplayDeadLetters implements domain.DLQReplayer: resets DEAD_LETTER rows
// back to NEW so the existing dispatch loop picks them up and republishes.
// eventType == "" replays across every event type.
func (r *GORMOutboxRepository) ReplayDeadLetters(ctx context.Context, eventType string, limit int) (int64, error) {
	var ids []uuid.UUID
	q := r.db.WithContext(ctx).Table(r.table).
		Where("status = ?", string(domain.OutboxStatusDeadLetter))
	if eventType != "" {
		q = q.Where("event_type = ?", eventType)
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
	res := r.db.WithContext(ctx).Table(r.table).
		Where("id IN ?", ids).
		Updates(map[string]any{
			"status":        string(domain.OutboxStatusNew),
			"retry_count":   0,
			"next_retry_at": nil,
			"last_error":    "",
		})
	return res.RowsAffected, res.Error
}

func toDomainOutbox(m outboxRow) *domain.OutboxMessage {
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
		EventType:      m.EventType,
		EventSubtype:   m.EventSubtype,
		NextRetryAt:    m.NextRetryAt,
	}
}

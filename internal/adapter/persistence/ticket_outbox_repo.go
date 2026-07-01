package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMTicketOutboxRepository struct {
	db *gorm.DB
}

func NewTicketOutboxRepository(db *gorm.DB) *GORMTicketOutboxRepository {
	return &GORMTicketOutboxRepository{db: db}
}

// Enqueue inserts the ticket-outbox row, deduping on the UNIQUE
// idempotency_key via ON CONFLICT DO NOTHING — a redelivered order (same
// event_id) is a safe no-op and reports created=false. Same pattern as
// GORMOutboxRepository.Enqueue.
func (r *GORMTicketOutboxRepository) Enqueue(ctx context.Context, _ domain.UnitOfWork, msg *domain.TicketOutboxMessage) (bool, error) {
	m := TicketOutboxModel{
		ID:             msg.ID,
		IdempotencyKey: msg.IdempotencyKey,
		EventID:        msg.EventID,
		Payload:        msg.Payload,
		Status:         string(msg.Status),
		CreatedAt:      msg.CreatedAt,
		ProcessedAt:    msg.ProcessedAt,
	}
	db := TxFromContext(ctx, r.db)
	tx := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&m)
	if tx.Error != nil {
		return false, tx.Error
	}
	return tx.RowsAffected > 0, nil
}

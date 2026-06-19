package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"gorm.io/gorm"
)

type GORMInboxRepository struct {
	db *gorm.DB
}

func NewInboxRepository(db *gorm.DB) *GORMInboxRepository {
	return &GORMInboxRepository{db: db}
}

func (r *GORMInboxRepository) Exists(ctx context.Context, uow domain.UnitOfWork, messageID string) (bool, error) {
	db := TxFromContext(ctx, r.db)
	var count int64
	err := db.Model(&InboxMessageModel{}).Where("message_id = ?", messageID).Count(&count).Error
	return count > 0, err
}

func (r *GORMInboxRepository) Insert(ctx context.Context, uow domain.UnitOfWork, msg *domain.InboxMessage) error {
	db := TxFromContext(ctx, r.db)
	return db.Create(&InboxMessageModel{
		MessageID:   msg.MessageID,
		Status:      msg.Status,
		ProcessedAt: msg.ProcessedAt,
	}).Error
}

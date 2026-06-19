package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"gorm.io/gorm"
)

type GORMRecordRepository struct {
	db *gorm.DB
}

func NewRecordRepository(db *gorm.DB) *GORMRecordRepository {
	return &GORMRecordRepository{db: db}
}

func (r *GORMRecordRepository) Save(ctx context.Context, uow domain.UnitOfWork, rec *domain.Record) error {
	db := TxFromContext(ctx, r.db)
	return db.Create(&RecordModel{
		ID:              rec.ID,
		SourceMessageID: rec.SourceMessageID,
		Method:          rec.Method,
		Route:           rec.Route,
		Payload:         rec.Payload,
		CreatedAt:       rec.CreatedAt,
	}).Error
}

package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMPaymentRepository struct {
	db *gorm.DB
}

func NewPaymentRepository(db *gorm.DB) *GORMPaymentRepository {
	return &GORMPaymentRepository{db: db}
}

// Save is idempotent: ON CONFLICT (source_message_id) DO NOTHING means a
// redelivered RabbitMQ message is silently absorbed instead of erroring.
func (r *GORMPaymentRepository) Save(ctx context.Context, uow domain.UnitOfWork, p *domain.Payment) error {
	db := TxFromContext(ctx, r.db)
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_message_id"}},
		DoNothing: true,
	}).Create(&PaymentModel{
		ID:                p.ID,
		SourceMessageID:   p.SourceMessageID,
		EventID:           p.EventID,
		ProviderName:      p.ProviderName,
		ProviderPaymentID: p.ProviderPaymentID,
		ExternalPaymentID: p.ExternalPaymentID,
		PayerID:           p.PayerID,
		RecipientID:       p.RecipientID,
		Amount:            p.Amount,
		Currency:          p.Currency,
		Method:            p.Method,
		MethodDetails:     p.MethodDetails,
		OccurredAt:        p.OccurredAt,
		CreatedAt:         p.CreatedAt,
		UpdatedAt:         p.UpdatedAt,
	}).Error
}

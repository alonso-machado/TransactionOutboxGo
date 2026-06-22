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

// Save is idempotent: ON CONFLICT (source_message_id, occurred_at) DO
// NOTHING means a redelivered RabbitMQ message is silently absorbed instead
// of erroring. The two-column key (rather than source_message_id alone) is
// required by TimescaleDB — every UNIQUE index on a hypertable must include
// its partitioning column (occurred_at) — and is still redelivery-safe
// because occurred_at comes from the wire payload and is identical across
// redeliveries of the same message (see migrate.go's doc comment).
//
// The insert targets the per-method hypertable (tableFor(p.Method)), not
// the shared "payments" name — that's the read-side UNION ALL view; GORM
// can't route a single static TableName() per row, so the table is
// resolved explicitly here.
func (r *GORMPaymentRepository) Save(ctx context.Context, uow domain.UnitOfWork, p *domain.Payment) error {
	db := TxFromContext(ctx, r.db)
	return db.Table(tableFor(p.Method)).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_message_id"}, {Name: "occurred_at"}},
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

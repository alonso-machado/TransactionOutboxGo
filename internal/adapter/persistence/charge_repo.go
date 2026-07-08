package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type GORMChargeRepository struct {
	db *gorm.DB
}

func NewChargeRepository(db *gorm.DB) *GORMChargeRepository {
	return &GORMChargeRepository{db: db}
}

func (r *GORMChargeRepository) Save(ctx context.Context, uow domain.UnitOfWork, c *domain.Charge) error {
	db := TxFromContext(ctx, r.db)
	m := ChargeModel{
		ID:          c.ID,
		OrderID:     c.OrderID,
		Provider:    c.Provider,
		ProviderRef: c.ProviderRef,
		CheckoutURL: c.CheckoutURL,
		Amount:      c.Amount,
		Currency:    c.Currency,
		Status:      string(c.Status),
		CreatedAt:   c.CreatedAt,
		UpdatedAt:   c.UpdatedAt,
	}
	return db.Create(&m).Error
}

func (r *GORMChargeRepository) FindByProviderRef(ctx context.Context, providerRef string) (*domain.Charge, error) {
	var m ChargeModel
	if err := r.db.WithContext(ctx).Where("provider_ref = ?", providerRef).First(&m).Error; err != nil {
		return nil, err
	}
	return toDomainCharge(m), nil
}

func (r *GORMChargeRepository) FindByOrderID(ctx context.Context, orderID uuid.UUID) (*domain.Charge, error) {
	var m ChargeModel
	if err := r.db.WithContext(ctx).Where("order_id = ?", orderID).First(&m).Error; err != nil {
		return nil, err
	}
	return toDomainCharge(m), nil
}

func (r *GORMChargeRepository) UpdateStatus(ctx context.Context, uow domain.UnitOfWork, id uuid.UUID, status domain.ChargeStatus) error {
	db := TxFromContext(ctx, r.db)
	return db.Model(&ChargeModel{}).Where("id = ?", id).Update("status", string(status)).Error
}

func toDomainCharge(m ChargeModel) *domain.Charge {
	return &domain.Charge{
		ID:          m.ID,
		OrderID:     m.OrderID,
		Provider:    m.Provider,
		ProviderRef: m.ProviderRef,
		CheckoutURL: m.CheckoutURL,
		Amount:      m.Amount,
		Currency:    m.Currency,
		Status:      domain.ChargeStatus(m.Status),
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
	}
}

package persistence

import (
	"context"
	"encoding/json"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMOrderRepository struct {
	db *gorm.DB
}

func NewOrderRepository(db *gorm.DB) *GORMOrderRepository {
	return &GORMOrderRepository{db: db}
}

// Save is idempotent: ON CONFLICT (source_order_id) DO NOTHING means a
// redelivered order-intake message is silently absorbed instead of erroring
// — created=false tells ProcessOrder to skip re-charging it through the
// gateway.
func (r *GORMOrderRepository) Save(ctx context.Context, uow domain.UnitOfWork, o *domain.Order) (bool, error) {
	itemsJSON, err := json.Marshal(o.Items)
	if err != nil {
		return false, err
	}
	m := OrderModel{
		ID:               o.ID,
		SourceOrderID:    o.SourceOrderID,
		EventType:        o.EventType,
		EventSubtype:     o.EventSubtype,
		SourceEventID:    o.SourceEventID,
		SourceVenueID:    o.SourceVenueID,
		VenueName:        o.VenueName,
		VenueCity:        o.VenueCity,
		Items:            itemsJSON,
		CustomerName:     o.Customer.Name,
		CustomerEmail:    o.Customer.Email,
		CustomerDocument: o.Customer.Document,
		Amount:           o.Amount,
		Currency:         o.Currency,
		Status:           string(o.Status),
		CreatedAt:        o.CreatedAt,
		UpdatedAt:        o.UpdatedAt,
	}
	db := TxFromContext(ctx, r.db)
	tx := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&m)
	if tx.Error != nil {
		return false, tx.Error
	}
	return tx.RowsAffected > 0, nil
}

func (r *GORMOrderRepository) FindBySourceOrderID(ctx context.Context, sourceOrderID string) (*domain.Order, error) {
	var m OrderModel
	if err := r.db.WithContext(ctx).Where("source_order_id = ?", sourceOrderID).First(&m).Error; err != nil {
		return nil, err
	}
	return toDomainOrder(m)
}

func (r *GORMOrderRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	var m OrderModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error; err != nil {
		return nil, err
	}
	return toDomainOrder(m)
}

func (r *GORMOrderRepository) UpdateStatus(ctx context.Context, uow domain.UnitOfWork, id uuid.UUID, status domain.OrderStatus) error {
	db := TxFromContext(ctx, r.db)
	return db.Model(&OrderModel{}).Where("id = ?", id).Update("status", string(status)).Error
}

func toDomainOrder(m OrderModel) (*domain.Order, error) {
	var items []domain.OrderItem
	if err := json.Unmarshal(m.Items, &items); err != nil {
		return nil, err
	}
	return &domain.Order{
		ID:            m.ID,
		SourceOrderID: m.SourceOrderID,
		EventType:     m.EventType,
		EventSubtype:  m.EventSubtype,
		SourceEventID: m.SourceEventID,
		SourceVenueID: m.SourceVenueID,
		VenueName:     m.VenueName,
		VenueCity:     m.VenueCity,
		Items:         items,
		Customer: domain.Customer{
			Name:     m.CustomerName,
			Email:    m.CustomerEmail,
			Document: m.CustomerDocument,
		},
		Amount:    m.Amount,
		Currency:  m.Currency,
		Status:    domain.OrderStatus(m.Status),
		CreatedAt: m.CreatedAt,
		UpdatedAt: m.UpdatedAt,
	}, nil
}

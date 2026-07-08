package persistence

import (
	"context"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMTicketRepository struct {
	db *gorm.DB
}

func NewTicketRepository(db *gorm.DB) *GORMTicketRepository {
	return &GORMTicketRepository{db: db}
}

// ReserveForOrder inserts one RESERVED row per ticket, deduping on the
// UNIQUE source_ticket_id via ON CONFLICT DO NOTHING — a redelivered
// order-intake message re-reserving the same tickets is a safe no-op.
func (r *GORMTicketRepository) ReserveForOrder(ctx context.Context, uow domain.UnitOfWork, tickets []*domain.Ticket) error {
	if len(tickets) == 0 {
		return nil
	}
	models := make([]TicketModel, len(tickets))
	for i, t := range tickets {
		models[i] = toTicketModel(t)
	}
	db := TxFromContext(ctx, r.db)
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&models).Error
}

func (r *GORMTicketRepository) FindByOrderID(ctx context.Context, orderID uuid.UUID) ([]*domain.Ticket, error) {
	var models []TicketModel
	if err := r.db.WithContext(ctx).Where("order_id = ?", orderID).Order("created_at ASC").Find(&models).Error; err != nil {
		return nil, err
	}
	tickets := make([]*domain.Ticket, len(models))
	for i, m := range models {
		tickets[i] = toDomainTicket(m)
	}
	return tickets, nil
}

func (r *GORMTicketRepository) MarkIssued(ctx context.Context, uow domain.UnitOfWork, t *domain.Ticket) error {
	db := TxFromContext(ctx, r.db)
	return db.Model(&TicketModel{}).Where("id = ?", t.ID).Updates(map[string]any{
		"status":          string(domain.TicketStatusValid),
		"qr_png":          t.QRPNG,
		"qr_content":      t.QRContent,
		"validation_code": t.ValidationCode,
		"signature":       t.Signature,
	}).Error
}

func (r *GORMTicketRepository) MarkVoid(ctx context.Context, uow domain.UnitOfWork, orderID uuid.UUID) error {
	db := TxFromContext(ctx, r.db)
	return db.Model(&TicketModel{}).
		Where("order_id = ? AND status = ?", orderID, string(domain.TicketStatusReserved)).
		Update("status", string(domain.TicketStatusVoid)).Error
}

func (r *GORMTicketRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.Ticket, error) {
	var m TicketModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error; err != nil {
		return nil, err
	}
	return toDomainTicket(m), nil
}

// CheckIn's WHERE clause only matches a currently-VALID row, so
// RowsAffected == 0 means the ticket was not VALID at the moment of the
// UPDATE — either already CHECKED_IN or never issued (RESERVED/VOID). The
// caller (usecase/checkin) already holds the ticket from FindByID and uses
// its Status to tell those two cases apart.
func (r *GORMTicketRepository) CheckIn(ctx context.Context, uow domain.UnitOfWork, id uuid.UUID, checkedInAt time.Time) (bool, error) {
	db := TxFromContext(ctx, r.db)
	tx := db.Model(&TicketModel{}).
		Where("id = ? AND status = ?", id, string(domain.TicketStatusValid)).
		Updates(map[string]any{
			"status":        string(domain.TicketStatusCheckedIn),
			"checked_in_at": checkedInAt,
		})
	if tx.Error != nil {
		return false, tx.Error
	}
	return tx.RowsAffected == 0, nil
}

func (r *GORMTicketRepository) UpdateHolderName(ctx context.Context, uow domain.UnitOfWork, id uuid.UUID, name string) error {
	db := TxFromContext(ctx, r.db)
	return db.Model(&TicketModel{}).Where("id = ?", id).Update("buyer_name", name).Error
}

func toTicketModel(t *domain.Ticket) TicketModel {
	return TicketModel{
		ID:             t.ID,
		OrderID:        t.OrderID,
		EventID:        t.EventID,
		SourceTicketID: t.SourceTicketID,
		Section:        t.Section,
		Row:            t.Row,
		Seat:           t.Seat,
		Price:          t.Price,
		Currency:       t.Currency,
		BuyerName:      t.BuyerName,
		BuyerEmail:     t.BuyerEmail,
		QRPNG:          t.QRPNG,
		QRContent:      t.QRContent,
		ValidationCode: t.ValidationCode,
		Signature:      t.Signature,
		Status:         string(t.Status),
		CreatedAt:      t.CreatedAt,
	}
}

func toDomainTicket(m TicketModel) *domain.Ticket {
	return &domain.Ticket{
		ID:             m.ID,
		OrderID:        m.OrderID,
		EventID:        m.EventID,
		SourceTicketID: m.SourceTicketID,
		Section:        m.Section,
		Row:            m.Row,
		Seat:           m.Seat,
		Price:          m.Price,
		Currency:       m.Currency,
		BuyerName:      m.BuyerName,
		BuyerEmail:     m.BuyerEmail,
		QRPNG:          m.QRPNG,
		QRContent:      m.QRContent,
		ValidationCode: m.ValidationCode,
		Signature:      m.Signature,
		Status:         domain.TicketStatus(m.Status),
		CreatedAt:      m.CreatedAt,
		CheckedInAt:    m.CheckedInAt,
	}
}

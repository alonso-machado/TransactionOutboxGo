package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMEventRepository struct {
	db *gorm.DB
}

func NewEventRepository(db *gorm.DB) *GORMEventRepository {
	return &GORMEventRepository{db: db}
}

// UpsertBySourceEventID mirrors GORMLocationRepository.UpsertBySourceVenueID
// — see its comment for why the RETURNING id is safe to trust on conflict.
func (r *GORMEventRepository) UpsertBySourceEventID(ctx context.Context, uow domain.UnitOfWork, e *domain.Event) (uuid.UUID, error) {
	db := TxFromContext(ctx, r.db)
	m := EventModel{
		ID:            e.ID,
		EventType:     e.EventType,
		EventSubtype:  e.EventSubtype,
		Name:          e.Name,
		LocationID:    e.LocationID,
		SourceEventID: e.SourceEventID,
		CreatedAt:     e.CreatedAt,
	}
	tx := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_event_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"name", "location_id"}),
	}).Create(&m)
	if tx.Error != nil {
		return uuid.Nil, tx.Error
	}
	return m.ID, nil
}

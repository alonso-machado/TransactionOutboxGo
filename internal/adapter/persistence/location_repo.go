package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GORMLocationRepository struct {
	db *gorm.DB
}

func NewLocationRepository(db *gorm.DB) *GORMLocationRepository {
	return &GORMLocationRepository{db: db}
}

// UpsertBySourceVenueID relies on Postgres's INSERT ... ON CONFLICT DO
// UPDATE ... RETURNING id: on a fresh venue the candidate loc.ID is kept; on
// a conflict, Postgres updates (and RETURNING reports) the existing row in
// place, so m.ID ends up holding the real, already-known Location ID either
// way — no separate find-then-create round trip.
func (r *GORMLocationRepository) UpsertBySourceVenueID(ctx context.Context, uow domain.UnitOfWork, loc *domain.Location) (uuid.UUID, error) {
	db := TxFromContext(ctx, r.db)
	m := LocationModel{
		ID:            loc.ID,
		Name:          loc.Name,
		City:          loc.City,
		SourceVenueID: loc.SourceVenueID,
		CreatedAt:     loc.CreatedAt,
	}
	tx := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "source_venue_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"name", "city"}),
	}).Create(&m)
	if tx.Error != nil {
		return uuid.Nil, tx.Error
	}
	return m.ID, nil
}

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
//
// The explicit clause.Returning is required, not optional: GORM only adds
// RETURNING for the primary key on its own when the field is left at its
// zero value for Create to fill in. Since loc.ID is always client-supplied
// (a fresh candidate UUID minted by the caller on every call, conflict or
// not), GORM has no reason to ask the DB for it back without this — so on a
// conflict, m.ID would silently keep the phantom candidate value instead of
// the real existing row's id, and the caller's next insert (Event.LocationID)
// would fail its foreign key check against a location that was never
// actually written.
func (r *GORMLocationRepository) UpsertBySourceVenueID(ctx context.Context, uow domain.UnitOfWork, loc *domain.Location) (uuid.UUID, error) {
	db := TxFromContext(ctx, r.db)
	m := LocationModel{
		ID:            loc.ID,
		Name:          loc.Name,
		City:          loc.City,
		SourceVenueID: loc.SourceVenueID,
		CreatedAt:     loc.CreatedAt,
	}
	tx := db.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "source_venue_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"name", "city"}),
		},
		clause.Returning{Columns: []clause.Column{{Name: "id"}}},
	).Create(&m)
	if tx.Error != nil {
		return uuid.Nil, tx.Error
	}
	return m.ID, nil
}

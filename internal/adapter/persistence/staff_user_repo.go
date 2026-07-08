package persistence

import (
	"context"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"gorm.io/gorm"
)

type GORMStaffUserRepository struct {
	db *gorm.DB
}

func NewStaffUserRepository(db *gorm.DB) *GORMStaffUserRepository {
	return &GORMStaffUserRepository{db: db}
}

func (r *GORMStaffUserRepository) FindByClerkUserID(ctx context.Context, clerkUserID string) (*domain.StaffUser, error) {
	var m StaffUserModel
	if err := r.db.WithContext(ctx).Where("clerk_user_id = ?", clerkUserID).First(&m).Error; err != nil {
		return nil, err
	}
	return &domain.StaffUser{
		ID:          m.ID,
		ClerkUserID: m.ClerkUserID,
		Name:        m.Name,
		Role:        m.Role,
		LocationID:  m.LocationID,
		CreatedAt:   m.CreatedAt,
	}, nil
}

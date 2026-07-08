package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// StaffUser is a venue staff member allowed to check tickets in at the
// door — a local record keyed on a Clerk-authenticated identity
// (StaffAuthenticator verifies the Bearer token; StaffUserRepository maps
// the resulting Clerk user ID to venue-scoping/role data Clerk itself
// doesn't know about).
type StaffUser struct {
	ID          uuid.UUID
	ClerkUserID string
	Name        string
	Role        string
	// LocationID nil means unscoped (can check in at any venue — an
	// admin/floater role); non-nil restricts check-in to that one venue.
	LocationID *uuid.UUID
	CreatedAt  time.Time
}

// StaffUserRepository is the port for the staff_users table (events DB).
type StaffUserRepository interface {
	// FindByClerkUserID looks up the staff record for an authenticated
	// Clerk identity. A not-found result means the token holder is a real
	// Clerk user but not a registered staff member.
	FindByClerkUserID(ctx context.Context, clerkUserID string) (*StaffUser, error)
}

// Package staffauth is the Gin middleware guarding tickets-api's
// POST /api/v1/checkin: extracts a Bearer token, verifies it via
// domain.StaffAuthenticator (Clerk or the fake test double), then resolves
// the authenticated identity to a domain.StaffUser via
// domain.StaffUserRepository. Only check-in requires this — ticket-holder
// update stays rate-limit-only, no auth (confirmed with the user).
package staffauth

import (
	"net/http"
	"strings"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/gin-gonic/gin"
)

// staffUserContextKey is unexported so only this package's
// Middleware/StaffUserFromContext pair can set/read it.
const staffUserContextKey = "staffUser"

// Middleware authenticates a Bearer token and looks up the registered
// staff member it belongs to, aborting with 401 (missing/malformed/invalid
// token) or 403 (a real, Clerk-verified identity that isn't a registered
// staff member) before the handler ever runs.
func Middleware(authenticator domain.StaffAuthenticator, staffRepo domain.StaffUserRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, ok := parseBearer(c.GetHeader("Authorization"))
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or malformed Authorization header"})
			return
		}

		clerkUserID, err := authenticator.VerifyToken(token)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		staff, err := staffRepo.FindByClerkUserID(c.Request.Context(), clerkUserID)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "not a registered staff member"})
			return
		}

		c.Set(staffUserContextKey, staff)
		c.Next()
	}
}

// StaffUserFromContext retrieves the *domain.StaffUser Middleware resolved
// for this request. Returns nil if called on a route without Middleware.
func StaffUserFromContext(c *gin.Context) *domain.StaffUser {
	v, ok := c.Get(staffUserContextKey)
	if !ok {
		return nil
	}
	staff, _ := v.(*domain.StaffUser)
	return staff
}

func parseBearer(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if token == "" {
		return "", false
	}
	return token, true
}

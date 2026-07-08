//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func postCheckin(t *testing.T, bearerToken, ticketID, validationCode, signature string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	router := newTicketsRouter()
	body := fmt.Sprintf(`{"ticketId":%q,"validationCode":%q,"signature":%q}`, ticketID, validationCode, signature)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/checkin", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	}
	return rec, resp
}

// TestCheckin_MissingToken_401s — check-in requires staff auth; no bearer
// token at all is rejected before the use-case ever runs.
func TestCheckin_MissingToken_401s(t *testing.T) {
	truncateAll(t)
	rec, _ := postCheckin(t, "", "00000000-0000-0000-0000-000000000000", "x", "y")
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestCheckin_UnregisteredStaffToken_403s — a Clerk-valid (well, fake-valid)
// token that has no staff_users row is a real identity, just not staff.
func TestCheckin_UnregisteredStaffToken_403s(t *testing.T) {
	truncateAll(t)
	rec, _ := postCheckin(t, fakeStaffToken, "00000000-0000-0000-0000-000000000000", "x", "y")
	require.Equal(t, http.StatusForbidden, rec.Code)
}

// TestCheckin_FullFlow issues a real ticket end-to-end, then exercises
// every check-in outcome against it: success, idempotent repeat, tampered
// signature, and a wrong-venue staff scope.
func TestCheckin_FullFlow(t *testing.T) {
	truncateAll(t)
	seedStaffUser(t, fakeClerkUserID, nil) // nil location_id = unscoped, any venue

	_, ticket := issueOneTicket(t, "order-checkin-1", "evt-checkin-1", "TKT-checkin-1")

	// Success.
	rec, resp := postCheckin(t, fakeStaffToken, ticket.ID.String(), ticket.ValidationCode, ticket.Signature)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "CHECKED_IN", resp["outcome"])
	ticketBody := resp["ticket"].(map[string]any)
	require.Equal(t, "Jane Doe", ticketBody["buyerName"])
	require.Equal(t, "A", ticketBody["section"])

	// Idempotent repeat.
	rec, resp = postCheckin(t, fakeStaffToken, ticket.ID.String(), ticket.ValidationCode, ticket.Signature)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ALREADY_CHECKED_IN", resp["outcome"])

	// Tampered signature against a second, still-VALID ticket.
	_, ticket2 := issueOneTicket(t, "order-checkin-2", "evt-checkin-2", "TKT-checkin-2")
	rec, resp = postCheckin(t, fakeStaffToken, ticket2.ID.String(), ticket2.ValidationCode, "not-the-real-signature")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "INVALID_SIGNATURE", resp["outcome"])
}

// TestCheckin_WrongVenue_403s — a staff member scoped to a different
// location than the ticket's event is rejected, even with a valid
// signature.
func TestCheckin_WrongVenue_403s(t *testing.T) {
	truncateAll(t)

	_, ticket := issueOneTicket(t, "order-checkin-venue-1", "evt-checkin-venue-1", "TKT-checkin-venue-1")

	var event struct {
		LocationID uuid.UUID `gorm:"column:location_id"`
	}
	require.NoError(t, suite.eventsDB.Table("events").Where("id = ?", ticket.EventID).First(&event).Error)

	// A location that is NOT the ticket's own event location.
	otherLocationID := seedLocation(t, "Other Venue")
	require.NotEqual(t, event.LocationID, otherLocationID)
	seedStaffUser(t, fakeClerkUserID, &otherLocationID)

	rec, resp := postCheckin(t, fakeStaffToken, ticket.ID.String(), ticket.ValidationCode, ticket.Signature)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Equal(t, "WRONG_VENUE", resp["outcome"])
}

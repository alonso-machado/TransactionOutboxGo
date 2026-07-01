//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/stretchr/testify/require"
)

// ticketBody builds a valid ticket-order payload mirroring the POST
// /api/v1/ticket wire format. eventID drives dedup.
func ticketBody(eventID string) string {
	return fmt.Sprintf(`{
		"order": {
			"event_id": "%s",
			"venue": {"id": "V99", "name": "Allianz Parque", "city": "São Paulo"},
			"tickets": [
				{"id": "TKT-909283", "section": "Premium Pista", "row": "AA", "seat": "12", "price": 350.00, "currency": "BRL"},
				{"id": "TKT-909284", "section": "Premium Pista", "row": "AA", "seat": "13", "price": 350.00, "currency": "BRL"}
			],
			"payment_details": {"method": "PIX", "total_amount": 700.00, "status": "pending"},
			"customer": {"id": "CUST-10492", "name": "Maria Silva", "email": "maria.silva@example.com"}
		}
	}`, eventID)
}

func postTicket(t *testing.T, body string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	router := newRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ticket", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	}
	return rec, resp
}

// A single POST creates exactly one NEW ticket_outbox row carrying the raw
// order payload.
func TestTicket_HappyPath_CreatesTicketOutboxRow(t *testing.T) {
	truncateAll(t)

	rec, resp := postTicket(t, ticketBody("evt-ticket-1"), nil)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])
	require.NotEmpty(t, resp["ticketId"])
	require.NotEmpty(t, resp["idempotencyKey"])

	require.Equal(t, int64(1), countTicketOutbox())

	var row persistence.TicketOutboxModel
	require.NoError(t, suite.db.First(&row).Error)
	require.Equal(t, "evt-ticket-1", row.EventID)
	require.Equal(t, "NEW", row.Status)
	// The full order object is stored opaquely, including nested fields.
	require.Contains(t, string(row.Payload), "Allianz Parque")
	require.Contains(t, string(row.Payload), "TKT-909283")
}

// Same event_id sent twice -> the second is a duplicate, still one row.
func TestTicket_DuplicateEventID_NoNewRow(t *testing.T) {
	truncateAll(t)

	rec1, resp1 := postTicket(t, ticketBody("evt-ticket-dup"), nil)
	require.Equal(t, http.StatusCreated, rec1.Code)
	require.Equal(t, "accepted", resp1["status"])

	rec2, resp2 := postTicket(t, ticketBody("evt-ticket-dup"), nil)
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "duplicate", resp2["status"])
	require.Equal(t, resp1["idempotencyKey"], resp2["idempotencyKey"])

	require.Equal(t, int64(1), countTicketOutbox())
}

// An order missing its event_id is rejected with 400 and creates no row.
func TestTicket_MissingEventID_Rejected(t *testing.T) {
	truncateAll(t)

	body := `{"order": {"tickets": [{"id": "TKT-1"}], "payment_details": {"method": "PIX"}}}`
	rec, _ := postTicket(t, body, nil)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countTicketOutbox())
}

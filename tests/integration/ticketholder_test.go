//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func patchTicketHolder(t *testing.T, router *gin.Engine, ticketID, name string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q}`, name)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/tickets/"+ticketID+"/holder", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestTicketHolder_HappyPath_UpdatesName corrects a VALID ticket's buyer
// name — no staff auth required, unlike check-in.
func TestTicketHolder_HappyPath_UpdatesName(t *testing.T) {
	truncateAll(t)
	_, ticket := issueOneTicket(t, "order-holder-1", "evt-holder-1", "TKT-holder-1")

	router := newTicketsRouter()
	rec := patchTicketHolder(t, router, ticket.ID.String(), "Jane Smith")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "Jane Smith", resp["buyerName"])

	var updated struct {
		BuyerName string `gorm:"column:buyer_name"`
	}
	require.NoError(t, suite.eventsDB.Table("tickets").Where("id = ?", ticket.ID).First(&updated).Error)
	require.Equal(t, "Jane Smith", updated.BuyerName)
}

// TestTicketHolder_ReservedTicket_409s rejects an edit on a ticket that was
// reserved but never issued (payment never confirmed) — nothing to
// correct yet.
func TestTicketHolder_ReservedTicket_409s(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-holder-reserved-1", "evt-holder-reserved-1", "TKT-holder-reserved-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	orderID := resp["orderId"].(string)

	orderDispatcher, _ := newOrderDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go orderDispatcher.Run(dispatchCtx, nil)

	checkoutConsumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	checkoutCtx, cancelCheckout := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCheckout()
	go func() { _ = checkoutConsumer.Run(checkoutCtx) }()

	ok := waitFor(t, 9*time.Second, func() bool {
		return countTicketsByStatus("RESERVED") == 1
	})
	require.True(t, ok, "expected the ticket to be reserved")

	var reserved struct {
		ID string `gorm:"column:id"`
	}
	require.NoError(t, suite.eventsDB.Table("tickets").Select("id").Where("order_id = ?", orderID).First(&reserved).Error)

	router := newTicketsRouter()
	patchRec := patchTicketHolder(t, router, reserved.ID, "Someone Else")
	require.Equal(t, http.StatusConflict, patchRec.Code)
}

// TestTicketHolder_UnknownTicket_404s reports a genuinely unknown ticketId
// as 404.
func TestTicketHolder_UnknownTicket_404s(t *testing.T) {
	truncateAll(t)
	router := newTicketsRouter()
	rec := patchTicketHolder(t, router, "00000000-0000-0000-0000-000000000000", "Someone")
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// TestTicketHolder_RateLimit_429s hammers the endpoint past its configured
// burst from one source and expects a 429 with Retry-After, reusing
// ratelimit.Middleware exactly as tickets-api does.
func TestTicketHolder_RateLimit_429s(t *testing.T) {
	truncateAll(t)
	_, ticket := issueOneTicket(t, "order-holder-rl-1", "evt-holder-rl-1", "TKT-holder-rl-1")

	store := ratelimit.NewInMemoryStore(time.Minute)
	router := newTicketsRouterRateLimited(store, 1, 1) // 1 req/s, burst 1

	first := patchTicketHolder(t, router, ticket.ID.String(), "First Name")
	require.Equal(t, http.StatusOK, first.Code)

	second := patchTicketHolder(t, router, ticket.ID.String(), "Second Name")
	require.Equal(t, http.StatusTooManyRequests, second.Code)
	require.NotEmpty(t, second.Header().Get("Retry-After"))
}

//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getOrderStatus(t *testing.T, orderID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	router := newTicketsRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/orders/"+orderID, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	}
	return rec, resp
}

// TestOrderStatus_BeforeAndAfterCheckout polls GET /api/v1/orders/{id}
// (tickets-api's endpoint) before and after order-consumer-worker opens a
// checkout for the order. Note the real (not originally assumed) timing:
// POST /orders only ever creates an order_outbox row (the outbox pattern's
// whole point) — the "orders" table row itself doesn't exist until
// order-consumer-worker's checkout.ProcessOrder processes that message, and
// it creates the Order row and the Charge/checkout URL in the SAME
// transaction (see internal/usecase/checkout/process_order.go). So there
// is no observable PENDING-with-no-charge-yet window: GET /orders/{id}
// legitimately 404s (not "200 with an empty checkoutUrl") for the brief
// period right after 201, before order-consumer-worker has run at all — a
// real client's polling loop must treat an early 404 on its own
// just-created orderId as "keep polling", not a hard failure.
func TestOrderStatus_BeforeAndAfterCheckout(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-status-1", "evt-status-1", "TKT-status-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	orderID := resp["orderId"].(string)

	// Before order-consumer-worker runs: no "orders" row exists yet at all.
	statusRec, _ := getOrderStatus(t, orderID)
	require.Equal(t, http.StatusNotFound, statusRec.Code)

	// Now let order-consumer-worker open a checkout.
	orderDispatcher, _ := newOrderDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go orderDispatcher.Run(dispatchCtx, nil)

	checkoutConsumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	checkoutCtx, cancelCheckout := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCheckout()
	go func() { _ = checkoutConsumer.Run(checkoutCtx) }()

	ok := waitFor(t, 9*time.Second, func() bool {
		afterRec, afterResp := getOrderStatus(t, orderID)
		return afterRec.Code == http.StatusOK && afterResp["checkoutUrl"] != ""
	})
	require.True(t, ok, "expected checkoutUrl to become non-empty once order-consumer-worker opens a checkout")

	finalRec, finalResp := getOrderStatus(t, orderID)
	require.Equal(t, http.StatusOK, finalRec.Code)
	require.NotEmpty(t, finalResp["checkoutUrl"])
	require.Equal(t, "RESERVED", finalResp["status"])
}

// TestOrderStatus_UnknownOrder_404s reports a genuinely unknown orderId as
// 404 — the one case that IS an error, unlike a missing Charge.
func TestOrderStatus_UnknownOrder_404s(t *testing.T) {
	truncateAll(t)
	rec, _ := getOrderStatus(t, "00000000-0000-0000-0000-000000000000")
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// TestOrderStatus_InvalidID_400s rejects a non-UUID path param.
func TestOrderStatus_InvalidID_400s(t *testing.T) {
	truncateAll(t)
	rec, _ := getOrderStatus(t, "not-a-uuid")
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// issueOneTicket drives a full order through checkout + fulfillment (the
// same pipeline fulfillment_test.go's end-to-end test exercises) and
// returns the resulting orderID + the one issued (VALID) ticket — shared
// setup for Phase 8's check-in/ticket-holder/notification tests, all of
// which need an already-issued ticket to act on rather than re-testing the
// order->fulfillment pipeline itself.
func issueOneTicket(t *testing.T, sourceOrderID, eventID, ticketID string) (uuid.UUID, persistence.TicketModel) {
	t.Helper()

	body, headers := orderBody(sourceOrderID, eventID, ticketID, "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	orderID, err := uuid.Parse(resp["orderId"].(string))
	require.NoError(t, err)

	orderDispatcher, _ := newOrderDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	t.Cleanup(cancelDispatch)
	go orderDispatcher.Run(dispatchCtx, nil)

	checkoutConsumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	checkoutCtx, cancelCheckout := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancelCheckout)
	go func() { _ = checkoutConsumer.Run(checkoutCtx) }()

	var charge persistence.ChargeModel
	ok := waitFor(t, 9*time.Second, func() bool {
		return suite.eventsDB.Where("order_id = ?", orderID).First(&charge).Error == nil
	})
	require.True(t, ok, "expected a Charge row for the order")

	webhookRec := postWebhook(t, testProvider, fakeWebhookBody(charge.ProviderRef, "webhook-"+sourceOrderID, "CONFIRMED"))
	require.Equal(t, http.StatusOK, webhookRec.Code)

	paymentDispatcher, _ := newPaymentEventDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	paymentDispatchCtx, cancelPaymentDispatch := context.WithCancel(context.Background())
	t.Cleanup(cancelPaymentDispatch)
	go paymentDispatcher.Run(paymentDispatchCtx, nil)

	fulfillmentConsumer := newFulfillmentConsumer(testEventType, testEventSubtype, 10, 5)
	fulfillmentCtx, cancelFulfillment := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancelFulfillment)
	go func() { _ = fulfillmentConsumer.Run(fulfillmentCtx) }()

	ok = waitFor(t, 9*time.Second, func() bool {
		return countTicketsByStatus("VALID") == 1
	})
	require.True(t, ok, "expected the ticket to be issued (VALID)")

	var issuedTicket persistence.TicketModel
	require.NoError(t, suite.eventsDB.Where("order_id = ?", orderID).First(&issuedTicket).Error)
	return orderID, issuedTicket
}

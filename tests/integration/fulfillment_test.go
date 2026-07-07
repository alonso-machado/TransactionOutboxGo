//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/ticketqr"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// paymentEventOutboxPayload builds a JSON body matching
// usecase/fulfillment.payloadDTO's wire shape.
func paymentEventOutboxPayload(providerRef, outcome string) []byte {
	b, _ := json.Marshal(map[string]any{
		"schemaVersion": "1",
		"provider":      testProvider,
		"providerRef":   providerRef,
		"outcome":       outcome,
	})
	return b
}

func publishPaymentEventRaw(t *testing.T, messageID string, body []byte) {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	routingKey := rmq.RoutingKeyFor(rmq.PaymentEventStream, testEventType, testEventSubtype)
	err = ch.PublishWithContext(context.Background(), rmq.Exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "application/json",
		MessageId:   messageID,
		Body:        body,
	})
	require.NoError(t, err)
	select {
	case c := <-confirms:
		require.True(t, c.Ack, "broker nacked test publish")
	case <-time.After(5 * time.Second):
		t.Fatal("publish confirm timeout")
	}
}

// End-to-end: place an order over HTTP, relay + process it into a reserved
// Charge/Tickets, confirm payment via the fake gateway's webhook, relay +
// process THAT into issued Tickets with a verifiable QR/HMAC signature.
func TestFulfillment_EndToEnd_OrderToIssuedTickets(t *testing.T) {
	truncateAll(t)

	// 1. Place the order.
	body, headers := orderBody("order-e2e-1", "evt-e2e-1", "TKT-e2e-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	orderID, err := uuid.Parse(resp["orderId"].(string))
	require.NoError(t, err)

	// 2. Relay order_outbox -> order queue, and process it into a reserved
	// Charge (order-consumer-worker's job).
	orderDispatcher, _ := newOrderDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go orderDispatcher.Run(dispatchCtx, nil)

	checkoutConsumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	checkoutCtx, cancelCheckout := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCheckout()
	go func() { _ = checkoutConsumer.Run(checkoutCtx) }()

	var charge persistence.ChargeModel
	ok := waitFor(t, 9*time.Second, func() bool {
		return suite.eventsDB.Where("order_id = ?", orderID).First(&charge).Error == nil
	})
	require.True(t, ok, "expected a Charge row for the order")
	require.NotEmpty(t, charge.ProviderRef)

	var reservedTickets []persistence.TicketModel
	require.NoError(t, suite.eventsDB.Where("order_id = ?", orderID).Find(&reservedTickets).Error)
	require.Len(t, reservedTickets, 1)
	require.Equal(t, "RESERVED", reservedTickets[0].Status)

	// 3. The customer pays; the fake gateway's webhook confirms it.
	webhookRec := postWebhook(t, testProvider, fakeWebhookBody(charge.ProviderRef, "webhook-e2e-1", "CONFIRMED"))
	require.Equal(t, http.StatusOK, webhookRec.Code)

	// 4. Relay payment_event_outbox -> payment queue, and process it into
	// issued Tickets (fulfillment-consumer-worker's job).
	paymentDispatcher, _ := newPaymentEventDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	paymentDispatchCtx, cancelPaymentDispatch := context.WithCancel(context.Background())
	defer cancelPaymentDispatch()
	go paymentDispatcher.Run(paymentDispatchCtx, nil)

	fulfillmentConsumer := newFulfillmentConsumer(testEventType, testEventSubtype, 10, 5)
	fulfillmentCtx, cancelFulfillment := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFulfillment()
	go func() { _ = fulfillmentConsumer.Run(fulfillmentCtx) }()

	ok = waitFor(t, 9*time.Second, func() bool {
		return countTicketsByStatus("VALID") == 1
	})
	require.True(t, ok, "expected the ticket to be issued (VALID)")

	var issuedTicket persistence.TicketModel
	require.NoError(t, suite.eventsDB.Where("order_id = ?", orderID).First(&issuedTicket).Error)
	require.Equal(t, "VALID", issuedTicket.Status)
	require.NotEmpty(t, issuedTicket.QRPNG)
	require.NotEmpty(t, issuedTicket.QRContent)
	require.NotEmpty(t, issuedTicket.ValidationCode)
	require.NotEmpty(t, issuedTicket.Signature)
	require.True(t, ticketqr.Verify(issuedTicket.ID.String(), issuedTicket.ValidationCode, issuedTicket.Signature, ticketSigningSecret),
		"the persisted signature must verify against the ticket's own id + validation code")

	var chargeAfter persistence.ChargeModel
	require.NoError(t, suite.eventsDB.First(&chargeAfter, "id = ?", charge.ID).Error)
	require.Equal(t, "PAID", chargeAfter.Status)

	var orderAfter persistence.OrderModel
	require.NoError(t, suite.eventsDB.First(&orderAfter, "id = ?", orderID).Error)
	require.Equal(t, "PAID", orderAfter.Status)
}

// A FAILED payment confirmation voids the reservation instead of issuing
// tickets.
func TestFulfillment_PaymentFailed_VoidsTickets(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-failed-1", "evt-failed-1", "TKT-failed-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	orderID, err := uuid.Parse(resp["orderId"].(string))
	require.NoError(t, err)

	orderDispatcher, _ := newOrderDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go orderDispatcher.Run(dispatchCtx, nil)

	checkoutConsumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	checkoutCtx, cancelCheckout := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCheckout()
	go func() { _ = checkoutConsumer.Run(checkoutCtx) }()

	var charge persistence.ChargeModel
	ok := waitFor(t, 9*time.Second, func() bool {
		return suite.eventsDB.Where("order_id = ?", orderID).First(&charge).Error == nil
	})
	require.True(t, ok, "expected a Charge row for the order")

	webhookRec := postWebhook(t, testProvider, fakeWebhookBody(charge.ProviderRef, "webhook-failed-1", "FAILED"))
	require.Equal(t, http.StatusOK, webhookRec.Code)

	paymentDispatcher, _ := newPaymentEventDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	paymentDispatchCtx, cancelPaymentDispatch := context.WithCancel(context.Background())
	defer cancelPaymentDispatch()
	go paymentDispatcher.Run(paymentDispatchCtx, nil)

	fulfillmentConsumer := newFulfillmentConsumer(testEventType, testEventSubtype, 10, 5)
	fulfillmentCtx, cancelFulfillment := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFulfillment()
	go func() { _ = fulfillmentConsumer.Run(fulfillmentCtx) }()

	ok = waitFor(t, 9*time.Second, func() bool {
		return countTicketsByStatus("VOID") == 1
	})
	require.True(t, ok, "expected the reserved ticket to be voided")

	var chargeAfter persistence.ChargeModel
	require.NoError(t, suite.eventsDB.First(&chargeAfter, "id = ?", charge.ID).Error)
	require.Equal(t, "FAILED", chargeAfter.Status)

	var orderAfter persistence.OrderModel
	require.NoError(t, suite.eventsDB.First(&orderAfter, "id = ?", orderID).Error)
	require.Equal(t, "FAILED", orderAfter.Status)
}

// A redelivered payment-event message for an already-terminal Charge (e.g.
// the gateway retries its webhook delivery) is a safe no-op, not a second
// issuance attempt.
func TestFulfillment_RedeliveredConfirmation_IsNoOp(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-redeliver-1", "evt-redeliver-1", "TKT-redeliver-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	orderID, err := uuid.Parse(resp["orderId"].(string))
	require.NoError(t, err)

	orderDispatcher, _ := newOrderDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go orderDispatcher.Run(dispatchCtx, nil)

	checkoutConsumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	checkoutCtx, cancelCheckout := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCheckout()
	go func() { _ = checkoutConsumer.Run(checkoutCtx) }()

	var charge persistence.ChargeModel
	ok := waitFor(t, 9*time.Second, func() bool {
		return suite.eventsDB.Where("order_id = ?", orderID).First(&charge).Error == nil
	})
	require.True(t, ok, "expected a Charge row for the order")

	// Publish the SAME confirmation payload twice directly onto the queue —
	// simulates a RabbitMQ requeue/redelivery rather than a second HTTP
	// webhook call (which would already be deduped upstream by
	// payment_event_outbox's idempotency key; this exercises
	// IssueTickets.Execute's own terminal-status guard instead).
	msgID := "msgid-redeliver-1"
	confirmedBody := paymentEventOutboxPayload(charge.ProviderRef, "CONFIRMED")
	publishPaymentEventRaw(t, msgID, confirmedBody)
	publishPaymentEventRaw(t, msgID, confirmedBody)

	fulfillmentConsumer := newFulfillmentConsumer(testEventType, testEventSubtype, 10, 5)
	fulfillmentCtx, cancelFulfillment := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFulfillment()
	go func() { _ = fulfillmentConsumer.Run(fulfillmentCtx) }()

	ok = waitFor(t, 9*time.Second, func() bool { return countTicketsByStatus("VALID") == 1 })
	require.True(t, ok, "expected the ticket to be issued exactly once")

	time.Sleep(1 * time.Second)
	require.Equal(t, int64(1), countTicketsByStatus("VALID"))
}

// A malformed (non-JSON) message can never be parsed by IssueTickets.Execute,
// so it gets requeued via the shard's retry queue on every attempt — mirrors
// TestCheckout_PoisonMessage_RoutesToDeadLetterQueue for the fulfillment
// stream, and exercises recordRedactedError's "error" outcome path.
func TestFulfillment_PoisonMessage_RoutesToDeadLetterQueue(t *testing.T) {
	truncateAll(t)

	msgID := "msgid-fulfillment-poison-1"
	body := []byte(`{not-valid-json`)

	publishPaymentEventRaw(t, msgID, body)

	const maxDeliveries = 2
	consumer := newFulfillmentConsumer(testEventType, testEventSubtype, 10, maxDeliveries)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 14*time.Second, func() bool { return fulfillmentDLQDepth(t) >= 1 })
	require.True(t, ok, fmt.Sprintf("expected poison message to land in %s", rmq.DLQFor(rmq.PaymentEventStream, testEventType, testEventSubtype)))
	require.Equal(t, int64(0), countTickets())
}

// A message carrying an unrecognised schemaVersion can never be parsed by
// this build, so the consumer must reject it straight to the DLQ on the
// FIRST attempt (not burn through maxDeliveries) — mirrors
// TestCheckout_UnknownSchemaVersion_RejectsToDLQOnFirstAttempt, and exercises
// recordRedactedError's "unknown_schema_version" outcome path.
func TestFulfillment_UnknownSchemaVersion_RejectsToDLQOnFirstAttempt(t *testing.T) {
	truncateAll(t)

	msgID := "msgid-fulfillment-unknownschema-1"
	body, _ := json.Marshal(map[string]any{
		"schemaVersion": "999", // unsupported major version
		"provider":      testProvider,
		"providerRef":   "provider-ref-unknownschema-1",
		"outcome":       "CONFIRMED",
	})

	publishPaymentEventRaw(t, msgID, body)

	// maxDeliveries high on purpose: an unknown schema must DLQ on attempt 1
	// regardless, never retrying.
	consumer := newFulfillmentConsumer(testEventType, testEventSubtype, 10, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 9*time.Second, func() bool { return fulfillmentDLQDepth(t) >= 1 })
	require.True(t, ok, "expected unknown-schema message to be dead-lettered on the first attempt")
	require.Equal(t, int64(0), countTickets())
}

// A well-formed, correctly-versioned message whose providerRef matches no
// Charge at all (as opposed to TestFulfillment_RedeliveredConfirmation_IsNoOp's
// already-terminal Charge) is a genuine "find charge" error — retried, then
// dead-lettered, never a silent no-op.
func TestFulfillment_UnknownProviderRef_RoutesToDeadLetterQueue(t *testing.T) {
	truncateAll(t)

	msgID := "msgid-fulfillment-unknownref-1"
	body := paymentEventOutboxPayload("provider-ref-does-not-exist", "CONFIRMED")

	publishPaymentEventRaw(t, msgID, body)

	const maxDeliveries = 2
	consumer := newFulfillmentConsumer(testEventType, testEventSubtype, 10, maxDeliveries)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 14*time.Second, func() bool { return fulfillmentDLQDepth(t) >= 1 })
	require.True(t, ok, fmt.Sprintf("expected poison message to land in %s", rmq.DLQFor(rmq.PaymentEventStream, testEventType, testEventSubtype)))
	require.Equal(t, int64(0), countTickets())
}

func fulfillmentDLQDepth(t *testing.T) int {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(rmq.DLQFor(rmq.PaymentEventStream, testEventType, testEventSubtype), true, false, false, false, nil)
	require.NoError(t, err)
	return q.Messages
}

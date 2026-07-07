//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// orderOutboxPayload builds a JSON body matching usecase/checkout.payloadDTO's
// wire shape — this is what DispatchOutbox's publisher actually puts on the
// wire (usecase/order's outboxPayload), which order-consumer-worker's
// ProcessOrder unmarshals.
func orderOutboxPayload(orderID, sourceOrderID, sourceTicketID string) []byte {
	b, _ := json.Marshal(map[string]any{
		"schemaVersion":    "1",
		"orderId":          orderID,
		"sourceOrderId":    sourceOrderID,
		"eventType":        testEventType,
		"eventSubtype":     testEventSubtype,
		"sourceEventId":    "evt-" + sourceOrderID,
		"eventName":        "Rock in Rio",
		"sourceVenueId":    "venue-1",
		"venueName":        "Estadio Nacional",
		"venueCity":        "Sao Paulo",
		"items":            []map[string]any{{"sourceTicketId": sourceTicketID, "section": "A", "row": "10", "seat": "5", "price": 15000, "currency": "BRL"}},
		"customerName":     "Jane Doe",
		"customerEmail":    "jane@example.com",
		"customerDocument": "12345678900",
		"amount":           15000,
		"currency":         "BRL",
	})
	return b
}

func publishOrderRaw(t *testing.T, messageID string, body []byte, headers amqp.Table) {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	routingKey := rmq.RoutingKeyFor(rmq.OrderStream, testEventType, testEventSubtype)
	err = ch.PublishWithContext(context.Background(), rmq.Exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "application/json",
		MessageId:   messageID,
		Body:        body,
		Headers:     headers,
	})
	require.NoError(t, err)
	select {
	case c := <-confirms:
		require.True(t, c.Ack, "broker nacked test publish")
	case <-time.After(5 * time.Second):
		t.Fatal("publish confirm timeout")
	}
}

// A well-formed order message reserves tickets and opens a checkout: after
// processing, an Order row (RESERVED), one Ticket row per item (RESERVED),
// and a Charge row (PENDING, fake provider ref) all exist.
func TestCheckout_HappyPath_ReservesTicketsAndOpensCheckout(t *testing.T) {
	truncateAll(t)

	orderID := "018f0000-0000-7000-8000-000000000001"
	body := orderOutboxPayload(orderID, "order-checkout-happy-1", "TKT-checkout-happy-1")

	publishOrderRaw(t, "msgid-checkout-happy-1", body, nil)

	consumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 7*time.Second, func() bool { return countOrders() == 1 })
	require.True(t, ok, "expected one order row")
	require.Equal(t, int64(1), countTicketsByStatus("RESERVED"))
	require.Equal(t, int64(1), countCharges())
}

// Delivering the same order message id twice (simulating a requeue after a
// transient consumer failure) must result in exactly one order/ticket/charge
// set, thanks to the ON CONFLICT (source_order_id) DO NOTHING dedup.
func TestCheckout_DuplicateDelivery_DedupesViaUniqueConstraint(t *testing.T) {
	truncateAll(t)

	orderID := "018f0000-0000-7000-8000-000000000002"
	body := orderOutboxPayload(orderID, "order-checkout-dup-1", "TKT-checkout-dup-1")
	msgID := "msgid-checkout-dup-1"

	publishOrderRaw(t, msgID, body, nil)
	publishOrderRaw(t, msgID, body, nil) // duplicate delivery, same MessageId

	consumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 7*time.Second, func() bool { return countOrders() >= 1 })
	require.True(t, ok, "expected at least one order row")

	// Give the second delivery a moment to be processed/acked too, then
	// assert it didn't create a second set of rows.
	time.Sleep(1 * time.Second)
	require.Equal(t, int64(1), countOrders())
	require.Equal(t, int64(1), countCharges())
}

// A malformed/unprocessable message (e.g. invalid orderId UUID) gets
// requeued (via the shard's retry queue) by the consumer's generic error
// path each time Execute fails to parse it. After maxDeliveries attempts,
// the consumer's poison-message check rejects it permanently, routing it to
// the shard's DLQ via the topology's dead-letter exchange/routing key.
func TestCheckout_PoisonMessage_RoutesToDeadLetterQueue(t *testing.T) {
	truncateAll(t)

	msgID := "msgid-checkout-poison-1"
	body := []byte(`{"orderId":"not-a-valid-uuid","sourceOrderId":"order-checkout-poison-1"}`)

	publishOrderRaw(t, msgID, body, nil)

	const maxDeliveries = 2
	consumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, maxDeliveries)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 14*time.Second, func() bool { return orderDLQDepth(t) >= 1 })
	require.True(t, ok, fmt.Sprintf("expected poison message to land in %s", rmq.DLQFor(rmq.OrderStream, testEventType, testEventSubtype)))
	require.Equal(t, int64(0), countOrders())
}

// A message carrying an unrecognised schemaVersion can never be parsed by
// this build, so the consumer must reject it straight to the DLQ on the
// FIRST attempt (not burn through maxDeliveries).
func TestCheckout_UnknownSchemaVersion_RejectsToDLQOnFirstAttempt(t *testing.T) {
	truncateAll(t)

	msgID := "msgid-checkout-unknownschema-1"
	body, _ := json.Marshal(map[string]any{
		"schemaVersion": "999", // unsupported major version
		"orderId":       "018f0000-0000-7000-8000-0000000000aa",
		"sourceOrderId": "order-checkout-unknownschema-1",
		"eventType":     testEventType,
		"eventSubtype":  testEventSubtype,
	})

	publishOrderRaw(t, msgID, body, nil)

	// maxDeliveries high on purpose: an unknown schema must DLQ on attempt 1
	// regardless, never retrying.
	consumer := newCheckoutConsumer(testEventType, testEventSubtype, 10, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 9*time.Second, func() bool { return orderDLQDepth(t) >= 1 })
	require.True(t, ok, "expected unknown-schema message to be dead-lettered on the first attempt")
	require.Equal(t, int64(0), countOrders())
}

func orderDLQDepth(t *testing.T) int {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(rmq.DLQFor(rmq.OrderStream, testEventType, testEventSubtype), true, false, false, false, nil)
	require.NoError(t, err)
	return q.Messages
}

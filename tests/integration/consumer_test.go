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

// consumerPayload builds a JSON body matching consume.payloadDTO's wire
// shape (paymentId/eventId/providerName/... at the top level, not nested
// under "payment" like the HTTP ingest DTO) — this is what DispatchOutbox's
// publisher actually puts on the wire (ingest's outboxPayload), which the
// consumer's payloadDTO unmarshals.
func consumerPayload(paymentID, eventID string) []byte {
	b, _ := json.Marshal(map[string]any{
		"paymentId":         paymentID,
		"eventId":           eventID,
		"providerName":      "MERCADO_PAGO",
		"providerPaymentId": "prov-" + eventID,
		"externalPaymentId": "pay_" + eventID,
		"amount":            10050,
		"currency":          "BRL",
		"method":            "PIX",
		"methodDetails":     json.RawMessage(`{"endToEndId":"E00000000000000000000000000","txid":"ORDER-` + eventID + `"}`),
		"occurredAt":        time.Now().UTC().Format(time.RFC3339),
	})
	return b
}

func publishRaw(t *testing.T, messageID string, body []byte, headers amqp.Table) {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	err = ch.PublishWithContext(context.Background(), rmq.Exchange, rmq.RoutingKey, false, false, amqp.Publishing{
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

// Path #7: delivering the same message id twice (simulating a requeue after
// a transient consumer failure) must result in exactly one payments row,
// thanks to the ON CONFLICT (source_message_id) DO NOTHING dedup.
func TestConsumer_DuplicateDelivery_DedupesViaUniqueConstraint(t *testing.T) {
	truncateAll(t)

	paymentID := "018f0000-0000-7000-8000-000000000001"
	eventID := "evt-consumer-dup-1"
	body := consumerPayload(paymentID, eventID)
	msgID := "msgid-consumer-dup-1"

	publishRaw(t, msgID, body, nil)
	publishRaw(t, msgID, body, nil) // duplicate delivery, same MessageId

	consumer := newConsumer(10, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 8*time.Second, func() bool {
		return countPayments() >= 1
	})
	require.True(t, ok, "expected at least one payments row")

	// Give the second delivery a moment to be processed/acked too, then
	// assert it didn't create a second row.
	time.Sleep(1 * time.Second)
	require.Equal(t, int64(1), countPayments())
}

// Path #8: a malformed/unprocessable message (e.g. invalid paymentId UUID)
// gets requeued (Nack(requeue=true)) by the consumer's generic error path
// each time process.Execute fails to parse it. After it has been redelivered
// maxDeliveries times, RabbitMQ's x-death header count reaches the
// threshold and the consumer's poison-message check rejects it permanently
// (Reject(requeue=false)), routing it to the DLQ via the topology's
// dead-letter exchange/routing key, and the consumer keeps running.
func TestConsumer_PoisonMessage_RoutesToDeadLetterQueue(t *testing.T) {
	truncateAll(t)

	msgID := "msgid-poison-1"
	body := []byte(`{"paymentId":"not-a-valid-uuid","eventId":"evt-poison-1"}`)

	publishRaw(t, msgID, body, nil)

	const maxDeliveries = 2
	consumer := newConsumer(10, maxDeliveries)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	ok := waitFor(t, 14*time.Second, func() bool {
		return dlqDepth(t) >= 1
	})
	require.True(t, ok, fmt.Sprintf("expected poison message to land in %s", rmq.DLQ))
	require.Equal(t, int64(0), countPayments())
}

func dlqDepth(t *testing.T) int {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(rmq.DLQ, true, false, false, false, nil)
	require.NoError(t, err)
	return q.Messages
}

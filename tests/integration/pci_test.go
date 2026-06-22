//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/stretchr/testify/require"
)

// fullPAN is a well-known test PAN (never a real one — see cardBody in
// card_test.go) used as the single needle this regression guard searches
// for. testPANLast4 is what's allowed to remain after masking.
const (
	fullPAN      = "4111111111111111"
	testPANLast4 = "1111"
)

// TestPCI_CardPayment_NeverLeaksFullPANOrCVV is a PCI-DSS regression guard
// (Phase 5 Track 5.B): it posts a CARTAO_CREDITO payment end-to-end —
// ingest -> outbox dispatch -> RabbitMQ -> the persisted payments row — and
// asserts the full PAN never appears in any of the three places a card
// payment's data passes through, and that no CVV field is ever accepted,
// stored, or published. This guards the masking that already happens in
// internal/adapter/http/card.go (maskPAN) and internal/domain/pii — it does
// not change that logic, it only fails loudly if a future change breaks it.
func TestPCI_CardPayment_NeverLeaksFullPANOrCVV(t *testing.T) {
	truncateAll(t)

	// 1. CVV is never even an accepted field: the wire format's card sibling
	// object (CardDetailsDTO) has no cvv field at all, so a client that
	// sends one is simply ignored by json.Unmarshal — assert that directly
	// against the DTO shape by round-tripping a card payload containing a
	// "cvv" key and confirming it never reaches storage.
	body := `{
		"eventId":"evt-pci-1",
		"provider":{"name":"ACQUIRER","providerPaymentId":"prov-pci-1"},
		"payment":{"paymentId":"pay_pci_1","amount":42.00,"currency":"BRL","method":"CARTAO_CREDITO"},
		"cartao_credito":{"cardNumber":"` + fullPAN + `","cardType":"CREDIT","cardIssuer":"VISA","cvv":"123"},
		"occurredAt":"` + time.Now().UTC().Format(time.RFC3339) + `"
	}`
	rec, resp := postPayment(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	// 2. Outbox payload (the row written to outbox_messages.payload before
	// any publish) must carry only the masked PAN and must never contain a
	// "cvv" key.
	var row persistence.OutboxMessageModel
	require.NoError(t, suite.db.First(&row).Error)
	outboxPayload := string(row.Payload)
	require.NotContains(t, outboxPayload, fullPAN, "full PAN must never reach the outbox payload")
	require.NotContains(t, outboxPayload, "cvv", "CVV must never be persisted in the outbox payload")
	require.Contains(t, outboxPayload, testPANLast4)

	// 3. Dispatch it onto RabbitMQ, then peek the raw message body with a
	// non-destructive Get (autoAck=false, immediately Nack(requeue=true)) so
	// the message goes back for the later consumer step — this is purely an
	// inspection of bytes that crossed the wire, never a normal consume.
	dispatcher, _ := newDispatch(10, 5, 100*time.Millisecond, 24*time.Hour)
	dispatchCtx, dispatchCancel := context.WithCancel(context.Background())
	defer dispatchCancel()
	go dispatcher.Run(dispatchCtx)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected outbox row to reach PUBLISHED")

	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()

	msg, ok, err := ch.Get(rmq.QueueFor("CARTAO_CREDITO"), false)
	require.NoError(t, err)
	require.True(t, ok, "expected a message on the cartao_credito queue")
	rabbitBody := string(msg.Body)
	require.NotContains(t, rabbitBody, fullPAN, "full PAN must never travel over RabbitMQ")
	require.NotContains(t, rabbitBody, "cvv", "CVV must never travel over RabbitMQ")
	require.Contains(t, rabbitBody, testPANLast4)
	require.NoError(t, msg.Nack(false, true)) // put it back for the consumer below

	// 4. Run the message through the real consumer and assert the persisted
	// payments row also carries only the masked PAN — the full
	// ingest->outbox->RabbitMQ->payments path leaves no full PAN anywhere.
	consumer := newConsumer("CARTAO_CREDITO", 10, 5)
	consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer consumeCancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	ok = waitFor(t, 8*time.Second, func() bool {
		return countPayments() == 1
	})
	require.True(t, ok, "expected payment to be persisted")

	var payment persistence.PaymentModel
	require.NoError(t, suite.db.First(&payment).Error)
	persistedDetails := string(payment.MethodDetails)
	require.NotContains(t, persistedDetails, fullPAN, "full PAN must never reach the payments table")
	require.NotContains(t, persistedDetails, "cvv", "CVV must never be persisted")
	require.Contains(t, persistedDetails, testPANLast4)

	// 5. pii.Redact is the second line of defense for anything that does
	// reach a log line (e.g. an error message echoing the raw payload) —
	// confirm it independently masks cardNumber so a future log call site
	// that forgets this regression test still gets covered.
	redacted := pii.Redact(`{"cardNumber":"` + fullPAN + `"}`)
	require.NotContains(t, redacted, fullPAN, "pii.Redact must mask a literal PAN if it ever reaches a log line")
}

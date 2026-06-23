//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/stretchr/testify/require"
)

// cardBody builds a valid CARTAO_CREDITO/CARTAO_DEBITO wire-format payload.
// cardType must match method (CREDIT for CARTAO_CREDITO, DEBIT for
// CARTAO_DEBITO) or the handler rejects it with 400.
func cardBody(eventID, method, cardType string) string {
	sibling := "cartao_credito"
	if method == "CARTAO_DEBITO" {
		sibling = "cartao_debito"
	}
	return fmt.Sprintf(`{
		"eventId":"%s",
		"provider":{"name":"ACQUIRER","providerPaymentId":"prov-%s"},
		"payment":{"paymentId":"pay_%s","amount":75.00,"currency":"BRL","method":"%s"},
		"%s":{"cardNumber":"4111111111111111","cardType":"%s","cardIssuer":"VISA"},
		"occurredAt":"%s"
	}`, eventID, eventID, eventID, method, sibling, cardType, time.Now().UTC().Format(time.RFC3339))
}

func queueDepth(t *testing.T, name string) int {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	require.NoError(t, err)
	return q.Messages
}

// Card POST is accepted and creates exactly one NEW outbox row tagged with
// the card method.
func TestIngest_CardCredito_Accepted(t *testing.T) {
	truncateAll(t)

	body := cardBody("evt-card-1", "CARTAO_CREDITO", "CREDIT")
	rec, resp := postPayment(t, body, map[string]string{"Content-Type": "application/json"})

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])
	require.Equal(t, int64(1), countOutboxByStatus("NEW"))

	var row persistence.OutboxMessageModel
	require.NoError(t, suite.db.First(&row).Error)
	require.Equal(t, "CARTAO_CREDITO", row.PaymentMethod)
}

// cardType must match the method (CARTAO_CREDITO requires CREDIT) — a
// mismatch is rejected at ingest with 400 and no outbox row.
func TestIngest_CardCredito_TypeMismatch_Rejected(t *testing.T) {
	truncateAll(t)

	body := cardBody("evt-card-mismatch-1", "CARTAO_CREDITO", "DEBIT")
	rec, _ := postPayment(t, body, map[string]string{"Content-Type": "application/json"})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countOutboxByStatus("NEW"))
}

// A method outside rmq.Methods (no bound queue) is rejected at ingest with
// 400 — never silently dropped by the broker.
func TestIngest_UnknownMethod_RejectedNoPublish(t *testing.T) {
	truncateAll(t)

	body := fmt.Sprintf(`{
		"eventId":"evt-unknown-1",
		"provider":{"name":"ACQUIRER","providerPaymentId":"prov-unknown-1"},
		"payment":{"paymentId":"pay_unknown_1","amount":10.00,"currency":"BRL","method":"CRYPTO"},
		"occurredAt":"%s"
	}`, time.Now().UTC().Format(time.RFC3339))

	rec, _ := postPayment(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countOutboxByStatus("NEW"))
}

// Full E2E path: ingest -> dispatch -> consume for a CARTAO_CREDITO payment.
// Asserts (a) the message routes only to payments.cartao_credito.queue,
// leaving every other method's queue empty, and (b) the persisted
// payments.method_details carries only the last-4 of the PAN — the full
// number must never reach Postgres or have travelled over RabbitMQ.
func TestCardE2E_FullPath_RoutesIsolatedAndMasksPAN(t *testing.T) {
	truncateAll(t)

	body := cardBody("evt-card-e2e-1", "CARTAO_CREDITO", "CREDIT")
	rec, resp := postPayment(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	dispatcher, _ := newDispatch(10, 5, 100*time.Millisecond, 24*time.Hour)
	dispatchCtx, dispatchCancel := context.WithCancel(context.Background())
	defer dispatchCancel()
	go dispatcher.Run(dispatchCtx, nil)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected outbox row to reach PUBLISHED")

	// Cross-method isolation: only the card-credito queue got the message.
	require.Equal(t, 1, queueDepth(t, rmq.QueueFor("CARTAO_CREDITO")))
	for _, m := range rmq.Methods {
		if m == "CARTAO_CREDITO" {
			continue
		}
		require.Equal(t, 0, queueDepth(t, rmq.QueueFor(m)), "method %s queue should stay empty", m)
	}

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
	require.Equal(t, "CARTAO_CREDITO", payment.Method)
	require.Contains(t, string(payment.MethodDetails), "************1111")
	require.NotContains(t, string(payment.MethodDetails), "4111111111111111")
}

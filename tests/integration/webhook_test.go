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

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/webhook"
	"github.com/stretchr/testify/require"
)

// fakeWebhookBody builds a body matching fake.Gateway.VerifyWebhook's wire
// shape.
func fakeWebhookBody(providerRef, eventID, outcome string) string {
	return fmt.Sprintf(`{
		"provider_ref":"%s",
		"event_id":"%s",
		"outcome":"%s",
		"event_type":"%s",
		"event_subtype":"%s"
	}`, providerRef, eventID, outcome, testEventType, testEventSubtype)
}

func postWebhook(t *testing.T, provider, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := newRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/payments/"+provider, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// A single valid fake-gateway webhook creates exactly one NEW
// payment_event_outbox row, carrying the shard's event_type/event_subtype.
func TestWebhook_FakeProvider_HappyPath_CreatesPaymentEventOutboxRow(t *testing.T) {
	truncateAll(t)

	rec := postWebhook(t, testProvider, fakeWebhookBody("fake_provider-ref-1", "webhook-evt-1", "CONFIRMED"))
	require.Equal(t, http.StatusOK, rec.Code)

	require.Equal(t, int64(1), countPaymentEventOutboxByStatus("NEW"))

	var row outboxRowFixture
	require.NoError(t, suite.db.Table("payment_event_outbox").First(&row).Error)
	require.Equal(t, "payment_event", row.AggregateType)
	require.Equal(t, testEventType, row.EventType)
	require.Equal(t, testEventSubtype, row.EventSubtype)

	var payload struct {
		Provider    string `json:"provider"`
		ProviderRef string `json:"providerRef"`
		Outcome     string `json:"outcome"`
	}
	require.NoError(t, json.Unmarshal(row.Payload, &payload))
	require.Equal(t, testProvider, payload.Provider)
	require.Equal(t, "fake_provider-ref-1", payload.ProviderRef)
	require.Equal(t, "CONFIRMED", payload.Outcome)
}

// Redelivering the SAME gateway event id is idempotent: the second webhook
// call still returns 200 (a webhook must never make the gateway retry
// forever), but creates no second row.
func TestWebhook_DuplicateEventID_NoNewRow(t *testing.T) {
	truncateAll(t)

	body := fakeWebhookBody("fake_provider-ref-dup", "webhook-evt-dup", "CONFIRMED")

	rec1 := postWebhook(t, testProvider, body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postWebhook(t, testProvider, body)
	require.Equal(t, http.StatusOK, rec2.Code)

	require.Equal(t, int64(1), countPaymentEventOutboxByStatus("NEW"))
}

// The webhook handler is bound to exactly one configured provider — a path
// segment naming a different provider is rejected with 404, never silently
// accepted.
func TestWebhook_UnknownProvider_404(t *testing.T) {
	truncateAll(t)

	rec := postWebhook(t, "stripe", fakeWebhookBody("fake_provider-ref-2", "webhook-evt-2", "CONFIRMED"))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, int64(0), countPaymentEventOutboxByStatus("NEW"))
}

// A body missing the fields the fake gateway requires is rejected with 400
// by VerifyWebhook, before anything is written to the outbox.
func TestWebhook_InvalidBody_Rejected(t *testing.T) {
	truncateAll(t)

	rec := postWebhook(t, testProvider, `{"outcome":"CONFIRMED"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countPaymentEventOutboxByStatus("NEW"))
}

// ReceivePaymentEvent.Execute wraps and returns any error the UnitOfWork's
// Execute itself surfaces — mirrors TestPlaceOrder_UowExecuteError, using an
// already-canceled context to make the transaction fail to even begin.
func TestReceivePaymentEvent_UowExecuteError_ReturnsWrappedError(t *testing.T) {
	truncateAll(t)

	uc := newReceivePaymentEvent()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := uc.Execute(ctx, webhook.Request{
		Provider: testProvider,
		Event: domain.PaymentEvent{
			ProviderRef:  "provider-ref-uow-error-1",
			RawEventID:   "webhook-uow-error-1",
			Outcome:      domain.PaymentOutcomeConfirmed,
			EventType:    testEventType,
			EventSubtype: testEventSubtype,
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "enqueue payment event outbox")
	require.Equal(t, int64(0), countPaymentEventOutboxByStatus("NEW"))
}

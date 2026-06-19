//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/stretchr/testify/require"
)

// pixBody builds a valid PIX wire-format payload. eventID/idemKey let callers
// control dedup behaviour; fixtures use obviously-fake PII values.
func pixBody(eventID, providerPaymentID, idemKey string) (string, map[string]string) {
	body := fmt.Sprintf(`{
		"eventId":"%s",
		"provider":{"name":"MERCADO_PAGO","providerPaymentId":"%s"},
		"payment":{"paymentId":"pay_%s","amount":100.50,"currency":"BRL","method":"PIX"},
		"pix":{"endToEndId":"E00000000000000000000000000","txid":"ORDER-%s"},
		"occurredAt":"%s"
	}`, eventID, providerPaymentID, eventID, eventID, time.Now().UTC().Format(time.RFC3339))
	headers := map[string]string{"Content-Type": "application/json"}
	if idemKey != "" {
		headers["Idempotency-Key"] = idemKey
	}
	return body, headers
}

func postPayment(t *testing.T, body string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	router := newRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", bytes.NewBufferString(body))
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

// Path #1 (ingest half): a single POST creates exactly one NEW outbox row.
func TestIngest_HappyPath_CreatesOutboxRow(t *testing.T) {
	truncateAll(t)

	body, headers := pixBody("evt-happy-1", "prov-happy-1", "")
	rec, resp := postPayment(t, body, headers)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])
	require.NotEmpty(t, resp["paymentId"])
	require.NotEmpty(t, resp["idempotencyKey"])

	require.Equal(t, int64(1), countOutboxByStatus("NEW"))

	var row persistence.OutboxMessageModel
	require.NoError(t, suite.db.First(&row).Error)
	require.Equal(t, resp["idempotencyKey"], row.IdempotencyKey)
	require.Equal(t, "payment", row.AggregateType)
	require.Equal(t, http.MethodPost, row.HTTPMethod)
}

// Path #2: same business fields (same eventId/provider), no Idempotency-Key
// header, sent twice -> second request is a duplicate, only one outbox row.
func TestIngest_DuplicateWithoutIdempotencyKey_NoNewRow(t *testing.T) {
	truncateAll(t)

	body, headers := pixBody("evt-dup-1", "prov-dup-1", "")

	rec1, resp1 := postPayment(t, body, headers)
	require.Equal(t, http.StatusCreated, rec1.Code)
	require.Equal(t, "accepted", resp1["status"])

	rec2, resp2 := postPayment(t, body, headers)
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "duplicate", resp2["status"])
	require.Equal(t, resp1["idempotencyKey"], resp2["idempotencyKey"])

	require.Equal(t, int64(1), countOutboxByStatus("NEW"))
}

// Path #3: two requests with different bodies (different eventId) but the
// SAME Idempotency-Key header -> the key formula folds the client key in,
// so distinct eventIds would normally produce distinct keys; but holding
// provider+eventId identical while varying only incidental fields and
// supplying the same Idempotency-Key must dedupe to one row.
func TestIngest_SameIdempotencyKeyHeader_Dedupes(t *testing.T) {
	truncateAll(t)

	idemKey := "client-key-shared-1"
	body1, headers1 := pixBody("evt-key-1", "prov-key-1", idemKey)
	// Same provider/eventId (the key source), different incidental body
	// content (amount) — still same idempotency key because key derivation
	// is provider:eventId + header, not amount/currency.
	body2 := fmt.Sprintf(`{
		"eventId":"evt-key-1",
		"provider":{"name":"MERCADO_PAGO","providerPaymentId":"prov-key-1"},
		"payment":{"paymentId":"pay_evt-key-1-different","amount":999.99,"currency":"BRL","method":"PIX"},
		"pix":{"endToEndId":"E00000000000000000000000000","txid":"ORDER-evt-key-1-b"},
		"occurredAt":"%s"
	}`, time.Now().UTC().Format(time.RFC3339))
	headers2 := headers1

	rec1, resp1 := postPayment(t, body1, headers1)
	require.Equal(t, http.StatusCreated, rec1.Code)
	require.Equal(t, "accepted", resp1["status"])

	rec2, resp2 := postPayment(t, body2, headers2)
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "duplicate", resp2["status"])
	require.Equal(t, resp1["idempotencyKey"], resp2["idempotencyKey"])

	require.Equal(t, int64(1), countOutboxByStatus("NEW"))
}

// Path #4: same body (same provider/eventId), but two DISTINCT
// Idempotency-Key header values -> the key formula folds the client key in,
// so distinct keys must NOT dedupe even though the event identity matches.
func TestIngest_DistinctIdempotencyKeys_NoDedup(t *testing.T) {
	truncateAll(t)

	body, _ := pixBody("evt-distinct-1", "prov-distinct-1", "")

	rec1, resp1 := postPayment(t, body, map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": "key-A",
	})
	require.Equal(t, http.StatusCreated, rec1.Code)
	require.Equal(t, "accepted", resp1["status"])

	rec2, resp2 := postPayment(t, body, map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": "key-B",
	})
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "accepted", resp2["status"])

	require.NotEqual(t, resp1["idempotencyKey"], resp2["idempotencyKey"])
	require.Equal(t, int64(2), countOutboxByStatus("NEW"))
}

// BOLETO wire format also exercises ValidateMethod's boleto branch — ensures
// the polymorphic MethodDetails sibling-object path round-trips through the
// outbox payload too.
func TestIngest_BoletoMethod_Accepted(t *testing.T) {
	truncateAll(t)

	body := fmt.Sprintf(`{
		"eventId":"evt-boleto-1",
		"provider":{"name":"BOLETO_BANCARIO","providerPaymentId":"prov-boleto-1"},
		"payment":{"paymentId":"pay_boleto_1","amount":250.00,"currency":"BRL","method":"BOLETO"},
		"boleto":{"barcode":"00000000000000000000000000000000000000000000","dueDate":"2026-07-01","payerDocument":"00000000000"},
		"occurredAt":"%s"
	}`, time.Now().UTC().Format(time.RFC3339))

	rec, resp := postPayment(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])
	require.Equal(t, int64(1), countOutboxByStatus("NEW"))
}

// Invalid payloads (missing required sibling object) must be rejected with
// 400 and create no outbox row at all.
func TestIngest_InvalidPixPayload_Rejected(t *testing.T) {
	truncateAll(t)

	body := fmt.Sprintf(`{
		"eventId":"evt-invalid-1",
		"provider":{"name":"MERCADO_PAGO","providerPaymentId":"prov-invalid-1"},
		"payment":{"paymentId":"pay_invalid_1","amount":10.00,"currency":"BRL","method":"PIX"},
		"occurredAt":"%s"
	}`, time.Now().UTC().Format(time.RFC3339))

	rec, _ := postPayment(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countOutboxByStatus("NEW"))
}

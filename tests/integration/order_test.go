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

	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/order"
	"github.com/stretchr/testify/require"
)

// orderBody builds a valid order wire-format payload for testEventType/
// testEventSubtype. sourceOrderID/eventID/ticketID/idemKey let callers
// control dedup behaviour.
func orderBody(sourceOrderID, eventID, ticketID, idemKey string) (string, map[string]string) {
	body := fmt.Sprintf(`{
		"sourceOrderId":"%s",
		"eventType":"%s",
		"eventSubtype":"%s",
		"eventId":"%s",
		"eventName":"Rock in Rio",
		"venue":{"id":"venue-1","name":"Estadio Nacional","city":"Sao Paulo"},
		"tickets":[{"id":"%s","section":"A","row":"10","seat":"5","price":150.00,"currency":"BRL"}],
		"customer":{"name":"Jane Doe","email":"jane@example.com","document":"12345678900"}
	}`, sourceOrderID, testEventType, testEventSubtype, eventID, ticketID)
	headers := map[string]string{"Content-Type": "application/json"}
	if idemKey != "" {
		headers["Idempotency-Key"] = idemKey
	}
	return body, headers
}

func postOrder(t *testing.T, body string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	router := newRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orders", bytes.NewBufferString(body))
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

// A single POST creates exactly one NEW order_outbox row.
func TestOrder_HappyPath_CreatesOrderOutboxRow(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-happy-1", "evt-happy-1", "TKT-happy-1", "")
	rec, resp := postOrder(t, body, headers)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])
	require.NotEmpty(t, resp["orderId"])
	require.NotEmpty(t, resp["idempotencyKey"])

	require.Equal(t, int64(1), countOrderOutboxByStatus("NEW"))

	var row outboxRowFixture
	require.NoError(t, suite.db.Table("order_outbox").First(&row).Error)
	require.Equal(t, resp["idempotencyKey"], row.IdempotencyKey)
	require.Equal(t, "order", row.AggregateType)
	require.Equal(t, http.MethodPost, row.HTTPMethod)
	require.Equal(t, testEventType, row.EventType)
	require.Equal(t, testEventSubtype, row.EventSubtype)
}

// Same sourceOrderId, no Idempotency-Key header, sent twice -> second request
// is a duplicate, only one outbox row.
func TestOrder_DuplicateWithoutIdempotencyKey_NoNewRow(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-dup-1", "evt-dup-1", "TKT-dup-1", "")

	rec1, resp1 := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec1.Code)
	require.Equal(t, "accepted", resp1["status"])

	rec2, resp2 := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "duplicate", resp2["status"])
	require.Equal(t, resp1["idempotencyKey"], resp2["idempotencyKey"])

	require.Equal(t, int64(1), countOrderOutboxByStatus("NEW"))
}

// Two requests with the SAME sourceOrderId and the SAME Idempotency-Key
// header, but different incidental fields (a different ticket id) -> still
// dedupes to one row, since the key is derived from sourceOrderId + the
// client key, not the request body's other fields.
func TestOrder_SameIdempotencyKeyHeader_Dedupes(t *testing.T) {
	truncateAll(t)

	idemKey := "client-key-shared-1"
	body1, headers1 := orderBody("order-key-1", "evt-key-1", "TKT-key-1", idemKey)
	body2, headers2 := orderBody("order-key-1", "evt-key-1", "TKT-key-1-different", idemKey)

	rec1, resp1 := postOrder(t, body1, headers1)
	require.Equal(t, http.StatusCreated, rec1.Code)
	require.Equal(t, "accepted", resp1["status"])

	rec2, resp2 := postOrder(t, body2, headers2)
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "duplicate", resp2["status"])
	require.Equal(t, resp1["idempotencyKey"], resp2["idempotencyKey"])

	require.Equal(t, int64(1), countOrderOutboxByStatus("NEW"))
}

// Same sourceOrderId, two DISTINCT Idempotency-Key header values -> the key
// formula folds the client key in, so distinct keys must NOT dedupe even
// though the order identity matches.
func TestOrder_DistinctIdempotencyKeys_NoDedup(t *testing.T) {
	truncateAll(t)

	body, _ := orderBody("order-distinct-1", "evt-distinct-1", "TKT-distinct-1", "")

	rec1, resp1 := postOrder(t, body, map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": "key-A",
	})
	require.Equal(t, http.StatusCreated, rec1.Code)
	require.Equal(t, "accepted", resp1["status"])

	rec2, resp2 := postOrder(t, body, map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": "key-B",
	})
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "accepted", resp2["status"])

	require.NotEqual(t, resp1["idempotencyKey"], resp2["idempotencyKey"])
	require.Equal(t, int64(2), countOrderOutboxByStatus("NEW"))
}

// Tickets with mismatched currencies are rejected — mixed-currency orders
// aren't supported (OrderRequestDTO.Validate).
func TestOrder_MixedCurrency_Rejected(t *testing.T) {
	truncateAll(t)

	body := `{
		"sourceOrderId":"order-mixed-currency-1",
		"eventType":"CONCERT","eventSubtype":"ROCK","eventId":"evt-mixed-1",
		"venue":{"id":"venue-1","name":"Estadio","city":"Sao Paulo"},
		"tickets":[
			{"id":"TKT-a","section":"A","row":"1","seat":"1","price":100.00,"currency":"BRL"},
			{"id":"TKT-b","section":"A","row":"1","seat":"2","price":100.00,"currency":"USD"}
		],
		"customer":{"name":"Jane Doe","email":"jane@example.com","document":"12345678900"}
	}`
	rec, _ := postOrder(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countOrderOutboxByStatus("NEW"))
}

// Invalid payloads (missing required field) must be rejected with 400 and
// create no outbox row at all.
func TestOrder_MissingVenueID_Rejected(t *testing.T) {
	truncateAll(t)

	body := `{
		"sourceOrderId":"order-invalid-1",
		"eventType":"CONCERT","eventSubtype":"ROCK","eventId":"evt-invalid-1",
		"venue":{"name":"Estadio","city":"Sao Paulo"},
		"tickets":[{"id":"TKT-invalid-1","section":"A","row":"1","seat":"1","price":100.00,"currency":"BRL"}],
		"customer":{"name":"Jane Doe","email":"jane@example.com","document":"12345678900"}
	}`
	rec, _ := postOrder(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countOrderOutboxByStatus("NEW"))
}

// PlaceOrder.Execute wraps and returns any error the UnitOfWork's Execute
// itself surfaces (e.g. a real DB/transaction failure, as opposed to the
// ON CONFLICT DO NOTHING dedup path, which is not an error at all) — an
// already-canceled context makes the transaction fail to even begin,
// exercising that wrapping without needing to break the real DB.
func TestPlaceOrder_UowExecuteError_ReturnsWrappedError(t *testing.T) {
	truncateAll(t)

	uc := newPlaceOrder()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := uc.Execute(ctx, order.Request{
		SourceOrderID: "order-uow-error-1",
		EventType:     testEventType,
		EventSubtype:  testEventSubtype,
		Items:         []order.ItemRequest{{SourceTicketID: "TKT-uow-error-1", Price: 1000, Currency: "BRL"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "enqueue order outbox")
	require.Equal(t, int64(0), countOrderOutboxByStatus("NEW"))
}

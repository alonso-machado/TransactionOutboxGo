//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 50 concurrent POSTs with unique business identities must result in 50
// distinct order_outbox rows, all of which eventually reach PUBLISHED and
// are processed exactly once by order-consumer-worker (no row lost, none
// duplicated, none double-processed thanks to FOR UPDATE SKIP LOCKED on
// FetchPending and the orders.source_order_id UNIQUE constraint).
func TestConcurrentIngestion_FiftyUniqueOrders_AllPublishedAndProcessedOnce(t *testing.T) {
	truncateAll(t)

	const n = 50
	var wg sync.WaitGroup
	statuses := make([]string, n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sourceOrderID := fmt.Sprintf("order-concurrent-%d", i)
			eventID := fmt.Sprintf("evt-concurrent-%d", i)
			ticketID := fmt.Sprintf("TKT-concurrent-%d", i)
			body, headers := orderBody(sourceOrderID, eventID, ticketID, "")
			code, resp, err := postOrderConcurrent(body, headers)
			if err != nil {
				errs[i] = err
				return
			}
			if code != http.StatusCreated {
				errs[i] = fmt.Errorf("status %d for index %d", code, i)
				return
			}
			statuses[i], _ = resp["status"].(string)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "request %d failed", i)
	}
	for i, s := range statuses {
		require.Equal(t, "accepted", s, "request %d should be accepted, not deduped", i)
	}

	require.Equal(t, int64(n), countOrderOutboxByStatus("NEW"))

	dispatcher, _ := newOrderDispatch(20, 5, 100*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go dispatcher.Run(dispatchCtx, nil)

	consumer := newCheckoutConsumer(testEventType, testEventSubtype, 20, 5)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()
	go func() { _ = consumer.Run(consumeCtx) }()

	ok := waitFor(t, 30*time.Second, func() bool {
		return countOrderOutboxByStatus("PUBLISHED") == int64(n)
	})
	require.True(t, ok, "expected all %d rows to reach PUBLISHED, got %d", n, countOrderOutboxByStatus("PUBLISHED"))

	ok2 := waitFor(t, 30*time.Second, func() bool {
		return countOrders() == int64(n)
	})
	require.True(t, ok2, "expected exactly %d order rows, got %d", n, countOrders())
}

// postOrderConcurrent mirrors postOrder but returns plain values (no
// *testing.T assertions) since it runs inside a goroutine; each call wires
// its own router/use-case instance, same as postOrder.
func postOrderConcurrent(body string, headers map[string]string) (int, map[string]any, error) {
	router := newRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orders", bytes.NewBufferString(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return rec.Code, nil, err
		}
	}
	return rec.Code, resp, nil
}

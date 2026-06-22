//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/stretchr/testify/require"
)

// Path #1 (dispatch half): after ingest creates a NEW row, running
// DispatchOutbox transitions it to PUBLISHED once the publisher confirm
// arrives from the real RabbitMQ broker.
func TestDispatchOutbox_PublishesAndMarksPublished(t *testing.T) {
	truncateAll(t)

	body, headers := pixBody("evt-dispatch-1", "prov-dispatch-1", "")
	rec, resp := postPayment(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	dispatcher, _ := newDispatch(10, 5, 100*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx, nil)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected outbox row to reach PUBLISHED")

	var row persistence.OutboxMessageModel
	require.NoError(t, suite.db.First(&row).Error)
	require.Equal(t, "PUBLISHED", row.Status)
	require.NotNil(t, row.PublishedAt)
}

// Path #5: while the broker connection backing the publisher is broken
// (simulated by closing the AMQP connection the publisher holds), publish
// attempts fail and the outbox row must stay NEW — never marked PUBLISHED
// without a confirm. Reconnecting and re-running dispatch then succeeds.
func TestDispatchOutbox_BrokerUnavailable_StaysNewUntilRecovered(t *testing.T) {
	truncateAll(t)

	body, headers := pixBody("evt-broker-down-1", "prov-broker-down-1", "")
	rec, resp := postPayment(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	// A publisher bound to a connection that is immediately closed: every
	// Publish call fails fast since p.conn.Channel() errors on a closed
	// connection, so the row can never reach PUBLISHED via this dispatcher.
	deadConn, err := amqpDial(t)
	require.NoError(t, err)
	require.NoError(t, deadConn.Close())

	badDispatcher, _ := newDispatchWithConn(deadConn, 10, 5, 100*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go badDispatcher.Run(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	cancel()

	require.Equal(t, int64(0), countOutboxByStatus("PUBLISHED"))
	require.True(t, countOutboxByStatus("NEW") == 1 || countOutboxByStatus("RETRYING") == 1,
		"row should remain undelivered (NEW or RETRYING), never PUBLISHED, while the broker is unreachable")

	// Recovery: dispatch again with the real shared (healthy) connection.
	goodDispatcher, _ := newDispatch(10, 5, 100*time.Millisecond, 24*time.Hour)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go goodDispatcher.Run(ctx2, nil)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected row to eventually publish once the broker is reachable again")
}

// Path #6: forcing every publish attempt to fail (by pointing the publisher
// at a dead connection for the whole run) must transition the row through
// NEW -> RETRYING -> ... -> DEAD_LETTER once retry_count reaches maxRetries.
func TestDispatchOutbox_MaxRetriesExceeded_DeadLetters(t *testing.T) {
	truncateAll(t)

	body, headers := pixBody("evt-deadletter-1", "prov-deadletter-1", "")
	rec, resp := postPayment(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	deadConn, err := amqpDial(t)
	require.NoError(t, err)
	require.NoError(t, deadConn.Close())

	const maxRetries = 3
	dispatcher, _ := newDispatchWithConn(deadConn, 10, maxRetries, 50*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx, nil)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOutboxByStatus("DEAD_LETTER") == 1
	})
	require.True(t, ok, "expected row to reach DEAD_LETTER after exceeding max retries")

	var row persistence.OutboxMessageModel
	require.NoError(t, suite.db.First(&row).Error)
	require.Equal(t, "DEAD_LETTER", row.Status)
	require.GreaterOrEqual(t, row.RetryCount, maxRetries-1)
	require.NotEmpty(t, row.LastError)
}

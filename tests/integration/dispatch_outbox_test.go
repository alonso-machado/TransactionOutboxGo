//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/stretchr/testify/require"
)

// After ingest creates a NEW order_outbox row, running DispatchOutbox
// transitions it to PUBLISHED once the publisher confirm arrives from the
// real RabbitMQ broker.
func TestDispatchOrderOutbox_PublishesAndMarksPublished(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-dispatch-1", "evt-dispatch-1", "TKT-dispatch-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	dispatcher, _ := newOrderDispatch(10, 5, 100*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx, nil)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOrderOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected order_outbox row to reach PUBLISHED")

	var row outboxRowFixture
	require.NoError(t, suite.db.Table("order_outbox").First(&row).Error)
	require.Equal(t, "PUBLISHED", row.Status)
	require.NotNil(t, row.PublishedAt)
}

// While the broker connection backing the publisher is broken (simulated by
// closing the AMQP connection the publisher holds), publish attempts fail
// and the outbox row must stay NEW — never marked PUBLISHED without a
// confirm. Reconnecting and re-running dispatch then succeeds.
func TestDispatchOrderOutbox_BrokerUnavailable_StaysNewUntilRecovered(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-broker-down-1", "evt-broker-down-1", "TKT-broker-down-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	// A publisher bound to a connection that is immediately closed: every
	// Publish call fails fast since p.conn.Channel() errors on a closed
	// connection, so the row can never reach PUBLISHED via this dispatcher.
	deadConn, err := amqpDial(t)
	require.NoError(t, err)
	require.NoError(t, deadConn.Close())

	badDispatcher, _ := newOrderDispatchWithConn(deadConn, 10, 5, 100*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go badDispatcher.Run(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	cancel()

	require.Equal(t, int64(0), countOrderOutboxByStatus("PUBLISHED"))
	require.True(t, countOrderOutboxByStatus("NEW") == 1 || countOrderOutboxByStatus("RETRYING") == 1,
		"row should remain undelivered (NEW or RETRYING), never PUBLISHED, while the broker is unreachable")

	// Recovery: dispatch again with the real shared (healthy) connection.
	goodDispatcher, _ := newOrderDispatch(10, 5, 100*time.Millisecond, 24*time.Hour)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go goodDispatcher.Run(ctx2, nil)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOrderOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected row to eventually publish once the broker is reachable again")
}

// Forcing every publish attempt to fail (by pointing the publisher at a dead
// connection for the whole run) must transition the row through
// NEW -> RETRYING -> ... -> DEAD_LETTER once retry_count reaches maxRetries.
func TestDispatchOrderOutbox_MaxRetriesExceeded_DeadLetters(t *testing.T) {
	truncateAll(t)

	body, headers := orderBody("order-deadletter-1", "evt-deadletter-1", "TKT-deadletter-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	deadConn, err := amqpDial(t)
	require.NoError(t, err)
	require.NoError(t, deadConn.Close())

	const maxRetries = 3
	dispatcher, _ := newOrderDispatchWithConn(deadConn, 10, maxRetries, 50*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx, nil)

	ok := waitFor(t, 10*time.Second, func() bool {
		return countOrderOutboxByStatus("DEAD_LETTER") == 1
	})
	require.True(t, ok, "expected row to reach DEAD_LETTER after exceeding max retries")

	var row outboxRowFixture
	require.NoError(t, suite.db.Table("order_outbox").First(&row).Error)
	require.Equal(t, "DEAD_LETTER", row.Status)
	require.GreaterOrEqual(t, row.RetryCount, maxRetries-1)
	require.NotEmpty(t, row.LastError)
}

// A LISTEN/NOTIFY trigger channel wakes Run's dispatch well before the poll
// interval fires — wire a real database.Listener as trigger (exactly how
// cmd/outbox-worker/main.go wires it against order_outbox) and set the poll
// interval far longer than this test's wait window, so a PUBLISHED row this
// fast can only be explained by the notify/debounce path, not the ticker.
func TestDispatchOrderOutbox_NotifyTrigger_DispatchesFasterThanPollInterval(t *testing.T) {
	truncateAll(t)

	listener := database.NewListener(suite.pgURI, "order_outbox_new")
	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	go listener.Run(listenerCtx)

	// Let the listener establish its LISTEN before the order_outbox insert
	// trigger fires the NOTIFY it needs to relay.
	time.Sleep(500 * time.Millisecond)

	dispatcher, _ := newOrderDispatch(10, 5, 10*time.Minute, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx, listener.Notify)

	body, headers := orderBody("order-notify-1", "evt-notify-1", "TKT-notify-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	ok := waitFor(t, 5*time.Second, func() bool {
		return countOrderOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected the NOTIFY trigger to dispatch well before the 10-minute poll interval")
}

// The metrics ticker inside Run fires every 5s and calls
// recordBacklogMetrics, which does two full-table COUNT(*) scans
// (CountPending/CountDeadLetter) purely to feed Grafana gauges — not
// otherwise exercised by any dispatch-focused test, since those all finish
// well under 5s. Leaving a NEW row and a DEAD_LETTER row both present and
// running Run long enough for at least one tick proves the ticker wiring and
// both repository calls it makes complete without error against a real,
// non-empty backlog.
func TestDispatchOutbox_MetricsTicker_RecordsBacklogWithoutError(t *testing.T) {
	truncateAll(t)

	// A dead-lettered row: broker unreachable, retries exhausted.
	deadBody, deadHeaders := orderBody("order-metrics-dead-1", "evt-metrics-dead-1", "TKT-metrics-dead-1", "")
	deadRec, _ := postOrder(t, deadBody, deadHeaders)
	require.Equal(t, http.StatusCreated, deadRec.Code)

	deadConn, err := amqpDial(t)
	require.NoError(t, err)
	require.NoError(t, deadConn.Close())

	const maxRetries = 2
	deadDispatcher, _ := newOrderDispatchWithConn(deadConn, 10, maxRetries, 50*time.Millisecond, 24*time.Hour)
	deadCtx, cancelDead := context.WithCancel(context.Background())
	go deadDispatcher.Run(deadCtx, nil)
	ok := waitFor(t, 10*time.Second, func() bool { return countOrderOutboxByStatus("DEAD_LETTER") == 1 })
	cancelDead()
	require.True(t, ok, "expected the first row to dead-letter before the metrics run")

	// A second, still-pending row — the poll interval below is deliberately
	// far longer than this test's wait, so it stays NEW throughout, keeping
	// CountPending's result non-zero for the metrics tick too.
	pendingBody, pendingHeaders := orderBody("order-metrics-pending-1", "evt-metrics-pending-1", "TKT-metrics-pending-1", "")
	pendingRec, _ := postOrder(t, pendingBody, pendingHeaders)
	require.Equal(t, http.StatusCreated, pendingRec.Code)

	dispatcher, _ := newOrderDispatch(10, 5, 10*time.Second, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Run(ctx, nil)
	time.Sleep(6 * time.Second) // past the 5s metrics tick at least once
	cancel()

	require.Equal(t, int64(1), countOrderOutboxByStatus("DEAD_LETTER"))
	require.Equal(t, int64(1), countOrderOutboxByStatus("NEW"))
}

//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/stretchr/testify/require"
)

// An order for a second shard (SPORTS/FOOTBALL, not the package-wide
// testEventType/testEventSubtype default) lands on ITS OWN queue after
// relay, not the CONCERT/ROCK one — proving (event_type, event_subtype)
// actually drives routing, not just that any queue receives it.
func TestRouting_DifferentShard_LandsOnItsOwnQueue(t *testing.T) {
	truncateAll(t)

	const eventType, eventSubtype = "SPORTS", "FOOTBALL"
	body := fmt.Sprintf(`{
		"sourceOrderId":"order-routing-1",
		"eventType":"%s","eventSubtype":"%s","eventId":"evt-routing-1",
		"venue":{"id":"venue-2","name":"Arena Sul","city":"Porto Alegre"},
		"tickets":[{"id":"TKT-routing-1","section":"B","row":"3","seat":"12","price":80.00,"currency":"BRL"}],
		"customer":{"name":"John Roe","email":"john@example.com","document":"98765432100"}
	}`, eventType, eventSubtype)

	rec, resp := postOrder(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	var row outboxRowFixture
	require.NoError(t, suite.db.Table("order_outbox").First(&row).Error)
	require.Equal(t, eventType, row.EventType)
	require.Equal(t, eventSubtype, row.EventSubtype)

	dispatcher, _ := newOrderDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx, nil)

	ok := waitFor(t, 9*time.Second, func() bool {
		return countOrderOutboxByStatus("PUBLISHED") == 1
	})
	require.True(t, ok, "expected the order to publish")

	// Peek (non-destructively) at the SPORTS/FOOTBALL queue — must have
	// exactly the message we sent; the CONCERT/ROCK queue must be empty.
	ch, err := suite.amqpConn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()

	msg, ok, err := ch.Get(rmq.QueueFor(rmq.OrderStream, eventType, eventSubtype), true)
	require.NoError(t, err)
	require.True(t, ok, "expected the message on the sports/football queue")
	require.Contains(t, string(msg.Body), "order-routing-1")

	otherQueue, ok, err := ch.Get(rmq.QueueFor(rmq.OrderStream, testEventType, testEventSubtype), true)
	require.NoError(t, err)
	require.False(t, ok, "expected the concert/rock queue to be empty, got %v", otherQueue)
}

// An (event_type, event_subtype) pair outside the registry has no bound
// queue — rejecting it at the HTTP boundary (rather than publishing into a
// topic-exchange black hole) is the whole point of ValidateEventType.
func TestRouting_UnknownEventTypeSubtype_Rejected(t *testing.T) {
	truncateAll(t)

	require.False(t, rmq.IsValidEventType("CONCERT", "OPERA"), "test assumes OPERA is not a registered CONCERT subtype")

	body := `{
		"sourceOrderId":"order-unknown-shard-1",
		"eventType":"CONCERT","eventSubtype":"OPERA","eventId":"evt-unknown-shard-1",
		"venue":{"id":"venue-3","name":"Teatro","city":"Rio de Janeiro"},
		"tickets":[{"id":"TKT-unknown-1","section":"A","row":"1","seat":"1","price":50.00,"currency":"BRL"}],
		"customer":{"name":"Jane Doe","email":"jane@example.com","document":"12345678900"}
	}`

	rec, _ := postOrder(t, body, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int64(0), countOrderOutboxByStatus("NEW"))
}

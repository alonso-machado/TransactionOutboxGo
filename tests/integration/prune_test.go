//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// PUBLISHED order_outbox rows older than the prune window must be removed by
// DeleteOldPublished, while recently-published rows survive. The
// DispatchOutbox.Run loop's internal prune ticker is hardcoded to 1 hour
// (not configurable), so this test calls the outbox repository's
// DeleteOldPublished directly — the same method DispatchOutbox.Run invokes
// on its own ticker — to exercise the exact pruning logic without waiting
// an hour in a test.
func TestOrderOutboxPruning_RemovesOldPublishedRowsOnly(t *testing.T) {
	truncateAll(t)

	// Ingest and publish a row, then backdate it to look old.
	body, headers := orderBody("order-prune-old-1", "evt-prune-old-1", "TKT-prune-old-1", "")
	rec, resp := postOrder(t, body, headers)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "accepted", resp["status"])

	dispatcher, outboxRepo := newOrderDispatch(10, 5, 50*time.Millisecond, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Run(ctx, nil)
	ok := waitFor(t, 10*time.Second, func() bool { return countOrderOutboxByStatus("PUBLISHED") == 1 })
	cancel()
	require.True(t, ok, "expected first row to publish before backdating")

	oldTime := time.Now().UTC().Add(-48 * time.Hour)
	require.NoError(t, suite.db.Table("order_outbox").
		Where("idempotency_key = ?", resp["idempotencyKey"]).
		Update("published_at", oldTime).Error)

	// Ingest and publish a second, recent row that must NOT be pruned.
	body2, headers2 := orderBody("order-prune-recent-1", "evt-prune-recent-1", "TKT-prune-recent-1", "")
	rec2, resp2 := postOrder(t, body2, headers2)
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, "accepted", resp2["status"])

	dispatcher2, _ := newOrderDispatch(10, 5, 50*time.Millisecond, time.Hour)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go dispatcher2.Run(ctx2, nil)
	ok2 := waitFor(t, 10*time.Second, func() bool { return countOrderOutboxByStatus("PUBLISHED") == 2 })
	require.True(t, ok2, "expected second row to publish")

	// Prune anything published more than 24h ago.
	require.NoError(t, outboxRepo.DeleteOldPublished(context.Background(), 24*time.Hour))

	var remaining []outboxRowFixture
	require.NoError(t, suite.db.Table("order_outbox").Find(&remaining).Error)
	require.Len(t, remaining, 1)
	require.Equal(t, resp2["idempotencyKey"], remaining[0].IdempotencyKey)
}

//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// insertDeadLetter writes a row already in the DEAD_LETTER terminal state, so
// the replay test has something to reset without driving a full
// publish-fail/max-retry cycle.
func insertDeadLetter(t *testing.T, method, key string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	row := &persistence.OutboxMessageModel{
		ID:             id,
		IdempotencyKey: key,
		AggregateType:  "payment",
		HTTPMethod:     "POST",
		Route:          "/api/v1/payments",
		Payload:        []byte(`{}`),
		Headers:        []byte(`{}`),
		Status:         string(domain.OutboxStatusDeadLetter),
		RetryCount:     5,
		LastError:      "broker unavailable",
		PaymentMethod:  method,
		CreatedAt:      time.Now().UTC(),
	}
	require.NoError(t, suite.db.Create(row).Error)
	return id
}

// Phase 5 Track 2.C: ReplayDeadLetters resets DEAD_LETTER rows back to NEW so
// the dispatch loop republishes them. Exercises the method filter, the
// per-row reset of status/retry_count/next_retry_at/last_error, and the
// no-rows short-circuit.
func TestReplayDeadLetters_ResetsRowsToNew(t *testing.T) {
	truncateAll(t)
	repo := persistence.NewOutboxRepository(suite.db, 0, 0)
	ctx := context.Background()

	pixID := insertDeadLetter(t, "PIX", "dl-replay-pix-1")
	insertDeadLetter(t, "BOLETO", "dl-replay-boleto-1")

	// Method-scoped replay touches only the PIX row.
	n, err := repo.ReplayDeadLetters(ctx, "PIX", 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, int64(1), countOutboxByStatus(domain.OutboxStatusNew))
	require.Equal(t, int64(1), countOutboxByStatus(domain.OutboxStatusDeadLetter))

	var m persistence.OutboxMessageModel
	require.NoError(t, suite.db.First(&m, "id = ?", pixID).Error)
	require.Equal(t, string(domain.OutboxStatusNew), m.Status)
	require.Equal(t, 0, m.RetryCount)
	require.Empty(t, m.LastError)
	require.Nil(t, m.NextRetryAt)

	// All-methods replay (method == "") catches the remaining BOLETO row.
	n, err = repo.ReplayDeadLetters(ctx, "", 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, int64(0), countOutboxByStatus(domain.OutboxStatusDeadLetter))

	// No dead letters left → the no-rows short-circuit returns 0.
	n, err = repo.ReplayDeadLetters(ctx, "", 100)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

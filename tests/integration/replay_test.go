//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// deadLetterRow is the insertable shape for a row already in the
// DEAD_LETTER terminal state, so the replay test has something to reset
// without driving a full publish-fail/max-retry cycle.
type deadLetterRow struct {
	ID             uuid.UUID `gorm:"column:id"`
	IdempotencyKey string    `gorm:"column:idempotency_key"`
	AggregateType  string    `gorm:"column:aggregate_type"`
	HTTPMethod     string    `gorm:"column:http_method"`
	Route          string
	Payload        []byte `gorm:"type:jsonb"`
	Status         string
	RetryCount     int    `gorm:"column:retry_count"`
	LastError      string `gorm:"column:last_error"`
	EventType      string `gorm:"column:event_type"`
	EventSubtype   string `gorm:"column:event_subtype"`
	CreatedAt      time.Time
}

func insertDeadLetter(t *testing.T, eventType, eventSubtype, key string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	row := &deadLetterRow{
		ID:             id,
		IdempotencyKey: key,
		AggregateType:  "order",
		HTTPMethod:     "POST",
		Route:          "/api/v1/orders",
		Payload:        []byte(`{}`),
		Status:         string(domain.OutboxStatusDeadLetter),
		RetryCount:     5,
		LastError:      "broker unavailable",
		EventType:      eventType,
		EventSubtype:   eventSubtype,
		CreatedAt:      time.Now().UTC(),
	}
	require.NoError(t, suite.db.Table("order_outbox").Create(row).Error)
	return id
}

// ReplayDeadLetters resets DEAD_LETTER rows back to NEW so the dispatch loop
// republishes them. Exercises the event-type filter, the per-row reset of
// status/retry_count/next_retry_at/last_error, and the no-rows short-circuit.
func TestReplayDeadLetters_ResetsRowsToNew(t *testing.T) {
	truncateAll(t)
	repo := newOrderOutboxRepo()
	ctx := context.Background()

	concertID := insertDeadLetter(t, "CONCERT", "ROCK", "dl-replay-concert-1")
	insertDeadLetter(t, "SPORTS", "FOOTBALL", "dl-replay-sports-1")

	// Event-type-scoped replay touches only the CONCERT row.
	n, err := repo.ReplayDeadLetters(ctx, "CONCERT", 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, int64(1), countOrderOutboxByStatus(domain.OutboxStatusNew))
	require.Equal(t, int64(1), countOrderOutboxByStatus(domain.OutboxStatusDeadLetter))

	var m outboxRowFixture
	require.NoError(t, suite.db.Table("order_outbox").Where("id = ?", concertID).First(&m).Error)
	require.Equal(t, string(domain.OutboxStatusNew), m.Status)
	require.Equal(t, 0, m.RetryCount)
	require.Empty(t, m.LastError)
	require.Nil(t, m.NextRetryAt)

	// All-event-types replay (eventType == "") catches the remaining row.
	n, err = repo.ReplayDeadLetters(ctx, "", 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, int64(0), countOrderOutboxByStatus(domain.OutboxStatusDeadLetter))

	// No dead letters left -> the no-rows short-circuit returns 0.
	n, err = repo.ReplayDeadLetters(ctx, "", 100)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

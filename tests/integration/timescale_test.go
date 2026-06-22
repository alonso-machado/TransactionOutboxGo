//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countTable counts rows in an arbitrary table/view by name — countPayments()
// (suite_test.go) only ever resolves to the "payments" UNION ALL view via
// PaymentModel's static TableName(), so per-method assertions need this.
func countTable(t *testing.T, name string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, suite.db.Table(name).Count(&n).Error)
	return n
}

// Phase 4 Track 2 (TimescaleDB): a PIX payment must land in the payments_pix
// hypertable specifically — not some other method's table — and be visible
// through the payments UNION ALL view, while every other method's table
// stays empty. This is the regression guard for "app routes the insert to
// the right per-method table" (persistence.tableFor).
func TestTimescale_PixPaymentLandsInPixTableAndView(t *testing.T) {
	truncateAll(t)

	paymentID := "018f0000-0000-7000-8000-000000000010"
	eventID := "evt-timescale-pix-1"
	msgID := "msgid-timescale-pix-1"
	publishRaw(t, msgID, consumerPayload(paymentID, eventID), nil)

	consumer := newConsumer("PIX", 10, 5)
	runCtx, runCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer runCancel()
	go func() { _ = consumer.Run(runCtx) }()

	ok := waitFor(t, 8*time.Second, func() bool {
		return countTable(t, "payments_pix") >= 1
	})
	require.True(t, ok, "expected payment to land in payments_pix")

	require.Equal(t, int64(1), countTable(t, "payments_pix"))
	require.Equal(t, int64(0), countTable(t, "payments_boleto"))
	require.Equal(t, int64(1), countPayments(), "expected the payments view to see the same row")
}

// Phase 4 Track 2's load-bearing regression: dedup on (source_message_id,
// occurred_at) — the two-column key TimescaleDB forces — must still hold
// across a redelivery of the SAME message (same occurred_at), exactly as
// the old single-column UNIQUE(source_message_id) did pre-Timescale.
func TestTimescale_RedeliveryDedupsOnSourceMessageIDAndOccurredAt(t *testing.T) {
	truncateAll(t)

	paymentID := "018f0000-0000-7000-8000-000000000011"
	eventID := "evt-timescale-dedup-1"
	msgID := "msgid-timescale-dedup-1"
	body := consumerPayload(paymentID, eventID) // fixed occurredAt baked into the body

	publishRaw(t, msgID, body, nil)
	publishRaw(t, msgID, body, nil) // redelivery: identical body -> identical occurred_at

	consumer := newConsumer("PIX", 10, 5)
	runCtx, runCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer runCancel()
	go func() { _ = consumer.Run(runCtx) }()

	ok := waitFor(t, 8*time.Second, func() bool {
		return countTable(t, "payments_pix") >= 1
	})
	require.True(t, ok, "expected at least one payments_pix row")

	time.Sleep(1 * time.Second) // let a possible second insert land before asserting
	require.Equal(t, int64(1), countTable(t, "payments_pix"), "redelivery must not create a second row")
}

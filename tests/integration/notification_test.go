//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/stretchr/testify/require"
)

// TestNotification_EndToEnd_TicketIssuedSendsEmail asserts the full
// Phase 8 email-delivery chain: fulfillment-consumer-worker enqueues a
// ticket_notification_outbox row in the SAME transaction as the ticket's
// MarkIssued (asserted immediately, no wait — this proves the atomicity
// claim, not just eventual delivery), then outbox-worker's third dispatch
// loop relays it, then notification-consumer-worker sends it through a
// recording fake domain.EmailSender.
func TestNotification_EndToEnd_TicketIssuedSendsEmail(t *testing.T) {
	truncateAll(t)

	_, ticket := issueOneTicket(t, "order-notif-1", "evt-notif-1", "TKT-notif-1")

	// The ticket's MarkIssued and the notification Enqueue share one
	// transaction (usecase/fulfillment.IssueTickets.confirmAndIssue) — by
	// the time issueOneTicket returns (it already waited for the ticket to
	// become VALID), the NEW notification row must already exist too, with
	// no additional wait.
	require.Equal(t, int64(1), countTicketNotificationOutboxByStatus(domain.OutboxStatusNew))

	// Relay ticket_notification_outbox -> the notification queue.
	notificationDispatcher, _ := newNotificationDispatch(10, 5, 50*time.Millisecond, 24*time.Hour)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	defer cancelDispatch()
	go notificationDispatcher.Run(dispatchCtx, nil)

	// Consume it with a recording fake sender instead of a real SMTP server.
	recorder := &recordingEmailSender{}
	consumer := newNotificationConsumer(recorder, 10, 5)
	consumeCtx, cancelConsume := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelConsume()
	go func() { _ = consumer.Run(consumeCtx) }()

	ok := waitFor(t, 9*time.Second, func() bool {
		return len(recorder.calls()) == 1
	})
	require.True(t, ok, "expected notification-consumer-worker to send exactly one email")

	sent := recorder.calls()[0]
	require.Equal(t, ticket.BuyerEmail, sent.ToEmail)
	require.Equal(t, ticket.BuyerName, sent.ToName)
	require.NotEmpty(t, sent.Attachment)
	require.Equal(t, "image/png", sent.AttachmentContentType)

	ok = waitFor(t, 9*time.Second, func() bool {
		return countTicketNotificationOutboxByStatus(domain.OutboxStatusPublished) == 1
	})
	require.True(t, ok, "expected the notification outbox row to be marked PUBLISHED")
}

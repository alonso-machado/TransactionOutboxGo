//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/stretchr/testify/require"
)

// TestNotification_EndToEnd_TicketIssuedSendsEmailSynchronously asserts the
// simplified (post-RabbitMQ) email-delivery chain: fulfillment-consumer-worker
// inserts a ticket_notifications row in the SAME transaction as the ticket's
// MarkIssued (asserted immediately, no wait — this proves the atomicity
// claim, not just eventual delivery), then emails the ticket synchronously,
// with no RabbitMQ hop at all.
func TestNotification_EndToEnd_TicketIssuedSendsEmailSynchronously(t *testing.T) {
	truncateAll(t)

	recorder := &recordingEmailSender{}
	_, ticket := issueOneTicketWithSender(t, "order-notif-1", "evt-notif-1", "TKT-notif-1", recorder)

	// notificationRepo.Create runs inside the SAME transaction as MarkIssued
	// (usecase/fulfillment.IssueTickets.confirmAndIssue) — by the time
	// issueOneTicketWithSender returns (it already waited for the ticket to
	// become VALID), the row is guaranteed to exist too, no additional wait
	// needed to prove that part of the atomicity claim.
	_ = getTicketNotification(t, ticket.ID)

	// The email send itself happens right after that transaction commits,
	// as a separate step (necessarily non-transactional — it's an SMTP
	// call, not a DB write) — still needs a short wait.
	ok := waitFor(t, 5*time.Second, func() bool {
		return len(recorder.calls()) == 1 && getTicketNotification(t, ticket.ID).EmailSentTimestamp != nil
	})
	require.True(t, ok, "expected fulfillment-consumer-worker to send exactly one email and record it")

	sent := recorder.calls()[0]
	require.Equal(t, ticket.BuyerEmail, sent.ToEmail)
	require.Equal(t, ticket.BuyerName, sent.ToName)
	require.NotEmpty(t, sent.Attachment)
	require.Equal(t, "image/png", sent.AttachmentContentType)

	row := getTicketNotification(t, ticket.ID)
	require.Empty(t, row.EmailSentError)
}

// flakyEmailSender fails its first failFirst calls, then succeeds — lets
// TestNotification_RetryCron_ResendsAfterInitialFailure simulate a
// transient SMTP outage without a real SMTP server.
type flakyEmailSender struct {
	mu        sync.Mutex
	failFirst int
	calls     int
}

func (s *flakyEmailSender) Send(_ domain.EmailRequest) (*domain.EmailResult, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n <= s.failFirst {
		return nil, fmt.Errorf("simulated smtp failure (call %d)", n)
	}
	return &domain.EmailResult{ProviderMessageID: "test"}, nil
}

// TestNotification_RetryCron_ResendsAfterInitialFailure asserts the retry
// path: fulfillment-consumer-worker's own immediate send fails, leaving
// ticket_notifications' row unsent with an error recorded; then
// notification-retry-cron's whole use-case
// (notification.SendTicketNotification.RetryPending, exercised directly —
// no RabbitMQ, no subprocess) finds it and resends successfully once its
// backoff window has passed.
func TestNotification_RetryCron_ResendsAfterInitialFailure(t *testing.T) {
	truncateAll(t)

	sender := &flakyEmailSender{failFirst: 1}
	_, ticket := issueOneTicketWithSender(t, "order-notif-retry-1", "evt-notif-retry-1", "TKT-notif-retry-1", sender)

	ok := waitFor(t, 5*time.Second, func() bool {
		row := getTicketNotification(t, ticket.ID)
		return row.EmailSentTimestamp == nil && row.EmailSentError != ""
	})
	require.True(t, ok, "expected the immediate send to fail and record an error")

	// MarkFailed schedules next_retry_at a short, jittered backoff out —
	// poll RetryPending (notification-retry-cron's whole job) until the
	// row's backoff window has passed and it actually gets retried, instead
	// of guessing a fixed sleep.
	retryUC := newSendTicketNotification(sender)
	var attempted int
	ok = waitFor(t, 5*time.Second, func() bool {
		n, err := retryUC.RetryPending(context.Background(), 50)
		require.NoError(t, err)
		attempted += n
		return attempted >= 1
	})
	require.True(t, ok, "expected notification-retry-cron to eventually retry the failed send")

	row := getTicketNotification(t, ticket.ID)
	require.NotNil(t, row.EmailSentTimestamp)
	require.Empty(t, row.EmailSentError)
}

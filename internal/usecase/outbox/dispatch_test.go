package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
)

// stubRepo counts FetchPending calls and otherwise behaves like an empty
// outbox, so a unit test can drive DispatchOutbox.Run's scheduling (the
// NOTIFY-trigger debounce path in particular) without a database.
type stubRepo struct{ fetches atomic.Int32 }

func (s *stubRepo) Enqueue(ctx context.Context, uow domain.UnitOfWork, msg *domain.OutboxMessage) (bool, error) {
	return false, nil
}
func (s *stubRepo) FetchPending(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	s.fetches.Add(1)
	return nil, nil
}
func (s *stubRepo) MarkPublished(ctx context.Context, id uuid.UUID, publishedAt time.Time) error {
	return nil
}
func (s *stubRepo) MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error {
	return nil
}
func (s *stubRepo) MarkDeadLetter(ctx context.Context, id uuid.UUID, lastError string) error {
	return nil
}
func (s *stubRepo) DeleteOldPublished(ctx context.Context, olderThan time.Duration) error {
	return nil
}
func (s *stubRepo) CountPending(ctx context.Context) (int64, error)    { return 0, nil }
func (s *stubRepo) CountDeadLetter(ctx context.Context) (int64, error) { return 0, nil }

type stubPublisher struct{}

func (stubPublisher) Publish(ctx context.Context, msg *domain.OutboxMessage) error { return nil }

// A NOTIFY trigger must cause a dispatch shortly after (via the debounce
// timer), independently of the much longer poll interval.
func TestRun_TriggerCausesDebouncedDispatch(t *testing.T) {
	repo := &stubRepo{}
	// Hour-long interval so the poll ticker can't be the cause of any dispatch.
	d := New(repo, stubPublisher{}, 10, 5, time.Hour, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trigger := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() { d.Run(ctx, trigger); close(done) }()

	trigger <- struct{}{}

	deadline := time.Now().Add(2 * time.Second)
	for repo.fetches.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if repo.fetches.Load() == 0 {
		t.Fatal("expected the trigger to cause a dispatch (FetchPending) before the poll interval")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// Cancelling while a debounce timer is pending must stop the timer and return
// cleanly (the ctx.Done branch that also tears the debounce down).
func TestRun_CancelWithPendingDebounce_ReturnsCleanly(t *testing.T) {
	repo := &stubRepo{}
	d := New(repo, stubPublisher{}, 10, 5, time.Hour, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	trigger := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() { d.Run(ctx, trigger); close(done) }()

	trigger <- struct{}{} // arm the 50ms debounce timer
	cancel()              // cancel immediately, before it fires

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation with a pending debounce")
	}
}

// Run must work with a nil trigger (NOTIFY not wired up): it falls back to the
// poll interval and dispatches on each tick.
func TestRun_NilTrigger_PollsOnInterval(t *testing.T) {
	repo := &stubRepo{}
	d := New(repo, stubPublisher{}, 10, 5, 20*time.Millisecond, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { d.Run(ctx, nil); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for repo.fetches.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if repo.fetches.Load() == 0 {
		t.Fatal("expected the poll interval to drive a dispatch with a nil trigger")
	}

	cancel()
	<-done
}

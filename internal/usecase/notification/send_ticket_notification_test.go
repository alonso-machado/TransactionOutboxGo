package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
)

// stubNotificationRepo is a minimal domain.TicketNotificationRepository —
// records MarkSent/MarkFailed calls and returns a fixed FetchPendingForRetry
// batch, so tests can drive Send/RetryPending without a database.
type stubNotificationRepo struct {
	pending     []*domain.TicketNotification
	fetchErr    error
	sentCalls   []uuid.UUID
	failedCalls []uuid.UUID
	failedErrs  []string
}

func (s *stubNotificationRepo) Create(context.Context, domain.UnitOfWork, uuid.UUID) error {
	return nil
}
func (s *stubNotificationRepo) FetchPendingForRetry(context.Context, int) ([]*domain.TicketNotification, error) {
	return s.pending, s.fetchErr
}
func (s *stubNotificationRepo) MarkSent(_ context.Context, ticketID uuid.UUID, _ time.Time) error {
	s.sentCalls = append(s.sentCalls, ticketID)
	return nil
}
func (s *stubNotificationRepo) MarkFailed(_ context.Context, ticketID uuid.UUID, lastError string) error {
	s.failedCalls = append(s.failedCalls, ticketID)
	s.failedErrs = append(s.failedErrs, lastError)
	return nil
}

// stubTicketRepo is a minimal domain.TicketRepository — only FindByID is
// exercised by this package, the rest are unused no-ops.
type stubTicketRepo struct {
	tickets map[uuid.UUID]*domain.Ticket
	findErr map[uuid.UUID]error
}

func (s *stubTicketRepo) ReserveForOrder(context.Context, domain.UnitOfWork, []*domain.Ticket) error {
	return nil
}
func (s *stubTicketRepo) FindByOrderID(context.Context, uuid.UUID) ([]*domain.Ticket, error) {
	return nil, nil
}
func (s *stubTicketRepo) FindByID(_ context.Context, id uuid.UUID) (*domain.Ticket, error) {
	if err, ok := s.findErr[id]; ok {
		return nil, err
	}
	t, ok := s.tickets[id]
	if !ok {
		return nil, errors.New("ticket not found")
	}
	return t, nil
}
func (s *stubTicketRepo) MarkIssued(context.Context, domain.UnitOfWork, *domain.Ticket) error {
	return nil
}
func (s *stubTicketRepo) MarkVoid(context.Context, domain.UnitOfWork, uuid.UUID) error { return nil }
func (s *stubTicketRepo) CheckIn(context.Context, domain.UnitOfWork, uuid.UUID, time.Time) (bool, error) {
	return false, nil
}
func (s *stubTicketRepo) UpdateHolderName(context.Context, domain.UnitOfWork, uuid.UUID, string) error {
	return nil
}

// stubSender is a minimal domain.EmailSender — records every request and
// optionally fails.
type stubSender struct {
	sendErr error
	calls   []domain.EmailRequest
}

func (s *stubSender) Send(req domain.EmailRequest) (*domain.EmailResult, error) {
	s.calls = append(s.calls, req)
	if s.sendErr != nil {
		return nil, s.sendErr
	}
	return &domain.EmailResult{ProviderMessageID: "test"}, nil
}

func testTicket(id uuid.UUID) *domain.Ticket {
	return &domain.Ticket{
		ID:         id,
		BuyerName:  "Jane Doe",
		BuyerEmail: "jane@example.com",
		Section:    "A",
		Row:        "1",
		Seat:       "2",
		QRPNG:      []byte("fake-png"),
	}
}

func TestSend_Success_MarksSentNotFailed(t *testing.T) {
	sender := &stubSender{}
	notifRepo := &stubNotificationRepo{}
	uc := New(sender, notifRepo, &stubTicketRepo{})

	ticketID := uuid.Must(uuid.NewV7())
	if err := uc.Send(context.Background(), testTicket(ticketID)); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}

	if len(sender.calls) != 1 {
		t.Fatalf("expected exactly one Send call, got %d", len(sender.calls))
	}
	if sender.calls[0].ToEmail != "jane@example.com" {
		t.Errorf("expected ToEmail to be the ticket's BuyerEmail, got %q", sender.calls[0].ToEmail)
	}
	if len(notifRepo.sentCalls) != 1 || notifRepo.sentCalls[0] != ticketID {
		t.Fatalf("expected MarkSent(%s), got %v", ticketID, notifRepo.sentCalls)
	}
	if len(notifRepo.failedCalls) != 0 {
		t.Fatalf("expected no MarkFailed calls, got %v", notifRepo.failedCalls)
	}
}

func TestSend_SenderError_MarksFailedAndReturnsError(t *testing.T) {
	sendErr := errors.New("smtp: connection refused")
	sender := &stubSender{sendErr: sendErr}
	notifRepo := &stubNotificationRepo{}
	uc := New(sender, notifRepo, &stubTicketRepo{})

	ticketID := uuid.Must(uuid.NewV7())
	err := uc.Send(context.Background(), testTicket(ticketID))
	if err == nil {
		t.Fatal("expected Send to return an error")
	}
	if len(notifRepo.failedCalls) != 1 || notifRepo.failedCalls[0] != ticketID {
		t.Fatalf("expected MarkFailed(%s), got %v", ticketID, notifRepo.failedCalls)
	}
	if len(notifRepo.sentCalls) != 0 {
		t.Fatalf("expected no MarkSent calls, got %v", notifRepo.sentCalls)
	}
}

func TestRetryPending_SendsEveryPendingTicket(t *testing.T) {
	idA, idB := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	sender := &stubSender{}
	notifRepo := &stubNotificationRepo{
		pending: []*domain.TicketNotification{{TicketID: idA}, {TicketID: idB}},
	}
	ticketRepo := &stubTicketRepo{tickets: map[uuid.UUID]*domain.Ticket{
		idA: testTicket(idA),
		idB: testTicket(idB),
	}}
	uc := New(sender, notifRepo, ticketRepo)

	attempted, err := uc.RetryPending(context.Background(), 50)
	if err != nil {
		t.Fatalf("RetryPending returned error: %v", err)
	}
	if attempted != 2 {
		t.Fatalf("expected 2 attempted, got %d", attempted)
	}
	if len(sender.calls) != 2 {
		t.Fatalf("expected 2 Send calls, got %d", len(sender.calls))
	}
	if len(notifRepo.sentCalls) != 2 {
		t.Fatalf("expected 2 MarkSent calls, got %d", len(notifRepo.sentCalls))
	}
}

// A ticket lookup failure for one row must not abort the whole retry batch —
// the cron should still attempt every other row it fetched.
func TestRetryPending_SkipsRowsWhoseTicketLookupFails(t *testing.T) {
	idOK, idMissing := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	sender := &stubSender{}
	notifRepo := &stubNotificationRepo{
		pending: []*domain.TicketNotification{{TicketID: idMissing}, {TicketID: idOK}},
	}
	ticketRepo := &stubTicketRepo{tickets: map[uuid.UUID]*domain.Ticket{
		idOK: testTicket(idOK),
	}}
	uc := New(sender, notifRepo, ticketRepo)

	attempted, err := uc.RetryPending(context.Background(), 50)
	if err != nil {
		t.Fatalf("RetryPending returned error: %v", err)
	}
	// attempted counts fetched rows, not successful sends — the missing
	// ticket is skipped before Send is ever called for it.
	if attempted != 2 {
		t.Fatalf("expected attempted to reflect the fetched batch size (2), got %d", attempted)
	}
	if len(sender.calls) != 1 {
		t.Fatalf("expected exactly one Send call (for the found ticket), got %d", len(sender.calls))
	}
	if sender.calls[0].ToEmail != testTicket(idOK).BuyerEmail {
		t.Errorf("expected the found ticket to be the one sent")
	}
}

func TestRetryPending_FetchError_ReturnsError(t *testing.T) {
	notifRepo := &stubNotificationRepo{fetchErr: errors.New("db unavailable")}
	uc := New(&stubSender{}, notifRepo, &stubTicketRepo{})

	if _, err := uc.RetryPending(context.Background(), 50); err == nil {
		t.Fatal("expected RetryPending to return an error when FetchPendingForRetry fails")
	}
}

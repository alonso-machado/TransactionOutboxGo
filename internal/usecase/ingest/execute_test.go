package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
)

// fakeUoW runs fn inline (no real transaction) unless err is set, in which
// case it short-circuits — letting a unit test exercise IngestPayment.Execute
// without any database.
type fakeUoW struct{ err error }

func (f fakeUoW) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if f.err != nil {
		return f.err
	}
	return fn(ctx)
}

// fakeOutboxRepo records the last enqueued message and returns canned results.
// Only Enqueue is exercised by Execute; the remaining methods satisfy the
// domain.OutboxRepository interface.
type fakeOutboxRepo struct {
	created  bool
	enqueErr error
	lastMsg  *domain.OutboxMessage
}

func (f *fakeOutboxRepo) Enqueue(ctx context.Context, uow domain.UnitOfWork, msg *domain.OutboxMessage) (bool, error) {
	f.lastMsg = msg
	return f.created, f.enqueErr
}
func (f *fakeOutboxRepo) FetchPending(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	return nil, nil
}
func (f *fakeOutboxRepo) MarkPublished(ctx context.Context, id uuid.UUID, publishedAt time.Time) error {
	return nil
}
func (f *fakeOutboxRepo) MarkRetrying(ctx context.Context, id uuid.UUID, lastError string) error {
	return nil
}
func (f *fakeOutboxRepo) MarkDeadLetter(ctx context.Context, id uuid.UUID, lastError string) error {
	return nil
}
func (f *fakeOutboxRepo) DeleteOldPublished(ctx context.Context, olderThan time.Duration) error {
	return nil
}
func (f *fakeOutboxRepo) CountPending(ctx context.Context) (int64, error)    { return 0, nil }
func (f *fakeOutboxRepo) CountDeadLetter(ctx context.Context) (int64, error) { return 0, nil }

func validRequest() Request {
	return Request{
		HTTPMethod:        "POST",
		Route:             "/api/v1/payments",
		EventID:           "evt-1",
		ProviderName:      "MERCADO_PAGO",
		ProviderPaymentID: "prov-1",
		ExternalPaymentID: "pay-1",
		Amount:            10050,
		Currency:          "BRL",
		Method:            "PIX",
		MethodDetails:     []byte(`{"endToEndId":"E1","txid":"t"}`),
		OccurredAt:        time.Now().UTC(),
		Headers:           map[string]string{"Idempotency-Key": "k"},
	}
}

func TestExecute_Created_StampsEnvelopeAndKey(t *testing.T) {
	repo := &fakeOutboxRepo{created: true}
	uc := New(repo, fakeUoW{})

	resp, err := uc.Execute(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Created {
		t.Fatal("expected Created=true")
	}
	if resp.PaymentID == uuid.Nil {
		t.Fatal("expected a generated PaymentID")
	}
	if len(resp.IdempotencyKey) != 64 {
		t.Fatalf("expected a 64-char idempotency key, got %d chars", len(resp.IdempotencyKey))
	}
	if repo.lastMsg == nil {
		t.Fatal("expected a message to be enqueued")
	}
	if repo.lastMsg.PaymentMethod != "PIX" {
		t.Fatalf("expected PaymentMethod PIX, got %q", repo.lastMsg.PaymentMethod)
	}
	if repo.lastMsg.Status != domain.OutboxStatusNew {
		t.Fatalf("expected status NEW, got %q", repo.lastMsg.Status)
	}
	if repo.lastMsg.Headers["schemaVersion"] != domain.SchemaVersion {
		t.Fatalf("expected schemaVersion header %q, got %q", domain.SchemaVersion, repo.lastMsg.Headers["schemaVersion"])
	}
	// The message id and the response payment id must be the same pre-generated UUID.
	if repo.lastMsg.ID != resp.PaymentID {
		t.Fatal("expected outbox message ID to equal the response PaymentID")
	}
}

func TestExecute_Duplicate_ReportsNotCreated(t *testing.T) {
	repo := &fakeOutboxRepo{created: false}
	uc := New(repo, fakeUoW{})

	resp, err := uc.Execute(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Created {
		t.Fatal("expected Created=false for a duplicate enqueue")
	}
}

func TestExecute_UoWError_Wrapped(t *testing.T) {
	repo := &fakeOutboxRepo{created: true}
	uc := New(repo, fakeUoW{err: errors.New("tx boom")})

	_, err := uc.Execute(context.Background(), validRequest())
	if err == nil {
		t.Fatal("expected an error when the unit of work fails")
	}
	if !strings.Contains(err.Error(), "enqueue outbox") {
		t.Fatalf("expected error wrapped with 'enqueue outbox', got %v", err)
	}
}

func TestExecute_MalformedMethodDetails_MarshalError(t *testing.T) {
	repo := &fakeOutboxRepo{created: true}
	uc := New(repo, fakeUoW{})

	req := validRequest()
	req.MethodDetails = []byte(`{not valid json`) // json.RawMessage marshalling validates this
	_, err := uc.Execute(context.Background(), req)
	if err == nil {
		t.Fatal("expected a marshal error for invalid MethodDetails JSON")
	}
	if !strings.Contains(err.Error(), "marshal payload") {
		t.Fatalf("expected error wrapped with 'marshal payload', got %v", err)
	}
}

package consume

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

// These error branches in Execute all return before ever touching
// uow/paymentRepo, so a nil ProcessMessage built without real
// infrastructure is enough to exercise them.
func newProcessMessageForValidationOnly() *ProcessMessage {
	return New(nil, nil)
}

func TestExecute_MalformedJSON_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	created, err := uc.Execute(context.Background(), "msg-1", []byte("not json"))
	if err == nil {
		t.Fatal("expected error for malformed JSON payload")
	}
	if created {
		t.Fatal("expected created=false on error")
	}
}

func TestExecute_InvalidPaymentID_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	body := []byte(`{"paymentId":"not-a-uuid","eventId":"evt-1"}`)
	_, err := uc.Execute(context.Background(), "msg-2", body)
	if err == nil {
		t.Fatal("expected error for invalid paymentId")
	}
}

func TestExecute_InvalidPayerID_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	body := []byte(`{"paymentId":"018f7f9e-6e8b-7c3a-8f2a-000000000001","payerId":"not-a-uuid"}`)
	_, err := uc.Execute(context.Background(), "msg-3", body)
	if err == nil {
		t.Fatal("expected error for invalid payerId")
	}
}

func TestExecute_InvalidRecipientID_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	body := []byte(`{"paymentId":"018f7f9e-6e8b-7c3a-8f2a-000000000001","recipientId":"not-a-uuid"}`)
	_, err := uc.Execute(context.Background(), "msg-4", body)
	if err == nil {
		t.Fatal("expected error for invalid recipientId")
	}
}

func TestExecute_UnknownSchemaVersion_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	body := []byte(`{"schemaVersion":"999","paymentId":"018f7f9e-6e8b-7c3a-8f2a-000000000001"}`)
	created, err := uc.Execute(context.Background(), "msg-5", body)
	if err == nil {
		t.Fatal("expected error for an unrecognized schema version")
	}
	if !errors.Is(err, ErrUnknownSchemaVersion) {
		t.Fatalf("expected ErrUnknownSchemaVersion, got %v", err)
	}
	if created {
		t.Fatal("expected created=false on error")
	}
}

// fakeUoW runs fn inline (no real transaction) unless err is set.
type fakeUoW struct{ err error }

func (f fakeUoW) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if f.err != nil {
		return f.err
	}
	return fn(ctx)
}

// fakePaymentRepo records the last saved payment and returns canned results,
// standing in for the real (source_message_id, occurred_at) UNIQUE
// constraint: created mirrors what a real INSERT ... ON CONFLICT DO NOTHING
// would report.
type fakePaymentRepo struct {
	created  bool
	saveErr  error
	lastSave *domain.Payment
}

func (f *fakePaymentRepo) Save(ctx context.Context, uow domain.UnitOfWork, p *domain.Payment) (bool, error) {
	f.lastSave = p
	return f.created, f.saveErr
}

func validPayloadBody() []byte {
	return []byte(`{
		"paymentId":"018f7f9e-6e8b-7c3a-8f2a-000000000001",
		"eventId":"evt-1",
		"providerName":"MERCADO_PAGO",
		"amount":10050,
		"currency":"BRL",
		"method":"PIX",
		"occurredAt":"2026-01-01T00:00:00Z"
	}`)
}

func TestExecute_FreshSave_ReturnsCreatedTrue(t *testing.T) {
	repo := &fakePaymentRepo{created: true}
	uc := New(repo, fakeUoW{})

	created, err := uc.Execute(context.Background(), "msg-fresh-1", validPayloadBody())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for a fresh save")
	}
	if repo.lastSave == nil || repo.lastSave.SourceMessageID != "msg-fresh-1" {
		t.Fatal("expected the payment to be saved with the message id as SourceMessageID")
	}
}

func TestExecute_DuplicateSave_ReturnsCreatedFalseNoError(t *testing.T) {
	repo := &fakePaymentRepo{created: false}
	uc := New(repo, fakeUoW{})

	created, err := uc.Execute(context.Background(), "msg-dup-1", validPayloadBody())
	if err != nil {
		t.Fatalf("expected a dedup conflict to be a successful no-op, got error: %v", err)
	}
	if created {
		t.Fatal("expected created=false for a duplicate (already-persisted) message")
	}
}

func TestExecute_SaveError_Wrapped(t *testing.T) {
	repo := &fakePaymentRepo{saveErr: errors.New("connection reset")}
	uc := New(repo, fakeUoW{})

	created, err := uc.Execute(context.Background(), "msg-err-1", validPayloadBody())
	if err == nil {
		t.Fatal("expected an error when Save fails")
	}
	if created {
		t.Fatal("expected created=false on error")
	}
	if !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("expected the underlying save error to propagate, got %v", err)
	}
}

func TestExecute_UoWError_Wrapped(t *testing.T) {
	repo := &fakePaymentRepo{created: true}
	uc := New(repo, fakeUoW{err: errors.New("tx boom")})

	created, err := uc.Execute(context.Background(), "msg-tx-1", validPayloadBody())
	if err == nil {
		t.Fatal("expected an error when the unit of work fails")
	}
	if created {
		t.Fatal("expected created=false on error")
	}
}

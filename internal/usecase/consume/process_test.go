package consume

import (
	"context"
	"testing"
)

// These error branches in Execute all return before ever touching
// uow/paymentRepo, so a nil ProcessMessage built without real
// infrastructure is enough to exercise them.
func newProcessMessageForValidationOnly() *ProcessMessage {
	return New(nil, nil)
}

func TestExecute_MalformedJSON_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	err := uc.Execute(context.Background(), "msg-1", []byte("not json"))
	if err == nil {
		t.Fatal("expected error for malformed JSON payload")
	}
}

func TestExecute_InvalidPaymentID_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	body := []byte(`{"paymentId":"not-a-uuid","eventId":"evt-1"}`)
	err := uc.Execute(context.Background(), "msg-2", body)
	if err == nil {
		t.Fatal("expected error for invalid paymentId")
	}
}

func TestExecute_InvalidPayerID_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	body := []byte(`{"paymentId":"018f7f9e-6e8b-7c3a-8f2a-000000000001","payerId":"not-a-uuid"}`)
	err := uc.Execute(context.Background(), "msg-3", body)
	if err == nil {
		t.Fatal("expected error for invalid payerId")
	}
}

func TestExecute_InvalidRecipientID_ReturnsError(t *testing.T) {
	uc := newProcessMessageForValidationOnly()
	body := []byte(`{"paymentId":"018f7f9e-6e8b-7c3a-8f2a-000000000001","recipientId":"not-a-uuid"}`)
	err := uc.Execute(context.Background(), "msg-4", body)
	if err == nil {
		t.Fatal("expected error for invalid recipientId")
	}
}

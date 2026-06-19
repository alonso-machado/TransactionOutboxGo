package handler

import (
	"encoding/json"
	"testing"
	"time"
)

func validDTO() PaymentEventRequestDTO {
	return PaymentEventRequestDTO{
		EventID:    "evt-1",
		Provider:   ProviderDTO{Name: "MERCADO_PAGO", ProviderPaymentID: "prov-1"},
		Payment:    PaymentDataDTO{PaymentID: "pay-1", Amount: 10.0, Currency: "BRL", Method: "PIX"},
		OccurredAt: time.Now(),
	}
}

func TestValidate_AllRequiredFieldsPresent_NoError(t *testing.T) {
	if err := validDTO().Validate(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidate_MissingFields_Errors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PaymentEventRequestDTO)
	}{
		{"eventId", func(d *PaymentEventRequestDTO) { d.EventID = "" }},
		{"provider.name", func(d *PaymentEventRequestDTO) { d.Provider.Name = "" }},
		{"provider.providerPaymentId", func(d *PaymentEventRequestDTO) { d.Provider.ProviderPaymentID = "" }},
		{"payment.paymentId", func(d *PaymentEventRequestDTO) { d.Payment.PaymentID = "" }},
		{"payment.amount", func(d *PaymentEventRequestDTO) { d.Payment.Amount = 0 }},
		{"payment.currency", func(d *PaymentEventRequestDTO) { d.Payment.Currency = "BR" }},
		{"payment.method", func(d *PaymentEventRequestDTO) { d.Payment.Method = "" }},
		{"occurredAt", func(d *PaymentEventRequestDTO) { d.OccurredAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dto := validDTO()
			tc.mutate(&dto)
			if err := dto.Validate(); err == nil {
				t.Fatalf("expected validation error for missing %s", tc.name)
			}
		})
	}
}

func TestValidateMethod_PIX_RequiresSiblingObject(t *testing.T) {
	dto := validDTO()
	dto.Payment.Method = "PIX"

	if err := dto.ValidateMethod(map[string]json.RawMessage{}); err == nil {
		t.Fatal("expected error when pix sibling object is missing")
	}

	raw := map[string]json.RawMessage{"pix": json.RawMessage(`{"endToEndId":"","txid":"t"}`)}
	if err := dto.ValidateMethod(raw); err == nil {
		t.Fatal("expected error when pix.endToEndId is empty")
	}

	raw["pix"] = json.RawMessage(`{"endToEndId":"E1","txid":"t"}`)
	if err := dto.ValidateMethod(raw); err != nil {
		t.Fatalf("expected valid pix details to pass, got %v", err)
	}

	raw["pix"] = json.RawMessage(`not-json`)
	if err := dto.ValidateMethod(raw); err == nil {
		t.Fatal("expected error when pix details are malformed JSON")
	}
}

func TestValidateMethod_BOLETO_RequiresSiblingObject(t *testing.T) {
	dto := validDTO()
	dto.Payment.Method = "BOLETO"

	if err := dto.ValidateMethod(map[string]json.RawMessage{}); err == nil {
		t.Fatal("expected error when boleto sibling object is missing")
	}

	raw := map[string]json.RawMessage{"boleto": json.RawMessage(`{"barcode":"","dueDate":"2026-01-01","payerDocument":"00000000000"}`)}
	if err := dto.ValidateMethod(raw); err == nil {
		t.Fatal("expected error when boleto.barcode is empty")
	}

	raw["boleto"] = json.RawMessage(`{"barcode":"123","dueDate":"","payerDocument":"00000000000"}`)
	if err := dto.ValidateMethod(raw); err == nil {
		t.Fatal("expected error when boleto.dueDate is empty")
	}

	raw["boleto"] = json.RawMessage(`{"barcode":"123","dueDate":"2026-01-01","payerDocument":""}`)
	if err := dto.ValidateMethod(raw); err == nil {
		t.Fatal("expected error when boleto.payerDocument is empty")
	}

	raw["boleto"] = json.RawMessage(`{"barcode":"123","dueDate":"2026-01-01","payerDocument":"00000000000"}`)
	if err := dto.ValidateMethod(raw); err != nil {
		t.Fatalf("expected valid boleto details to pass, got %v", err)
	}
}

func TestValidateMethod_TRANSFER_RequiresBothParties(t *testing.T) {
	dto := validDTO()
	dto.Payment.Method = "TRANSFER"

	if err := dto.ValidateMethod(nil); err == nil {
		t.Fatal("expected error when payerId/recipientId are both missing")
	}

	payer := "018f7f9e-6e8b-7c3a-8f2a-000000000001"
	dto.Payment.PayerID = &payer
	if err := dto.ValidateMethod(nil); err == nil {
		t.Fatal("expected error when recipientId is missing")
	}

	recipient := "018f7f9e-6e8b-7c3a-8f2a-000000000002"
	dto.Payment.RecipientID = &recipient
	if err := dto.ValidateMethod(nil); err != nil {
		t.Fatalf("expected valid transfer parties to pass, got %v", err)
	}
}

func TestValidateMethod_UnknownMethod_PassesUnvalidated(t *testing.T) {
	dto := validDTO()
	dto.Payment.Method = "CARD"
	if err := dto.ValidateMethod(nil); err != nil {
		t.Fatalf("expected unknown method to pass through unvalidated, got %v", err)
	}
}

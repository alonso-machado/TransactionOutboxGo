package handler

import (
	"encoding/json"
	"testing"
)

func TestMaskPAN_KeepsLast4(t *testing.T) {
	raw := json.RawMessage(`{"cardNumber":"4111111111111111","cardType":"CREDIT","cardIssuer":"VISA"}`)
	masked, err := maskPAN(raw)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	var card CardDetailsDTO
	if err := json.Unmarshal(masked, &card); err != nil {
		t.Fatalf("expected valid json, got %v", err)
	}
	if card.CardNumber != "************1111" {
		t.Fatalf("expected last-4 mask, got %q", card.CardNumber)
	}
	if card.CardType != "CREDIT" || card.CardIssuer != "VISA" {
		t.Fatalf("expected other fields untouched, got %+v", card)
	}
}

func TestMaskPAN_OddLength(t *testing.T) {
	raw := json.RawMessage(`{"cardNumber":"123456789012345","cardType":"DEBIT","cardIssuer":"MASTERCARD"}`)
	masked, err := maskPAN(raw)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	var card CardDetailsDTO
	if err := json.Unmarshal(masked, &card); err != nil {
		t.Fatalf("expected valid json, got %v", err)
	}
	if card.CardNumber != "***********2345" {
		t.Fatalf("expected last-4 mask, got %q", card.CardNumber)
	}
}

func TestMaskPAN_NonNumeric_ErrorsBeforeMasking(t *testing.T) {
	raw := json.RawMessage(`{"cardNumber":"4111-1111-1111-1111","cardType":"CREDIT","cardIssuer":"VISA"}`)
	if _, err := maskPAN(raw); err == nil {
		t.Fatal("expected error for non-numeric card number")
	}
}

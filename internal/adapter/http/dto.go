package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ProviderDTO identifies the upstream payment provider that emitted this event.
type ProviderDTO struct {
	Name              string `json:"name"`
	ProviderPaymentID string `json:"providerPaymentId"`
}

// PaymentDataDTO is the core payment fields shared by every payment method.
// PayerID/RecipientID are optional: a provider webhook describes a payment
// the provider already tracked between two parties, not necessarily two
// parties known to our own ledger.
type PaymentDataDTO struct {
	PaymentID   string  `json:"paymentId"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	Method      string  `json:"method"`
	PayerID     *string `json:"payerId,omitempty"`
	RecipientID *string `json:"recipientId,omitempty"`
}

// PaymentEventRequestDTO is the inbound HTTP body for POST/PUT/PATCH
// /api/v1/payments. It mirrors a payment provider's webhook notification
// shape (e.g. Mercado Pago PIX): a generic envelope (eventId, provider,
// payment, occurredAt) plus a method-specific sibling object (e.g. "pix")
// that the handler extracts dynamically based on payment.method, so adding
// a new method never requires a DTO change.
type PaymentEventRequestDTO struct {
	EventID    string         `json:"eventId"`
	Provider   ProviderDTO    `json:"provider"`
	Payment    PaymentDataDTO `json:"payment"`
	OccurredAt time.Time      `json:"occurredAt"`
}

func (dto PaymentEventRequestDTO) Validate() error {
	switch {
	case dto.EventID == "":
		return errors.New("eventId is required")
	case dto.Provider.Name == "":
		return errors.New("provider.name is required")
	case dto.Provider.ProviderPaymentID == "":
		return errors.New("provider.providerPaymentId is required")
	case dto.Payment.PaymentID == "":
		return errors.New("payment.paymentId is required")
	case dto.Payment.Amount <= 0:
		return errors.New("payment.amount must be > 0")
	case len(dto.Payment.Currency) != 3:
		return errors.New("payment.currency must be a 3-letter ISO 4217 code")
	case dto.Payment.Method == "":
		return errors.New("payment.method is required")
	case dto.OccurredAt.IsZero():
		return errors.New("occurredAt is required")
	default:
		return nil
	}
}

// PixDetailsDTO is the "pix" sibling object required when payment.method == "PIX".
type PixDetailsDTO struct {
	EndToEndID string `json:"endToEndId"`
	Txid       string `json:"txid"`
}

func (d PixDetailsDTO) Validate() error {
	switch {
	case d.EndToEndID == "":
		return errors.New("pix.endToEndId is required")
	case d.Txid == "":
		return errors.New("pix.txid is required")
	default:
		return nil
	}
}

// BoletoDetailsDTO is the "boleto" sibling object required when payment.method == "BOLETO".
type BoletoDetailsDTO struct {
	Barcode       string `json:"barcode"`
	DueDate       string `json:"dueDate"`
	PayerDocument string `json:"payerDocument"`
}

func (d BoletoDetailsDTO) Validate() error {
	switch {
	case d.Barcode == "":
		return errors.New("boleto.barcode is required")
	case d.DueDate == "":
		return errors.New("boleto.dueDate is required")
	case d.PayerDocument == "":
		return errors.New("boleto.payerDocument is required")
	default:
		return nil
	}
}

// ValidateMethod applies method-specific rules on top of Validate's generic
// envelope checks. TRANSFER is an internally-originated method (no external
// provider event drives it) and requires both parties to be known; PIX and
// BOLETO require their respective sibling detail object, looked up in raw by
// the lowercased method name. Unknown methods pass through unvalidated —
// that's the point of the polymorphic MethodDetails design: new methods
// don't require a code change here.
func (dto PaymentEventRequestDTO) ValidateMethod(raw map[string]json.RawMessage) error {
	switch strings.ToUpper(dto.Payment.Method) {
	case "PIX":
		details, ok := raw["pix"]
		if !ok {
			return errors.New("pix details are required for method PIX")
		}
		var pix PixDetailsDTO
		if err := json.Unmarshal(details, &pix); err != nil {
			return fmt.Errorf("invalid pix details: %w", err)
		}
		return pix.Validate()
	case "BOLETO":
		details, ok := raw["boleto"]
		if !ok {
			return errors.New("boleto details are required for method BOLETO")
		}
		var boleto BoletoDetailsDTO
		if err := json.Unmarshal(details, &boleto); err != nil {
			return fmt.Errorf("invalid boleto details: %w", err)
		}
		return boleto.Validate()
	case "TRANSFER":
		switch {
		case dto.Payment.PayerID == nil || *dto.Payment.PayerID == "":
			return errors.New("payment.payerId is required for method TRANSFER")
		case dto.Payment.RecipientID == nil || *dto.Payment.RecipientID == "":
			return errors.New("payment.recipientId is required for method TRANSFER")
		default:
			return nil
		}
	default:
		return nil
	}
}

// PaymentResponseDTO is the 201 Created response body.
type PaymentResponseDTO struct {
	PaymentID      string `json:"paymentId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Status         string `json:"status"` // "accepted" | "duplicate"
}

// ErrorResponseDTO is the standard error body returned on 4xx/5xx responses.
type ErrorResponseDTO struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

// PaymentRequest is the swagger-facing alias for the inbound payment event
// body documented by PaymentEventRequestDTO (eventId, provider, payment,
// occurredAt, plus a method-specific sibling object such as "pix" or
// "boleto" — see PixDetailsDTO / BoletoDetailsDTO).
//
//	@Description	Inbound payment event. Mirrors a payment-provider webhook: a generic envelope
//	@Description	(eventId, provider, payment, occurredAt) plus a method-specific sibling object
//	@Description	(e.g. "pix" or "boleto") named after payment.method lowercased.
type PaymentRequest = PaymentEventRequestDTO

// PaymentResponse is the swagger-facing alias for PaymentResponseDTO.
type PaymentResponse = PaymentResponseDTO

// ErrorResponse is the swagger-facing alias for ErrorResponseDTO.
type ErrorResponse = ErrorResponseDTO

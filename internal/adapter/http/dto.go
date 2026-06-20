package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
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

// CardDetailsDTO is the "cartao_credito"/"cartao_debito" sibling object
// required when payment.method is one of the card methods. cardNumber is
// the raw PAN as received from the client — it is masked to last-4 at the
// HTTP boundary (see maskPAN in card.go) before it ever reaches ingest, so
// the full number is never persisted, published, or logged.
type CardDetailsDTO struct {
	CardNumber string `json:"cardNumber"`
	CardType   string `json:"cardType"`
	CardIssuer string `json:"cardIssuer"`
}

var cardIssuers = map[string]struct{}{
	"VISA":       {},
	"MASTERCARD": {},
	"AMERICAN":   {},
}

// cardTypeForMethod maps a card method to the cardType it must declare.
var cardTypeForMethod = map[string]string{
	"CARTAO_CREDITO": "CREDIT",
	"CARTAO_DEBITO":  "DEBIT",
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// Validate checks card.cardNumber/cardType/cardIssuer, including that
// cardType is consistent with method (CARTAO_CREDITO ⇒ CREDIT,
// CARTAO_DEBITO ⇒ DEBIT). cardNumber's value is never echoed in an error.
func (d CardDetailsDTO) Validate(method string) error {
	switch {
	case !isDigits(d.CardNumber) || len(d.CardNumber) < 13 || len(d.CardNumber) > 19:
		return errors.New("card.cardNumber must be 13-19 digits")
	case d.CardType != cardTypeForMethod[method]:
		return fmt.Errorf("card.cardType must be %q for method %s", cardTypeForMethod[method], method)
	default:
		if _, ok := cardIssuers[d.CardIssuer]; !ok {
			return errors.New("card.cardIssuer must be one of VISA, MASTERCARD, AMERICAN")
		}
		return nil
	}
}

// ValidateMethod applies method-specific rules on top of Validate's generic
// envelope checks. TRANSFER is an internally-originated method (no external
// provider event drives it) and requires both parties to be known; PIX and
// BOLETO require their respective sibling detail object, looked up in raw by
// the lowercased method name. A method outside rmq.Methods is rejected here:
// since Phase 3 routes by method onto a dedicated queue, a method with no
// bound queue would be published to the topic exchange, match no binding,
// and be silently dropped — rejecting at ingest avoids that black hole.
func (dto PaymentEventRequestDTO) ValidateMethod(raw map[string]json.RawMessage) error {
	method := strings.ToUpper(dto.Payment.Method)
	if !rmq.IsValidMethod(method) {
		return fmt.Errorf("payment.method %q is not supported (expected one of: %v)", dto.Payment.Method, rmq.Methods)
	}

	switch method {
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
	case "CARTAO_CREDITO", "CARTAO_DEBITO":
		sibling := strings.ToLower(method)
		details, ok := raw[sibling]
		if !ok {
			return fmt.Errorf("%s details are required for method %s", sibling, method)
		}
		var card CardDetailsDTO
		if err := json.Unmarshal(details, &card); err != nil {
			return fmt.Errorf("invalid %s details: %w", sibling, err)
		}
		return card.Validate(method)
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

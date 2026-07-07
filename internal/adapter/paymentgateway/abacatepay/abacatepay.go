// Package abacatepay is a scaffold for a future domain.PaymentGateway
// adapter to AbacatePay — selectable via PAYMENT_PROVIDER=abacatepay but not
// yet implemented. Wire it up the same way internal/adapter/paymentgateway/
// stripe is wired (CreateCheckout opens a hosted checkout, VerifyWebhook
// authenticates and normalizes a webhook delivery) once needed. The
// CheckoutRequest/CheckoutResponseEnvelope/WebhookPayload types below
// document the wire format researched from the docs below;
// CreateCheckout/VerifyWebhook return ErrNotImplemented and make no
// network calls or third-party SDK imports — a real implementation only
// needs net/http against these shapes.
//
// Docs: https://docs.abacatepay.com (API reference + glossary),
// https://github.com/AbacatePay/documentation (source of the official
// docs). Confirm field names against current docs before implementing —
// this was researched, not built or tested against the live API; docs.abacatepay.com
// itself was unreachable during research (403), so this is reconstructed
// from AbacatePay's llms.txt summary and third-party integration guides.
//
// Base URL: https://api.abacatepay.com/v1. Auth: Bearer API key. Every
// response is wrapped in the envelope below regardless of endpoint;
// monetary values are minor units (centavos), same convention as
// domain.ChargeRequest.Amount.
//
// CreateCheckout → POST /checkouts/create. Response's Data.URL is the
// hosted checkout URL. EventType/EventSubtype round-trip via
// CheckoutRequest.Metadata; ExternalID carries our OrderID (AbacatePay
// requires each Items[].ID to reference a product already registered in
// the dashboard — there is no free-form line-item price field here, unlike
// Stripe/Mercado Pago, so a real implementation would need a
// product-per-ticket-price mapping step or a single generic "ticket
// order" product plus a custom Metadata amount override — confirm current
// API support before implementing).
//
// VerifyWebhook → AbacatePay POSTs a WebhookPayload to the configured
// webhook URL on a billing status change. "billing.paid" →
// PaymentOutcomeConfirmed; a failed/expired equivalent event (exact name
// unconfirmed — docs.abacatepay.com's webhook event list was not reachable
// during research) → PaymentOutcomeFailed. Payloads are signed via HMAC
// with the configured webhook secret, but the exact header name and
// digest encoding weren't confirmed from available docs — check the
// current "Webhooks" page before implementing signature verification.
package abacatepay

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("abacatepay: not implemented")

// CheckoutRequest is the POST /checkouts/create request body.
type CheckoutRequest struct {
	Items         []CheckoutItem    `json:"items"`
	Methods       []string          `json:"methods,omitempty"` // "PIX" | "CARD"
	CustomerID    string            `json:"customerId,omitempty"`
	ReturnURL     string            `json:"returnUrl,omitempty"`
	CompletionURL string            `json:"completionUrl,omitempty"`
	Coupons       []string          `json:"coupons,omitempty"`
	ExternalID    string            `json:"externalId,omitempty"` // our domain.ChargeRequest.OrderID
	Metadata      map[string]string `json:"metadata,omitempty"`   // event_type/event_subtype
}

type CheckoutItem struct {
	ID       string `json:"id"` // pre-registered product id, not a free-form price
	Quantity int    `json:"quantity"`
}

// CheckoutResponseEnvelope wraps every AbacatePay API response.
type CheckoutResponseEnvelope struct {
	Data    *CheckoutResponse `json:"data"`
	Success bool              `json:"success"`
	Error   *string           `json:"error"`
}

type CheckoutResponse struct {
	ID     string `json:"id"`  // -> CheckoutSession.ProviderRef
	URL    string `json:"url"` // -> CheckoutSession.CheckoutURL
	Status string `json:"status"`
	Amount int64  `json:"amount"` // minor units
}

// WebhookPayload is the body AbacatePay POSTs on a billing status change.
type WebhookPayload struct {
	Event string             `json:"event"` // e.g. "billing.paid"
	Data  WebhookPayloadData `json:"data"`
}

type WebhookPayloadData struct {
	Billing WebhookBilling `json:"billing"`
}

type WebhookBilling struct {
	ID     string `json:"id"` // -> PaymentEvent.ProviderRef
	Status string `json:"status"`
	Amount int64  `json:"amount"`
}

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

// Package pagarme is a scaffold for a future domain.PaymentGateway adapter
// to Pagar.me — selectable via PAYMENT_PROVIDER=pagarme but not yet
// implemented. Wire it up the same way internal/adapter/paymentgateway/
// stripe is wired (CreateCheckout opens a hosted checkout, VerifyWebhook
// authenticates and normalizes a webhook delivery) once needed. The
// OrderRequest/OrderResponse/WebhookPayload types below document the wire
// format researched from the docs below; CreateCheckout/VerifyWebhook
// return ErrNotImplemented and make no network calls or third-party SDK
// imports — a real implementation only needs net/http against these shapes.
//
// Docs: https://docs.pagar.me/reference/pedidos-1 (create order),
// https://docs.pagar.me/reference/eventos-de-webhook-1 (event types),
// https://docs.pagar.me/reference/exemplo-de-webhook-1 (webhook example),
// https://docs.pagar.me/docs/webhooks (webhook setup). Confirm field names
// against current docs before implementing — this was researched, not
// built or tested against the live API.
//
// Base URL: https://api.pagar.me/core/v5. Auth: HTTP Basic, the secret API
// key as username, blank password.
//
// CreateCheckout → POST /core/v5/orders with payments[].payment_method set
// to open the hosted "Pagar.me Checkout" page (there is also a separate
// Link de Pagamento / checkout-link product, not modeled here).
// EventType/EventSubtype round-trip via OrderRequest.Metadata, the same
// trick used by the stripe/mercadopago adapters.
//
// VerifyWebhook → Pagar.me POSTs a WebhookPayload to the configured
// webhook URL. "order.paid"/"charge.paid" → PaymentOutcomeConfirmed;
// "order.payment_failed"/"charge.payment_failed" → PaymentOutcomeFailed.
// WebhookPayload.Data's shape depends on Type (an order or a charge
// object), hence the map[string]any rather than a fixed struct. Pagar.me's
// webhook authenticity model is HTTP Basic Auth embedded in the registered
// webhook URL itself, not a signed-body header like Stripe's
// Stripe-Signature — confirm the current recommended scheme in the
// "Webhooks" docs before implementing VerifyWebhook.
package pagarme

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("pagarme: not implemented")

// OrderRequest is the POST /core/v5/orders request body.
type OrderRequest struct {
	Code     string            `json:"code"` // our domain.ChargeRequest.OrderID
	Customer OrderCustomer     `json:"customer"`
	Items    []OrderItem       `json:"items"`
	Payments []OrderPayment    `json:"payments"`
	Closed   bool              `json:"closed"`
	Metadata map[string]string `json:"metadata,omitempty"` // event_type/event_subtype
}

type OrderCustomer struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Document string `json:"document"`
	Type     string `json:"type"` // "individual"
}

// OrderItem amounts are in minor units (centavos), same convention as
// domain.ChargeRequest.Amount.
type OrderItem struct {
	Amount      int64  `json:"amount"`
	Description string `json:"description"`
	Quantity    int    `json:"quantity"`
	Code        string `json:"code"`
}

type OrderPayment struct {
	PaymentMethod string         `json:"payment_method"` // "checkout"
	Checkout      *OrderCheckout `json:"checkout,omitempty"`
}

type OrderCheckout struct {
	SuccessURL string `json:"success_url"` // domain.ChargeRequest.SuccessURL
}

// OrderResponse is the POST /core/v5/orders response body.
type OrderResponse struct {
	ID        string              `json:"id"` // "or_..." -> CheckoutSession.ProviderRef
	Code      string              `json:"code"`
	Status    string              `json:"status"` // pending|paid|canceled|failed
	Charges   []OrderCharge       `json:"charges"`
	Checkouts []OrderCheckoutLink `json:"checkouts"` // [0].PaymentURL -> CheckoutSession.CheckoutURL
}

type OrderCharge struct {
	ID     string `json:"id"` // "ch_..."
	Status string `json:"status"`
}

type OrderCheckoutLink struct {
	PaymentURL string `json:"payment_url"`
}

// WebhookPayload is the body Pagar.me POSTs to the configured webhook URL.
// Data's shape depends on Type: an order object for "order.*" events, a
// charge object for "charge.*" events.
type WebhookPayload struct {
	ID        string         `json:"id"` // "hook_..." -> PaymentEvent.RawEventID
	Account   WebhookAccount `json:"account"`
	Type      string         `json:"type"` // order.paid | order.payment_failed | charge.paid | charge.payment_failed | ...
	CreatedAt string         `json:"created_at"`
	Data      map[string]any `json:"data"`
}

type WebhookAccount struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

// Package pagseguro is a scaffold for a future domain.PaymentGateway adapter
// to PagSeguro/PagBank — selectable via PAYMENT_PROVIDER=pagseguro but not
// yet implemented. Wire it up the same way internal/adapter/paymentgateway/
// stripe is wired (CreateCheckout opens a hosted checkout, VerifyWebhook
// authenticates and normalizes a webhook delivery) once needed. The
// CheckoutRequest/CheckoutResponse/WebhookPayload types below document the
// wire format researched from the docs below; CreateCheckout/VerifyWebhook
// return ErrNotImplemented and make no network calls or third-party SDK
// imports — a real implementation only needs net/http against these
// shapes.
//
// Docs: https://developer.pagbank.com.br/docs/checkout (create checkout),
// https://developer.pagbank.com.br/reference/webhooks-checkout (webhook
// payload + signature). Confirm field names against current docs before
// implementing — this was researched, not built or tested against the
// live API.
//
// Base URL: https://api.pagseguro.com (sandbox:
// https://sandbox.api.pagseguro.com). Auth: Bearer API token.
//
// CreateCheckout → POST /checkouts. The checkout URL is
// CheckoutResponse.Links[] where Rel == "PAY". EventType/EventSubtype have
// no first-class metadata field in this API — would need to round-trip via
// ReferenceID encoding or a custom-fields equivalent (confirm current API
// support before implementing).
//
// VerifyWebhook → PagBank POSTs a WebhookPayload to each
// notification_urls entry. "PAID" → PaymentOutcomeConfirmed;
// "DECLINED"/"CANCELED"/"EXPIRED" → PaymentOutcomeFailed. WebhookPayload.ID
// → PaymentEvent.ProviderRef. PagBank does not appear to send a separate
// webhook-delivery id distinct from the order/charge id, so RawEventID may
// need to be derived (e.g. ID + Status + PaidAt) to dedupe redelivered
// notifications, rather than relying on a single provider-issued event id
// like Stripe's. Signature: x-payload-signature header (base64), verified
// by fetching PagBank's public key from GET /public-keys and checking a
// SHA256 signature over the raw request body with that key (asymmetric,
// not an HMAC shared secret like Stripe/Mercado Pago/Lemon Squeezy).
package pagseguro

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("pagseguro: not implemented")

// CheckoutRequest is the POST /checkouts request body.
type CheckoutRequest struct {
	ReferenceID      string                  `json:"reference_id"` // our domain.ChargeRequest.OrderID
	Items            []CheckoutItem          `json:"items"`
	Customer         *CheckoutCustomer       `json:"customer,omitempty"`
	RedirectURL      string                  `json:"redirect_url,omitempty"` // domain.ChargeRequest.SuccessURL
	NotificationURLs []string                `json:"notification_urls,omitempty"`
	PaymentMethods   []CheckoutPaymentMethod `json:"payment_methods,omitempty"`
}

// CheckoutItem.UnitAmount is in minor units (cents).
type CheckoutItem struct {
	Name       string `json:"name"`
	Quantity   int    `json:"quantity"`
	UnitAmount int64  `json:"unit_amount"`
}

type CheckoutCustomer struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	TaxID string `json:"tax_id"`
}

type CheckoutPaymentMethod struct {
	Type string `json:"type"` // CREDIT_CARD | PIX | BOLETO | ...
}

// CheckoutResponse is the POST /checkouts response body.
type CheckoutResponse struct {
	ID          string         `json:"id"` // "CHKT_..." -> CheckoutSession.ProviderRef
	ReferenceID string         `json:"reference_id"`
	Status      string         `json:"status"` // "ACTIVE"
	Links       []CheckoutLink `json:"links"`  // Rel == "PAY" -> CheckoutSession.CheckoutURL
}

type CheckoutLink struct {
	Rel    string `json:"rel"` // "PAY" | "SELF"
	Href   string `json:"href"`
	Method string `json:"method"`
}

// WebhookPayload is the body PagBank POSTs to each notification_urls
// entry on an order/charge status change.
type WebhookPayload struct {
	ID          string          `json:"id"` // "ORDE_..." or "CHAR_..." -> PaymentEvent.ProviderRef
	ReferenceID string          `json:"reference_id"`
	Status      string          `json:"status"` // PAID|DECLINED|CANCELED|IN_ANALYSIS|WAITING|EXPIRED
	Charges     []WebhookCharge `json:"charges"`
	CreatedAt   string          `json:"created_at"`
	PaidAt      string          `json:"paid_at"`
}

type WebhookCharge struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

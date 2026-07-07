// Package sumup is a scaffold for a future domain.PaymentGateway adapter to
// SumUp — selectable via PAYMENT_PROVIDER=sumup but not yet implemented.
// Wire it up the same way internal/adapter/paymentgateway/stripe is wired
// (CreateCheckout opens a hosted checkout, VerifyWebhook authenticates and
// normalizes a webhook delivery) once needed. The
// CheckoutRequest/CheckoutResponse/WebhookPayload types below document the
// wire format researched from the docs below; CreateCheckout/VerifyWebhook
// return ErrNotImplemented and make no network calls or third-party SDK
// imports — a real implementation only needs net/http against these
// shapes.
//
// Docs: https://developer.sumup.com/api/checkouts/create (create
// checkout), https://developer.sumup.com/online-payments/webhooks
// (webhook payload + subscribing via return_url). Confirm field names
// against current docs before implementing — this was researched, not
// built or tested against the live API. Not Brazilian-specific — a
// pan-European/global card-first gateway, included alongside the Brazilian
// options as a general-purpose card-processing alternative.
//
// Base URL: https://api.sumup.com/v0.1. Auth: Bearer OAuth token
// (SumUp uses OAuth2 client-credentials, not a static secret key like
// Stripe/Pagar.me — a real implementation needs a token refresh flow too).
//
// CreateCheckout → POST /checkouts. Amount/currency are in major units
// (e.g. 150.00), unlike domain.ChargeRequest.Amount's minor units —
// convert at the adapter boundary. There is no hosted-checkout-URL field
// in the response the way Stripe/Mercado Pago/PagBank return one; SumUp's
// "Hosted Checkout" product renders its own page keyed by
// CheckoutResponse.ID, so CheckoutSession.CheckoutURL would need to be
// constructed as a SumUp-hosted-checkout URL template around that ID —
// confirm the current hosted-checkout URL shape before implementing.
// EventType/EventSubtype have no metadata field in this API at all; would
// need to be encoded into CheckoutReference (e.g.
// "<orderID>:<type>:<subtype>") and parsed back out, since there's nowhere
// else to round-trip them.
//
// VerifyWebhook → SumUp POSTs a thin WebhookPayload pointer, similar to
// Mercado Pago's: just an EventType and the changed Checkout's ID. The
// handler must make a second outbound call, GET /checkouts/{ID} (Bearer
// token), to read the real CheckoutResponse.Status ("PAID" →
// PaymentOutcomeConfirmed; "FAILED"/"EXPIRED" → PaymentOutcomeFailed).
// RawEventID: SumUp's webhook body has no delivery/event id distinct from
// the checkout id itself (confirm before implementing — may need to dedupe
// on ID + the fetched Status instead of a single provider event id, same
// caveat as PagBank). Signature: an x-payload-signature header, HMAC-SHA256
// per SumUp's docs, but the exact encoding (hex/base64) and whether it's a
// shared secret or SumUp's public key were not confirmed from available
// docs during research — check the current "Webhooks" and
// "Webhook signature validation" pages before implementing.
package sumup

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("sumup: not implemented")

// CheckoutRequest is the POST /checkouts request body.
type CheckoutRequest struct {
	CheckoutReference string  `json:"checkout_reference"` // max 90 chars; our OrderID (+ event_type/subtype encoded in, see doc above)
	Amount            float64 `json:"amount"`             // major units
	Currency          string  `json:"currency"`
	MerchantCode      string  `json:"merchant_code"`
	Description       string  `json:"description,omitempty"`
	ReturnURL         string  `json:"return_url,omitempty"` // also how a webhook subscription is registered
	CustomerID        string  `json:"customer_id,omitempty"`
}

// CheckoutResponse is the POST /checkouts (and GET /checkouts/{id})
// response body.
type CheckoutResponse struct {
	ID                string                `json:"id"`     // -> CheckoutSession.ProviderRef
	Status            string                `json:"status"` // PENDING|FAILED|PAID|EXPIRED
	CheckoutReference string                `json:"checkout_reference"`
	Amount            float64               `json:"amount"`
	Currency          string                `json:"currency"`
	MerchantCode      string                `json:"merchant_code"`
	Date              string                `json:"date"`
	Transactions      []CheckoutTransaction `json:"transactions"`
}

type CheckoutTransaction struct {
	ID              string  `json:"id"`
	TransactionCode string  `json:"transaction_code"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	Status          string  `json:"status"` // SUCCESSFUL|CANCELLED|FAILED|PENDING|REFUNDED
	Timestamp       string  `json:"timestamp"`
}

// WebhookPayload is the thin pointer SumUp POSTs on a checkout status
// change — see VerifyWebhook's doc comment above for why a second GET is
// needed to learn the actual outcome.
type WebhookPayload struct {
	EventType string `json:"event_type"` // "CHECKOUT_STATUS_CHANGED"
	ID        string `json:"id"`         // the changed checkout's id -> GET /checkouts/{id}
}

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

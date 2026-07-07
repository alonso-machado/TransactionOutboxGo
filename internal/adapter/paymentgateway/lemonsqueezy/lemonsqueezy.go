// Package lemonsqueezy is a scaffold for a future domain.PaymentGateway
// adapter to Lemon Squeezy — selectable via PAYMENT_PROVIDER=lemonsqueezy
// but not yet implemented. Wire it up the same way
// internal/adapter/paymentgateway/stripe is wired (CreateCheckout opens a
// hosted checkout, VerifyWebhook authenticates and normalizes a webhook
// delivery) once needed. The CheckoutRequest/CheckoutResponse/
// WebhookPayload types below document the wire format researched from the
// docs below; CreateCheckout/VerifyWebhook return ErrNotImplemented and
// make no network calls or third-party SDK imports — a real
// implementation only needs net/http against these shapes.
//
// Docs: https://docs.lemonsqueezy.com/api/checkouts/create-checkout
// (create checkout), https://docs.lemonsqueezy.com/help/webhooks
// (event types), https://docs.lemonsqueezy.com/help/webhooks/signing-requests
// (signature verification). Confirm field names against current docs
// before implementing — this was researched, not built or tested against
// the live API. Lemon Squeezy is a Merchant-of-Record (like Stripe, unlike
// the Brazilian gateways above), so it's the digital-goods alternative
// rather than a Brazilian-market one.
//
// Base URL: https://api.lemonsqueezy.com/v1. Content-Type/Accept:
// application/vnd.api+json (strict JSON:API). Auth: Bearer API key.
//
// CreateCheckout → POST /checkouts. The request/response bodies are
// JSON:API resource documents, hence the nested Data/Attributes/
// Relationships shape below rather than a flat object. The checkout URL is
// CheckoutResponse.Data.Attributes.URL. EventType/EventSubtype round-trip
// via CheckoutAttributes.CheckoutData's Custom map (a free-form
// string-to-string map meant for exactly this purpose — Lemon Squeezy
// echoes it back on the resulting order's meta.custom_data). A Store and
// Variant id (Lemon Squeezy's catalog concepts, roughly the equivalent of
// "the product a customer is buying") must already exist — there's no
// free-form price-per-line-item request shape like Stripe's, only
// CustomPrice to override one variant's price.
//
// VerifyWebhook → Lemon Squeezy POSTs a WebhookPayload — a JSON:API
// resource whose Data.Attributes.Status/Type depends on which event fired
// (WebhookMeta.EventName, e.g. "order_created"). "order_created" with
// Attributes.Status == "paid" → PaymentOutcomeConfirmed; a refunded/failed
// equivalent → PaymentOutcomeFailed. EventType/EventSubtype round-trip via
// WebhookMeta.CustomData (mirrors CheckoutData.Custom above).
// RawEventID: Lemon Squeezy doesn't send a distinct delivery id in the
// body, so dedup would need Data.ID + WebhookMeta.EventName (confirm
// whether that pair is unique enough, or whether the X-Event-Name +
// delivery headers carry a better id, before implementing). Signature: an
// X-Signature request header, HMAC-SHA256 over the raw request body, keyed
// with the webhook signing secret from the dashboard — compare with a
// constant-time equality check, not ==.
package lemonsqueezy

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("lemonsqueezy: not implemented")

// CheckoutRequest is the POST /checkouts request body (JSON:API).
type CheckoutRequest struct {
	Data CheckoutRequestData `json:"data"`
}

type CheckoutRequestData struct {
	Type          string                `json:"type"` // "checkouts"
	Attributes    CheckoutAttributes    `json:"attributes"`
	Relationships CheckoutRelationships `json:"relationships"`
}

// CheckoutAttributes.CustomPrice is in minor units (cents), when set.
type CheckoutAttributes struct {
	CustomPrice     *int64         `json:"custom_price,omitempty"`
	ProductOptions  map[string]any `json:"product_options,omitempty"`
	CheckoutOptions map[string]any `json:"checkout_options,omitempty"`
	CheckoutData    *CheckoutData  `json:"checkout_data,omitempty"`
	ExpiresAt       *string        `json:"expires_at,omitempty"`
}

type CheckoutData struct {
	Email  string            `json:"email,omitempty"`
	Custom map[string]string `json:"custom,omitempty"` // event_type/event_subtype/our OrderID round-trip here
}

type CheckoutRelationships struct {
	Store   Relationship `json:"store"`
	Variant Relationship `json:"variant"`
}

type Relationship struct {
	Data RelationshipData `json:"data"`
}

type RelationshipData struct {
	Type string `json:"type"` // "stores" | "variants"
	ID   string `json:"id"`
}

// CheckoutResponse is the POST /checkouts response body (JSON:API).
type CheckoutResponse struct {
	Data CheckoutResponseData `json:"data"`
}

type CheckoutResponseData struct {
	ID         string                     `json:"id"` // -> CheckoutSession.ProviderRef
	Attributes CheckoutResponseAttributes `json:"attributes"`
}

type CheckoutResponseAttributes struct {
	URL string `json:"url"` // -> CheckoutSession.CheckoutURL
}

// WebhookPayload is the body Lemon Squeezy POSTs on a subscribed event.
type WebhookPayload struct {
	Meta WebhookMeta        `json:"meta"`
	Data WebhookPayloadData `json:"data"`
}

type WebhookMeta struct {
	EventName  string            `json:"event_name"`  // e.g. "order_created"
	CustomData map[string]string `json:"custom_data"` // mirrors CheckoutData.Custom
}

type WebhookPayloadData struct {
	ID         string                `json:"id"` // -> PaymentEvent.ProviderRef
	Type       string                `json:"type"`
	Attributes WebhookDataAttributes `json:"attributes"`
}

type WebhookDataAttributes struct {
	Status string `json:"status"` // "paid" | "refunded" | ... (depends on resource type)
}

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

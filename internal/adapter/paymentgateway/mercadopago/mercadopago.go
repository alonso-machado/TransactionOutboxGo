// Package mercadopago is a scaffold for a future domain.PaymentGateway
// adapter to Mercado Pago — selectable via PAYMENT_PROVIDER=mercadopago but
// not yet implemented. Wire it up the same way
// internal/adapter/paymentgateway/stripe is wired (CreateCheckout opens a
// hosted checkout, VerifyWebhook authenticates and normalizes a webhook
// delivery) once needed. The PreferenceRequest/PreferenceResponse/
// WebhookNotification/PaymentResource types below document the wire format
// researched from the docs below; CreateCheckout/VerifyWebhook return
// ErrNotImplemented and make no network calls or third-party SDK imports —
// a real implementation only needs net/http against these shapes.
//
// Docs: https://www.mercadopago.com.br/developers/en/docs/checkout-pro/payment-notifications
// (create preference + notifications), https://www.mercadopago.com.br/developers/en/docs/your-integrations/notifications/webhooks
// (webhook payload + x-signature verification). Confirm field names
// against current docs before implementing — this was researched, not
// built or tested against the live API.
//
// Base URL: https://api.mercadopago.com. Auth: Bearer access token.
//
// CreateCheckout → POST /checkout/preferences. Response's InitPoint is the
// hosted checkout URL. EventType/EventSubtype round-trip via
// PreferenceRequest.Metadata; ExternalReference carries our OrderID.
//
// VerifyWebhook → unlike Stripe/Pagar.me, Mercado Pago's webhook body
// (WebhookNotification) is only a thin pointer, not the outcome itself —
// the handler must make a second outbound call, GET
// /v1/payments/{WebhookNotification.Data.ID} (Bearer token), to read the
// real PaymentResource.Status ("approved" → PaymentOutcomeConfirmed;
// "rejected"/"cancelled" → PaymentOutcomeFailed) and recover
// EventType/EventSubtype from PaymentResource.Metadata/ExternalReference.
// RawEventID: the notification's own ID (confirm whether ID or Data.ID is
// stable across redeliveries before implementing dedup on it). Signature:
// x-signature request header, format "ts=<millis>,v1=<hmac>" — v1 is
// HMAC-SHA256 over a manifest string built from the request's
// x-request-id header + Data.ID + ts, keyed with the webhook secret from
// the integrations dashboard (distinct from the access token).
package mercadopago

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("mercadopago: not implemented")

// PreferenceRequest is the POST /checkout/preferences request body.
type PreferenceRequest struct {
	Items             []PreferenceItem    `json:"items"`
	Payer             *PreferencePayer    `json:"payer,omitempty"`
	BackURLs          *PreferenceBackURLs `json:"back_urls,omitempty"`
	NotificationURL   string              `json:"notification_url,omitempty"`
	ExternalReference string              `json:"external_reference,omitempty"` // our OrderID
	Metadata          map[string]string   `json:"metadata,omitempty"`           // event_type/event_subtype
}

// PreferenceItem.UnitPrice is in major currency units (e.g. 150.00), unlike
// domain.ChargeRequest.Amount's minor units — convert at the adapter
// boundary.
type PreferenceItem struct {
	Title      string  `json:"title"`
	Quantity   int     `json:"quantity"`
	UnitPrice  float64 `json:"unit_price"`
	CurrencyID string  `json:"currency_id"`
}

type PreferencePayer struct {
	Email string `json:"email"`
}

type PreferenceBackURLs struct {
	Success string `json:"success"`
	Pending string `json:"pending"`
	Failure string `json:"failure"`
}

// PreferenceResponse is the POST /checkout/preferences response body.
type PreferenceResponse struct {
	ID               string `json:"id"`                 // -> CheckoutSession.ProviderRef
	InitPoint        string `json:"init_point"`         // -> CheckoutSession.CheckoutURL (production)
	SandboxInitPoint string `json:"sandbox_init_point"` // sandbox equivalent
}

// WebhookNotification is the body Mercado Pago POSTs to notification_url —
// a pointer only, see VerifyWebhook's doc comment above.
type WebhookNotification struct {
	ID         int64                   `json:"id"`
	LiveMode   bool                    `json:"live_mode"`
	Type       string                  `json:"type"`   // "payment"
	Action     string                  `json:"action"` // "payment.created" | "payment.updated"
	APIVersion string                  `json:"api_version"`
	Data       WebhookNotificationData `json:"data"`
}

type WebhookNotificationData struct {
	ID string `json:"id"`
}

// PaymentResource is the (partial, only fields we'd need) shape of
// GET /v1/payments/{id}, fetched after a WebhookNotification arrives.
type PaymentResource struct {
	ID                int64             `json:"id"`
	Status            string            `json:"status"` // approved|rejected|cancelled|pending|in_process
	StatusDetail      string            `json:"status_detail"`
	ExternalReference string            `json:"external_reference"`
	Metadata          map[string]string `json:"metadata"`
}

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

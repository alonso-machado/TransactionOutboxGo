package domain

import "github.com/google/uuid"

// PaymentGateway is the outbound port to an external payment provider
// (Stripe, and later AbacatePay/LemonSqueezy) — the only place the domain
// touches payment at all. Ticket orders are charged through a gateway
// instead of being paid by webhook-sourced events directly: order-consumer-worker
// calls CreateCheckout to start a charge, and the gateway's own webhook
// (verified via VerifyWebhook, called from the ingestion-api webhook
// handler) later confirms or fails it, which fulfillment-consumer-worker acts on.
type PaymentGateway interface {
	CreateCheckout(req ChargeRequest) (*CheckoutSession, error)
	// VerifyWebhook authenticates and parses a raw gateway webhook body
	// (signature verification is provider-specific — e.g. Stripe's
	// webhook.ConstructEvent) into the outcome the domain understands.
	VerifyWebhook(body []byte, headers map[string]string) (*PaymentEvent, error)
}

// ChargeRequest is what order-consumer-worker asks the gateway to charge for one
// Order. Amount is minor units, same convention as every other money field
// in this system. EventType/EventSubtype are round-tripped through the
// gateway's own metadata (e.g. a Stripe Checkout Session's metadata) so that
// VerifyWebhook can hand them back on the confirmation — this is how the
// ingestion-api webhook handler learns which RabbitMQ shard to route
// payment_event_outbox onto without ever reading the events DB (it only
// ever writes outbox rows, by design).
type ChargeRequest struct {
	OrderID       uuid.UUID
	EventType     string
	EventSubtype  string
	Amount        int64
	Currency      string
	CustomerName  string
	CustomerEmail string
	SuccessURL    string
}

// CheckoutSession is what the gateway hands back for a ChargeRequest.
// ProviderRef is persisted on the Charge row and is the join key a later
// webhook confirmation is looked up by (ChargeRepository.FindByProviderRef).
type CheckoutSession struct {
	ProviderRef string
	CheckoutURL string
}

type PaymentOutcome string

const (
	PaymentOutcomeConfirmed PaymentOutcome = "CONFIRMED"
	PaymentOutcomeFailed    PaymentOutcome = "FAILED"
)

// PaymentEvent is the gateway webhook, verified and normalized.
// RawEventID is the gateway's own event identifier — the dedup boundary for
// payment_event_outbox, since a webhook can be redelivered by the provider.
// EventType/EventSubtype are read back from the gateway's own metadata (see
// ChargeRequest) — round-tripped, never looked up in a database.
type PaymentEvent struct {
	ProviderRef  string
	RawEventID   string
	Outcome      PaymentOutcome
	EventType    string
	EventSubtype string
}

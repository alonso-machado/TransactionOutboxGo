// Package stripe implements domain.PaymentGateway against the real Stripe
// API: CreateCheckout opens a hosted Checkout Session, VerifyWebhook
// authenticates a Stripe webhook delivery and normalizes it to a
// domain.PaymentEvent.
package stripe

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/checkout/session"
	"github.com/stripe/stripe-go/v82/webhook"
)

// SignatureHeader is the HTTP header Stripe signs webhook deliveries with.
// The webhook handler must pass its value through in the headers map given
// to VerifyWebhook.
const SignatureHeader = "Stripe-Signature"

type Gateway struct {
	webhookSecret string
}

// New configures the process-wide Stripe API key — the SDK's package-level
// functions (session.New, webhook.ConstructEvent) read stripe.Key globally,
// so this must run once per process before any Gateway method is called —
// and returns a Gateway bound to webhookSecret for VerifyWebhook.
func New(secretKey, webhookSecret string) *Gateway {
	stripe.Key = secretKey
	return &Gateway{webhookSecret: webhookSecret}
}

// CreateCheckout opens a one-time-payment hosted Checkout Session for
// req.Amount (minor units) — ClientReferenceID carries our OrderID so a
// human reconciling in the Stripe dashboard can find the order, but
// fulfillment-consumer-worker itself resolves the order via
// ChargeRepository.FindByProviderRef(session.ID), not this field.
func (g *Gateway) CreateCheckout(req domain.ChargeRequest) (*domain.CheckoutSession, error) {
	params := &stripe.CheckoutSessionParams{
		Mode:              stripe.String(string(stripe.CheckoutSessionModePayment)),
		SuccessURL:        stripe.String(req.SuccessURL),
		ClientReferenceID: stripe.String(req.OrderID.String()),
		CustomerEmail:     stripe.String(req.CustomerEmail),
		LineItems: []*stripe.CheckoutSessionLineItemParams{
			{
				Quantity: stripe.Int64(1),
				PriceData: &stripe.CheckoutSessionLineItemPriceDataParams{
					Currency:   stripe.String(strings.ToLower(req.Currency)),
					UnitAmount: stripe.Int64(req.Amount),
					ProductData: &stripe.CheckoutSessionLineItemPriceDataProductDataParams{
						Name: stripe.String("Order " + req.OrderID.String()),
					},
				},
			},
		},
	}

	params.AddMetadata("event_type", req.EventType)
	params.AddMetadata("event_subtype", req.EventSubtype)

	sess, err := session.New(params)
	if err != nil {
		return nil, fmt.Errorf("stripe create checkout session: %w", err)
	}
	return &domain.CheckoutSession{ProviderRef: sess.ID, CheckoutURL: sess.URL}, nil
}

// VerifyWebhook authenticates the delivery via webhook.ConstructEvent
// (rejects a bad/missing signature or a stale timestamp) before trusting
// anything in body. Only checkout.session.completed (CONFIRMED) and
// checkout.session.expired/payment_intent.payment_failed (FAILED) are
// handled; any other event type is reported as an error so the ingestion-api
// webhook handler 400s rather than silently dropping an event type we don't
// yet act on.
func (g *Gateway) VerifyWebhook(body []byte, headers map[string]string) (*domain.PaymentEvent, error) {
	event, err := webhook.ConstructEvent(body, headers[SignatureHeader], g.webhookSecret)
	if err != nil {
		return nil, fmt.Errorf("verify stripe webhook: %w", err)
	}

	var outcome domain.PaymentOutcome
	switch event.Type {
	case stripe.EventTypeCheckoutSessionCompleted:
		outcome = domain.PaymentOutcomeConfirmed
	case stripe.EventTypeCheckoutSessionExpired, stripe.EventTypePaymentIntentPaymentFailed:
		outcome = domain.PaymentOutcomeFailed
	default:
		return nil, fmt.Errorf("unhandled stripe event type %q", event.Type)
	}

	var sess stripe.CheckoutSession
	if err := json.Unmarshal(event.Data.Raw, &sess); err != nil {
		return nil, fmt.Errorf("unmarshal stripe event data: %w", err)
	}

	return &domain.PaymentEvent{
		ProviderRef:  sess.ID,
		RawEventID:   event.ID,
		Outcome:      outcome,
		EventType:    sess.Metadata["event_type"],
		EventSubtype: sess.Metadata["event_subtype"],
	}, nil
}

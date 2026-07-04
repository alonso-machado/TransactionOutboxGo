// Package fake implements domain.PaymentGateway with no network calls — the
// default provider for local dev (make up), the integration test suite, and
// k6, none of which have a real gateway to redirect a browser to or receive
// a signed webhook from.
package fake

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

type Gateway struct {
	baseURL string
}

// New builds a Gateway; baseURL is only used to shape a plausible-looking
// (never actually reachable) CheckoutURL. Empty defaults to a placeholder
// host.
func New(baseURL string) *Gateway {
	if baseURL == "" {
		baseURL = "http://fake-gateway.local"
	}
	return &Gateway{baseURL: baseURL}
}

func (g *Gateway) CreateCheckout(req domain.ChargeRequest) (*domain.CheckoutSession, error) {
	ref := "fake_" + req.OrderID.String()
	return &domain.CheckoutSession{
		ProviderRef: ref,
		CheckoutURL: fmt.Sprintf("%s/checkout/%s", g.baseURL, ref),
	}, nil
}

// webhookBody is the shape a test/k6 harness POSTs to
// /api/v1/webhooks/payments/fake to simulate a gateway confirmation — no
// signature scheme, since this provider only exists for environments with
// no real gateway to sign anything. EventType/EventSubtype are carried
// directly in the body (rather than round-tripped through a real gateway
// session, since there is no real session here) — the harness echoes back
// whatever it received in CreateCheckout's request.
type webhookBody struct {
	ProviderRef  string `json:"provider_ref"`
	EventID      string `json:"event_id"`
	Outcome      string `json:"outcome"` // "CONFIRMED" | "FAILED"
	EventType    string `json:"event_type"`
	EventSubtype string `json:"event_subtype"`
}

func (g *Gateway) VerifyWebhook(body []byte, _ map[string]string) (*domain.PaymentEvent, error) {
	var b webhookBody
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, fmt.Errorf("unmarshal fake webhook: %w", err)
	}
	if b.ProviderRef == "" || b.EventID == "" {
		return nil, errors.New("fake webhook requires provider_ref and event_id")
	}

	var outcome domain.PaymentOutcome
	switch b.Outcome {
	case string(domain.PaymentOutcomeConfirmed):
		outcome = domain.PaymentOutcomeConfirmed
	case string(domain.PaymentOutcomeFailed):
		outcome = domain.PaymentOutcomeFailed
	default:
		return nil, fmt.Errorf("unknown fake webhook outcome %q", b.Outcome)
	}

	return &domain.PaymentEvent{
		ProviderRef:  b.ProviderRef,
		RawEventID:   b.EventID,
		Outcome:      outcome,
		EventType:    b.EventType,
		EventSubtype: b.EventSubtype,
	}, nil
}

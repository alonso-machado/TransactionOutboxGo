// Package lemonsqueezy is a scaffold for a future domain.PaymentGateway
// adapter to LemonSqueezy — selectable via PAYMENT_PROVIDER=lemonsqueezy but
// not yet implemented. Wire it up the same way
// internal/adapter/paymentgateway/stripe is wired (CreateCheckout opens a
// hosted checkout, VerifyWebhook authenticates and normalizes a webhook
// delivery) once needed.
package lemonsqueezy

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("lemonsqueezy: not implemented")

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

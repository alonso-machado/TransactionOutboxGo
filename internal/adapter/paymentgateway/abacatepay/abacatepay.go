// Package abacatepay is a scaffold for a future domain.PaymentGateway
// adapter to AbacatePay — selectable via PAYMENT_PROVIDER=abacatepay but not
// yet implemented. Wire it up the same way internal/adapter/paymentgateway/
// stripe is wired (CreateCheckout opens a hosted checkout, VerifyWebhook
// authenticates and normalizes a webhook delivery) once needed.
package abacatepay

import (
	"errors"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

var ErrNotImplemented = errors.New("abacatepay: not implemented")

type Gateway struct{}

func New() *Gateway { return &Gateway{} }

func (g *Gateway) CreateCheckout(domain.ChargeRequest) (*domain.CheckoutSession, error) {
	return nil, ErrNotImplemented
}

func (g *Gateway) VerifyWebhook([]byte, map[string]string) (*domain.PaymentEvent, error) {
	return nil, ErrNotImplemented
}

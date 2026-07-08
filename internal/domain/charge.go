package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type ChargeStatus string

const (
	ChargeStatusPending ChargeStatus = "PENDING"
	ChargeStatusPaid    ChargeStatus = "PAID"
	ChargeStatusFailed  ChargeStatus = "FAILED"
)

// Charge is the gateway-transaction ledger row for an Order — one row per
// checkout attempt. ProviderRef is the gateway's own session/charge
// identifier (e.g. a Stripe Checkout Session ID) and is how
// fulfillment-consumer-worker looks up the Order a webhook confirmation belongs to,
// since the webhook body is shaped by the gateway, not by us.
type Charge struct {
	ID          uuid.UUID
	OrderID     uuid.UUID
	Provider    string
	ProviderRef string
	CheckoutURL string
	Amount      int64
	Currency    string
	Status      ChargeStatus
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ChargeRepository is the port for the charges table (events DB).
type ChargeRepository interface {
	// Save inserts a new PENDING charge for an order (one order may only
	// ever have one charge — order-consumer-worker calls this once, right after
	// PaymentGateway.CreateCheckout).
	Save(ctx context.Context, uow UnitOfWork, c *Charge) error
	// FindByProviderRef is how fulfillment-consumer-worker resolves a webhook
	// confirmation (which only carries the gateway's own reference) back to
	// the Order it belongs to.
	FindByProviderRef(ctx context.Context, providerRef string) (*Charge, error)
	// FindByOrderID is how tickets-api's GET /orders/{id} resolves an
	// order's CheckoutURL — order_id is uniqueIndex (one charge per order),
	// so this is a single-row lookup. A not-found result is the normal case
	// while the client is still polling right after 201Created, before
	// order-consumer-worker has created the Charge yet.
	FindByOrderID(ctx context.Context, orderID uuid.UUID) (*Charge, error)
	// UpdateStatus is idempotent in effect: applying PAID/FAILED to an
	// already-terminal charge is a safe no-op at the use-case layer (see
	// usecase/fulfillment), not enforced by the repository itself.
	UpdateStatus(ctx context.Context, uow UnitOfWork, id uuid.UUID, status ChargeStatus) error
}

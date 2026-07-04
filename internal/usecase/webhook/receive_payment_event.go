// Package webhook holds the ReceivePaymentEvent use-case: ingestion-api's
// POST /api/v1/webhooks/payments/{provider} lands an already-verified
// payment-gateway confirmation in payment_event_outbox. Verification itself
// (signature check, event parsing) happens one layer up, in the HTTP
// handler via domain.PaymentGateway.VerifyWebhook — this use-case only ever
// sees the normalized, trusted domain.PaymentEvent.
package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

var tracer = otel.Tracer("usecase/webhook")

type ReceivePaymentEvent struct {
	outboxRepo     domain.OutboxRepository
	uow            domain.UnitOfWork
	duplicateTotal metric.Int64Counter
}

func New(outboxRepo domain.OutboxRepository, uow domain.UnitOfWork) *ReceivePaymentEvent {
	meter := otel.GetMeterProvider().Meter("usecase/webhook")
	return &ReceivePaymentEvent{
		outboxRepo:     outboxRepo,
		uow:            uow,
		duplicateTotal: observability.Int64Counter(meter, "webhook_ingestion.duplicate_total"),
	}
}

type Request struct {
	Provider string
	Event    domain.PaymentEvent // already verified+normalized by the gateway
}

type Response struct {
	IdempotencyKey string
	Created        bool // false => duplicate delivery of an already-landed event
}

// outboxPayload is the JSON body carried on payment_event_outbox and, once
// relayed, the RabbitMQ message.
type outboxPayload struct {
	SchemaVersion string `json:"schemaVersion"`
	Provider      string `json:"provider"`
	ProviderRef   string `json:"providerRef"`
	Outcome       string `json:"outcome"`
}

// Execute is idempotent on the gateway's own RawEventID — a provider can
// redeliver the same webhook, and (provider, rawEventID) is the dedup
// boundary, never the gateway session/ProviderRef (one charge can, in
// principle, emit more than one event over its lifetime).
func (uc *ReceivePaymentEvent) Execute(ctx context.Context, req Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "webhook.receive_payment_event")
	defer span.End()

	id, err := uuid.NewV7()
	if err != nil {
		err = fmt.Errorf("generate payment event id: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	payload, err := json.Marshal(outboxPayload{
		SchemaVersion: domain.SchemaVersion,
		Provider:      req.Provider,
		ProviderRef:   req.Event.ProviderRef,
		Outcome:       string(req.Event.Outcome),
	})
	if err != nil {
		err = fmt.Errorf("marshal payload: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	key := computeKey(req.Provider, req.Event.RawEventID)
	span.SetAttributes(attribute.String("idempotency_key", key))

	msg := &domain.OutboxMessage{
		ID:             id,
		IdempotencyKey: key,
		AggregateType:  "payment_event",
		HTTPMethod:     "POST",
		Route:          "/api/v1/webhooks/payments/" + req.Provider,
		Payload:        payload,
		Headers:        map[string]string{"schemaVersion": domain.SchemaVersion},
		Status:         domain.OutboxStatusNew,
		CreatedAt:      time.Now().UTC(),
		EventType:      req.Event.EventType,
		EventSubtype:   req.Event.EventSubtype,
	}

	var created bool
	if err := uc.uow.Execute(ctx, func(ctx context.Context) error {
		var err error
		created, err = uc.outboxRepo.Enqueue(ctx, uc.uow, msg)
		return err
	}); err != nil {
		err = fmt.Errorf("enqueue payment event outbox: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Bool("dedup_hit", !created))
	if !created {
		uc.duplicateTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("provider", req.Provider)))
	}

	return &Response{IdempotencyKey: key, Created: created}, nil
}

func computeKey(provider, rawEventID string) string {
	sum := sha256.Sum256([]byte(provider + ":" + rawEventID))
	return hex.EncodeToString(sum[:])
}

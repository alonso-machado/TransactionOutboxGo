package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("usecase/ingest")

type IngestPayment struct {
	outboxRepo domain.OutboxRepository
	uow        domain.UnitOfWork
}

func New(outboxRepo domain.OutboxRepository, uow domain.UnitOfWork) *IngestPayment {
	return &IngestPayment{outboxRepo: outboxRepo, uow: uow}
}

type Request struct {
	HTTPMethod        string
	Route             string
	EventID           string
	ProviderName      string
	ProviderPaymentID string
	ExternalPaymentID string
	PayerID           *uuid.UUID
	RecipientID       *uuid.UUID
	Amount            int64 // minor units — converted from the wire float at the HTTP boundary
	Currency          string
	Method            string
	MethodDetails     []byte // raw JSON sub-object, e.g. the "pix" object
	OccurredAt        time.Time
	Headers           map[string]string
	IdempotencyKey    string // optional client-supplied Idempotency-Key header
}

type Response struct {
	PaymentID      uuid.UUID
	IdempotencyKey string
	Created        bool // false => duplicate of an existing outbox entry
}

// outboxPayload is the JSON body carried on the outbox row and, eventually,
// the RabbitMQ message — it pre-commits the Payment's primary key so the
// consumer doesn't need to mint a new one.
type outboxPayload struct {
	SchemaVersion     string          `json:"schemaVersion"`
	PaymentID         uuid.UUID       `json:"paymentId"`
	EventID           string          `json:"eventId"`
	ProviderName      string          `json:"providerName"`
	ProviderPaymentID string          `json:"providerPaymentId"`
	ExternalPaymentID string          `json:"externalPaymentId"`
	PayerID           *uuid.UUID      `json:"payerId,omitempty"`
	RecipientID       *uuid.UUID      `json:"recipientId,omitempty"`
	Amount            int64           `json:"amount"`
	Currency          string          `json:"currency"`
	Method            string          `json:"method"`
	MethodDetails     json.RawMessage `json:"methodDetails,omitempty"`
	OccurredAt        time.Time       `json:"occurredAt"`
}

func (uc *IngestPayment) Execute(ctx context.Context, req Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ingest.payment", trace.WithAttributes(
		attribute.String("http_method", req.HTTPMethod),
	))
	defer span.End()

	paymentID, err := uuid.NewV7()
	if err != nil {
		err = fmt.Errorf("generate payment id: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	payload, err := json.Marshal(outboxPayload{
		SchemaVersion:     domain.SchemaVersion,
		PaymentID:         paymentID,
		EventID:           req.EventID,
		ProviderName:      req.ProviderName,
		ProviderPaymentID: req.ProviderPaymentID,
		ExternalPaymentID: req.ExternalPaymentID,
		PayerID:           req.PayerID,
		RecipientID:       req.RecipientID,
		Amount:            req.Amount,
		Currency:          req.Currency,
		Method:            req.Method,
		MethodDetails:     req.MethodDetails,
		OccurredAt:        req.OccurredAt,
	})
	if err != nil {
		err = fmt.Errorf("marshal payload: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// The idempotency key is derived from the provider's own event identity
	// (provider name + eventId), not from amount/currency — a webhook
	// redelivery carries the same eventId, so this is the natural dedup
	// boundary for provider-sourced events.
	keySource := req.ProviderName + ":" + req.EventID
	key := computeKey(req.HTTPMethod, []byte(keySource), req.IdempotencyKey)
	span.SetAttributes(attribute.String("idempotency_key", key))

	// Carry schemaVersion as a RabbitMQ message header too (Track 2.D), not
	// just in the body — lets a future consumer-side filter/inspection tool
	// branch on it without parsing the body.
	headers := make(map[string]string, len(req.Headers)+1)
	for k, v := range req.Headers {
		headers[k] = v
	}
	headers["schemaVersion"] = domain.SchemaVersion

	msg := &domain.OutboxMessage{
		ID:             paymentID,
		IdempotencyKey: key,
		AggregateType:  "payment",
		HTTPMethod:     req.HTTPMethod,
		Route:          req.Route,
		Payload:        payload,
		Headers:        headers,
		Status:         domain.OutboxStatusNew,
		CreatedAt:      time.Now().UTC(),
		PaymentMethod:  req.Method,
	}

	var created bool
	if err := uc.uow.Execute(ctx, func(ctx context.Context) error {
		var err error
		created, err = uc.outboxRepo.Enqueue(ctx, uc.uow, msg)
		return err
	}); err != nil {
		err = fmt.Errorf("enqueue outbox: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Bool("dedup_hit", !created))

	return &Response{PaymentID: paymentID, IdempotencyKey: key, Created: created}, nil
}

func computeKey(method string, source []byte, clientKey string) string {
	sourceHash := sha256.Sum256(source)
	combined := method + hex.EncodeToString(sourceHash[:])
	if clientKey != "" {
		combined += clientKey
	}
	sum := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(sum[:])
}

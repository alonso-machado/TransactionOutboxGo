package consume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("usecase/consume")

// ErrUnknownSchemaVersion is returned when the message's schemaVersion
// doesn't match domain.SchemaVersion — the caller (AMQPConsumer) treats this
// like any other processing error, but it's exhausted to DLQ on the FIRST
// attempt rather than retried, since retrying a structurally-incompatible
// message can never succeed.
var ErrUnknownSchemaVersion = errors.New("unknown schema version")

type ProcessMessage struct {
	paymentRepo          domain.PaymentRepository
	uow                  domain.UnitOfWork
	unknownSchemaVersion metric.Int64Counter
	duplicateTotal       metric.Int64Counter
}

func New(paymentRepo domain.PaymentRepository, uow domain.UnitOfWork) *ProcessMessage {
	meter := otel.GetMeterProvider().Meter("usecase/consume")
	return &ProcessMessage{
		paymentRepo:          paymentRepo,
		uow:                  uow,
		unknownSchemaVersion: observability.Int64Counter(meter, "consumer.unknown_schema_version_total"),
		duplicateTotal:       observability.Int64Counter(meter, "consumer.duplicate_total"),
	}
}

type payloadDTO struct {
	SchemaVersion     string          `json:"schemaVersion"`
	PaymentID         string          `json:"paymentId"`
	EventID           string          `json:"eventId"`
	ProviderName      string          `json:"providerName"`
	ProviderPaymentID string          `json:"providerPaymentId"`
	ExternalPaymentID string          `json:"externalPaymentId"`
	PayerID           *string         `json:"payerId,omitempty"`
	RecipientID       *string         `json:"recipientId,omitempty"`
	Amount            int64           `json:"amount"`
	Currency          string          `json:"currency"`
	Method            string          `json:"method"`
	MethodDetails     json.RawMessage `json:"methodDetails,omitempty"`
	OccurredAt        time.Time       `json:"occurredAt"`
}

// Execute is idempotent: PaymentRepository.Save uses ON CONFLICT
// (source_message_id, occurred_at) DO NOTHING, so redelivering the same
// RabbitMQ message is a safe no-op rather than an error — reported back via
// created=false so the caller (AMQPConsumer) can tell a "duplicate" outcome
// apart from a fresh "saved" one on its own trace/metrics. Every return path
// also tags the "process.message" span's outcome attribute ("error" /
// "unknown_schema_version" / "duplicate" / "saved"), so a single dimension
// name distinguishes the kind of thing that happened across logs, metrics,
// and traces.
func (uc *ProcessMessage) Execute(ctx context.Context, messageID string, body []byte) (bool, error) {
	ctx, span := tracer.Start(ctx, "process.message")
	defer span.End()

	var dto payloadDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return false, recordRedactedError(span, "error", fmt.Errorf("unmarshal payload: %w", err))
	}

	span.SetAttributes(attribute.String("schema_version", dto.SchemaVersion))
	// Same major version → proceed as today. Unknown/newer major version →
	// don't crash-loop trying to parse a structurally-incompatible payload;
	// record the operator signal and reject (the caller dead-letters on the
	// first attempt for this specific error — see AMQPConsumer.handle).
	if dto.SchemaVersion != "" && dto.SchemaVersion != domain.SchemaVersion {
		uc.unknownSchemaVersion.Add(ctx, 1, metric.WithAttributes(attribute.String("schema_version", dto.SchemaVersion)))
		return false, recordRedactedError(span, "unknown_schema_version", fmt.Errorf("%w: %q (supported: %q)", ErrUnknownSchemaVersion, dto.SchemaVersion, domain.SchemaVersion))
	}

	paymentID, err := uuid.Parse(dto.PaymentID)
	if err != nil {
		return false, recordRedactedError(span, "error", fmt.Errorf("parse paymentId: %w", err))
	}
	span.SetAttributes(attribute.String("payment_id", paymentID.String()))

	payerID, err := domain.ParseOptionalUUID(dto.PayerID)
	if err != nil {
		return false, recordRedactedError(span, "error", fmt.Errorf("parse payerId: %w", err))
	}
	recipientID, err := domain.ParseOptionalUUID(dto.RecipientID)
	if err != nil {
		return false, recordRedactedError(span, "error", fmt.Errorf("parse recipientId: %w", err))
	}

	now := time.Now().UTC()
	payment := &domain.Payment{
		ID:                paymentID,
		SourceMessageID:   messageID,
		EventID:           dto.EventID,
		ProviderName:      dto.ProviderName,
		ProviderPaymentID: dto.ProviderPaymentID,
		ExternalPaymentID: dto.ExternalPaymentID,
		PayerID:           payerID,
		RecipientID:       recipientID,
		Amount:            dto.Amount,
		Currency:          dto.Currency,
		Method:            dto.Method,
		MethodDetails:     dto.MethodDetails,
		OccurredAt:        dto.OccurredAt,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	var created bool
	if err := uc.uow.Execute(ctx, func(ctx context.Context) error {
		var saveErr error
		created, saveErr = uc.paymentRepo.Save(ctx, uc.uow, payment)
		return saveErr
	}); err != nil {
		return false, recordRedactedError(span, "error", err)
	}

	if !created {
		uc.duplicateTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("method", payment.Method)))
		span.SetAttributes(attribute.String("outcome", "duplicate"))
		return false, nil
	}
	span.SetAttributes(attribute.String("outcome", "saved"))
	return true, nil
}

// recordRedactedError tags the span's outcome attribute, masks any PII the
// underlying driver/library may have embedded in err's message (e.g. a
// constraint-violation DETAIL line) before attaching it to the span, then
// returns the original err for the caller.
func recordRedactedError(span trace.Span, outcome string, err error) error {
	span.SetAttributes(attribute.String("outcome", outcome))
	span.RecordError(errors.New(pii.Redact(err.Error())))
	span.SetStatus(codes.Error, pii.Redact(err.Error()))
	return err
}

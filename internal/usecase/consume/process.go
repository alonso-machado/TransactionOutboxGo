package consume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("usecase/consume")

type ProcessMessage struct {
	paymentRepo domain.PaymentRepository
	uow         domain.UnitOfWork
}

func New(paymentRepo domain.PaymentRepository, uow domain.UnitOfWork) *ProcessMessage {
	return &ProcessMessage{paymentRepo: paymentRepo, uow: uow}
}

type payloadDTO struct {
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

// Execute is idempotent: PaymentRepository.Save uses ON CONFLICT (source_message_id)
// DO NOTHING, so redelivering the same RabbitMQ message is a safe no-op.
func (uc *ProcessMessage) Execute(ctx context.Context, messageID string, body []byte) error {
	ctx, span := tracer.Start(ctx, "process.message")
	defer span.End()

	var dto payloadDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return recordRedactedError(span, fmt.Errorf("unmarshal payload: %w", err))
	}

	paymentID, err := uuid.Parse(dto.PaymentID)
	if err != nil {
		return recordRedactedError(span, fmt.Errorf("parse paymentId: %w", err))
	}
	span.SetAttributes(attribute.String("payment_id", paymentID.String()))

	payerID, err := parseOptionalUUID(dto.PayerID)
	if err != nil {
		return recordRedactedError(span, fmt.Errorf("parse payerId: %w", err))
	}
	recipientID, err := parseOptionalUUID(dto.RecipientID)
	if err != nil {
		return recordRedactedError(span, fmt.Errorf("parse recipientId: %w", err))
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

	err = uc.uow.Execute(ctx, func(ctx context.Context) error {
		return uc.paymentRepo.Save(ctx, uc.uow, payment)
	})
	if err != nil {
		span.SetAttributes(attribute.String("outcome", "error"))
		return recordRedactedError(span, err)
	}
	span.SetAttributes(attribute.String("outcome", "saved"))
	return nil
}

// recordRedactedError masks any PII the underlying driver/library may have
// embedded in err's message (e.g. a constraint-violation DETAIL line) before
// attaching it to the span, then returns the original err for the caller.
func recordRedactedError(span trace.Span, err error) error {
	span.RecordError(errors.New(pii.Redact(err.Error())))
	span.SetStatus(codes.Error, pii.Redact(err.Error()))
	return err
}

func parseOptionalUUID(s *string) (*uuid.UUID, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	id, err := uuid.Parse(*s)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

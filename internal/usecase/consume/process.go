package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
)

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
	var dto payloadDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	paymentID, err := uuid.Parse(dto.PaymentID)
	if err != nil {
		return fmt.Errorf("parse paymentId: %w", err)
	}

	payerID, err := parseOptionalUUID(dto.PayerID)
	if err != nil {
		return fmt.Errorf("parse payerId: %w", err)
	}
	recipientID, err := parseOptionalUUID(dto.RecipientID)
	if err != nil {
		return fmt.Errorf("parse recipientId: %w", err)
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

	return uc.uow.Execute(ctx, func(ctx context.Context) error {
		return uc.paymentRepo.Save(ctx, uc.uow, payment)
	})
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

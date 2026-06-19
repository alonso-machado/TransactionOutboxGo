package consume

import (
	"context"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ProcessMessage struct {
	inboxRepo  domain.InboxRepository
	recordRepo domain.RecordRepository
	uow        domain.UnitOfWork
}

func New(inboxRepo domain.InboxRepository, recordRepo domain.RecordRepository, uow domain.UnitOfWork) *ProcessMessage {
	return &ProcessMessage{inboxRepo: inboxRepo, recordRepo: recordRepo, uow: uow}
}

func (uc *ProcessMessage) Execute(ctx context.Context, messageID string, body []byte, headers amqp.Table) error {
	return uc.uow.Execute(ctx, func(ctx context.Context) error {
		exists, err := uc.inboxRepo.Exists(ctx, uc.uow, messageID)
		if err != nil {
			return fmt.Errorf("inbox check: %w", err)
		}
		if exists {
			return nil
		}

		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generate id: %w", err)
		}

		method, _ := headers["http_method"].(string)
		route, _ := headers["route"].(string)

		record := &domain.Record{
			ID:              id,
			SourceMessageID: messageID,
			Method:          method,
			Route:           route,
			Payload:         body,
			CreatedAt:       time.Now().UTC(),
		}
		if err := uc.recordRepo.Save(ctx, uc.uow, record); err != nil {
			return fmt.Errorf("save record: %w", err)
		}

		return uc.inboxRepo.Insert(ctx, uc.uow, &domain.InboxMessage{
			MessageID:   messageID,
			Status:      "processed",
			ProcessedAt: time.Now().UTC(),
		})
	})
}

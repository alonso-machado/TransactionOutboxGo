package domain

import (
	"context"
	"time"
)

type InboxMessage struct {
	MessageID   string
	Status      string
	ProcessedAt time.Time
}

type InboxRepository interface {
	Exists(ctx context.Context, uow UnitOfWork, messageID string) (bool, error)
	Insert(ctx context.Context, uow UnitOfWork, msg *InboxMessage) error
}

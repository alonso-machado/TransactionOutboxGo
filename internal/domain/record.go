package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Record struct {
	ID              uuid.UUID
	SourceMessageID string
	Method          string
	Route           string
	Payload         []byte
	CreatedAt       time.Time
}

type RecordRepository interface {
	Save(ctx context.Context, uow UnitOfWork, r *Record) error
}

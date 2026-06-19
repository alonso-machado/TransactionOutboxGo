package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
)

type IngestRecord struct {
	outboxRepo domain.OutboxRepository
	uow        domain.UnitOfWork
}

func New(outboxRepo domain.OutboxRepository, uow domain.UnitOfWork) *IngestRecord {
	return &IngestRecord{outboxRepo: outboxRepo, uow: uow}
}

type Request struct {
	HTTPMethod     string
	Route          string
	Payload        []byte
	Headers        map[string]string
	IdempotencyKey string
}

type Response struct {
	MessageID      uuid.UUID
	IdempotencyKey string
}

func (uc *IngestRecord) Execute(ctx context.Context, req Request) (*Response, error) {
	key := computeKey(req.HTTPMethod, req.Payload, req.IdempotencyKey)

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate id: %w", err)
	}

	msg := &domain.OutboxMessage{
		ID:             id,
		IdempotencyKey: key,
		AggregateType:  "record",
		HTTPMethod:     req.HTTPMethod,
		Route:          req.Route,
		Payload:        req.Payload,
		Headers:        req.Headers,
		Status:         domain.OutboxStatusPending,
		CreatedAt:      time.Now().UTC(),
	}

	if err := uc.uow.Execute(ctx, func(ctx context.Context) error {
		return uc.outboxRepo.Enqueue(ctx, uc.uow, msg)
	}); err != nil {
		return nil, fmt.Errorf("enqueue outbox: %w", err)
	}

	return &Response{MessageID: id, IdempotencyKey: key}, nil
}

func computeKey(method string, payload []byte, clientKey string) string {
	payloadHash := sha256.Sum256(payload)
	combined := method + hex.EncodeToString(payloadHash[:])
	if clientKey != "" {
		combined += clientKey
	}
	sum := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(sum[:])
}

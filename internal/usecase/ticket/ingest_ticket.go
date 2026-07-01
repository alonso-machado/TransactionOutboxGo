// Package ticket holds the IngestTicket use-case: it lands a ticket-order
// event in the ticket_outbox table. It deliberately mirrors usecase/ingest
// (the payments outbox) but is simpler — there's no per-method routing and no
// relay yet; a future ticket-processing microservice will consume the table.
package ticket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

var tracer = otel.Tracer("usecase/ticket")

type IngestTicket struct {
	ticketRepo     domain.TicketOutboxRepository
	uow            domain.UnitOfWork
	duplicateTotal metric.Int64Counter
}

func New(ticketRepo domain.TicketOutboxRepository, uow domain.UnitOfWork) *IngestTicket {
	meter := otel.GetMeterProvider().Meter("usecase/ticket")
	return &IngestTicket{
		ticketRepo:     ticketRepo,
		uow:            uow,
		duplicateTotal: observability.Int64Counter(meter, "ticket_ingestion.duplicate_total"),
	}
}

type Request struct {
	EventID        string
	Payload        []byte // the raw "order" object
	IdempotencyKey string // optional client-supplied Idempotency-Key header
}

type Response struct {
	TicketID       uuid.UUID
	IdempotencyKey string
	Created        bool // false => duplicate of an existing ticket_outbox entry
}

func (uc *IngestTicket) Execute(ctx context.Context, req Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ingest.ticket")
	defer span.End()

	id, err := uuid.NewV7()
	if err != nil {
		err = fmt.Errorf("generate ticket id: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// Idempotency boundary is the order's own event_id — a duplicate delivery
	// carries the same event_id, so it's the natural dedup key (same rationale
	// as the payments outbox's provider.name+eventId key). The optional client
	// Idempotency-Key header is folded in when present.
	key := computeKey(req.EventID, req.IdempotencyKey)
	span.SetAttributes(attribute.String("idempotency_key", key))

	msg := &domain.TicketOutboxMessage{
		ID:             id,
		IdempotencyKey: key,
		EventID:        req.EventID,
		Payload:        req.Payload,
		Status:         domain.OutboxStatusNew,
		CreatedAt:      time.Now().UTC(),
	}

	var created bool
	if err := uc.uow.Execute(ctx, func(ctx context.Context) error {
		var err error
		created, err = uc.ticketRepo.Enqueue(ctx, uc.uow, msg)
		return err
	}); err != nil {
		err = fmt.Errorf("enqueue ticket outbox: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Bool("dedup_hit", !created))
	if !created {
		uc.duplicateTotal.Add(ctx, 1)
	}

	return &Response{TicketID: id, IdempotencyKey: key, Created: created}, nil
}

func computeKey(eventID, clientKey string) string {
	combined := "ticket:" + eventID
	if clientKey != "" {
		combined += ":" + clientKey
	}
	sum := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(sum[:])
}

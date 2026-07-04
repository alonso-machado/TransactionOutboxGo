// Package order holds the PlaceOrder use-case: ingestion-api's
// POST /api/v1/orders lands a ticket order in order_outbox. It mirrors the
// old payments-domain ingest use-case's shape (pre-commit the primary key,
// compute an idempotency key, enqueue inside a UnitOfWork) but for the
// events domain: EventType/EventSubtype (rather than a payment method)
// drive the RabbitMQ routing key order-consumer-worker consumes from.
package order

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

var tracer = otel.Tracer("usecase/order")

type PlaceOrder struct {
	outboxRepo     domain.OutboxRepository
	uow            domain.UnitOfWork
	duplicateTotal metric.Int64Counter
}

func New(outboxRepo domain.OutboxRepository, uow domain.UnitOfWork) *PlaceOrder {
	meter := otel.GetMeterProvider().Meter("usecase/order")
	return &PlaceOrder{
		outboxRepo:     outboxRepo,
		uow:            uow,
		duplicateTotal: observability.Int64Counter(meter, "order_ingestion.duplicate_total"),
	}
}

// ItemRequest is one requested ticket line. JSON tags matter here: this
// struct is marshalled verbatim into the outbox payload, and
// usecase/checkout's itemDTO must unmarshal the exact same tags.
type ItemRequest struct {
	SourceTicketID string `json:"sourceTicketId"`
	Section        string `json:"section"`
	Row            string `json:"row"`
	Seat           string `json:"seat"`
	Price          int64  `json:"price"` // minor units
	Currency       string `json:"currency"`
}

type Request struct {
	SourceOrderID    string // the order payload's own event_id — the dedup boundary
	EventType        string
	EventSubtype     string
	SourceEventID    string
	EventName        string // human-readable event name; falls back to SourceEventID if empty
	SourceVenueID    string
	VenueName        string
	VenueCity        string
	Items            []ItemRequest
	CustomerName     string
	CustomerEmail    string
	CustomerDocument string
	Currency         string
	Headers          map[string]string
	IdempotencyKey   string // optional client-supplied Idempotency-Key header
}

type Response struct {
	OrderID        uuid.UUID
	IdempotencyKey string
	Created        bool // false => duplicate of an existing order_outbox entry
}

// outboxPayload is the JSON body carried on order_outbox and, once relayed,
// the RabbitMQ message — pre-commits the Order's primary key so order-consumer-worker
// doesn't need to mint a new one.
type outboxPayload struct {
	SchemaVersion    string        `json:"schemaVersion"`
	OrderID          uuid.UUID     `json:"orderId"`
	SourceOrderID    string        `json:"sourceOrderId"`
	EventType        string        `json:"eventType"`
	EventSubtype     string        `json:"eventSubtype"`
	SourceEventID    string        `json:"sourceEventId"`
	EventName        string        `json:"eventName,omitempty"`
	SourceVenueID    string        `json:"sourceVenueId,omitempty"`
	VenueName        string        `json:"venueName,omitempty"`
	VenueCity        string        `json:"venueCity,omitempty"`
	Items            []ItemRequest `json:"items"`
	CustomerName     string        `json:"customerName,omitempty"`
	CustomerEmail    string        `json:"customerEmail,omitempty"`
	CustomerDocument string        `json:"customerDocument,omitempty"`
	Amount           int64         `json:"amount"`
	Currency         string        `json:"currency"`
}

func (uc *PlaceOrder) Execute(ctx context.Context, req Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "order.place")
	defer span.End()

	orderID, err := uuid.NewV7()
	if err != nil {
		err = fmt.Errorf("generate order id: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var amount int64
	for _, it := range req.Items {
		amount += it.Price
	}

	payload, err := json.Marshal(outboxPayload{
		SchemaVersion:    domain.SchemaVersion,
		OrderID:          orderID,
		SourceOrderID:    req.SourceOrderID,
		EventType:        req.EventType,
		EventSubtype:     req.EventSubtype,
		SourceEventID:    req.SourceEventID,
		EventName:        req.EventName,
		SourceVenueID:    req.SourceVenueID,
		VenueName:        req.VenueName,
		VenueCity:        req.VenueCity,
		Items:            req.Items,
		CustomerName:     req.CustomerName,
		CustomerEmail:    req.CustomerEmail,
		CustomerDocument: req.CustomerDocument,
		Amount:           amount,
		Currency:         req.Currency,
	})
	if err != nil {
		err = fmt.Errorf("marshal payload: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// The idempotency key is derived from the order's own identity
	// (sourceOrderId), never the server-generated OrderID — a redelivery
	// carries the same sourceOrderId, so it's the natural dedup boundary.
	key := computeKey(req.SourceOrderID, req.IdempotencyKey)
	span.SetAttributes(attribute.String("idempotency_key", key))

	headers := make(map[string]string, len(req.Headers)+1)
	for k, v := range req.Headers {
		headers[k] = v
	}
	headers["schemaVersion"] = domain.SchemaVersion

	msg := &domain.OutboxMessage{
		ID:             orderID,
		IdempotencyKey: key,
		AggregateType:  "order",
		HTTPMethod:     "POST",
		Route:          "/api/v1/orders",
		Payload:        payload,
		Headers:        headers,
		Status:         domain.OutboxStatusNew,
		CreatedAt:      time.Now().UTC(),
		EventType:      req.EventType,
		EventSubtype:   req.EventSubtype,
	}

	var created bool
	if err := uc.uow.Execute(ctx, func(ctx context.Context) error {
		var err error
		created, err = uc.outboxRepo.Enqueue(ctx, uc.uow, msg)
		return err
	}); err != nil {
		err = fmt.Errorf("enqueue order outbox: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Bool("dedup_hit", !created))
	if !created {
		uc.duplicateTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("event_type", req.EventType),
			attribute.String("event_subtype", req.EventSubtype),
		))
	}

	return &Response{OrderID: orderID, IdempotencyKey: key, Created: created}, nil
}

func computeKey(sourceOrderID, clientKey string) string {
	combined := "order:" + sourceOrderID
	if clientKey != "" {
		combined += ":" + clientKey
	}
	sum := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(sum[:])
}

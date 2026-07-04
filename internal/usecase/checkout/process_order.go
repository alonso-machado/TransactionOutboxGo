// Package checkout holds the ProcessOrder use-case: order-consumer-worker's
// consumer-side handling of an order_outbox message. It upserts the
// Location/Event the order belongs to, reserves Tickets for every item,
// opens a checkout with the configured PaymentGateway, and persists the
// resulting Charge — all inside one UnitOfWork, so a crash between any two
// steps leaves nothing half-committed. It implements
// messaging.MessageProcessor.
package checkout

import (
	"context"
	"encoding/json"
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

var tracer = otel.Tracer("usecase/checkout")

type ProcessOrder struct {
	locationRepo   domain.LocationRepository
	eventRepo      domain.EventRepository
	orderRepo      domain.OrderRepository
	ticketRepo     domain.TicketRepository
	chargeRepo     domain.ChargeRepository
	gateway        domain.PaymentGateway
	uow            domain.UnitOfWork
	provider       string // gateway provider name, stamped onto Charge.Provider
	successURL     string // where the gateway redirects the customer after payment
	ordersTotal    metric.Int64Counter
	duplicateTotal metric.Int64Counter
}

func New(
	locationRepo domain.LocationRepository,
	eventRepo domain.EventRepository,
	orderRepo domain.OrderRepository,
	ticketRepo domain.TicketRepository,
	chargeRepo domain.ChargeRepository,
	gateway domain.PaymentGateway,
	uow domain.UnitOfWork,
	provider, successURL string,
) *ProcessOrder {
	meter := otel.GetMeterProvider().Meter("usecase/checkout")
	return &ProcessOrder{
		locationRepo:   locationRepo,
		eventRepo:      eventRepo,
		orderRepo:      orderRepo,
		ticketRepo:     ticketRepo,
		chargeRepo:     chargeRepo,
		gateway:        gateway,
		uow:            uow,
		provider:       provider,
		successURL:     successURL,
		ordersTotal:    observability.Int64Counter(meter, "checkout.orders_processed_total"),
		duplicateTotal: observability.Int64Counter(meter, "checkout.duplicate_total"),
	}
}

// itemDTO/payloadDTO mirror usecase/order.ItemRequest/outboxPayload's JSON
// shape exactly (field-for-field) without importing that package —
// use-cases must not depend on one another; domain.SchemaVersion is the
// shared contract instead.
type itemDTO struct {
	SourceTicketID string `json:"sourceTicketId"`
	Section        string `json:"section"`
	Row            string `json:"row"`
	Seat           string `json:"seat"`
	Price          int64  `json:"price"`
	Currency       string `json:"currency"`
}

type payloadDTO struct {
	SchemaVersion    string    `json:"schemaVersion"`
	OrderID          string    `json:"orderId"`
	SourceOrderID    string    `json:"sourceOrderId"`
	EventType        string    `json:"eventType"`
	EventSubtype     string    `json:"eventSubtype"`
	SourceEventID    string    `json:"sourceEventId"`
	EventName        string    `json:"eventName"`
	SourceVenueID    string    `json:"sourceVenueId"`
	VenueName        string    `json:"venueName"`
	VenueCity        string    `json:"venueCity"`
	Items            []itemDTO `json:"items"`
	CustomerName     string    `json:"customerName"`
	CustomerEmail    string    `json:"customerEmail"`
	CustomerDocument string    `json:"customerDocument"`
	Amount           int64     `json:"amount"`
	Currency         string    `json:"currency"`
}

// Execute implements messaging.MessageProcessor. messageID is unused
// directly — Order dedup is on SourceOrderID (the outbox idempotency key,
// which is messageID by construction, see AMQPPublisher.fire) — but the
// parameter is kept so this satisfies the same interface as
// usecase/fulfillment.IssueTickets.
func (uc *ProcessOrder) Execute(ctx context.Context, _ string, body []byte) (bool, error) {
	ctx, span := tracer.Start(ctx, "checkout.process_order", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var dto payloadDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return false, recordRedactedError(span, "error", fmt.Errorf("unmarshal payload: %w", err))
	}

	span.SetAttributes(attribute.String("schema_version", dto.SchemaVersion))
	if dto.SchemaVersion != "" && dto.SchemaVersion != domain.SchemaVersion {
		return false, recordRedactedError(span, "unknown_schema_version",
			fmt.Errorf("%w: %q (supported: %q)", domain.ErrUnknownSchemaVersion, dto.SchemaVersion, domain.SchemaVersion))
	}

	orderID, err := uuid.Parse(dto.OrderID)
	if err != nil {
		return false, recordRedactedError(span, "error", fmt.Errorf("parse orderId: %w", err))
	}
	span.SetAttributes(
		attribute.String("order_id", orderID.String()),
		attribute.String("event_type", dto.EventType),
		attribute.String("event_subtype", dto.EventSubtype),
	)

	items := make([]domain.OrderItem, len(dto.Items))
	for i, it := range dto.Items {
		items[i] = domain.OrderItem{
			SourceTicketID: it.SourceTicketID,
			Section:        it.Section,
			Row:            it.Row,
			Seat:           it.Seat,
			Price:          it.Price,
			Currency:       it.Currency,
		}
	}

	now := time.Now().UTC()
	newOrder := &domain.Order{
		ID:            orderID,
		SourceOrderID: dto.SourceOrderID,
		EventType:     dto.EventType,
		EventSubtype:  dto.EventSubtype,
		SourceEventID: dto.SourceEventID,
		SourceVenueID: dto.SourceVenueID,
		VenueName:     dto.VenueName,
		VenueCity:     dto.VenueCity,
		Items:         items,
		Customer: domain.Customer{
			Name:     dto.CustomerName,
			Email:    dto.CustomerEmail,
			Document: dto.CustomerDocument,
		},
		Amount:    dto.Amount,
		Currency:  dto.Currency,
		Status:    domain.OrderStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	// The gateway checkout call runs inside the same transaction as the
	// order/ticket/charge writes so a crash mid-way never leaves reserved
	// tickets with no charge to pay them — at the cost of holding the DB
	// transaction open for the round trip to the gateway. Acceptable at this
	// system's scale; a higher-throughput deployment would split this into
	// "reserve" (own transaction, commit) then "open checkout" (best-effort,
	// with a reconciliation sweep for orders stuck RESERVED with no Charge).
	var created bool
	if txErr := uc.uow.Execute(ctx, func(ctx context.Context) error {
		var err error
		created, err = uc.orderRepo.Save(ctx, uc.uow, newOrder)
		if err != nil {
			return fmt.Errorf("save order: %w", err)
		}
		if !created {
			// Redelivery of an already-processed order: the charge/tickets
			// were created on the first delivery — nothing further to do.
			return nil
		}

		locID, err := uc.locationRepo.UpsertBySourceVenueID(ctx, uc.uow, &domain.Location{
			ID:            uuid.Must(uuid.NewV7()),
			Name:          dto.VenueName,
			City:          dto.VenueCity,
			SourceVenueID: dto.SourceVenueID,
			CreatedAt:     now,
		})
		if err != nil {
			return fmt.Errorf("upsert location: %w", err)
		}

		eventName := dto.EventName
		if eventName == "" {
			eventName = dto.SourceEventID
		}
		eventID, err := uc.eventRepo.UpsertBySourceEventID(ctx, uc.uow, &domain.Event{
			ID:            uuid.Must(uuid.NewV7()),
			EventType:     dto.EventType,
			EventSubtype:  dto.EventSubtype,
			Name:          eventName,
			LocationID:    locID,
			SourceEventID: dto.SourceEventID,
			CreatedAt:     now,
		})
		if err != nil {
			return fmt.Errorf("upsert event: %w", err)
		}

		tickets := make([]*domain.Ticket, len(items))
		for i, it := range items {
			tickets[i] = &domain.Ticket{
				ID:             uuid.Must(uuid.NewV7()),
				OrderID:        orderID,
				EventID:        eventID,
				SourceTicketID: it.SourceTicketID,
				Section:        it.Section,
				Row:            it.Row,
				Seat:           it.Seat,
				Price:          it.Price,
				Currency:       it.Currency,
				BuyerName:      dto.CustomerName,
				BuyerEmail:     dto.CustomerEmail,
				Status:         domain.TicketStatusReserved,
				CreatedAt:      now,
			}
		}
		if err := uc.ticketRepo.ReserveForOrder(ctx, uc.uow, tickets); err != nil {
			return fmt.Errorf("reserve tickets: %w", err)
		}

		if err := uc.orderRepo.UpdateStatus(ctx, uc.uow, orderID, domain.OrderStatusReserved); err != nil {
			return fmt.Errorf("update order status: %w", err)
		}

		session, err := uc.gateway.CreateCheckout(domain.ChargeRequest{
			OrderID:       orderID,
			EventType:     dto.EventType,
			EventSubtype:  dto.EventSubtype,
			Amount:        dto.Amount,
			Currency:      dto.Currency,
			CustomerName:  dto.CustomerName,
			CustomerEmail: dto.CustomerEmail,
			SuccessURL:    uc.successURL,
		})
		if err != nil {
			return fmt.Errorf("create checkout: %w", err)
		}

		charge := &domain.Charge{
			ID:          uuid.Must(uuid.NewV7()),
			OrderID:     orderID,
			Provider:    uc.provider,
			ProviderRef: session.ProviderRef,
			CheckoutURL: session.CheckoutURL,
			Amount:      dto.Amount,
			Currency:    dto.Currency,
			Status:      domain.ChargeStatusPending,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := uc.chargeRepo.Save(ctx, uc.uow, charge); err != nil {
			return fmt.Errorf("save charge: %w", err)
		}
		return nil
	}); txErr != nil {
		return false, recordRedactedError(span, "error", txErr)
	}

	outcome := "processed"
	if !created {
		outcome = "duplicate"
		uc.duplicateTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("event_type", dto.EventType),
			attribute.String("event_subtype", dto.EventSubtype),
		))
	}
	span.SetAttributes(attribute.String("outcome", outcome))
	uc.ordersTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event_type", dto.EventType),
		attribute.String("event_subtype", dto.EventSubtype),
		attribute.String("outcome", outcome),
	))

	return created, nil
}

func recordRedactedError(span trace.Span, outcome string, err error) error {
	span.SetAttributes(attribute.String("outcome", outcome))
	redacted := pii.Redact(err.Error())
	span.RecordError(fmt.Errorf("%s", redacted))
	span.SetStatus(codes.Error, redacted)
	return err
}

// Package fulfillment holds the IssueTickets use-case: fulfillment-consumer-worker's
// consumer-side handling of a payment_event_outbox message. It resolves the
// Charge the gateway confirmation belongs to (via ProviderRef), and on
// CONFIRMED issues every RESERVED ticket for the order (QR PNG + HMAC
// signature via domain.TicketQR); on FAILED it voids the reservation. It
// implements messaging.MessageProcessor.
package fulfillment

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

var tracer = otel.Tracer("usecase/fulfillment")

type IssueTickets struct {
	chargeRepo             domain.ChargeRepository
	ticketRepo             domain.TicketRepository
	orderRepo              domain.OrderRepository
	qr                     domain.TicketQR
	notificationOutboxRepo domain.OutboxRepository
	uow                    domain.UnitOfWork
	// eventType/eventSubtype are this process's own shard (parsed once at
	// startup from CONSUMER_QUEUE, see cmd/fulfillment-consumer-worker) —
	// constant for the whole process's lifetime, unlike checkout.ProcessOrder
	// which reads them per-message from the order payload itself.
	// payment_event_outbox's payload carries no event_type/event_subtype
	// (only OutboxMessage's own columns do), so this is the only place
	// IssueTickets can get them from to stamp onto a new notification outbox
	// row.
	eventType      string
	eventSubtype   string
	eventsTotal    metric.Int64Counter
	duplicateTotal metric.Int64Counter
}

func New(
	chargeRepo domain.ChargeRepository,
	ticketRepo domain.TicketRepository,
	orderRepo domain.OrderRepository,
	qr domain.TicketQR,
	notificationOutboxRepo domain.OutboxRepository,
	uow domain.UnitOfWork,
	eventType, eventSubtype string,
) *IssueTickets {
	meter := otel.GetMeterProvider().Meter("usecase/fulfillment")
	return &IssueTickets{
		chargeRepo:             chargeRepo,
		ticketRepo:             ticketRepo,
		orderRepo:              orderRepo,
		qr:                     qr,
		notificationOutboxRepo: notificationOutboxRepo,
		uow:                    uow,
		eventType:              eventType,
		eventSubtype:           eventSubtype,
		eventsTotal:            observability.Int64Counter(meter, "fulfillment.events_processed_total"),
		duplicateTotal:         observability.Int64Counter(meter, "fulfillment.duplicate_total"),
	}
}

// payloadDTO mirrors usecase/webhook.outboxPayload's JSON shape exactly
// without importing that package (use-cases must not depend on one
// another).
type payloadDTO struct {
	SchemaVersion string `json:"schemaVersion"`
	Provider      string `json:"provider"`
	ProviderRef   string `json:"providerRef"`
	Outcome       string `json:"outcome"`
}

// Execute implements messaging.MessageProcessor.
func (uc *IssueTickets) Execute(ctx context.Context, _ string, body []byte) (bool, error) {
	ctx, span := tracer.Start(ctx, "fulfillment.issue_tickets", trace.WithSpanKind(trace.SpanKindConsumer))
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
	span.SetAttributes(attribute.String("provider", dto.Provider), attribute.String("provider_ref", dto.ProviderRef))

	charge, err := uc.chargeRepo.FindByProviderRef(ctx, dto.ProviderRef)
	if err != nil {
		return false, recordRedactedError(span, "error", fmt.Errorf("find charge: %w", err))
	}

	if charge.Status != domain.ChargeStatusPending {
		// Already terminal (PAID/FAILED) — a redelivered webhook
		// confirmation is a safe no-op, not an error.
		span.SetAttributes(attribute.String("outcome", "duplicate"))
		uc.duplicateTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("provider", dto.Provider)))
		return false, nil
	}

	var processed bool
	var issuedTickets []*domain.Ticket
	if txErr := uc.uow.Execute(ctx, func(txCtx context.Context) error {
		switch dto.Outcome {
		case string(domain.PaymentOutcomeConfirmed):
			tickets, err := uc.confirmAndIssue(txCtx, charge)
			if err != nil {
				return err
			}
			issuedTickets = tickets
		case string(domain.PaymentOutcomeFailed):
			if err := uc.failAndVoid(txCtx, charge); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown payment outcome %q", dto.Outcome)
		}
		processed = true
		return nil
	}); txErr != nil {
		return false, recordRedactedError(span, "error", txErr)
	}

	// Notification enqueue happens AFTER the events-DB transaction above has
	// committed, as its own separate write against the outbox database —
	// order_outbox/payment_event_outbox/ticket_notification_outbox all live
	// in a different logical database than charges/orders/tickets, and
	// Postgres has no cross-database transactions, so this write can never
	// be made atomic with MarkIssued the way a same-database outbox write
	// normally would be (see CLAUDE.md's "no transaction spans the two
	// [outbox and events] databases" invariant). Best-effort: log and move
	// on rather than fail the whole message, since the payment confirmation
	// itself already succeeded and must not be undone or redelivered just
	// because the notification enqueue failed — a real, documented gap (a
	// ticket can end up issued with no notification enqueued if this write
	// fails), not silently papered over.
	for _, t := range issuedTickets {
		if err := uc.enqueueNotification(ctx, t); err != nil {
			slog.ErrorContext(ctx, "enqueue ticket notification failed", "ticket_id", t.ID.String(), "err", err.Error())
		}
	}

	span.SetAttributes(attribute.String("outcome", dto.Outcome))
	uc.eventsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("provider", dto.Provider),
		attribute.String("outcome", dto.Outcome),
	))
	return processed, nil
}

// confirmAndIssue marks charge/order PAID and every RESERVED ticket for the
// order VALID (QR PNG + HMAC signature), all inside the caller's events-DB
// transaction. Returns the tickets it just issued so the caller can enqueue
// their notifications after this transaction commits (see Execute) — that
// enqueue can't happen in here, since it targets a different database.
func (uc *IssueTickets) confirmAndIssue(ctx context.Context, charge *domain.Charge) ([]*domain.Ticket, error) {
	if err := uc.chargeRepo.UpdateStatus(ctx, uc.uow, charge.ID, domain.ChargeStatusPaid); err != nil {
		return nil, fmt.Errorf("mark charge paid: %w", err)
	}
	if err := uc.orderRepo.UpdateStatus(ctx, uc.uow, charge.OrderID, domain.OrderStatusPaid); err != nil {
		return nil, fmt.Errorf("mark order paid: %w", err)
	}
	tickets, err := uc.ticketRepo.FindByOrderID(ctx, charge.OrderID)
	if err != nil {
		return nil, fmt.Errorf("find tickets: %w", err)
	}
	var issued []*domain.Ticket
	for _, t := range tickets {
		if t.Status != domain.TicketStatusReserved {
			continue
		}
		qrPNG, qrContent, validationCode, signature, err := uc.qr.Generate(t.ID)
		if err != nil {
			return nil, fmt.Errorf("generate qr for ticket %s: %w", t.ID, err)
		}
		t.QRPNG = qrPNG
		t.QRContent = qrContent
		t.ValidationCode = validationCode
		t.Signature = signature
		if err := uc.ticketRepo.MarkIssued(ctx, uc.uow, t); err != nil {
			return nil, fmt.Errorf("mark ticket issued %s: %w", t.ID, err)
		}
		issued = append(issued, t)
	}
	return issued, nil
}

// notificationOutboxEventType/Subtype are the fixed sentinel pair
// notification-consumer-worker's single unsharded queue is bound to (see
// rmq.NotificationSentinelType/NotificationSentinelSubtype's doc comment in
// internal/infrastructure/rabbitmq/rabbitmq.go — duplicated here as a bare
// literal, not an import, since usecase must never import infrastructure,
// same convention OutboxMessage.AggregateType's string values already
// follow). AMQPPublisher.fire computes the routing key from
// OutboxMessage.EventType/EventSubtype directly — stamping the ticket's
// REAL event type/subtype here instead would route the message to
// "notification.<real-type>.<real-subtype>", which nothing is bound to (a
// topic-exchange black hole), since only "notification._all._all" has a
// queue. The real event type/subtype are still carried in
// notificationPayload below, for reporting.
const (
	notificationOutboxEventType    = "_ALL"
	notificationOutboxEventSubtype = "_ALL"
)

// notificationPayload is the JSON body landed on ticket_notification_outbox.
type notificationPayload struct {
	SchemaVersion string `json:"schemaVersion"`
	TicketID      string `json:"ticketId"`
	BuyerName     string `json:"buyerName"`
	BuyerEmail    string `json:"buyerEmail"`
	EventType     string `json:"eventType"`
	EventSubtype  string `json:"eventSubtype"`
	Section       string `json:"section"`
	Row           string `json:"row"`
	Seat          string `json:"seat"`
	QRPNG         []byte `json:"qrPng"`
	QRContent     string `json:"qrContent"`
}

func (uc *IssueTickets) enqueueNotification(ctx context.Context, t *domain.Ticket) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate notification id: %w", err)
	}
	payload, err := json.Marshal(notificationPayload{
		SchemaVersion: domain.SchemaVersion,
		TicketID:      t.ID.String(),
		BuyerName:     t.BuyerName,
		BuyerEmail:    t.BuyerEmail,
		EventType:     uc.eventType,
		EventSubtype:  uc.eventSubtype,
		Section:       t.Section,
		Row:           t.Row,
		Seat:          t.Seat,
		QRPNG:         t.QRPNG,
		QRContent:     t.QRContent,
	})
	if err != nil {
		return fmt.Errorf("marshal notification payload: %w", err)
	}
	// nil, not uc.uow: this Enqueue targets the outbox database, a
	// different one than uc.uow's events-DB transaction — see the doc
	// comment on the enqueueNotification call site in Execute.
	_, err = uc.notificationOutboxRepo.Enqueue(ctx, nil, &domain.OutboxMessage{
		ID:             id,
		IdempotencyKey: t.ID.String(), // one notification per issued ticket
		AggregateType:  "ticket_notification",
		HTTPMethod:     "",
		Route:          "",
		Payload:        payload,
		Headers:        map[string]string{"schemaVersion": domain.SchemaVersion},
		Status:         domain.OutboxStatusNew,
		CreatedAt:      time.Now().UTC(),
		EventType:      notificationOutboxEventType,
		EventSubtype:   notificationOutboxEventSubtype,
	})
	return err
}

func (uc *IssueTickets) failAndVoid(ctx context.Context, charge *domain.Charge) error {
	if err := uc.chargeRepo.UpdateStatus(ctx, uc.uow, charge.ID, domain.ChargeStatusFailed); err != nil {
		return fmt.Errorf("mark charge failed: %w", err)
	}
	if err := uc.orderRepo.UpdateStatus(ctx, uc.uow, charge.OrderID, domain.OrderStatusFailed); err != nil {
		return fmt.Errorf("mark order failed: %w", err)
	}
	if err := uc.ticketRepo.MarkVoid(ctx, uc.uow, charge.OrderID); err != nil {
		return fmt.Errorf("void tickets: %w", err)
	}
	return nil
}

func recordRedactedError(span trace.Span, outcome string, err error) error {
	span.SetAttributes(attribute.String("outcome", outcome))
	redacted := pii.Redact(err.Error())
	span.RecordError(fmt.Errorf("%s", redacted))
	span.SetStatus(codes.Error, redacted)
	return err
}

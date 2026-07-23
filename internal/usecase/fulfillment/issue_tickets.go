// Package fulfillment holds the IssueTickets use-case: fulfillment-consumer-worker's
// consumer-side handling of a payment_event_outbox message. It resolves the
// Charge the gateway confirmation belongs to (via ProviderRef), and on
// CONFIRMED issues every RESERVED ticket for the order (QR PNG + HMAC
// signature via domain.TicketQR) and emails it; on FAILED it voids the
// reservation. It implements messaging.MessageProcessor.
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("usecase/fulfillment")

type IssueTickets struct {
	chargeRepo       domain.ChargeRepository
	ticketRepo       domain.TicketRepository
	orderRepo        domain.OrderRepository
	qr               domain.TicketQR
	notificationRepo domain.TicketNotificationRepository
	sender           domain.EmailSender
	uow              domain.UnitOfWork
	eventsTotal      metric.Int64Counter
	duplicateTotal   metric.Int64Counter
}

func New(
	chargeRepo domain.ChargeRepository,
	ticketRepo domain.TicketRepository,
	orderRepo domain.OrderRepository,
	qr domain.TicketQR,
	notificationRepo domain.TicketNotificationRepository,
	sender domain.EmailSender,
	uow domain.UnitOfWork,
) *IssueTickets {
	meter := otel.GetMeterProvider().Meter("usecase/fulfillment")
	return &IssueTickets{
		chargeRepo:       chargeRepo,
		ticketRepo:       ticketRepo,
		orderRepo:        orderRepo,
		qr:               qr,
		notificationRepo: notificationRepo,
		sender:           sender,
		uow:              uow,
		eventsTotal:      observability.Int64Counter(meter, "fulfillment.events_processed_total"),
		duplicateTotal:   observability.Int64Counter(meter, "fulfillment.duplicate_total"),
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

	// The email is sent AFTER the transaction above has committed — a send
	// failure must never undo the payment confirmation that already
	// succeeded, so this stays best-effort/log-only. Unlike the old
	// cross-database ticket_notification_outbox enqueue, the
	// ticket_notifications ROW itself was already created atomically with
	// MarkIssued inside confirmAndIssue (same events-DB transaction) — only
	// the act of sending happens outside it, since an SMTP call can't be
	// part of a DB transaction anyway. A send failure here just leaves the
	// row's email_sent_timestamp NULL for notification-retry-cron to retry.
	for _, t := range issuedTickets {
		if err := uc.sendNotification(ctx, t); err != nil {
			slog.ErrorContext(ctx, "send ticket notification failed", "ticket_id", t.ID.String(), "err", pii.Redact(err.Error()))
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
// transaction — including the ticket_notifications row created right after
// MarkIssued, so a ticket can never end up issued without one (or vice
// versa). Returns the tickets it just issued so the caller can email them
// once this transaction has committed (see Execute).
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
		if err := uc.notificationRepo.Create(ctx, uc.uow, t.ID); err != nil {
			return nil, fmt.Errorf("create ticket notification %s: %w", t.ID, err)
		}
		issued = append(issued, t)
	}
	return issued, nil
}

// sendNotification emails ticket t's QR code and records the outcome on its
// ticket_notifications row (MarkSent/MarkFailed). Deliberately
// self-contained rather than calling into usecase/notification directly —
// use-cases must not depend on one another (see payloadDTO's doc comment
// above); usecase/notification.SendTicketNotification.Send duplicates this
// same logic for notification-retry-cron's retry path.
func (uc *IssueTickets) sendNotification(ctx context.Context, t *domain.Ticket) error {
	bodyText := fmt.Sprintf("Hi %s,\n\nYour ticket (Section %s, Row %s, Seat %s) is attached as a QR code.\n",
		t.BuyerName, t.Section, t.Row, t.Seat)
	_, sendErr := uc.sender.Send(domain.EmailRequest{
		ToEmail:               t.BuyerEmail,
		ToName:                t.BuyerName,
		Subject:               "Your ticket is ready",
		BodyText:              bodyText,
		AttachmentName:        "ticket.png",
		AttachmentContentType: "image/png",
		Attachment:            t.QRPNG,
	})
	if sendErr != nil {
		if markErr := uc.notificationRepo.MarkFailed(ctx, t.ID, sendErr.Error()); markErr != nil {
			slog.ErrorContext(ctx, "mark notification failed error", "ticket_id", t.ID.String(), "err", markErr.Error())
		}
		return fmt.Errorf("send email: %w", sendErr)
	}
	if markErr := uc.notificationRepo.MarkSent(ctx, t.ID, time.Now().UTC()); markErr != nil {
		slog.ErrorContext(ctx, "mark notification sent error", "ticket_id", t.ID.String(), "err", markErr.Error())
	}
	return nil
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

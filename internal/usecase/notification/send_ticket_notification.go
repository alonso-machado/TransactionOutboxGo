// Package notification holds the SendTicketNotification use-case: emails an
// issued ticket's QR code. Send is called synchronously by
// usecase/fulfillment.IssueTickets right after a ticket is issued, and
// RetryPending is notification-retry-cron's whole job (a Kubernetes
// CronJob, no RabbitMQ involved) — both share the same send logic, the only
// difference is where the domain.Ticket comes from.
package notification

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("usecase/notification")

type SendTicketNotification struct {
	sender           domain.EmailSender
	notificationRepo domain.TicketNotificationRepository
	ticketRepo       domain.TicketRepository
	sentTotal        metric.Int64Counter
}

func New(sender domain.EmailSender, notificationRepo domain.TicketNotificationRepository, ticketRepo domain.TicketRepository) *SendTicketNotification {
	meter := otel.GetMeterProvider().Meter("usecase/notification")
	return &SendTicketNotification{
		sender:           sender,
		notificationRepo: notificationRepo,
		ticketRepo:       ticketRepo,
		sentTotal:        observability.Int64Counter(meter, "notification.sent_total"),
	}
}

// Send emails ticket t's QR code, then records the outcome on its
// ticket_notifications row (MarkSent/MarkFailed). Always best-effort: the
// caller must not undo an already-committed payment confirmation just
// because the email failed — a failure is recorded (for
// notification-retry-cron to pick up later) and also returned so the caller
// can log it, never anything more disruptive than that.
func (uc *SendTicketNotification) Send(ctx context.Context, t *domain.Ticket) error {
	ctx, span := tracer.Start(ctx, "notification.send_ticket_notification", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()
	span.SetAttributes(attribute.String("ticket_id", t.ID.String()))

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
		redacted := pii.Redact(sendErr.Error())
		span.SetAttributes(attribute.String("outcome", "error"))
		span.RecordError(fmt.Errorf("%s", redacted))
		uc.sentTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "error")))
		if markErr := uc.notificationRepo.MarkFailed(ctx, t.ID, sendErr.Error()); markErr != nil {
			slog.ErrorContext(ctx, "mark notification failed error", "ticket_id", t.ID.String(), "err", markErr.Error())
		}
		return fmt.Errorf("send email: %w", sendErr)
	}

	span.SetAttributes(attribute.String("outcome", "sent"))
	uc.sentTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "sent")))
	if markErr := uc.notificationRepo.MarkSent(ctx, t.ID, time.Now().UTC()); markErr != nil {
		slog.ErrorContext(ctx, "mark notification sent error", "ticket_id", t.ID.String(), "err", markErr.Error())
	}
	return nil
}

// RetryPending is notification-retry-cron's whole job: find every ticket
// whose email hasn't been sent yet (or failed and its backoff window has
// passed) and retry Send for each. Returns how many it attempted —
// individual send failures are already recorded via MarkFailed inside Send
// and logged here, not surfaced as this call's error; only a failure to
// even fetch the batch is.
func (uc *SendTicketNotification) RetryPending(ctx context.Context, limit int) (int, error) {
	pending, err := uc.notificationRepo.FetchPendingForRetry(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("fetch pending notifications: %w", err)
	}
	for _, n := range pending {
		t, err := uc.ticketRepo.FindByID(ctx, n.TicketID)
		if err != nil {
			slog.ErrorContext(ctx, "retry: find ticket failed", "ticket_id", n.TicketID.String(), "err", err.Error())
			continue
		}
		if err := uc.Send(ctx, t); err != nil {
			slog.ErrorContext(ctx, "retry: send failed", "ticket_id", n.TicketID.String(), "err", pii.Redact(err.Error()))
		}
	}
	return len(pending), nil
}

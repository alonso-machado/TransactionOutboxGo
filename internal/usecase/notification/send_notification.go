// Package notification holds the SendNotification use-case:
// notification-consumer-worker's handling of a ticket_notification_outbox
// message. Unlike checkout.ProcessOrder/fulfillment.IssueTickets, this
// consumer never touches the events DB — email-sending is fire-and-forget,
// with no local state transition to record (see the Phase 8 plan's
// explicit scope-cut note on the lack of send-dedup). It implements
// messaging.MessageProcessor.
package notification

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("usecase/notification")

type SendNotification struct {
	sender    domain.EmailSender
	sentTotal metric.Int64Counter
}

func New(sender domain.EmailSender) *SendNotification {
	meter := otel.GetMeterProvider().Meter("usecase/notification")
	return &SendNotification{
		sender:    sender,
		sentTotal: observability.Int64Counter(meter, "notification.sent_total"),
	}
}

// payloadDTO is self-contained — everything Send needs, no DB round trip —
// mirroring how usecase/checkout/usecase/fulfillment's payload DTOs mirror
// usecase/order/usecase/webhook's outbox payload shapes field-for-field.
// QRPNG is a []byte field, so encoding/json base64-encodes/decodes it
// automatically; no manual encoding needed.
type payloadDTO struct {
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

// Execute implements messaging.MessageProcessor. There is no dedup concept
// here (a redelivered message just resends the email — an explicit,
// documented scope cut, see the Phase 8 plan's Notes), so this always
// reports created=true on a successful send.
func (uc *SendNotification) Execute(ctx context.Context, _ string, body []byte) (bool, error) {
	ctx, span := tracer.Start(ctx, "notification.send_notification", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var dto payloadDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return false, recordOutcome(ctx, span, uc.sentTotal, "error", fmt.Errorf("unmarshal payload: %w", err))
	}
	span.SetAttributes(attribute.String("schema_version", dto.SchemaVersion), attribute.String("ticket_id", dto.TicketID))
	if dto.SchemaVersion != "" && dto.SchemaVersion != domain.SchemaVersion {
		return false, recordOutcome(ctx, span, uc.sentTotal, "unknown_schema_version",
			fmt.Errorf("%w: %q (supported: %q)", domain.ErrUnknownSchemaVersion, dto.SchemaVersion, domain.SchemaVersion))
	}

	eventLabel := fmt.Sprintf("%s %s", dto.EventType, dto.EventSubtype)
	subject := fmt.Sprintf("Your ticket for %s", eventLabel)
	bodyText := fmt.Sprintf("Hi %s,\n\nYour ticket for %s (Section %s, Row %s, Seat %s) is attached as a QR code.\n",
		dto.BuyerName, eventLabel, dto.Section, dto.Row, dto.Seat)

	if _, err := uc.sender.Send(domain.EmailRequest{
		ToEmail:               dto.BuyerEmail,
		ToName:                dto.BuyerName,
		Subject:               subject,
		BodyText:              bodyText,
		AttachmentName:        "ticket.png",
		AttachmentContentType: "image/png",
		Attachment:            dto.QRPNG,
	}); err != nil {
		return false, recordOutcome(ctx, span, uc.sentTotal, "error", fmt.Errorf("send email: %w", err))
	}

	span.SetAttributes(attribute.String("outcome", "sent"))
	uc.sentTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "sent")))
	return true, nil
}

func recordOutcome(ctx context.Context, span trace.Span, counter metric.Int64Counter, outcome string, err error) error {
	span.SetAttributes(attribute.String("outcome", outcome))
	span.RecordError(err)
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	return err
}

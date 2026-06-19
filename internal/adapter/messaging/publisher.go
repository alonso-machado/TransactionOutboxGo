package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("rabbitmq")

type amqpHeaderCarrier amqp.Table

func (c amqpHeaderCarrier) Get(key string) string {
	v, _ := c[key].(string)
	return v
}

func (c amqpHeaderCarrier) Set(key, value string) {
	c[key] = value
}

func (c amqpHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

type AMQPPublisher struct {
	conn *amqp.Connection
}

func NewPublisher(conn *amqp.Connection) *AMQPPublisher {
	return &AMQPPublisher{conn: conn}
}

func (p *AMQPPublisher) Publish(ctx context.Context, msg *domain.OutboxMessage) error {
	ctx, span := tracer.Start(ctx, "rabbitmq.publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	ch, err := p.conn.Channel()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Confirm(false); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("enable confirms: %w", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	headers := amqp.Table{}
	for k, v := range msg.Headers {
		headers[k] = v
	}
	otel.GetTextMapPropagator().Inject(ctx, amqpHeaderCarrier(headers))

	span.SetAttributes(attribute.String("message_id", msg.IdempotencyKey))

	err = ch.PublishWithContext(ctx, rmq.Exchange, rmq.RoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.IdempotencyKey,
		Timestamp:    time.Now().UTC(),
		Body:         msg.Payload,
		Headers:      headers,
	})
	if err != nil {
		err = fmt.Errorf("publish: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			err := fmt.Errorf("broker nacked message %s", msg.IdempotencyKey)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	case <-time.After(5 * time.Second):
		err := fmt.Errorf("confirm timeout for message %s", msg.IdempotencyKey)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return ctx.Err()
	}
	return nil
}

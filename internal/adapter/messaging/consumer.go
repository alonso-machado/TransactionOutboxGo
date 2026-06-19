package messaging

import (
	"context"
	"errors"
	"log"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/consume"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type AMQPConsumer struct {
	conn          *amqp.Connection
	processMsg    *consume.ProcessMessage
	prefetch      int
	maxDeliveries int
}

func NewConsumer(conn *amqp.Connection, processMsg *consume.ProcessMessage, prefetch, maxDeliveries int) *AMQPConsumer {
	return &AMQPConsumer{conn: conn, processMsg: processMsg, prefetch: prefetch, maxDeliveries: maxDeliveries}
}

func (c *AMQPConsumer) Run(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(rmq.Queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			c.handle(ctx, d)
		}
	}
}

func (c *AMQPConsumer) handle(ctx context.Context, d amqp.Delivery) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, amqpHeaderCarrier(d.Headers))
	ctx, span := tracer.Start(ctx, "rabbitmq.consume", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("message_id", d.MessageId),
			attribute.Bool("redelivered", d.Redelivered),
		),
	)
	defer span.End()

	xDeath, _ := d.Headers["x-death"].([]interface{})
	if len(xDeath) > 0 {
		if table, ok := xDeath[0].(amqp.Table); ok {
			if count, ok := table["count"].(int64); ok && int(count) >= c.maxDeliveries {
				log.Printf("poison message %s after %d deliveries — rejecting to DLQ", d.MessageId, count)
				span.SetAttributes(attribute.String("outcome", "poison_dlq"))
				_ = d.Reject(false)
				return
			}
		}
	}

	if err := c.processMsg.Execute(ctx, d.MessageId, d.Body); err != nil {
		log.Printf("process message %s error: %s — requeuing", d.MessageId, pii.Redact(err.Error()))
		span.RecordError(errors.New(pii.Redact(err.Error())))
		span.SetStatus(codes.Error, pii.Redact(err.Error()))
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

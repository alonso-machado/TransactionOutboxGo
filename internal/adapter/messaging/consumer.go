package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/consume"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// retryCountHeader tracks delivery attempts ourselves rather than relying on
// the broker's redelivery bookkeeping — see DeclareTopology's doc comment
// for why.
const retryCountHeader = "x-retry-count"

type AMQPConsumer struct {
	conn          *amqp.Connection
	processMsg    *consume.ProcessMessage
	method        string
	prefetch      int
	maxDeliveries int
}

// NewConsumer builds a consumer bound to method's queue (e.g. "PIX" ->
// payments.pix.queue) — each consumer-worker instance consumes exactly one
// method's queue, decoupling payment methods from consumer scaling.
func NewConsumer(conn *amqp.Connection, processMsg *consume.ProcessMessage, method string, prefetch, maxDeliveries int) *AMQPConsumer {
	return &AMQPConsumer{conn: conn, processMsg: processMsg, method: method, prefetch: prefetch, maxDeliveries: maxDeliveries}
}

func (c *AMQPConsumer) Run(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if err := rmq.DeclareQueue(ch, c.method); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(rmq.QueueFor(c.method), "", false, false, false, false, nil)
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
	retryCount := retryCountFromHeaders(d.Headers)
	ctx, span := tracer.Start(ctx, "rabbitmq.consume", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("message_id", d.MessageId),
			attribute.Bool("redelivered", d.Redelivered),
			attribute.Int("retry_count", retryCount),
		),
	)
	defer span.End()

	if err := c.processMsg.Execute(ctx, d.MessageId, d.Body); err != nil {
		log.Printf("process message %s error: %s — attempt %d", d.MessageId, pii.Redact(err.Error()), retryCount+1)
		span.RecordError(errors.New(pii.Redact(err.Error())))
		span.SetStatus(codes.Error, pii.Redact(err.Error()))

		if retryCount+1 >= c.maxDeliveries {
			log.Printf("poison message %s after %d attempts — rejecting to DLQ", d.MessageId, retryCount+1)
			span.SetAttributes(attribute.String("outcome", "poison_dlq"))
			_ = d.Reject(false)
			return
		}

		if reErr := c.requeueWithRetryCount(d, retryCount+1); reErr != nil {
			log.Printf("requeue message %s error: %v — falling back to broker requeue", d.MessageId, reErr)
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)
		return
	}
	_ = d.Ack(false)
}

func retryCountFromHeaders(h amqp.Table) int {
	switch v := h[retryCountHeader].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return int(v)
	default:
		return 0
	}
}

// requeueWithRetryCount republishes the delivery to the same queue with its
// retry count incremented, then the caller Acks the original delivery — a
// plain Nack(requeue=true) can't carry an updated header, so the retry has
// to be a fresh publish.
func (c *AMQPConsumer) requeueWithRetryCount(d amqp.Delivery, retryCount int) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[retryCountHeader] = int32(retryCount)

	err = ch.PublishWithContext(context.Background(), rmq.Exchange, rmq.RoutingKeyFor(c.method), false, false, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    time.Now().UTC(),
		Body:         d.Body,
		Headers:      headers,
	})
	if err != nil {
		return fmt.Errorf("publish retry: %w", err)
	}

	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("broker nacked retry publish for %s", d.MessageId)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("retry publish confirm timeout for %s", d.MessageId)
	}
	return nil
}

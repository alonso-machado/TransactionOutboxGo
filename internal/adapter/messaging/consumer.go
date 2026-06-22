package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/consume"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// retryCountHeader tracks delivery attempts ourselves rather than relying on
// the broker's redelivery bookkeeping — see DeclareTopology's doc comment
// for why.
const retryCountHeader = "x-retry-count"

type AMQPConsumer struct {
	conn             *amqp.Connection
	processMsg       *consume.ProcessMessage
	method           string
	prefetch         int
	maxDeliveries    int
	retryBackoffBase time.Duration
	retryBackoffCap  time.Duration
	messagesTotal    metric.Int64Counter
	retryAttempts    metric.Int64Counter
}

// NewConsumer builds a consumer bound to method's queue (e.g. "PIX" ->
// payments.pix.queue) — each consumer-worker instance consumes exactly one
// method's queue, decoupling payment methods from consumer scaling.
// retryBackoffBase/Cap drive the per-message TTL on the *.retry queue
// (Phase 5 Track 2.A); zero values fall back to 1s/5m.
func NewConsumer(conn *amqp.Connection, processMsg *consume.ProcessMessage, method string, prefetch, maxDeliveries int, retryBackoffBase, retryBackoffCap time.Duration) *AMQPConsumer {
	if retryBackoffBase <= 0 {
		retryBackoffBase = time.Second
	}
	if retryBackoffCap <= 0 {
		retryBackoffCap = 5 * time.Minute
	}
	meter := otel.GetMeterProvider().Meter("adapter/messaging")
	messagesTotal, err := meter.Int64Counter("consumer.messages_processed_total")
	if err != nil {
		slog.ErrorContext(context.Background(), "create consumer.messages_processed_total counter failed", "err", err.Error())
	}
	retryAttempts, err := meter.Int64Counter("consumer.retry_attempts_total")
	if err != nil {
		slog.ErrorContext(context.Background(), "create consumer.retry_attempts_total counter failed", "err", err.Error())
	}
	return &AMQPConsumer{
		conn:             conn,
		processMsg:       processMsg,
		method:           method,
		prefetch:         prefetch,
		maxDeliveries:    maxDeliveries,
		retryBackoffBase: retryBackoffBase,
		retryBackoffCap:  retryBackoffCap,
		messagesTotal:    messagesTotal,
		retryAttempts:    retryAttempts,
	}
}

// metricAttrs are the dimensions every consumer metric is sliced by — method
// and its bound queue/routing (binding) key, so a Grafana panel can isolate
// one payment method's throughput/retry/poison rate from the others even
// though every method's consumer shares this same code path.
func (c *AMQPConsumer) metricAttrs(extra ...attribute.KeyValue) metric.AddOption {
	attrs := append([]attribute.KeyValue{
		attribute.String("payment_method", c.method),
		attribute.String("payment_queue", rmq.QueueFor(c.method)),
		attribute.String("routing_key", rmq.RoutingKeyFor(c.method)),
	}, extra...)
	return metric.WithAttributes(attrs...)
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
	isRetry := retryCount > 0
	routingKey := rmq.RoutingKeyFor(c.method)
	ctx, span := tracer.Start(ctx, "rabbitmq.consume", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("message_id", d.MessageId),
			attribute.Bool("redelivered", d.Redelivered),
			attribute.Int("retry_count", retryCount),
			attribute.Bool("is_retry", isRetry),
			// Lets traces/logs be sliced per payment method even though
			// every method's consumer shares this same code path — each
			// consumer-worker instance only ever handles its own method.
			attribute.String("payment_method", c.method),
			attribute.String("payment_queue", rmq.QueueFor(c.method)),
			attribute.String("routing_key", routingKey), // the topic-exchange binding key this message arrived on
		),
	)
	defer span.End()

	if c.retryAttempts != nil && isRetry {
		c.retryAttempts.Add(ctx, 1, c.metricAttrs(attribute.Int("retry_count", retryCount)))
	}

	if err := c.processMsg.Execute(ctx, d.MessageId, d.Body); err != nil {
		slog.ErrorContext(ctx, "process message error", "message_id", d.MessageId, "err", pii.Redact(err.Error()), "attempt", retryCount+1)
		span.RecordError(errors.New(pii.Redact(err.Error())))
		span.SetStatus(codes.Error, pii.Redact(err.Error()))

		// An unknown/newer schema version can never succeed on retry — it's
		// a structural incompatibility, not a transient failure — so reject
		// straight to DLQ on the first attempt instead of burning through
		// maxDeliveries (Phase 5 Track 2.D).
		if errors.Is(err, consume.ErrUnknownSchemaVersion) {
			slog.ErrorContext(ctx, "unknown schema version — rejecting to DLQ", "message_id", d.MessageId)
			span.SetAttributes(attribute.String("outcome", "unknown_schema_version"))
			if c.messagesTotal != nil {
				c.messagesTotal.Add(ctx, 1, c.metricAttrs(attribute.String("outcome", "unknown_schema_version")))
			}
			_ = d.Reject(false)
			return
		}

		if retryCount+1 >= c.maxDeliveries {
			slog.ErrorContext(ctx, "poison message — rejecting to DLQ", "message_id", d.MessageId, "attempts", retryCount+1)
			span.SetAttributes(attribute.String("outcome", "poison_dlq"))
			if c.messagesTotal != nil {
				c.messagesTotal.Add(ctx, 1, c.metricAttrs(attribute.String("outcome", "poison_dlq")))
			}
			_ = d.Reject(false)
			return
		}

		if reErr := c.requeueWithRetryCount(d, retryCount+1); reErr != nil {
			slog.ErrorContext(ctx, "requeue message error — falling back to broker requeue", "message_id", d.MessageId, "err", reErr.Error())
			_ = d.Nack(false, true)
			return
		}
		if c.messagesTotal != nil {
			c.messagesTotal.Add(ctx, 1, c.metricAttrs(attribute.String("outcome", "retry_scheduled")))
		}
		_ = d.Ack(false)
		return
	}
	if c.messagesTotal != nil {
		c.messagesTotal.Add(ctx, 1, c.metricAttrs(attribute.String("outcome", "ack")))
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

// requeueWithRetryCount publishes the delivery onto the per-method
// *.retry queue (Phase 5 Track 2.A) with its retry count incremented and a
// per-message `expiration` (TTL) set to the computed backoff, then the
// caller Acks the original delivery — a plain Nack(requeue=true) can't
// carry an updated header or a delay, so the retry has to be a fresh
// publish. The retry queue has no consumer; once its TTL elapses the broker
// dead-letters the message back onto the main queue (see
// rmq.declareMethodQueue's retryArgs), giving variable per-message
// exponential backoff with no broker plugin.
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

	backoff := domain.Backoff(retryCount, c.retryBackoffBase, c.retryBackoffCap)
	expirationMs := strconv.FormatInt(backoff.Milliseconds(), 10)

	// Publish directly to the retry queue (default exchange, routing key =
	// queue name) — it isn't bound to rmq.Exchange, it's a holding pen the
	// message sits in for its TTL before the broker's own DLX redelivers it
	// onto the main queue.
	err = ch.PublishWithContext(context.Background(), "", rmq.RetryQueueFor(c.method), false, false, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    time.Now().UTC(),
		Body:         d.Body,
		Headers:      headers,
		Expiration:   expirationMs,
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

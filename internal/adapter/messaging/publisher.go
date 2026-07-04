package messaging

import (
	"context"
	"fmt"
	"sync"
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

const confirmBufferSize = 4096

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

	// mu guards ch/confirms — DispatchOutbox.dispatch() calls Publish
	// sequentially today, but the mutex makes the channel-reuse safe even if
	// that ever changes. One long-lived channel with confirms enabled once
	// (CLAUDE.md: "Publisher confirms must be enabled on the DispatchOutbox
	// AMQP channel" — singular), not a fresh open/Confirm/close per message:
	// each of those is its own network round trip to the broker, and doing
	// all of them per message inside a 200-message dispatch batch is what
	// was capping outbox->RabbitMQ throughput at ~50-60 msg/s regardless of
	// OUTBOX_DISPATCH_BATCH_SIZE/INTERVAL_MS. getChannel reopens lazily only
	// when the cached channel is nil or has been invalidated by a prior
	// error.
	mu       sync.Mutex
	ch       *amqp.Channel
	confirms <-chan amqp.Confirmation
}

func NewPublisher(conn *amqp.Connection) *AMQPPublisher {
	return &AMQPPublisher{conn: conn}
}

// getChannel returns the cached channel, opening and confirm-enabling a new
// one if there isn't one yet. Caller must hold p.mu.
func (p *AMQPPublisher) getChannel() (*amqp.Channel, <-chan amqp.Confirmation, error) {
	if p.ch != nil {
		return p.ch, p.confirms, nil
	}

	ch, err := p.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("enable confirms: %w", err)
	}

	// Buffered well past any realistic OUTBOX_DISPATCH_BATCH_SIZE —
	// PublishBatch fires every publish in a batch before reading any
	// confirm back, so the buffer must hold a full batch's worth of
	// confirmations without blocking amqp091-go's internal read loop.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, confirmBufferSize))
	p.ch = ch
	p.confirms = confirms
	return ch, confirms, nil
}

// invalidate drops the cached channel so the next Publish call reopens one —
// called when the channel errors or a confirm comes back nacked/timed out,
// since the channel may be in a broken state at that point. Caller must hold
// p.mu.
func (p *AMQPPublisher) invalidate() {
	if p.ch != nil {
		_ = p.ch.Close()
	}
	p.ch = nil
	p.confirms = nil
}

// fire publishes msg on ch without waiting for its confirm — the part of
// Publish/PublishBatch that's identical either way. Caller must hold p.mu.
func (p *AMQPPublisher) fire(ctx context.Context, ch *amqp.Channel, msg *domain.OutboxMessage) error {
	headers := amqp.Table{}
	for k, v := range msg.Headers {
		headers[k] = v
	}
	otel.GetTextMapPropagator().Inject(ctx, amqpHeaderCarrier(headers))

	stream, ok := rmq.StreamForAggregateType(msg.AggregateType)
	if !ok {
		return fmt.Errorf("unknown aggregate type %q for message %s", msg.AggregateType, msg.IdempotencyKey)
	}
	routingKey := rmq.RoutingKeyFor(stream, msg.EventType, msg.EventSubtype)

	return ch.PublishWithContext(ctx, rmq.Exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.IdempotencyKey,
		Timestamp:    time.Now().UTC(),
		Body:         msg.Payload,
		Headers:      headers,
	})
}

// awaitConfirm waits for the next confirmation on confirms, matched to msg
// purely by FIFO order (RabbitMQ confirms publishes on a channel in the
// order they were published) — correct as long as callers never publish on
// the same channel from two goroutines concurrently (p.mu enforces that).
func (p *AMQPPublisher) awaitConfirm(ctx context.Context, confirms <-chan amqp.Confirmation, msg *domain.OutboxMessage) error {
	select {
	case confirm, ok := <-confirms:
		if !ok {
			// Channel's NotifyPublish closed out from under us (broker-side
			// channel/connection error) — invalidate so the next call reopens.
			p.invalidate()
			return fmt.Errorf("confirm channel closed for message %s", msg.IdempotencyKey)
		}
		if !confirm.Ack {
			return fmt.Errorf("broker nacked message %s", msg.IdempotencyKey)
		}
		return nil
	case <-time.After(5 * time.Second):
		p.invalidate()
		return fmt.Errorf("confirm timeout for message %s", msg.IdempotencyKey)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *AMQPPublisher) Publish(ctx context.Context, msg *domain.OutboxMessage) error {
	ctx, span := tracer.Start(ctx, "rabbitmq.publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	span.SetAttributes(attribute.String("message_id", msg.IdempotencyKey))

	p.mu.Lock()
	defer p.mu.Unlock()

	ch, confirms, err := p.getChannel()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := p.fire(ctx, ch, msg); err != nil {
		p.invalidate()
		err = fmt.Errorf("publish: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := p.awaitConfirm(ctx, confirms, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// PublishBatch pipelines the whole batch: every message is fired before any
// confirm is awaited, so the batch's total wall time is roughly one
// round-trip plus broker processing, not len(msgs) round trips. This is the
// difference between DispatchOutbox draining its backlog at ~50-400 msg/s
// (one round trip per message, serialized) and saturating the channel's
// actual throughput.
func (p *AMQPPublisher) PublishBatch(ctx context.Context, msgs []*domain.OutboxMessage) []error {
	ctx, span := tracer.Start(ctx, "rabbitmq.publish_batch", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	span.SetAttributes(attribute.Int("batch_size", len(msgs)))

	results := make([]error, len(msgs))
	if len(msgs) == 0 {
		return results
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	ch, confirms, err := p.getChannel()
	if err != nil {
		for i := range results {
			results[i] = err
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return results
	}

	sent := 0
	for i, msg := range msgs {
		if fireErr := p.fire(ctx, ch, msg); fireErr != nil {
			p.invalidate()
			fireErr = fmt.Errorf("publish: %w", fireErr)
			// The channel is gone — every message from here on, sent or
			// not, has no confirm to wait for, so they all fail together.
			for j := i; j < len(msgs); j++ {
				results[j] = fireErr
			}
			span.RecordError(fireErr)
			span.SetStatus(codes.Error, fireErr.Error())
			return results
		}
		sent = i + 1
	}

	for i := 0; i < sent; i++ {
		results[i] = p.awaitConfirm(ctx, confirms, msgs[i])
	}
	return results
}

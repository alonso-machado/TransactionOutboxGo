package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPPublisher struct {
	conn *amqp.Connection
}

func NewPublisher(conn *amqp.Connection) *AMQPPublisher {
	return &AMQPPublisher{conn: conn}
}

func (p *AMQPPublisher) Publish(ctx context.Context, msg *domain.OutboxMessage) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable confirms: %w", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	headers := amqp.Table{
		"http_method": msg.HTTPMethod,
		"route":       msg.Route,
	}
	for k, v := range msg.Headers {
		headers[k] = v
	}

	err = ch.PublishWithContext(ctx, rmq.Exchange, rmq.RoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.IdempotencyKey,
		Timestamp:    time.Now().UTC(),
		Body:         msg.Payload,
		Headers:      headers,
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("broker nacked message %s", msg.IdempotencyKey)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("confirm timeout for message %s", msg.IdempotencyKey)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

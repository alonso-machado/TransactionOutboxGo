package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange      = "outbox.exchange"
	Queue         = "outbox.queue"
	DLX           = "outbox.dlx"
	DLQ           = "outbox.dlq"
	RoutingKey    = "record.created"
	DLXRoutingKey = "record.dead"
)

func Connect(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

func DeclareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(DLX, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}
	if _, err := ch.QueueDeclare(DLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	if err := ch.QueueBind(DLQ, DLXRoutingKey, DLX, false, nil); err != nil {
		return fmt.Errorf("bind dlq: %w", err)
	}
	if err := ch.ExchangeDeclare(Exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	args := amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    DLX,
		"x-dead-letter-routing-key": DLXRoutingKey,
	}
	if _, err := ch.QueueDeclare(Queue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := ch.QueueBind(Queue, RoutingKey, Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}
	return nil
}

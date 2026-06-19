package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange      = "payments.exchange"
	Queue         = "payments.queue"
	DLX           = "payments.dlx"
	DLQ           = "payments.dlq"
	RoutingKey    = "payment.created"
	DLXRoutingKey = "payment.dead"
)

func Connect(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

// DeclareTopology declares the exchange/queue/DLX/DLQ topology.
//
// Poison-message handling does NOT rely on the quorum queue's own
// x-delivery-limit: testing against RabbitMQ 4.3 showed it set correctly via
// the management API (delivery_limit visible in queue args) but never
// actually triggering dead-lettering after thousands of Nack(requeue=true)
// redeliveries on a single long-lived consumer connection — see
// AMQPConsumer.handle, which instead tracks its own retry count via an
// "x-retry-count" message header and explicitly Reject(requeue=false)s once
// exhausted. Explicit reject-without-requeue against a queue with
// x-dead-letter-exchange configured is the basic DLX mechanism and always
// dead-letters, independent of any quorum-queue feature.
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

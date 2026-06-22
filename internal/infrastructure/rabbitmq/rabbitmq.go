package rabbitmq

import (
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange = "payments.exchange"
	DLX      = "payments.dlx"
)

// Methods is the canonical list of payment methods that have a dedicated
// queue. ValidateMethod (adapter/http) rejects any payment.method not in
// this list with a 400 — a method without a bound queue would be a topic
// exchange black hole (published, matched by no binding, silently dropped).
var Methods = []string{"PIX", "BOLETO", "TRANSFER", "CARTAO_CREDITO", "CARTAO_DEBITO"}

// Connect dials the AMQP broker. tlsEnabled is the PCI-DSS encryption-in-
// transit toggle (Phase 5 Track 5.B, config.Config.RabbitMQTLS) — when true
// and url uses the plain amqp:// scheme, it's switched to amqps:// before
// dialing (Amazon MQ in cloud requires amqps://; local/compose stays
// amqp://). If url already specifies a scheme other than "amqp", it's left
// untouched.
func Connect(url string, tlsEnabled bool) (*amqp.Connection, error) {
	return amqp.Dial(withAMQPS(url, tlsEnabled))
}

func withAMQPS(url string, tlsEnabled bool) string {
	if tlsEnabled && strings.HasPrefix(url, "amqp://") {
		return "amqps://" + strings.TrimPrefix(url, "amqp://")
	}
	return url
}

// IsValidMethod reports whether method (expected upper-case) has a bound queue.
func IsValidMethod(method string) bool {
	for _, m := range Methods {
		if m == method {
			return true
		}
	}
	return false
}

// QueueFor returns the durable quorum queue name for method, e.g.
// "payments.pix.queue".
func QueueFor(method string) string {
	return "payments." + strings.ToLower(method) + ".queue"
}

// DLQFor returns the dead-letter queue name for method, e.g.
// "payments.pix.dlq".
func DLQFor(method string) string {
	return "payments." + strings.ToLower(method) + ".dlq"
}

// RetryQueueFor returns the per-method retry-holding queue name for method,
// e.g. "payments.pix.retry" (Phase 5 Track 2.A). It has no consumer; a
// message sits here for its backoff TTL (set per-message via the
// `expiration` field on publish), then the broker dead-letters it back onto
// QueueFor(method) via this queue's DLX/dead-letter-routing-key args.
func RetryQueueFor(method string) string {
	return "payments." + strings.ToLower(method) + ".retry"
}

// RoutingKeyFor returns the topic-exchange routing key for method, e.g.
// "payment.pix".
func RoutingKeyFor(method string) string {
	return "payment." + strings.ToLower(method)
}

// DLXRoutingKeyFor returns the dead-letter routing key for method, e.g.
// "payment.pix.dead".
func DLXRoutingKeyFor(method string) string {
	return "payment." + strings.ToLower(method) + ".dead"
}

// MethodForQueue reverse-looks-up the method that owns queue (e.g.
// "payments.pix.queue" -> "PIX", true). It's how consumer-worker turns the
// single PAYMENT_QUEUE env var it's given into the method used to derive the
// retry routing key, keeping the method<->queue mapping in one place.
func MethodForQueue(queue string) (string, bool) {
	for _, m := range Methods {
		if QueueFor(m) == queue {
			return m, true
		}
	}
	return "", false
}

// DeclareTopology declares the shared exchange/DLX plus every method's queue
// and DLQ. Called by ingestion-api on startup, since DispatchOutbox
// publishes to all of them.
func DeclareTopology(ch *amqp.Channel) error {
	if err := declareExchanges(ch); err != nil {
		return err
	}
	for _, method := range Methods {
		if err := declareMethodQueue(ch, method); err != nil {
			return err
		}
	}
	return nil
}

// DeclareQueue idempotently declares the shared exchange/DLX plus just one
// method's queue and DLQ. Used by consumer-worker, which only ever binds to
// its own queue — re-declaring the exchanges too makes a worker
// self-sufficient even if it starts before ingestion-api (declare is
// idempotent, so this is safe to repeat).
func DeclareQueue(ch *amqp.Channel, method string) error {
	if err := declareExchanges(ch); err != nil {
		return err
	}
	return declareMethodQueue(ch, method)
}

func declareExchanges(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(DLX, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}
	if err := ch.ExchangeDeclare(Exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	return nil
}

// declareMethodQueue declares method's DLQ (bound to DLX) and its queue
// (bound to Exchange), wiring the queue's dead-letter args to its own DLQ.
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
func declareMethodQueue(ch *amqp.Channel, method string) error {
	dlq := DLQFor(method)
	dlxRoutingKey := DLXRoutingKeyFor(method)
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq %s: %w", dlq, err)
	}
	if err := ch.QueueBind(dlq, dlxRoutingKey, DLX, false, nil); err != nil {
		return fmt.Errorf("bind dlq %s: %w", dlq, err)
	}

	queue := QueueFor(method)
	args := amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    DLX,
		"x-dead-letter-routing-key": dlxRoutingKey,
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue %s: %w", queue, err)
	}
	if err := ch.QueueBind(queue, RoutingKeyFor(method), Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue %s: %w", queue, err)
	}

	// Phase 5 Track 2.A: per-method retry-holding queue, no consumer. A
	// message republished here with a per-message `expiration` (the
	// computed backoff) sits for that TTL, then the broker dead-letters it
	// back onto the routing key below — straight back onto `queue` — via
	// this queue's own DLX args. Bound directly to Exchange/RoutingKeyFor so
	// it's reachable the same way the main queue is, with no separate
	// binding needed for the retry republish.
	retryQueue := RetryQueueFor(method)
	retryArgs := amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    Exchange,
		"x-dead-letter-routing-key": RoutingKeyFor(method),
	}
	if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare retry queue %s: %w", retryQueue, err)
	}
	return nil
}

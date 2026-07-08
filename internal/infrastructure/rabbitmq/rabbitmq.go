package rabbitmq

import (
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange = "tickets.exchange"
	DLX      = "tickets.dlx"
)

// Stream distinguishes the two message flows that both route by
// (EventType, EventSubtype): order intake (ingestion-api ->
// order-consumer-worker) and payment-gateway webhook confirmations
// (ingestion-api -> fulfillment-consumer-worker). QueuePrefix names the
// queue/DLQ/retry-queue; RoutingPrefix names
// the topic-exchange routing key. Kept distinct (rather than one flat name)
// so a queue like "events.concert.rock.queue" and a routing key like
// "order.concert.rock" both read naturally.
type Stream struct {
	QueuePrefix   string
	RoutingPrefix string
}

var (
	// OrderStream carries ticket orders from ingestion-api's
	// POST /api/v1/orders to order-consumer-worker, e.g. queue
	// "events.concert.rock.queue", routing key "order.concert.rock".
	OrderStream = Stream{QueuePrefix: "events", RoutingPrefix: "order"}
	// PaymentEventStream carries verified payment-gateway webhook
	// confirmations from ingestion-api to fulfillment-consumer-worker, e.g.
	// queue "payments.concert.rock.queue", routing key "payment.concert.rock".
	PaymentEventStream = Stream{QueuePrefix: "payments", RoutingPrefix: "payment"}
	// NotificationStream carries ticket_notification_outbox rows from
	// fulfillment-consumer-worker (via outbox-worker's third dispatch loop)
	// to notification-consumer-worker. Deliberately NOT sharded by
	// (event_type, event_subtype) like the other two streams — email-sending
	// has no per-genre resource contention to isolate and this consumer
	// makes zero DB calls, so a shard-per-genre Deployment/ScaledObject would
	// be pure ceremony (see Phase 8 plan, Part D). Bound to exactly one
	// queue via the sentinel pair (NotificationSentinelType,
	// NotificationSentinelSubtype) below instead of a real (type, subtype)
	// from EventTypes.
	NotificationStream = Stream{QueuePrefix: "notifications", RoutingPrefix: "notification"}
)

// NotificationSentinelType/NotificationSentinelSubtype are a fixed
// pseudo-(event_type, event_subtype) pair used only to address
// NotificationStream's single queue through the existing
// Stream/QueueFor/AMQPConsumer machinery, without building a parallel
// unsharded-queue abstraction. Never added to EventTypes — an order intake
// request can never legitimately carry this pair (ValidateEventType would
// reject it), so it can't collide with a real shard.
const (
	NotificationSentinelType    = "_ALL"
	NotificationSentinelSubtype = "_ALL"
)

// StreamForAggregateType maps an OutboxMessage.AggregateType ("order" /
// "payment_event" / "ticket_notification") to its Stream — how
// AMQPPublisher decides which queue prefix/routing-key prefix a row
// publishes under without needing the caller to pass the stream through
// separately.
func StreamForAggregateType(aggregateType string) (Stream, bool) {
	switch aggregateType {
	case "order":
		return OrderStream, true
	case "payment_event":
		return PaymentEventStream, true
	case "ticket_notification":
		return NotificationStream, true
	default:
		return Stream{}, false
	}
}

// EventTypes is the canonical event_type -> []event_subtype registry that
// replaces the old per-payment-method Methods list. An order's
// (event_type, event_subtype) not found here has no bound queue on either
// stream — ValidateEventType (adapter/http) rejects it with a 400 rather
// than publishing into a topic-exchange black hole (matched by no binding,
// silently dropped), the same rationale the old per-method validation used.
var EventTypes = map[string][]string{
	"CONCERT":    {"ROCK", "POP", "ELECTRONIC", "SAMBA"},
	"SPORTS":     {"FOOTBALL", "BASKETBALL", "UFC"},
	"THEATER":    {"PLAY", "STANDUP", "MUSICAL"},
	"CONFERENCE": {"TECH", "BUSINESS"},
}

// IsValidEventType reports whether (eventType, eventSubtype) has a bound
// queue on every stream.
func IsValidEventType(eventType, eventSubtype string) bool {
	subtypes, ok := EventTypes[eventType]
	if !ok {
		return false
	}
	for _, s := range subtypes {
		if s == eventSubtype {
			return true
		}
	}
	return false
}

// Connect dials the AMQP broker. tlsEnabled is the PCI-DSS encryption-in-
// transit toggle (config.Config.RabbitMQTLS) — when true and url uses the
// plain amqp:// scheme, it's switched to amqps:// before dialing (Amazon MQ
// in cloud requires amqps://; local/compose stays amqp://). If url already
// specifies a scheme other than "amqp", it's left untouched.
func Connect(url string, tlsEnabled bool) (*amqp.Connection, error) {
	return amqp.Dial(withAMQPS(url, tlsEnabled))
}

func withAMQPS(url string, tlsEnabled bool) string {
	if tlsEnabled && strings.HasPrefix(url, "amqp://") {
		return "amqps://" + strings.TrimPrefix(url, "amqp://")
	}
	return url
}

func shardKey(eventType, eventSubtype string) string {
	return strings.ToLower(eventType) + "." + strings.ToLower(eventSubtype)
}

// QueueFor returns the durable quorum queue name for (stream, eventType,
// eventSubtype), e.g. "events.concert.rock.queue".
func QueueFor(stream Stream, eventType, eventSubtype string) string {
	return stream.QueuePrefix + "." + shardKey(eventType, eventSubtype) + ".queue"
}

// DLQFor returns the dead-letter queue name, e.g. "events.concert.rock.dlq".
func DLQFor(stream Stream, eventType, eventSubtype string) string {
	return stream.QueuePrefix + "." + shardKey(eventType, eventSubtype) + ".dlq"
}

// RetryQueueFor returns the per-shard retry-holding queue name, e.g.
// "events.concert.rock.retry". It has no consumer; a message sits here for
// its backoff TTL (set per-message via the `expiration` field on publish),
// then the broker dead-letters it back onto QueueFor(...) via this queue's
// DLX/dead-letter-routing-key args.
func RetryQueueFor(stream Stream, eventType, eventSubtype string) string {
	return stream.QueuePrefix + "." + shardKey(eventType, eventSubtype) + ".retry"
}

// RoutingKeyFor returns the topic-exchange routing key, e.g.
// "order.concert.rock".
func RoutingKeyFor(stream Stream, eventType, eventSubtype string) string {
	return stream.RoutingPrefix + "." + shardKey(eventType, eventSubtype)
}

// DLXRoutingKeyFor returns the dead-letter routing key, e.g.
// "order.concert.rock.dead".
func DLXRoutingKeyFor(stream Stream, eventType, eventSubtype string) string {
	return RoutingKeyFor(stream, eventType, eventSubtype) + ".dead"
}

// ParseQueueName reverse-looks-up the (stream, eventType, eventSubtype) a
// queue name belongs to, e.g. "events.concert.rock.queue" ->
// (OrderStream, "CONCERT", "ROCK", true). It's how
// order-consumer-worker/fulfillment-consumer-worker turn the single
// CONSUMER_QUEUE env var they're given into the routing info needed for
// their retry queue, keeping the
// queue<->(stream,type,subtype) mapping in one place.
func ParseQueueName(queue string) (stream Stream, eventType, eventSubtype string, ok bool) {
	if queue == QueueFor(NotificationStream, NotificationSentinelType, NotificationSentinelSubtype) {
		return NotificationStream, NotificationSentinelType, NotificationSentinelSubtype, true
	}
	for _, s := range []Stream{OrderStream, PaymentEventStream} {
		for et, subtypes := range EventTypes {
			for _, est := range subtypes {
				if QueueFor(s, et, est) == queue {
					return s, et, est, true
				}
			}
		}
	}
	return Stream{}, "", "", false
}

// DeclareTopology declares the shared exchange/DLX plus every (stream,
// event_type, event_subtype) queue and DLQ, on both streams. Called by
// outbox-worker on startup, since it publishes to all of them.
func DeclareTopology(ch *amqp.Channel) error {
	if err := declareExchanges(ch); err != nil {
		return err
	}
	for _, stream := range []Stream{OrderStream, PaymentEventStream} {
		for eventType, subtypes := range EventTypes {
			for _, eventSubtype := range subtypes {
				if err := declareShardQueue(ch, stream, eventType, eventSubtype); err != nil {
					return err
				}
			}
		}
	}
	// NotificationStream is a single unsharded queue, declared once outside
	// the EventTypes-driven double loop above — see its doc comment.
	if err := declareShardQueue(ch, NotificationStream, NotificationSentinelType, NotificationSentinelSubtype); err != nil {
		return err
	}
	return nil
}

// DeclareQueue idempotently declares the shared exchange/DLX plus just one
// (stream, event_type, event_subtype) queue and DLQ. Used by
// order-consumer-worker/fulfillment-consumer-worker, which only ever bind to
// their own queue — re-declaring
// the exchanges too makes a worker self-sufficient even if it starts before
// outbox-worker (declare is idempotent, so this is safe to repeat).
func DeclareQueue(ch *amqp.Channel, stream Stream, eventType, eventSubtype string) error {
	if err := declareExchanges(ch); err != nil {
		return err
	}
	return declareShardQueue(ch, stream, eventType, eventSubtype)
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

// declareShardQueue declares a (stream, event_type, event_subtype) shard's
// DLQ (bound to DLX) and its queue (bound to Exchange), wiring the queue's
// dead-letter args to its own DLQ.
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
func declareShardQueue(ch *amqp.Channel, stream Stream, eventType, eventSubtype string) error {
	dlq := DLQFor(stream, eventType, eventSubtype)
	dlxRoutingKey := DLXRoutingKeyFor(stream, eventType, eventSubtype)
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq %s: %w", dlq, err)
	}
	if err := ch.QueueBind(dlq, dlxRoutingKey, DLX, false, nil); err != nil {
		return fmt.Errorf("bind dlq %s: %w", dlq, err)
	}

	queue := QueueFor(stream, eventType, eventSubtype)
	args := amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    DLX,
		"x-dead-letter-routing-key": dlxRoutingKey,
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue %s: %w", queue, err)
	}
	routingKey := RoutingKeyFor(stream, eventType, eventSubtype)
	if err := ch.QueueBind(queue, routingKey, Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue %s: %w", queue, err)
	}

	// Per-shard retry-holding queue, no consumer. A message republished here
	// with a per-message `expiration` (the computed backoff) sits for that
	// TTL, then the broker dead-letters it back onto the routing key below —
	// straight back onto `queue` — via this queue's own DLX args. Bound
	// directly to Exchange/routingKey so it's reachable the same way the
	// main queue is, with no separate binding needed for the retry republish.
	retryQueue := RetryQueueFor(stream, eventType, eventSubtype)
	retryArgs := amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    Exchange,
		"x-dead-letter-routing-key": routingKey,
	}
	if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare retry queue %s: %w", retryQueue, err)
	}
	return nil
}

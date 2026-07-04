// Package main is a small, short-lived maintenance CLI — NOT a long-running
// service. It shares DATABASE_URL/RABBITMQ_URL config with the other
// services but never binds an HTTP port and is never reachable from the
// edge.
//
//	replay-dead        --outbox order|payment --event-type CONCERT --limit 100
//	                    resets DEAD_LETTER rows in order_outbox/payment_event_outbox back to NEW
//	drain-dlq          --stream order|payment --event-type CONCERT --event-subtype ROCK
//	                    moves messages from a shard's DLQ back onto its queue
//	purge-loadtest-dlq --stream order|payment --event-type CONCERT --event-subtype ROCK
//	                    drops only messages a loadtest run marked (see loadtestMarker)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// loadtestMarker is the value loadtest/*.js stamps into an order's
// customerName or a payment event's provider field — purge-loadtest-dlq
// matches on either.
const loadtestMarker = "LOADTEST"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	switch os.Args[1] {
	case "replay-dead":
		runReplayDead(cfg, os.Args[2:])
	case "drain-dlq":
		runDrainDLQ(cfg, os.Args[2:])
	case "purge-loadtest-dlq":
		runPurgeLoadtestDLQ(cfg, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `outbox-admin: maintenance commands for the transactional outbox

Usage:
  outbox-admin replay-dead --outbox order --event-type CONCERT --limit 100
      Reset DEAD_LETTER rows in order_outbox (or payment_event_outbox with
      --outbox payment) back to NEW. --event-type "" replays every event type.

  outbox-admin drain-dlq --stream order --event-type CONCERT --event-subtype ROCK
      Move messages sitting in the shard's DLQ back onto its queue,
      resetting the x-retry-count header. --stream is "order" or "payment".

  outbox-admin purge-loadtest-dlq --stream order --event-type CONCERT --event-subtype ROCK
      Scan the shard's DLQ and permanently remove only messages a loadtest
      run marked (see loadtestMarker). Any other message is left in the DLQ
      untouched — safe to run against a DLQ with a mix of real and loadtest
      messages, e.g. in UAT.`)
}

func outboxTable(name string) (string, error) {
	switch name {
	case "order":
		return "order_outbox", nil
	case "payment":
		return "payment_event_outbox", nil
	default:
		return "", fmt.Errorf("--outbox must be \"order\" or \"payment\", got %q", name)
	}
}

func streamFor(name string) (rmq.Stream, error) {
	switch name {
	case "order":
		return rmq.OrderStream, nil
	case "payment":
		return rmq.PaymentEventStream, nil
	default:
		return rmq.Stream{}, fmt.Errorf("--stream must be \"order\" or \"payment\", got %q", name)
	}
}

func runReplayDead(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("replay-dead", flag.ExitOnError)
	outbox := fs.String("outbox", "order", "which outbox: order | payment")
	eventType := fs.String("event-type", "", "event type to replay (empty = all)")
	limit := fs.Int("limit", 100, "max number of rows to reset")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse args: %v", err)
	}

	table, err := outboxTable(*outbox)
	if err != nil {
		log.Fatal(err)
	}

	db, err := database.Connect(cfg.DatabaseURL, cfg.DBSSLMode)
	if err != nil {
		log.Fatalf("database: %v", err)
	}

	replayer := persistence.NewOutboxRepository(db, table, cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	n, err := replayer.ReplayDeadLetters(ctx, *eventType, *limit)
	if err != nil {
		log.Fatalf("replay dead letters: %v", err)
	}
	log.Printf("replay-dead: reset %d row(s) to NEW (outbox=%q, event_type=%q, limit=%d)", n, table, *eventType, *limit)
}

func runDrainDLQ(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("drain-dlq", flag.ExitOnError)
	streamName := fs.String("stream", "", "stream whose shard to drain: order | payment (required)")
	eventType := fs.String("event-type", "", "event type (required)")
	eventSubtype := fs.String("event-subtype", "", "event subtype (required)")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse args: %v", err)
	}
	stream, err := streamFor(*streamName)
	if err != nil {
		log.Fatal(err)
	}
	if !rmq.IsValidEventType(*eventType, *eventSubtype) {
		log.Fatalf("unknown (event-type, event-subtype) = (%q, %q)", *eventType, *eventSubtype)
	}

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	if err := rmq.DeclareQueue(ch, stream, *eventType, *eventSubtype); err != nil {
		log.Fatalf("declare queue: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	dlq := rmq.DLQFor(stream, *eventType, *eventSubtype)
	queue := rmq.QueueFor(stream, *eventType, *eventSubtype)
	routingKey := rmq.RoutingKeyFor(stream, *eventType, *eventSubtype)

	moved := 0
	for {
		msg, ok, err := ch.Get(dlq, false)
		if err != nil {
			log.Fatalf("get from %s: %v", dlq, err)
		}
		if !ok {
			break
		}

		headers := amqp.Table{}
		for k, v := range msg.Headers {
			headers[k] = v
		}
		headers["x-retry-count"] = int32(0)

		err = ch.PublishWithContext(context.Background(), rmq.Exchange, routingKey, false, false, amqp.Publishing{
			ContentType:  msg.ContentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    msg.MessageId,
			Timestamp:    time.Now().UTC(),
			Body:         msg.Body,
			Headers:      headers,
		})
		if err != nil {
			_ = msg.Nack(false, true)
			log.Fatalf("republish to %s: %v", queue, err)
		}

		select {
		case confirm := <-confirms:
			if !confirm.Ack {
				_ = msg.Nack(false, true)
				log.Fatalf("broker nacked republish for %s", msg.MessageId)
			}
		case <-time.After(5 * time.Second):
			_ = msg.Nack(false, true)
			log.Fatalf("republish confirm timeout for %s", msg.MessageId)
		}

		if err := msg.Ack(false); err != nil {
			log.Fatalf("ack dlq message: %v", err)
		}
		moved++
	}

	log.Printf("drain-dlq: moved %d message(s) from %s to %s", moved, dlq, queue)
}

func runPurgeLoadtestDLQ(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("purge-loadtest-dlq", flag.ExitOnError)
	streamName := fs.String("stream", "", "stream whose shard to clean: order | payment (required)")
	eventType := fs.String("event-type", "", "event type (required)")
	eventSubtype := fs.String("event-subtype", "", "event subtype (required)")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse args: %v", err)
	}
	stream, err := streamFor(*streamName)
	if err != nil {
		log.Fatal(err)
	}
	if !rmq.IsValidEventType(*eventType, *eventSubtype) {
		log.Fatalf("unknown (event-type, event-subtype) = (%q, %q)", *eventType, *eventSubtype)
	}

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	if err := rmq.DeclareQueue(ch, stream, *eventType, *eventSubtype); err != nil {
		log.Fatalf("declare queue: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	dlq := rmq.DLQFor(stream, *eventType, *eventSubtype)

	// Bound the scan to the queue's depth at start: a kept (non-loadtest)
	// message is republished onto the back of the same queue before its
	// original delivery is acked, so an unbounded "until empty" loop (like
	// drain-dlq's) would cycle through real messages forever instead of
	// terminating once every original message has been inspected once.
	q, err := ch.QueueDeclarePassive(dlq, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("inspect %s: %v", dlq, err)
	}

	var removed, kept int
	for i := 0; i < q.Messages; i++ {
		msg, ok, err := ch.Get(dlq, false)
		if err != nil {
			log.Fatalf("get from %s: %v", dlq, err)
		}
		if !ok {
			break
		}

		if isLoadtestMessage(msg.Body) {
			if err := msg.Ack(false); err != nil {
				log.Fatalf("ack loadtest message %s: %v", msg.MessageId, err)
			}
			removed++
			continue
		}

		// Not a loadtest message: republish a fresh copy (same body,
		// headers, MessageId) before acking the original, so it survives
		// the scan instead of being dropped.
		err = ch.PublishWithContext(context.Background(), "", dlq, false, false, amqp.Publishing{
			ContentType:  msg.ContentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    msg.MessageId,
			Timestamp:    msg.Timestamp,
			Body:         msg.Body,
			Headers:      msg.Headers,
		})
		if err != nil {
			_ = msg.Nack(false, true)
			log.Fatalf("republish kept message %s: %v", msg.MessageId, err)
		}

		select {
		case confirm := <-confirms:
			if !confirm.Ack {
				_ = msg.Nack(false, true)
				log.Fatalf("broker nacked republish for %s", msg.MessageId)
			}
		case <-time.After(5 * time.Second):
			_ = msg.Nack(false, true)
			log.Fatalf("republish confirm timeout for %s", msg.MessageId)
		}

		if err := msg.Ack(false); err != nil {
			log.Fatalf("ack original kept message %s: %v", msg.MessageId, err)
		}
		kept++
	}

	log.Printf("purge-loadtest-dlq: removed %d loadtest message(s), kept %d real message(s) in %s", removed, kept, dlq)
}

// isLoadtestMessage reports whether body's payload carries loadtestMarker in
// either the order stream's customerName or the payment-event stream's
// provider field.
func isLoadtestMessage(body []byte) bool {
	var payload struct {
		CustomerName string `json:"customerName"`
		Provider     string `json:"provider"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	return payload.CustomerName == loadtestMarker || payload.Provider == loadtestMarker
}

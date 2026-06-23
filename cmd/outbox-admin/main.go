// Package main is a small, short-lived maintenance CLI (Phase 5 Track 2.C)
// — NOT a third long-running service. DispatchOutbox stays a goroutine
// inside ingestion-api; this binary is a one-shot `go run` / `make` target
// invocation (or a one-shot K8s Job in cloud) for dead-letter maintenance:
//
//	replay-dead       --method PIX --limit 100   outbox DEAD_LETTER rows -> NEW
//	drain-dlq         --method PIX                payments.pix.dlq -> payments.pix.queue
//	purge-loadtest-dlq --method PIX                payments.pix.dlq: drop only providerName="LOADTEST" messages
//
// It shares the same DATABASE_URL/RABBITMQ_URL config as the two services,
// but never binds an HTTP port and is never reachable from the edge.
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

// loadtestProviderName is the providerName both loadtest/k6-baseline.js
// (via payloads.js's envelope) and loadtest/k6-consumer.js stamp on every
// message they produce — the marker purge-loadtest-dlq matches on.
const loadtestProviderName = "LOADTEST"

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
  outbox-admin replay-dead --method PIX --limit 100
      Reset outbox DEAD_LETTER rows back to NEW (status=NEW, retry_count=0,
      next_retry_at=NULL, last_error cleared) so the dispatch loop picks
      them up and republishes. --method "" replays across every method.

  outbox-admin drain-dlq --method PIX
      Move messages sitting in payments.<method>.dlq back onto
      payments.<method>.queue, resetting the x-retry-count header.

  outbox-admin purge-loadtest-dlq --method PIX
      Scan payments.<method>.dlq and permanently remove only messages whose
      body has providerName="LOADTEST" (set by every loadtest/*.js script).
      Any other message is left in the DLQ untouched (republished with its
      original body/headers, never dropped) — safe to run against a DLQ
      that has a mix of real and loadtest messages, e.g. in UAT.`)
}

func runReplayDead(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("replay-dead", flag.ExitOnError)
	method := fs.String("method", "", "payment method to replay (empty = all methods)")
	limit := fs.Int("limit", 100, "max number of rows to reset")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse args: %v", err)
	}
	if *method != "" && !rmq.IsValidMethod(*method) {
		log.Fatalf("unknown method %q (expected one of: %v)", *method, rmq.Methods)
	}

	db, err := database.Connect(cfg.DatabaseURL, cfg.DBSSLMode)
	if err != nil {
		log.Fatalf("database: %v", err)
	}

	replayer := persistence.NewOutboxRepository(db, cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	n, err := replayer.ReplayDeadLetters(ctx, *method, *limit)
	if err != nil {
		log.Fatalf("replay dead letters: %v", err)
	}
	log.Printf("replay-dead: reset %d row(s) to NEW (method=%q, limit=%d)", n, *method, *limit)
}

func runDrainDLQ(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("drain-dlq", flag.ExitOnError)
	method := fs.String("method", "", "payment method whose DLQ to drain (required)")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse args: %v", err)
	}
	if *method == "" || !rmq.IsValidMethod(*method) {
		log.Fatalf("--method is required and must be one of: %v", rmq.Methods)
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

	if err := rmq.DeclareQueue(ch, *method); err != nil {
		log.Fatalf("declare queue: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	dlq := rmq.DLQFor(*method)
	queue := rmq.QueueFor(*method)
	routingKey := rmq.RoutingKeyFor(*method)

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
	method := fs.String("method", "", "payment method whose DLQ to clean (required)")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse args: %v", err)
	}
	if *method == "" || !rmq.IsValidMethod(*method) {
		log.Fatalf("--method is required and must be one of: %v", rmq.Methods)
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

	if err := rmq.DeclareQueue(ch, *method); err != nil {
		log.Fatalf("declare queue: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	dlq := rmq.DLQFor(*method)

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

		var payload struct {
			ProviderName string `json:"providerName"`
		}
		if json.Unmarshal(msg.Body, &payload) == nil && payload.ProviderName == loadtestProviderName {
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

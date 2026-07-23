// Package main is the composition root for outbox-worker.
//
// outbox-worker is the Transactional Outbox relay (DispatchOutbox), run
// twice over: once for order_outbox (order intake -> order-consumer-worker,
// with a LISTEN/NOTIFY fast path) and once for payment_event_outbox (payment-
// gateway webhook confirmations -> fulfillment-consumer-worker, poll-only — lower
// volume, doesn't need the low-latency path). Both publish with confirms
// and mark PUBLISHED/RETRYING/DEAD_LETTER; it was split out of ingestion-api
// so the relay scales on outbox backlog (KEDA) independently of the
// fixed-size HTTP front door — ingestion-api only writes the outbox rows,
// and this process is the only thing that reads them and talks to
// RabbitMQ.
//
// It connects to the *outbox* database (the same Postgres instance as the
// events DB, different logical database) and declares the RabbitMQ topology
// shared with order-consumer-worker/fulfillment-consumer-worker.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	outboxuc "github.com/alonsomachado/transaction-outbox-go/internal/usecase/outbox"
)

func main() {
	ctx := context.Background()
	startupLog := logging.NewLogger("startup", "json", os.Stdout)

	cfg, err := config.Load()
	if err != nil {
		startupLog.ErrorContext(ctx, "config load failed", "err", err.Error())
		os.Exit(1)
	}

	telemetryShutdown, err := telemetry.Setup(ctx, cfg.OtelServiceName, cfg.OtelEndpoint, cfg.MetricsPort, cfg.LogFormat)
	if err != nil {
		startupLog.ErrorContext(ctx, "telemetry setup failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := telemetryShutdown(shutdownCtx); err != nil {
			slog.ErrorContext(shutdownCtx, "telemetry shutdown error", "err", err.Error())
		}
	}()

	db, err := database.Connect(cfg.DatabaseURL, cfg.DBSSLMode)
	if err != nil {
		slog.ErrorContext(ctx, "database connect failed", "err", err.Error())
		os.Exit(1)
	}
	// Schema migrations are applied by the migrate/migrate one-shot against
	// the outbox DB before this starts (see docker-compose.yml's
	// migrate-outbox service / `make migrate`), never here.

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq connect failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	// outbox-worker is the sole publisher, so it owns topology declaration
	// (the shared exchange/DLX + every shard's queue/retry/DLQ, on both
	// streams). Idempotent — order-consumer-worker/fulfillment-consumer-worker never declare
	// the full topology, they rely on this having run.
	ch, err := conn.Channel()
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq channel failed", "err", err.Error())
		os.Exit(1)
	}
	if err := rmq.DeclareTopology(ch); err != nil {
		slog.ErrorContext(ctx, "rabbitmq topology failed", "err", err.Error())
		os.Exit(1)
	}
	if err := ch.Close(); err != nil {
		slog.ErrorContext(ctx, "close topology channel error", "err", err.Error())
	}

	publisher := messaging.NewPublisher(conn)
	dispatchInterval := time.Duration(cfg.DispatchInterval) * time.Millisecond
	pruneAfter := time.Duration(cfg.PruneAfterHours) * time.Hour

	orderOutboxRepo := persistence.NewOutboxRepository(db, "order_outbox", cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	orderDispatchUC := outboxuc.New(orderOutboxRepo, publisher, cfg.DispatchBatchSize, cfg.MaxRetries, dispatchInterval, pruneAfter)

	paymentEventOutboxRepo := persistence.NewOutboxRepository(db, "payment_event_outbox", cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	paymentEventDispatchUC := outboxuc.New(paymentEventOutboxRepo, publisher, cfg.DispatchBatchSize, cfg.MaxRetries, dispatchInterval, pruneAfter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A dedicated LISTEN connection wakes the order dispatcher immediately
	// on enqueue; the poll ticker inside Run remains the correctness
	// fallback if this connection ever drops. payment_event_outbox has no
	// Listener — it's poll-only (lower volume, no low-latency need).
	orderNotifyListener := database.NewListener(cfg.DatabaseURL, "order_outbox_new")
	go orderNotifyListener.Run(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.InfoContext(ctx, "outbox-worker shutting down...")
		cancel()
	}()

	slog.InfoContext(ctx, "outbox-worker started")
	go paymentEventDispatchUC.Run(ctx, nil)
	orderDispatchUC.Run(ctx, orderNotifyListener.Notify)
}

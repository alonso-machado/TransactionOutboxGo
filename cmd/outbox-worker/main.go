// Package main is the composition root for outbox-worker.
//
// outbox-worker is the Transactional Outbox relay (DispatchOutbox): it polls
// the outbox_messages table (with a LISTEN/NOTIFY fast path), publishes NEW/
// RETRYING rows to RabbitMQ with publisher confirms, and marks them
// PUBLISHED / RETRYING / DEAD_LETTER. It was split out of ingestion-api so the
// relay can scale on outbox backlog (KEDA) independently of the fixed-size
// HTTP front door — ingestion-api now only writes the outbox row, and this
// process is the only thing that reads it and talks to RabbitMQ.
//
// It connects to the *outbox* database (the same Postgres instance as the
// payments DB, different logical database) and shares the RabbitMQ topology it
// declares here with consumer-worker.
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
	// Startup logger: telemetry.Setup (which installs the trace-correlating
	// default logger) needs cfg first, so config-load failures use a bare
	// logger with no trace correlation — there's no span yet anyway.
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
	// Schema migrations are applied by the migrate/migrate one-shot against the
	// outbox DB before this starts (see docker-compose.yml's migrate-outbox
	// service / `make migrate`), never here.

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq connect failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	// outbox-worker is the sole publisher, so it owns topology declaration
	// (the shared exchange/DLX + every method's queue/retry/DLQ). Idempotent —
	// consumer-worker never declares, it relies on this having run.
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

	outboxRepo := persistence.NewOutboxRepository(db, cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	publisher := messaging.NewPublisher(conn)

	dispatchUC := outboxuc.New(
		outboxRepo,
		publisher,
		cfg.DispatchBatchSize,
		cfg.MaxRetries,
		time.Duration(cfg.DispatchInterval)*time.Millisecond,
		time.Duration(cfg.PruneAfterHours)*time.Hour,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Phase 5 Track 3.A — a dedicated LISTEN connection wakes DispatchOutbox
	// immediately on enqueue; the poll ticker inside Run remains the
	// correctness fallback if this connection ever drops. Now that the relay
	// is a separate process, the NOTIFY emitted by ingestion-api's INSERT
	// (migrations/outbox/..._outbox_notify) still wakes this listener — NOTIFY
	// is delivered to every backend LISTENing on the channel, across processes.
	notifyListener := database.NewListener(cfg.DatabaseURL)
	go notifyListener.Run(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.InfoContext(ctx, "outbox-worker shutting down...")
		cancel()
	}()

	slog.InfoContext(ctx, "outbox-worker started")
	dispatchUC.Run(ctx, notifyListener.Notify)
}

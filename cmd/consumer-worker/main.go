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
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/consume"
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
	// Schema migrations are no longer applied here — see
	// cmd/ingestion-api/main.go's comment; Phase 5 Track 1 moved them to
	// migrations/, applied via `make migrate` / the compose `migrate` service.

	method, ok := rmq.MethodForQueue(cfg.PaymentQueue)
	if !ok {
		slog.ErrorContext(ctx, "PAYMENT_QUEUE is not a known queue", "queue", cfg.PaymentQueue, "known_queues", rmq.Methods)
		os.Exit(1)
	}

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq connect failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	uow := persistence.NewUnitOfWork(db)
	paymentRepo := persistence.NewPaymentRepository(db)

	processUC := consume.New(paymentRepo, uow)
	consumer := messaging.NewConsumer(conn, processUC, method, cfg.PrefetchCount, cfg.MaxDeliveries, cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	runCtx, cancel := context.WithCancel(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		slog.InfoContext(ctx, "consumer-worker shutting down...")
		cancel()
	}()

	slog.InfoContext(ctx, "consumer-worker started")
	if err := consumer.Run(runCtx); err != nil {
		slog.ErrorContext(ctx, "consumer error", "err", err.Error())
	}
}

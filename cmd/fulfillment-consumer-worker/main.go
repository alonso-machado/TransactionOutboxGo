// Package main is the composition root for fulfillment-consumer-worker —
// new in the pivot to an Event Ticket System (the "-consumer-worker" suffix
// is a company naming convention for any service that consumes from
// RabbitMQ). fulfillment-consumer-worker consumes one payment_event_outbox
// shard's queue (CONSUMER_QUEUE, e.g.
// "payments.concert.rock.queue"): on a CONFIRMED payment it marks the
// Charge/Order PAID and issues every RESERVED ticket for the order (QR PNG +
// HMAC signature); on FAILED it marks them FAILED/VOID, releasing the
// reservation.
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
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/ticketqr"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/fulfillment"
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
	// Schema migrations are applied by `make migrate` / the migrate-events
	// compose one-shot before this starts, never here.

	stream, eventType, eventSubtype, ok := rmq.ParseQueueName(cfg.ConsumerQueue)
	if !ok || stream != rmq.PaymentEventStream {
		slog.ErrorContext(ctx, "CONSUMER_QUEUE is not a known payment-event-stream queue", "queue", cfg.ConsumerQueue)
		os.Exit(1)
	}

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq connect failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	uow := persistence.NewUnitOfWork(db)
	chargeRepo := persistence.NewChargeRepository(db)
	ticketRepo := persistence.NewTicketRepository(db)
	orderRepo := persistence.NewOrderRepository(db)
	qr := ticketqr.New(cfg.TicketSigningSecret)

	issueTicketsUC := fulfillment.New(chargeRepo, ticketRepo, orderRepo, qr, uow)
	consumer := messaging.NewConsumer(conn, issueTicketsUC, stream, eventType, eventSubtype, cfg.PrefetchCount, cfg.MaxDeliveries, cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	runCtx, cancel := context.WithCancel(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		slog.InfoContext(ctx, "fulfillment-consumer-worker shutting down...")
		cancel()
	}()

	slog.InfoContext(ctx, "fulfillment-consumer-worker started", "event_type", eventType, "event_subtype", eventSubtype)
	if err := consumer.Run(runCtx); err != nil {
		slog.ErrorContext(ctx, "consumer error", "err", err.Error())
	}
}

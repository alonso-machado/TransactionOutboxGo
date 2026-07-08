// Package main is the composition root for notification-consumer-worker —
// new in Phase 8 (the "-consumer-worker" suffix is a company naming
// convention for any service that consumes from RabbitMQ).
// notification-consumer-worker consumes ticket_notification_outbox's
// single, unsharded queue (rmq.NotificationStream's sentinel pair — see
// its doc comment for why this stream is deliberately not sharded by
// event_type/event_subtype like the other two) and sends the issued
// ticket's QR by email via the configured domain.EmailSender. It never
// touches the events DB — email delivery is fire-and-forget, with no
// local state transition to record.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/emailsender/fake"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/emailsender/smtp"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/notification"
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

	// DATABASE_URL is unused by this binary (usecase/notification.SendNotification
	// never queries a database) but still required — config.Config's
	// DatabaseURL carries a shared `required:"true"` tag across every
	// binary. Same "provide but ignore" precedent ingestion-api already
	// sets for its own unused RABBITMQ_URL.

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq connect failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	sender, err := newSender(cfg)
	if err != nil {
		slog.ErrorContext(ctx, "email sender init failed", "err", err.Error())
		os.Exit(1)
	}

	sendNotificationUC := notification.New(sender)
	// Only one queue ever exists for this stream (see rmq.NotificationStream's
	// doc comment) — no CONSUMER_QUEUE env parsing needed, unlike
	// order-consumer-worker/fulfillment-consumer-worker.
	consumer := messaging.NewConsumer(conn, sendNotificationUC, rmq.NotificationStream, rmq.NotificationSentinelType, rmq.NotificationSentinelSubtype, cfg.PrefetchCount, cfg.MaxDeliveries, cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	runCtx, cancel := context.WithCancel(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		slog.InfoContext(ctx, "notification-consumer-worker shutting down...")
		cancel()
	}()

	slog.InfoContext(ctx, "notification-consumer-worker started")
	if err := consumer.Run(runCtx); err != nil {
		slog.ErrorContext(ctx, "consumer error", "err", err.Error())
	}
}

// newSender selects the domain.EmailSender adapter by config.EmailProvider.
func newSender(cfg *config.Config) (domain.EmailSender, error) {
	switch cfg.EmailProvider {
	case "smtp":
		return smtp.New(cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUsername, cfg.SMTPPassword, cfg.SMTPFromEmail, cfg.SMTPFromName), nil
	case "fake", "":
		return fake.New(), nil
	default:
		return nil, fmt.Errorf("unknown EMAIL_PROVIDER %q", cfg.EmailProvider)
	}
}

// Package main is the composition root for notification-retry-cron — a
// Kubernetes CronJob (not a "-consumer-worker": it never consumes from
// RabbitMQ, so the company's -consumer-worker naming convention for
// RabbitMQ consumers doesn't apply to it). It wakes up on a schedule, finds
// every ticket_notifications row still missing email_sent_timestamp whose
// backoff window (next_retry_at) has passed, retries sending each one via
// usecase/notification.SendTicketNotification.RetryPending, and exits. No
// RabbitMQ connection, no long-running loop, no telemetry/metrics server —
// a CronJob pod lives seconds, so a scrape endpoint would never get
// scraped; plain structured logs are enough for a job this small.
package main

import (
	"context"
	"os"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/emailsender"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/notification"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startupLog := logging.NewLogger("notification-retry-cron", "json", os.Stdout)

	cfg, err := config.Load()
	if err != nil {
		startupLog.ErrorContext(ctx, "config load failed", "err", err.Error())
		os.Exit(1)
	}
	// RABBITMQ_URL is unused by this binary but still required —
	// config.Config's RabbitMQURL carries a shared `required:"true"` tag
	// across every binary. Same "provide but ignore" precedent ingestion-api
	// already sets for its own unused RABBITMQ_URL.
	log := logging.NewLogger("notification-retry-cron", cfg.LogFormat, os.Stdout)

	db, err := database.Connect(cfg.DatabaseURL, cfg.DBSSLMode)
	if err != nil {
		log.ErrorContext(ctx, "database connect failed", "err", err.Error())
		os.Exit(1)
	}
	// Schema migrations are applied by `make migrate` / the migrate-events
	// compose one-shot before this ever runs, never here.

	sender, err := emailsender.New(cfg.EmailProvider, cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUsername, cfg.SMTPPassword, cfg.SMTPFromEmail, cfg.SMTPFromName)
	if err != nil {
		log.ErrorContext(ctx, "email sender init failed", "err", err.Error())
		os.Exit(1)
	}

	ticketRepo := persistence.NewTicketRepository(db)
	notificationRepo := persistence.NewTicketNotificationRepository(db, cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	retryUC := notification.New(sender, notificationRepo, ticketRepo)

	attempted, err := retryUC.RetryPending(ctx, cfg.NotificationRetryBatchSize)
	if err != nil {
		log.ErrorContext(ctx, "retry pending notifications failed", "err", err.Error())
		os.Exit(1)
	}
	log.InfoContext(ctx, "notification-retry-cron run complete", "attempted", attempted)
}

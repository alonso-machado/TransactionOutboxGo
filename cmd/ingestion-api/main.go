// Package main is the composition root for ingestion-api.
//
//	@title			Transaction Outbox — Ingestion API
//	@version		1.0
//	@description	Accepts payment-provider webhook-shaped events and durably stores them in the
//	@description	transactional outbox for asynchronous relay to RabbitMQ.
//	@BasePath		/
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	handler "github.com/alonsomachado/transaction-outbox-go/internal/adapter/http"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ingest"
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
	// Schema migrations are no longer applied here — Phase 5 Track 1 moved
	// them to versioned golang-migrate files under migrations/, applied by
	// `make migrate` / the migrate/migrate compose service before the app
	// starts (see docker-compose.yml's `migrate` service).

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq connect failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

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

	uow := persistence.NewUnitOfWork(db)
	outboxRepo := persistence.NewOutboxRepository(db, cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	publisher := messaging.NewPublisher(conn)

	ingestUC := ingest.New(outboxRepo, uow)
	dispatchUC := outboxuc.New(
		outboxRepo,
		publisher,
		cfg.DispatchBatchSize,
		cfg.MaxRetries,
		time.Duration(cfg.DispatchInterval)*time.Millisecond,
		time.Duration(cfg.PruneAfterHours)*time.Hour,
	)

	paymentHandler := handler.NewPaymentHandler(ingestUC)

	rateLimitStore := ratelimit.NewInMemoryStore(10 * time.Minute)

	router := handler.NewRouter(paymentHandler, cfg.OtelServiceName, cfg.SwaggerEnabled, handler.RouterConfig{
		TrustedProxies:   cfg.TrustedProxies,
		RateLimitEnabled: cfg.RateLimitEnabled,
		RateLimitStore:   rateLimitStore,
		RateLimitRate:    cfg.RateLimitRate,
		RateLimitBurst:   cfg.RateLimitBurst,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.RateLimitEnabled {
		janitorStop := make(chan struct{})
		go rateLimitStore.Janitor(janitorStop, time.Minute)
		go func() {
			<-ctx.Done()
			close(janitorStop)
		}()
	}

	// Phase 5 Track 3.A — a dedicated LISTEN connection wakes DispatchOutbox
	// immediately on enqueue; the poll ticker inside Run remains the
	// correctness fallback if this connection ever drops.
	notifyListener := database.NewListener(cfg.DatabaseURL)
	go notifyListener.Run(ctx)

	go dispatchUC.Run(ctx, notifyListener.Notify)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: router,
	}

	go func() {
		slog.InfoContext(ctx, "ingestion-api listening", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(ctx, "http server error", "err", err.Error())
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.InfoContext(ctx, "shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.ErrorContext(shutdownCtx, "graceful shutdown error", "err", err.Error())
	}
}

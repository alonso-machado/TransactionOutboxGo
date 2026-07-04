// Package main is the composition root for ingestion-api.
//
//	@title			Transaction Outbox — Event Ticket System Ingestion API
//	@version		1.0
//	@description	Accepts ticket orders and payment-gateway webhook confirmations, durably storing
//	@description	both in the transactional outbox for asynchronous relay to RabbitMQ.
//	@BasePath		/
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	handler "github.com/alonsomachado/transaction-outbox-go/internal/adapter/http"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/abacatepay"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/fake"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/lemonsqueezy"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/stripe"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/order"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/webhook"
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
	// Schema migrations are applied by `make migrate` / the migrate-outbox
	// compose one-shot before this starts, never here.
	//
	// ingestion-api never talks to RabbitMQ or the events DB: the
	// Transactional Outbox relay (DispatchOutbox) is outbox-worker, and the
	// events domain is only ever written by
	// order-consumer-worker/fulfillment-consumer-worker.
	// ingestion-api only writes the two outbox tables; the INSERT fires the
	// LISTEN/NOTIFY trigger that wakes outbox-worker. This keeps
	// ingestion-api accepting writes even when the broker or the events DB
	// is down.

	uow := persistence.NewUnitOfWork(db)
	orderOutboxRepo := persistence.NewOutboxRepository(db, "order_outbox", cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	paymentEventOutboxRepo := persistence.NewOutboxRepository(db, "payment_event_outbox", cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	placeOrderUC := order.New(orderOutboxRepo, uow)
	receivePaymentEventUC := webhook.New(paymentEventOutboxRepo, uow)

	gateway, err := newGateway(cfg)
	if err != nil {
		slog.ErrorContext(ctx, "payment gateway init failed", "err", err.Error())
		os.Exit(1)
	}

	orderHandler := handler.NewOrderHandler(placeOrderUC)
	webhookHandler := handler.NewWebhookHandler(gateway, receivePaymentEventUC, cfg.PaymentProvider)

	rateLimitStore := ratelimit.NewInMemoryStore(10 * time.Minute)

	router := handler.NewRouter(orderHandler, webhookHandler, cfg.OtelServiceName, cfg.SwaggerEnabled, handler.RouterConfig{
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

// newGateway selects the domain.PaymentGateway adapter by
// config.PaymentProvider. This composition-root wiring is duplicated (in
// spirit) in cmd/order-consumer-worker/main.go — both processes need one, and it
// can't live in internal/adapter, which must not import
// internal/infrastructure/config.
func newGateway(cfg *config.Config) (domain.PaymentGateway, error) {
	switch cfg.PaymentProvider {
	case "stripe":
		return stripe.New(cfg.StripeSecretKey, cfg.StripeWebhookSecret), nil
	case "abacatepay":
		return abacatepay.New(), nil
	case "lemonsqueezy":
		return lemonsqueezy.New(), nil
	case "fake", "":
		return fake.New(""), nil
	default:
		return nil, fmt.Errorf("unknown PAYMENT_PROVIDER %q", cfg.PaymentProvider)
	}
}

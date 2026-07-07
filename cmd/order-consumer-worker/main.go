// Package main is the composition root for order-consumer-worker (renamed
// from consumer-worker in the pivot from a payments domain to an Event
// Ticket System; the "-consumer-worker" suffix is a company naming
// convention for any service that consumes from RabbitMQ).
// order-consumer-worker consumes one order_outbox shard's queue
// (CONSUMER_QUEUE, e.g. "events.concert.rock.queue"), upserts the
// Location/Event the order belongs to, reserves Tickets, opens a checkout
// with the configured PaymentGateway, and persists the Charge. It is the
// only writer of the events DB's locations/events/orders/tickets/charges
// tables; dedup is the orders.source_order_id UNIQUE constraint (no
// separate inbox table).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/abacatepay"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/fake"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/lemonsqueezy"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/mercadopago"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/pagarme"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/pagseguro"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/stripe"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/sumup"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/checkout"
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
	if !ok || stream != rmq.OrderStream {
		slog.ErrorContext(ctx, "CONSUMER_QUEUE is not a known order-stream queue", "queue", cfg.ConsumerQueue)
		os.Exit(1)
	}

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq connect failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	gateway, err := newGateway(cfg)
	if err != nil {
		slog.ErrorContext(ctx, "payment gateway init failed", "err", err.Error())
		os.Exit(1)
	}

	uow := persistence.NewUnitOfWork(db)
	locationRepo := persistence.NewLocationRepository(db)
	eventRepo := persistence.NewEventRepository(db)
	orderRepo := persistence.NewOrderRepository(db)
	ticketRepo := persistence.NewTicketRepository(db)
	chargeRepo := persistence.NewChargeRepository(db)

	processOrderUC := checkout.New(locationRepo, eventRepo, orderRepo, ticketRepo, chargeRepo, gateway, uow, cfg.PaymentProvider, cfg.CheckoutSuccessURL)
	consumer := messaging.NewConsumer(conn, processOrderUC, stream, eventType, eventSubtype, cfg.PrefetchCount, cfg.MaxDeliveries, cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	runCtx, cancel := context.WithCancel(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		slog.InfoContext(ctx, "order-consumer-worker shutting down...")
		cancel()
	}()

	slog.InfoContext(ctx, "order-consumer-worker started", "event_type", eventType, "event_subtype", eventSubtype)
	if err := consumer.Run(runCtx); err != nil {
		slog.ErrorContext(ctx, "consumer error", "err", err.Error())
	}
}

// newGateway selects the domain.PaymentGateway adapter by
// config.PaymentProvider — mirrors cmd/ingestion-api/main.go's newGateway
// (composition-root wiring, duplicated rather than shared since
// internal/adapter must not import internal/infrastructure/config).
// order-consumer-worker needs one to CreateCheckout when it processes an
// order.
func newGateway(cfg *config.Config) (domain.PaymentGateway, error) {
	switch cfg.PaymentProvider {
	case "stripe":
		return stripe.New(cfg.StripeSecretKey, cfg.StripeWebhookSecret), nil
	case "abacatepay":
		return abacatepay.New(), nil
	case "lemonsqueezy":
		return lemonsqueezy.New(), nil
	case "pagarme":
		return pagarme.New(), nil
	case "mercadopago":
		return mercadopago.New(), nil
	case "pagseguro":
		return pagseguro.New(), nil
	case "sumup":
		return sumup.New(), nil
	case "fake", "":
		return fake.New(""), nil
	default:
		return nil, fmt.Errorf("unknown PAYMENT_PROVIDER %q", cfg.PaymentProvider)
	}
}

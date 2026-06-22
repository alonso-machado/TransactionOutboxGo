package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/consume"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	telemetryShutdown, err := telemetry.Setup(context.Background(), cfg.OtelServiceName, cfg.OtelEndpoint, cfg.MetricsPort)
	if err != nil {
		log.Fatalf("telemetry: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := telemetryShutdown(shutdownCtx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	db, err := database.Connect(cfg.DatabaseURL, cfg.DBSSLMode)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	// Schema migrations are no longer applied here — see
	// cmd/ingestion-api/main.go's comment; Phase 5 Track 1 moved them to
	// migrations/, applied via `make migrate` / the compose `migrate` service.

	method, ok := rmq.MethodForQueue(cfg.PaymentQueue)
	if !ok {
		log.Fatalf("PAYMENT_QUEUE %q is not a known queue (expected one of: %v)", cfg.PaymentQueue, rmq.Methods)
	}

	conn, err := rmq.Connect(cfg.RabbitMQURL, cfg.RabbitMQTLS)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer func() { _ = conn.Close() }()

	uow := persistence.NewUnitOfWork(db)
	paymentRepo := persistence.NewPaymentRepository(db)

	processUC := consume.New(paymentRepo, uow)
	consumer := messaging.NewConsumer(conn, processUC, method, cfg.PrefetchCount, cfg.MaxDeliveries, cfg.RetryBackoffBase, cfg.RetryBackoffCap)

	ctx, cancel := context.WithCancel(context.Background())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("consumer-worker shutting down...")
		cancel()
	}()

	log.Println("consumer-worker started")
	if err := consumer.Run(ctx); err != nil {
		log.Printf("consumer error: %v", err)
	}
}

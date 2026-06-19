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
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	handler "github.com/alonsomachado/transaction-outbox-go/internal/adapter/http"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ingest"
	outboxuc "github.com/alonsomachado/transaction-outbox-go/internal/usecase/outbox"
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

	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	if err := persistence.AutoMigrate(db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	conn, err := rmq.Connect(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq channel: %v", err)
	}
	if err := rmq.DeclareTopology(ch); err != nil {
		log.Fatalf("rabbitmq topology: %v", err)
	}
	if err := ch.Close(); err != nil {
		log.Printf("close topology channel: %v", err)
	}

	uow := persistence.NewUnitOfWork(db)
	outboxRepo := persistence.NewOutboxRepository(db)
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
	router := handler.NewRouter(paymentHandler, cfg.OtelServiceName, cfg.SwaggerEnabled)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go dispatchUC.Run(ctx)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: router,
	}

	go func() {
		log.Printf("ingestion-api listening on :%s", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown: %v", err)
	}
}

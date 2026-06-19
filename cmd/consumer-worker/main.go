package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/consume"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

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
	paymentRepo := persistence.NewPaymentRepository(db)

	processUC := consume.New(paymentRepo, uow)
	consumer := messaging.NewConsumer(conn, processUC, cfg.PrefetchCount, cfg.MaxDeliveries)

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

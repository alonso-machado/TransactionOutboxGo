//go:build integration

package integration

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	handler "github.com/alonsomachado/transaction-outbox-go/internal/adapter/http"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/consume"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ingest"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/outbox"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"gorm.io/gorm"
)

// suite holds everything shared across the integration test package: one
// Postgres 17 + RabbitMQ 4.3 container pair, started once in TestMain, plus
// the wired GORM DB and AMQP connection used by every test file in this
// package. Tables are truncated between tests, not containers restarted.
var suite struct {
	db       *gorm.DB
	amqpConn *amqp.Connection
	pgURI    string
	amqpURI  string
	pgC      *tcpostgres.PostgresContainer
	rmqC     *tcrabbitmq.RabbitMQContainer
}

func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()

	pgC, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("outbox_test"),
		tcpostgres.WithUsername("outbox"),
		tcpostgres.WithPassword("outbox"),
	)
	if err != nil {
		log.Printf("start postgres container: %v", err)
		os.Exit(1)
	}
	suite.pgC = pgC

	rmqC, err := tcrabbitmq.Run(ctx, "rabbitmq:4.3-management-alpine")
	if err != nil {
		log.Printf("start rabbitmq container: %v", err)
		os.Exit(1)
	}
	suite.rmqC = rmqC

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("postgres connection string: %v", err)
		os.Exit(1)
	}
	suite.pgURI = dsn

	amqpURL, err := rmqC.AmqpURL(ctx)
	if err != nil {
		log.Printf("rabbitmq amqp url: %v", err)
		os.Exit(1)
	}
	suite.amqpURI = amqpURL

	db, err := database.Connect(dsn)
	if err != nil {
		log.Printf("connect db: %v", err)
		os.Exit(1)
	}
	suite.db = db

	// AutoMigrate against the ephemeral testcontainer schema only — this is
	// not the "shared/real schema" scenario CLAUDE.md's AutoMigrate ban
	// guards against; the container (and its schema) is destroyed at suite
	// teardown and never reused outside this test run.
	if err := persistence.AutoMigrate(db); err != nil {
		log.Printf("automigrate: %v", err)
		os.Exit(1)
	}

	conn, err := rmq.Connect(amqpURL)
	if err != nil {
		log.Printf("connect amqp: %v", err)
		os.Exit(1)
	}
	suite.amqpConn = conn

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("open channel: %v", err)
		os.Exit(1)
	}
	if err := rmq.DeclareTopology(ch); err != nil {
		log.Printf("declare topology: %v", err)
		os.Exit(1)
	}
	_ = ch.Close()

	code := m.Run()

	_ = suite.amqpConn.Close()
	_ = pgC.Terminate(ctx)
	_ = rmqC.Terminate(ctx)

	os.Exit(code)
}

// truncateAll resets both tables and purges queues between tests, preserving
// the shared container pair and RabbitMQ topology for speed.
func truncateAll(t *testing.T) {
	t.Helper()
	if err := suite.db.Exec("TRUNCATE TABLE payments, outbox_messages").Error; err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	purgeQueue(t, rmq.Queue)
	purgeQueue(t, rmq.DLQ)
}

func purgeQueue(t *testing.T, name string) {
	t.Helper()
	ch, err := suite.amqpConn.Channel()
	if err != nil {
		t.Fatalf("open channel for purge: %v", err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueuePurge(name, false); err != nil {
		t.Fatalf("purge queue %s: %v", name, err)
	}
}

// newIngest wires a fresh IngestPayment use case against the shared DB.
func newIngest() *ingest.IngestPayment {
	outboxRepo := persistence.NewOutboxRepository(suite.db)
	uow := persistence.NewUnitOfWork(suite.db)
	return ingest.New(outboxRepo, uow)
}

// newRouter wires the full HTTP stack (router + handler + ingest use case)
// against the shared DB, mirroring cmd/ingestion-api/main.go's DI.
func newRouter() *gin.Engine {
	h := handler.NewPaymentHandler(newIngest())
	return handler.NewRouter(h, "ingestion-api-test", false)
}

// newDispatch wires a DispatchOutbox use case against the shared DB and a
// real AMQP publisher over the shared connection.
func newDispatch(batchSize, maxRetries int, interval, pruneAfter time.Duration) (*outbox.DispatchOutbox, *persistence.GORMOutboxRepository) {
	return newDispatchWithConn(suite.amqpConn, batchSize, maxRetries, interval, pruneAfter)
}

// newDispatchWithConn wires a DispatchOutbox against an arbitrary AMQP
// connection (e.g. a deliberately closed one) so tests can simulate broker
// unavailability and max-retry/dead-letter scenarios without touching the
// shared connection other tests depend on.
func newDispatchWithConn(conn *amqp.Connection, batchSize, maxRetries int, interval, pruneAfter time.Duration) (*outbox.DispatchOutbox, *persistence.GORMOutboxRepository) {
	outboxRepo := persistence.NewOutboxRepository(suite.db)
	publisher := messaging.NewPublisher(conn)
	return outbox.New(outboxRepo, publisher, batchSize, maxRetries, interval, pruneAfter), outboxRepo
}

// amqpDial opens a brand-new AMQP connection to the shared RabbitMQ
// container, independent of suite.amqpConn, so a test can close it to
// simulate broker unavailability without affecting other tests.
func amqpDial(t *testing.T) (*amqp.Connection, error) {
	t.Helper()
	return rmq.Connect(suite.amqpURI)
}

// newConsumer wires a real AMQPConsumer + ProcessMessage against the shared
// DB and AMQP connection.
func newConsumer(prefetch, maxDeliveries int) *messaging.AMQPConsumer {
	paymentRepo := persistence.NewPaymentRepository(suite.db)
	uow := persistence.NewUnitOfWork(suite.db)
	processMsg := consume.New(paymentRepo, uow)
	return messaging.NewConsumer(suite.amqpConn, processMsg, prefetch, maxDeliveries)
}

// waitFor polls cond until it returns true or timeout elapses, returning the
// final evaluation — used to await async dispatch/consume transitions
// without a fixed sleep.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

func countOutboxByStatus(status domain.OutboxStatus) int64 {
	var n int64
	suite.db.Model(&persistence.OutboxMessageModel{}).Where("status = ?", string(status)).Count(&n)
	return n
}

func countPayments() int64 {
	var n int64
	suite.db.Model(&persistence.PaymentModel{}).Count(&n)
	return n
}

//go:build integration

package integration

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ticket"
	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	amqp "github.com/rabbitmq/amqp091-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"gorm.io/gorm"
)

// suite holds everything shared across the integration test package: one
// Postgres 17 + RabbitMQ 4.3 container pair, started once in TestMain, plus
// the wired GORM DB and AMQP connection used by every test file in this
// package. Tables are truncated between tests, not containers restarted.
// Two-DB split: the outbox use-cases (ingest/dispatch) talk to the `outbox`
// database (db / pgURI); the consumer writes the `payments` database
// (paymentsDB / paymentsURI). Both live in the SAME testcontainer Postgres —
// the split is logical, exactly as in production (one instance, two
// databases), so no second container is needed.
var suite struct {
	db          *gorm.DB
	paymentsDB  *gorm.DB
	amqpConn    *amqp.Connection
	pgURI       string
	paymentsURI string
	amqpURI     string
	pgC         *tcpostgres.PostgresContainer
	rmqC        *tcrabbitmq.RabbitMQContainer
}

// migrationsDir resolves the repo's migrations/<set> directory relative to
// this source file (not the test binary's working directory), so the suite
// finds it regardless of where `go test` is invoked from. set is "outbox" or
// "payments" — the two per-database migration sets.
func migrationsDir(set string) (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrNotExist
	}
	return filepath.Abs(filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", set))
}

// applyMigrations runs every up migration in migrations/<set> against dsn via
// golang-migrate's Go library — the in-process equivalent of `make migrate`
// /the compose migrate-outbox/migrate-payments services, used here so the
// ephemeral testcontainer databases end up with exactly the schema production
// gets, with no AutoMigrate anywhere in the suite.
func applyMigrations(dsn, set string) error {
	dir, err := migrationsDir(set)
	if err != nil {
		return err
	}
	m, err := migrate.New("file://"+filepath.ToSlash(dir), dsn)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()

	pgC, err := tcpostgres.Run(ctx,
		// Phase 4 Track 2: the suite needs the timescaledb extension for the
		// hypertables created by migrations/000002_timescale.up.sql (applied
		// below via applyMigrations) — plain postgres can't run it.
		"timescale/timescaledb:latest-pg18",
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

	db, err := database.Connect(dsn, "")
	if err != nil {
		log.Printf("connect db: %v", err)
		os.Exit(1)
	}
	suite.db = db

	// Two-DB split: the container auto-creates only the outbox_test database
	// (WithDatabase above). Create the payments_test database in the same
	// instance — the cloud analogue of observability/postgres/init-payments.sql
	// — and derive its DSN from the outbox one (same host/creds, different
	// dbname).
	if err := db.Exec("CREATE DATABASE payments_test").Error; err != nil {
		log.Printf("create payments database: %v", err)
		os.Exit(1)
	}
	paymentsDSN := strings.Replace(dsn, "/outbox_test?", "/payments_test?", 1)
	suite.paymentsURI = paymentsDSN

	paymentsDB, err := database.Connect(paymentsDSN, "")
	if err != nil {
		log.Printf("connect payments db: %v", err)
		os.Exit(1)
	}
	suite.paymentsDB = paymentsDB

	// Phase 5 Track 1: apply the same versioned migrations the real services
	// rely on (via golang-migrate) against the ephemeral testcontainer
	// databases, instead of AutoMigrate/MigrateTimescale — so the integration
	// suite is also a regression test that the migration sets alone produce a
	// working schema. Two sets now (the outbox/payments split), one per DB.
	if err := applyMigrations(dsn, "outbox"); err != nil {
		log.Printf("apply outbox migrations: %v", err)
		os.Exit(1)
	}
	if err := applyMigrations(paymentsDSN, "payments"); err != nil {
		log.Printf("apply payments migrations: %v", err)
		os.Exit(1)
	}

	conn, err := rmq.Connect(amqpURL, false)
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

// truncateAll resets outbox_messages and every per-method payments_<method>
// hypertable (payments itself is a UNION ALL view as of Phase 4 Track 2 —
// TRUNCATE can't target a view), and purges every method's queue + DLQ
// between tests, preserving the shared container pair and RabbitMQ topology
// for speed.
func truncateAll(t *testing.T) {
	t.Helper()
	// outbox_messages lives in the outbox DB; the payments_<method>
	// hypertables in the payments DB (the two-DB split) — truncate each in its
	// own database.
	if err := suite.db.Exec("TRUNCATE TABLE outbox_messages, ticket_outbox").Error; err != nil {
		t.Fatalf("truncate outbox tables: %v", err)
	}
	paymentsTables := make([]string, 0, len(rmq.Methods))
	for _, method := range rmq.Methods {
		paymentsTables = append(paymentsTables, "payments_"+strings.ToLower(method))
	}
	if err := suite.paymentsDB.Exec("TRUNCATE TABLE " + strings.Join(paymentsTables, ", ")).Error; err != nil {
		t.Fatalf("truncate payments tables: %v", err)
	}
	for _, method := range rmq.Methods {
		purgeQueue(t, rmq.QueueFor(method))
		purgeQueue(t, rmq.DLQFor(method))
	}
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
	outboxRepo := persistence.NewOutboxRepository(suite.db, 0, 0)
	uow := persistence.NewUnitOfWork(suite.db)
	return ingest.New(outboxRepo, uow)
}

// newRouter wires the full HTTP stack (router + handler + ingest use case)
// against the shared DB, mirroring cmd/ingestion-api/main.go's DI.
func newRouter() *gin.Engine {
	h := handler.NewPaymentHandler(newIngest())
	th := handler.NewTicketHandler(newTicketIngest())
	return handler.NewRouter(h, th, "ingestion-api-test", false, handler.RouterConfig{})
}

// newTicketIngest wires an IngestTicket use case against the shared outbox DB
// (ticket_outbox lives in the same database as outbox_messages).
func newTicketIngest() *ticket.IngestTicket {
	ticketRepo := persistence.NewTicketOutboxRepository(suite.db)
	uow := persistence.NewUnitOfWork(suite.db)
	return ticket.New(ticketRepo, uow)
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
	outboxRepo := persistence.NewOutboxRepository(suite.db, 0, 0)
	publisher := messaging.NewPublisher(conn)
	return outbox.New(outboxRepo, publisher, batchSize, maxRetries, interval, pruneAfter), outboxRepo
}

// amqpDial opens a brand-new AMQP connection to the shared RabbitMQ
// container, independent of suite.amqpConn, so a test can close it to
// simulate broker unavailability without affecting other tests.
func amqpDial(t *testing.T) (*amqp.Connection, error) {
	t.Helper()
	return rmq.Connect(suite.amqpURI, false)
}

// newConsumer wires a real AMQPConsumer + ProcessMessage against the shared
// DB and AMQP connection, bound to method's queue.
func newConsumer(method string, prefetch, maxDeliveries int) *messaging.AMQPConsumer {
	// consumer-worker writes the payments DB (the two-DB split).
	paymentRepo := persistence.NewPaymentRepository(suite.paymentsDB)
	uow := persistence.NewUnitOfWork(suite.paymentsDB)
	processMsg := consume.New(paymentRepo, uow)
	return messaging.NewConsumer(suite.amqpConn, processMsg, method, prefetch, maxDeliveries, 0, 0)
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
	suite.paymentsDB.Model(&persistence.PaymentModel{}).Count(&n)
	return n
}

func countTicketOutbox() int64 {
	var n int64
	suite.db.Model(&persistence.TicketOutboxModel{}).Count(&n)
	return n
}

//go:build integration

package integration

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	handler "github.com/alonsomachado/transaction-outbox-go/internal/adapter/http"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/messaging"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/paymentgateway/fake"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	fakeauth "github.com/alonsomachado/transaction-outbox-go/internal/adapter/staffauth/fake"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/ticketqr"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/checkin"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/checkout"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/fulfillment"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/notification"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/order"
	outboxuc "github.com/alonsomachado/transaction-outbox-go/internal/usecase/outbox"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ticketholder"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/webhook"
	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"gorm.io/gorm"
)

// testEventType/testEventSubtype is the single shard most tests in this
// package exercise (CONCERT/ROCK — the same shard docker-compose.yml runs
// locally by default); routing_test.go exercises a second shard
// (SPORTS/FOOTBALL) to prove the routing itself, not just one queue.
const (
	testEventType    = "CONCERT"
	testEventSubtype = "ROCK"

	// ticketSigningSecret is the HMAC key every fulfillment-side test signs
	// and verifies tickets with.
	ticketSigningSecret = "integration-test-ticket-signing-secret"
	// testProvider is the PaymentGateway provider name every test's
	// WebhookHandler/gateway is configured with — the fake sandbox adapter,
	// no network calls.
	testProvider = "fake"

	// fakeStaffToken/fakeClerkUserID are the fixed staffauth/fake pair every
	// check-in test authenticates with — no real Clerk account needed.
	fakeStaffToken  = "integration-test-staff-token"
	fakeClerkUserID = "integration-test-staff-user"
)

// suite holds everything shared across the integration test package: one
// Postgres + RabbitMQ 4.3 container pair, started once in TestMain, plus the
// wired GORM DBs and AMQP connection used by every test file in this
// package. Tables are truncated between tests, not containers restarted.
// Two-DB split: the ingestion/relay use-cases (order/webhook/outbox) talk to
// the `outbox` database (db / pgURI); order-consumer-worker and
// fulfillment-consumer-worker write the `events` database (eventsDB /
// eventsURI). Both live in the SAME testcontainer Postgres — the split is
// logical, exactly as in production (one instance, two databases), so no
// second container is needed.
var suite struct {
	db        *gorm.DB
	eventsDB  *gorm.DB
	amqpConn  *amqp.Connection
	pgURI     string
	eventsURI string
	amqpURI   string
	pgC       *tcpostgres.PostgresContainer
	rmqC      *tcrabbitmq.RabbitMQContainer
}

// migrationsDir resolves the repo's migrations/<set> directory relative to
// this source file (not the test binary's working directory), so the suite
// finds it regardless of where `go test` is invoked from. set is "outbox" or
// "events" — the two per-database migration sets.
func migrationsDir(set string) (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrNotExist
	}
	return filepath.Abs(filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", set))
}

// applyMigrations runs every up migration in migrations/<set> against dsn via
// golang-migrate's Go library — the in-process equivalent of `make migrate`
// /the compose migrate-outbox/migrate-events services, used here so the
// ephemeral testcontainer databases end up with exactly the schema
// production gets, with no AutoMigrate anywhere in the suite.
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

	// Plain postgres is enough now — TimescaleDB was only needed for the old
	// payments_* hypertables, which are gone with the payments domain.
	pgC, err := tcpostgres.Run(ctx,
		"postgres:18-alpine",
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
	// (WithDatabase above). Create the events_test database in the same
	// instance — the cloud analogue of observability/postgres/init-events.sql
	// — and derive its DSN from the outbox one (same host/creds, different
	// dbname).
	if err := db.Exec("CREATE DATABASE events_test").Error; err != nil {
		log.Printf("create events database: %v", err)
		os.Exit(1)
	}
	eventsDSN := strings.Replace(dsn, "/outbox_test?", "/events_test?", 1)
	suite.eventsURI = eventsDSN

	eventsDB, err := database.Connect(eventsDSN, "")
	if err != nil {
		log.Printf("connect events db: %v", err)
		os.Exit(1)
	}
	suite.eventsDB = eventsDB

	// Apply the same versioned migrations the real services rely on (via
	// golang-migrate) against the ephemeral testcontainer databases, instead
	// of AutoMigrate — so the integration suite is also a regression test
	// that the migration sets alone produce a working schema. Two sets, one
	// per DB (the outbox/events split).
	if err := applyMigrations(dsn, "outbox"); err != nil {
		log.Printf("apply outbox migrations: %v", err)
		os.Exit(1)
	}
	if err := applyMigrations(eventsDSN, "events"); err != nil {
		log.Printf("apply events migrations: %v", err)
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

// truncateAll resets both outbox tables and every events-domain table, and
// purges every registered (event_type, event_subtype) shard's queues + DLQs
// on both streams between tests, preserving the shared container pair and
// RabbitMQ topology for speed.
func truncateAll(t *testing.T) {
	t.Helper()
	if err := suite.db.Exec("TRUNCATE TABLE order_outbox, payment_event_outbox, ticket_notification_outbox").Error; err != nil {
		t.Fatalf("truncate outbox tables: %v", err)
	}
	if err := suite.eventsDB.Exec("TRUNCATE TABLE charges, tickets, orders, event_areas, events, producers, locations, staff_users").Error; err != nil {
		t.Fatalf("truncate events tables: %v", err)
	}
	for eventType, subtypes := range rmq.EventTypes {
		for _, eventSubtype := range subtypes {
			for _, stream := range []rmq.Stream{rmq.OrderStream, rmq.PaymentEventStream} {
				purgeQueue(t, rmq.QueueFor(stream, eventType, eventSubtype))
				purgeQueue(t, rmq.DLQFor(stream, eventType, eventSubtype))
			}
		}
	}
	purgeQueue(t, rmq.QueueFor(rmq.NotificationStream, rmq.NotificationSentinelType, rmq.NotificationSentinelSubtype))
	purgeQueue(t, rmq.DLQFor(rmq.NotificationStream, rmq.NotificationSentinelType, rmq.NotificationSentinelSubtype))
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

// outboxRowFixture mirrors persistence's unexported outboxRow shape so tests
// can inspect a raw order_outbox/payment_event_outbox row via GORM's
// .Table(name) — the production type is deliberately unexported (repository
// internal), so this is the test-side equivalent, matched by column name.
type outboxRowFixture struct {
	IdempotencyKey string `gorm:"column:idempotency_key"`
	AggregateType  string `gorm:"column:aggregate_type"`
	HTTPMethod     string `gorm:"column:http_method"`
	Route          string
	Payload        []byte `gorm:"type:jsonb"`
	Status         string
	RetryCount     int        `gorm:"column:retry_count"`
	LastError      string     `gorm:"column:last_error"`
	EventType      string     `gorm:"column:event_type"`
	EventSubtype   string     `gorm:"column:event_subtype"`
	PublishedAt    *time.Time `gorm:"column:published_at"`
	NextRetryAt    *time.Time `gorm:"column:next_retry_at"`
}

func testGateway() *fake.Gateway { return fake.New("") }

func newOrderOutboxRepo() *persistence.GORMOutboxRepository {
	return persistence.NewOutboxRepository(suite.db, "order_outbox", 0, 0)
}

func newPaymentEventOutboxRepo() *persistence.GORMOutboxRepository {
	return persistence.NewOutboxRepository(suite.db, "payment_event_outbox", 0, 0)
}

func newPlaceOrder() *order.PlaceOrder {
	return order.New(newOrderOutboxRepo(), persistence.NewUnitOfWork(suite.db))
}

func newReceivePaymentEvent() *webhook.ReceivePaymentEvent {
	return webhook.New(newPaymentEventOutboxRepo(), persistence.NewUnitOfWork(suite.db))
}

// newRouter wires the full HTTP stack (router + order/webhook handlers)
// against the shared outbox DB, mirroring cmd/ingestion-api/main.go's DI.
func newRouter() *gin.Engine {
	orderHandler := handler.NewOrderHandler(newPlaceOrder())
	webhookHandler := handler.NewWebhookHandler(testGateway(), newReceivePaymentEvent(), testProvider)
	return handler.NewRouter(orderHandler, webhookHandler, "ingestion-api-test", false, handler.RouterConfig{})
}

// newOrderDispatch wires a DispatchOutbox use case for order_outbox against
// the shared DB and a real AMQP publisher over the shared connection.
func newOrderDispatch(batchSize, maxRetries int, interval, pruneAfter time.Duration) (*outboxuc.DispatchOutbox, *persistence.GORMOutboxRepository) {
	return newOrderDispatchWithConn(suite.amqpConn, batchSize, maxRetries, interval, pruneAfter)
}

// newOrderDispatchWithConn wires a DispatchOutbox against an arbitrary AMQP
// connection (e.g. a deliberately closed one) so tests can simulate broker
// unavailability and max-retry/dead-letter scenarios without touching the
// shared connection other tests depend on.
func newOrderDispatchWithConn(conn *amqp.Connection, batchSize, maxRetries int, interval, pruneAfter time.Duration) (*outboxuc.DispatchOutbox, *persistence.GORMOutboxRepository) {
	repo := newOrderOutboxRepo()
	publisher := messaging.NewPublisher(conn)
	return outboxuc.New(repo, publisher, batchSize, maxRetries, interval, pruneAfter), repo
}

// newPaymentEventDispatch mirrors newOrderDispatch for payment_event_outbox.
func newPaymentEventDispatch(batchSize, maxRetries int, interval, pruneAfter time.Duration) (*outboxuc.DispatchOutbox, *persistence.GORMOutboxRepository) {
	repo := newPaymentEventOutboxRepo()
	publisher := messaging.NewPublisher(suite.amqpConn)
	return outboxuc.New(repo, publisher, batchSize, maxRetries, interval, pruneAfter), repo
}

// amqpDial opens a brand-new AMQP connection to the shared RabbitMQ
// container, independent of suite.amqpConn, so a test can close it to
// simulate broker unavailability without affecting other tests.
func amqpDial(t *testing.T) (*amqp.Connection, error) {
	t.Helper()
	return rmq.Connect(suite.amqpURI, false)
}

// newCheckoutConsumer wires a real AMQPConsumer + ProcessOrder against the
// shared events DB and the fake gateway, bound to (eventType, eventSubtype)'s
// order_outbox shard queue.
func newCheckoutConsumer(eventType, eventSubtype string, prefetch, maxDeliveries int) *messaging.AMQPConsumer {
	locationRepo := persistence.NewLocationRepository(suite.eventsDB)
	eventRepo := persistence.NewEventRepository(suite.eventsDB)
	orderRepo := persistence.NewOrderRepository(suite.eventsDB)
	ticketRepo := persistence.NewTicketRepository(suite.eventsDB)
	chargeRepo := persistence.NewChargeRepository(suite.eventsDB)
	uow := persistence.NewUnitOfWork(suite.eventsDB)
	processOrder := checkout.New(locationRepo, eventRepo, orderRepo, ticketRepo, chargeRepo, testGateway(), uow, testProvider, "http://localhost:8080/orders/success")
	return messaging.NewConsumer(suite.amqpConn, processOrder, rmq.OrderStream, eventType, eventSubtype, prefetch, maxDeliveries, 0, 0)
}

// newFulfillmentConsumer wires a real AMQPConsumer + IssueTickets against the
// shared events DB, bound to (eventType, eventSubtype)'s payment_event_outbox
// shard queue. notificationOutboxRepo points at the outbox DB's
// ticket_notification_outbox (Phase 8) — a real repo, not a stub, so tests
// can assert the enqueued row directly (see notification_test.go).
func newFulfillmentConsumer(eventType, eventSubtype string, prefetch, maxDeliveries int) *messaging.AMQPConsumer {
	chargeRepo := persistence.NewChargeRepository(suite.eventsDB)
	ticketRepo := persistence.NewTicketRepository(suite.eventsDB)
	orderRepo := persistence.NewOrderRepository(suite.eventsDB)
	uow := persistence.NewUnitOfWork(suite.eventsDB)
	qr := ticketqr.New(ticketSigningSecret)
	notificationOutboxRepo := newTicketNotificationOutboxRepo()
	issueTickets := fulfillment.New(chargeRepo, ticketRepo, orderRepo, qr, notificationOutboxRepo, uow, eventType, eventSubtype)
	return messaging.NewConsumer(suite.amqpConn, issueTickets, rmq.PaymentEventStream, eventType, eventSubtype, prefetch, maxDeliveries, 0, 0)
}

// newTicketNotificationOutboxRepo mirrors newOrderOutboxRepo/
// newPaymentEventOutboxRepo for the third outbox table (Phase 8) — it lives
// in the outbox DB (suite.db), like the other two, even though it's enqueued
// by fulfillment-consumer-worker (which otherwise only touches the events DB).
func newTicketNotificationOutboxRepo() *persistence.GORMOutboxRepository {
	return persistence.NewOutboxRepository(suite.db, "ticket_notification_outbox", 0, 0)
}

// newNotificationDispatch mirrors newOrderDispatch/newPaymentEventDispatch
// for ticket_notification_outbox.
func newNotificationDispatch(batchSize, maxRetries int, interval, pruneAfter time.Duration) (*outboxuc.DispatchOutbox, *persistence.GORMOutboxRepository) {
	repo := newTicketNotificationOutboxRepo()
	publisher := messaging.NewPublisher(suite.amqpConn)
	return outboxuc.New(repo, publisher, batchSize, maxRetries, interval, pruneAfter), repo
}

// newNotificationConsumer wires a real AMQPConsumer + SendNotification
// against recorder (a spy domain.EmailSender), bound to
// NotificationStream's single unsharded queue.
func newNotificationConsumer(recorder domain.EmailSender, prefetch, maxDeliveries int) *messaging.AMQPConsumer {
	sendNotification := notification.New(recorder)
	return messaging.NewConsumer(suite.amqpConn, sendNotification, rmq.NotificationStream, rmq.NotificationSentinelType, rmq.NotificationSentinelSubtype, prefetch, maxDeliveries, 0, 0)
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

func countOrderOutboxByStatus(status domain.OutboxStatus) int64 {
	var n int64
	suite.db.Table("order_outbox").Where("status = ?", string(status)).Count(&n)
	return n
}

func countPaymentEventOutboxByStatus(status domain.OutboxStatus) int64 {
	var n int64
	suite.db.Table("payment_event_outbox").Where("status = ?", string(status)).Count(&n)
	return n
}

func countOrders() int64 {
	var n int64
	suite.eventsDB.Table("orders").Count(&n)
	return n
}

func countTickets() int64 {
	var n int64
	suite.eventsDB.Table("tickets").Count(&n)
	return n
}

func countTicketsByStatus(status string) int64 {
	var n int64
	suite.eventsDB.Table("tickets").Where("status = ?", status).Count(&n)
	return n
}

func countCharges() int64 {
	var n int64
	suite.eventsDB.Table("charges").Count(&n)
	return n
}

func countTicketNotificationOutboxByStatus(status domain.OutboxStatus) int64 {
	var n int64
	suite.db.Table("ticket_notification_outbox").Where("status = ?", string(status)).Count(&n)
	return n
}

// newOrderStatusHandler wires OrderStatusHandler against the shared events
// DB, mirroring cmd/tickets-api/main.go's DI.
func newOrderStatusHandler() *handler.OrderStatusHandler {
	return handler.NewOrderStatusHandler(persistence.NewOrderRepository(suite.eventsDB), persistence.NewChargeRepository(suite.eventsDB))
}

// newTicketsRouter wires the full tickets-api HTTP stack (router + order-
// status/check-in/ticket-holder handlers) against the shared events DB,
// mirroring cmd/tickets-api/main.go's DI. Uses the fake staffauth adapter
// (fakeStaffToken/fakeClerkUserID below) so check-in tests don't need a
// real Clerk account.
func newTicketsRouter() *gin.Engine {
	return newTicketsRouterWithConfig(handler.RouterConfig{})
}

// newTicketsRouterRateLimited builds tickets-api's router with rate
// limiting enabled on the PATCH ticket-holder route (see
// handler.NewTicketsRouter), for TestTicketHolder_RateLimit_429s.
func newTicketsRouterRateLimited(store ratelimit.BucketStore, rate float64, burst int) *gin.Engine {
	return newTicketsRouterWithConfig(handler.RouterConfig{
		RateLimitEnabled: true,
		RateLimitStore:   store,
		RateLimitRate:    rate,
		RateLimitBurst:   burst,
	})
}

func newTicketsRouterWithConfig(rl handler.RouterConfig) *gin.Engine {
	orderStatusHandler := newOrderStatusHandler()
	checkinHandler := handler.NewCheckinHandler(newCheckinUC())
	ticketHolderHandler := handler.NewTicketHolderHandler(newUpdateHolderUC())
	staffAuthenticator := fakeauth.New(fakeStaffToken, fakeClerkUserID)
	staffUserRepo := persistence.NewStaffUserRepository(suite.eventsDB)
	return handler.NewTicketsRouter(orderStatusHandler, checkinHandler, ticketHolderHandler, staffAuthenticator, staffUserRepo, "tickets-api-test", false, rl)
}

// newCheckinUC wires checkin.CheckIn against the shared events DB.
func newCheckinUC() *checkin.CheckIn {
	return checkin.New(persistence.NewTicketRepository(suite.eventsDB), persistence.NewEventRepository(suite.eventsDB), ticketSigningSecret)
}

// newUpdateHolderUC wires ticketholder.UpdateHolder against the shared
// events DB.
func newUpdateHolderUC() *ticketholder.UpdateHolder {
	return ticketholder.New(persistence.NewTicketRepository(suite.eventsDB))
}

// seedLocation inserts a locations row directly (independent of any order,
// unlike the locations upserted by usecase/checkout.ProcessOrder), for tests
// that need a second, distinct venue to check staff venue-scoping against.
func seedLocation(t *testing.T, name string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	if err := suite.eventsDB.Exec(
		"INSERT INTO locations (id, name, city, source_venue_id, created_at) VALUES (?, ?, ?, ?, now())",
		id, name, "Test City", "venue-"+id.String(),
	).Error; err != nil {
		t.Fatalf("seed location: %v", err)
	}
	return id
}

// seedStaffUser inserts a staff_users row directly (no HTTP endpoint creates
// these — they're provisioned out of band, e.g. by an admin tool not yet
// built), returning its id.
func seedStaffUser(t *testing.T, clerkUserID string, locationID *uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	if err := suite.eventsDB.Exec(
		"INSERT INTO staff_users (id, clerk_user_id, name, role, location_id, created_at) VALUES (?, ?, ?, ?, ?, now())",
		id, clerkUserID, "Test Staff", "door", locationID,
	).Error; err != nil {
		t.Fatalf("seed staff user: %v", err)
	}
	return id
}

// recordingEmailSender is a spy domain.EmailSender for notification_test.go
// — records every Send call instead of delivering anything, so a test can
// assert the recipient/attachment notification-consumer-worker actually sent.
type recordingEmailSender struct {
	mu   sync.Mutex
	sent []domain.EmailRequest
}

func (s *recordingEmailSender) Send(req domain.EmailRequest) (*domain.EmailResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, req)
	return &domain.EmailResult{ProviderMessageID: "test"}, nil
}

func (s *recordingEmailSender) calls() []domain.EmailRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.EmailRequest, len(s.sent))
	copy(out, s.sent)
	return out
}

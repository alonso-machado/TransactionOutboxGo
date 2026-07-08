package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	DatabaseURL       string `envconfig:"DATABASE_URL" required:"true"`
	RabbitMQURL       string `envconfig:"RABBITMQ_URL" required:"true"`
	HTTPPort          string `envconfig:"HTTP_PORT" default:"8080"`
	DispatchInterval  int    `envconfig:"OUTBOX_DISPATCH_INTERVAL_MS" default:"250"`
	DispatchBatchSize int    `envconfig:"OUTBOX_DISPATCH_BATCH_SIZE" default:"50"`
	MaxRetries        int    `envconfig:"OUTBOX_MAX_RETRIES" default:"5"`
	PruneAfterHours   int    `envconfig:"OUTBOX_PRUNE_AFTER_HOURS" default:"48"`

	// Retry backoff (Phase 5 Track 2.A) — shared by the outbox dispatcher's
	// next_retry_at scheduling and the consumer's *.retry queue per-message
	// TTL, so both sides back off on the same exponential+full-jitter
	// schedule: min(base*2^retry_count, cap), jittered.
	RetryBackoffBase time.Duration `envconfig:"RETRY_BACKOFF_BASE" default:"1s"`
	RetryBackoffCap  time.Duration `envconfig:"RETRY_BACKOFF_CAP" default:"5m"`
	PrefetchCount    int           `envconfig:"PREFETCH_COUNT" default:"10"`
	MaxDeliveries    int           `envconfig:"MAX_DELIVERIES" default:"5"`
	// ConsumerQueue is order-consumer-worker/fulfillment-consumer-worker-only — the single
	// RabbitMQ queue name that process instance binds to (one shard, e.g.
	// "events.concert.rock.queue"). Not `required` here because Config is
	// shared with ingestion-api, which never sets it — each consumer
	// binary's main.go fails fast itself if this is empty or not a known
	// queue (rmq.ParseQueueName).
	ConsumerQueue string `envconfig:"CONSUMER_QUEUE"`

	// PaymentProvider selects the domain.PaymentGateway adapter order-consumer-worker
	// (CreateCheckout) and ingestion-api (VerifyWebhook) wire up: "fake" (no
	// network, the default — local dev/tests/k6), "stripe" (real), or a
	// scaffolded stub ("abacatepay", "lemonsqueezy", "pagarme", "mercadopago",
	// "pagseguro", "sumup").
	PaymentProvider     string `envconfig:"PAYMENT_PROVIDER" default:"fake"`
	StripeSecretKey     string `envconfig:"STRIPE_SECRET_KEY"`
	StripeWebhookSecret string `envconfig:"STRIPE_WEBHOOK_SECRET"`
	// CheckoutSuccessURL is where the gateway redirects the customer after a
	// successful hosted checkout.
	CheckoutSuccessURL string `envconfig:"CHECKOUT_SUCCESS_URL" default:"http://localhost:8080/orders/success"`
	// TicketSigningSecret is the HMAC key internal/adapter/ticketqr signs
	// every issued ticket's validation code with.
	TicketSigningSecret string `envconfig:"TICKET_SIGNING_SECRET" default:"dev-ticket-signing-secret"`

	// EmailProvider selects the domain.EmailSender adapter
	// notification-consumer-worker wires up: "fake" (no network, the
	// default — local dev/tests) or "smtp" (real, stdlib net/smtp).
	EmailProvider string `envconfig:"EMAIL_PROVIDER" default:"fake"`
	SMTPHost      string `envconfig:"SMTP_HOST"`
	SMTPPort      int    `envconfig:"SMTP_PORT" default:"587"`
	SMTPUsername  string `envconfig:"SMTP_USERNAME"`
	SMTPPassword  string `envconfig:"SMTP_PASSWORD"`
	SMTPFromEmail string `envconfig:"SMTP_FROM_EMAIL" default:"tickets@example.com"`
	SMTPFromName  string `envconfig:"SMTP_FROM_NAME" default:"Event Tickets"`

	// Staff auth (tickets-api's POST /api/v1/checkin only). StaffAuthProvider
	// selects the domain.StaffAuthenticator adapter: "fake" (a fixed test
	// token, no network — the default, for local dev/tests, since a real
	// Clerk account isn't required to run this system) or "clerk" (real).
	// ClerkSecretKey is not `required` — only tickets-api with
	// STAFF_AUTH_PROVIDER=clerk needs it, so every other binary simply never
	// sets it.
	StaffAuthProvider        string `envconfig:"STAFF_AUTH_PROVIDER" default:"fake"`
	ClerkSecretKey           string `envconfig:"CLERK_SECRET_KEY"`
	StaffAuthFakeToken       string `envconfig:"STAFF_AUTH_FAKE_TOKEN" default:"dev-staff-token"`
	StaffAuthFakeClerkUserID string `envconfig:"STAFF_AUTH_FAKE_CLERK_USER_ID" default:"dev-staff-user"`

	OtelServiceName string `envconfig:"OTEL_SERVICE_NAME" default:"transaction-outbox-go"`
	OtelEndpoint    string `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT" default:"localhost:4318"`
	MetricsPort     string `envconfig:"METRICS_PORT" default:"9090"`
	SwaggerEnabled  bool   `envconfig:"SWAGGER_ENABLED" default:"false"`
	// LogFormat selects the slog handler (internal/infrastructure/logging) —
	// "json" for production/containers, "text" for a human-readable local
	// console. Phase 5 Track 4.A.
	LogFormat string `envconfig:"LOG_FORMAT" default:"json"`

	// Rate limiting (ingestion-api only) — Phase 4 Track 1.
	RateLimitEnabled bool     `envconfig:"RATE_LIMIT_ENABLED" default:"true"`
	RateLimitRate    float64  `envconfig:"RATE_LIMIT_RATE" default:"50"`
	RateLimitBurst   int      `envconfig:"RATE_LIMIT_BURST" default:"100"`
	TrustedProxies   []string `envconfig:"TRUSTED_PROXIES"`

	// PCI-DSS encryption-in-transit toggles (Phase 5 Track 5.B). Both default
	// to the plaintext local posture (`make up`/compose) so nothing changes
	// for the demo; cloud deploys set these to enforce TLS.
	// DBSSLMode is honored by database.Connect (appended as the Postgres
	// `sslmode` DSN param); RabbitMQTLS is honored by rabbitmq.Connect
	// (switches the AMQP URL scheme from amqp:// to amqps://).
	DBSSLMode   string `envconfig:"DB_SSL_MODE" default:"disable"`
	RabbitMQTLS bool   `envconfig:"RABBITMQ_TLS" default:"false"`
}

func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, err
	}
	return &c, nil
}

package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	DatabaseURL       string `envconfig:"DATABASE_URL" required:"true"`
	RabbitMQURL       string `envconfig:"RABBITMQ_URL" required:"true"`
	HTTPPort          string `envconfig:"HTTP_PORT" default:"8080"`
	DispatchInterval  int    `envconfig:"OUTBOX_DISPATCH_INTERVAL_MS" default:"500"`
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
	// PaymentQueue is consumer-worker-only. Not `required` here because Config
	// is shared with ingestion-api, which never sets it — consumer-worker's
	// main.go fails fast itself if this is empty or not a known queue.
	PaymentQueue    string `envconfig:"PAYMENT_QUEUE"`
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
	// for the demo; cloud deploys (Pulumi) set these to enforce TLS.
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

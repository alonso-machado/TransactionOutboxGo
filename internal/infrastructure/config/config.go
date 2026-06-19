package config

import "github.com/kelseyhightower/envconfig"

type Config struct {
	DatabaseURL       string `envconfig:"DATABASE_URL" required:"true"`
	RabbitMQURL       string `envconfig:"RABBITMQ_URL" required:"true"`
	HTTPPort          string `envconfig:"HTTP_PORT" default:"8080"`
	DispatchInterval  int    `envconfig:"DISPATCH_INTERVAL_MS" default:"500"`
	DispatchBatchSize int    `envconfig:"DISPATCH_BATCH_SIZE" default:"50"`
	MaxRetries        int    `envconfig:"MAX_RETRIES" default:"5"`
	PruneAfterHours   int    `envconfig:"PRUNE_AFTER_HOURS" default:"24"`
	PrefetchCount     int    `envconfig:"PREFETCH_COUNT" default:"10"`
	MaxDeliveries     int    `envconfig:"MAX_DELIVERIES" default:"5"`
	OtelServiceName   string `envconfig:"OTEL_SERVICE_NAME" default:"transaction-outbox-go"`
	OtelEndpoint      string `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT" default:"localhost:4318"`
	MetricsPort       string `envconfig:"METRICS_PORT" default:"9090"`
	SwaggerEnabled    bool   `envconfig:"SWAGGER_ENABLED" default:"false"`
}

func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, err
	}
	return &c, nil
}

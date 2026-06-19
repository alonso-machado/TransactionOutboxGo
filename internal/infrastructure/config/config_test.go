package config

import (
	"os"
	"testing"
)

func TestLoad_RequiredVarsMissing_ReturnsError(t *testing.T) {
	for _, key := range []string{"DATABASE_URL", "RABBITMQ_URL"} {
		prev, had := os.LookupEnv(key)
		_ = os.Unsetenv(key)
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(key, prev)
			}
		})
	}
	if _, err := Load(); err == nil {
		t.Fatal("expected error when required env vars are missing")
	}
}

func TestLoad_RequiredVarsPresent_AppliesDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("RABBITMQ_URL", "amqp://localhost")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPPort != "8080" {
		t.Errorf("expected default HTTPPort 8080, got %s", cfg.HTTPPort)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("expected default MaxRetries 5, got %d", cfg.MaxRetries)
	}
	if cfg.SwaggerEnabled {
		t.Error("expected default SwaggerEnabled to be false")
	}
}

func TestLoad_EnvOverridesDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("RABBITMQ_URL", "amqp://localhost")
	t.Setenv("HTTP_PORT", "9999")
	t.Setenv("SWAGGER_ENABLED", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPPort != "9999" {
		t.Errorf("expected overridden HTTPPort 9999, got %s", cfg.HTTPPort)
	}
	if !cfg.SwaggerEnabled {
		t.Error("expected overridden SwaggerEnabled to be true")
	}
}

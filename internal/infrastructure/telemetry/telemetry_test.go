package telemetry

import (
	"context"
	"testing"
	"time"
)

// Setup wires the tracer/meter providers, the W3C propagator, the slog
// default, and the /metrics HTTP server. There is nothing to containerize
// here — the OTLP exporter connects lazily and the Prometheus exporter is
// in-process — so this exercises the happy path directly: metricsPort "0"
// binds an ephemeral port (no conflicts), and the returned Shutdown must tear
// everything down without error.
func TestSetup_InitialisesAndShutsDownCleanly(t *testing.T) {
	ctx := context.Background()

	shutdown, err := Setup(ctx, "telemetry-test", "localhost:4318", "0", "json")
	if err != nil {
		t.Fatalf("Setup returned an error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("Setup returned a nil Shutdown func")
	}

	// Give the background metrics server a moment to bind before shutting down.
	time.Sleep(50 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown returned an error: %v", err)
	}
}

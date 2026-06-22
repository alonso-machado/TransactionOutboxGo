// Package observability holds small cross-cutting helpers over the
// OpenTelemetry metric API. It is a dependency-free leaf (it imports only the
// otel metric API and stdlib slog), so any layer — use-cases and adapters
// alike — may import it without violating the Clean Architecture dependency
// rule, the same way internal/domain/pii is shared.
package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/metric"
)

// Int64Counter creates a named Int64 counter on meter. The OpenTelemetry spec
// guarantees the returned instrument is always non-nil and usable even when
// construction returns an error (it falls back to a no-op), so callers can use
// the result unconditionally; the error is logged here once rather than
// re-checked at every call site.
func Int64Counter(meter metric.Meter, name string) metric.Int64Counter {
	c, err := meter.Int64Counter(name)
	if err != nil {
		slog.ErrorContext(context.Background(), "create counter failed", "name", name, "err", err.Error())
	}
	return c
}

// Int64Gauge creates a named Int64 gauge on meter. See Int64Counter for why
// the returned instrument is safe to use unconditionally.
func Int64Gauge(meter metric.Meter, name string) metric.Int64Gauge {
	g, err := meter.Int64Gauge(name)
	if err != nil {
		slog.ErrorContext(context.Background(), "create gauge failed", "name", name, "err", err.Error())
	}
	return g
}

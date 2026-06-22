// Package logging provides the project-wide structured logger: a stdlib
// log/slog JSON (or text) handler wrapped so every *Context call
// automatically carries trace_id/span_id from the active OTel span — Phase 5
// Track 4.A. Pure stdlib + otel/trace, so it's safe to import from any layer
// (including domain/usecase) per CLAUDE.md's dependency rule.
package logging

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// NewLogger builds the standard application logger: a JSON (or text)
// handler wrapped with trace correlation, with a "service" attribute on
// every line. format is typically Config.LogFormat ("json" or "text");
// anything other than "text" defaults to JSON.
func NewLogger(service, format string, w io.Writer) *slog.Logger {
	var base slog.Handler
	if format == "text" {
		base = slog.NewTextHandler(w, nil)
	} else {
		base = slog.NewJSONHandler(w, nil)
	}
	handler := newTraceHandler(base).WithAttrs([]slog.Attr{slog.String("service", service)})
	return slog.New(handler)
}

// traceHandler wraps an slog.Handler and injects trace_id/span_id attributes
// from the active span in ctx, so every log line correlates with its trace.
type traceHandler struct {
	next slog.Handler
}

func newTraceHandler(next slog.Handler) *traceHandler {
	return &traceHandler{next: next}
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, record slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, record)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{next: h.next.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{next: h.next.WithGroup(name)}
}

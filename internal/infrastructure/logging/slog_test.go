package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace"
)

func TestTraceHandler_NoActiveSpan_LogsWithoutTraceID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(newTraceHandler(slog.NewJSONHandler(&buf, nil)))

	logger.InfoContext(context.Background(), "hello")

	out := buf.String()
	if strings.Contains(out, "trace_id") {
		t.Fatalf("expected no trace_id without an active span, got %q", out)
	}
	if !strings.Contains(out, "hello") {
		t.Fatalf("expected log message preserved, got %q", out)
	}
}

func TestTraceHandler_ActiveSpan_InjectsTraceAndSpanID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(newTraceHandler(slog.NewJSONHandler(&buf, nil)))

	tp := trace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := tp.Tracer("test").Start(context.Background(), "test-span")
	defer span.End()

	logger.InfoContext(ctx, "hello")

	out := buf.String()
	if !strings.Contains(out, "trace_id") || !strings.Contains(out, "span_id") {
		t.Fatalf("expected trace_id and span_id in output, got %q", out)
	}
}

func TestTraceHandler_WithAttrsAndWithGroup_DelegateToNext(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	h := newTraceHandler(base)

	withAttrs := h.WithAttrs([]slog.Attr{slog.String("k", "v")})
	if withAttrs == nil {
		t.Fatal("expected non-nil handler from WithAttrs")
	}
	withGroup := h.WithGroup("g")
	if withGroup == nil {
		t.Fatal("expected non-nil handler from WithGroup")
	}

	logger := slog.New(withAttrs)
	logger.Info("hi")
	if !strings.Contains(buf.String(), `"k":"v"`) {
		t.Fatalf("expected attribute carried through WithAttrs, got %q", buf.String())
	}
}

func TestTraceHandler_Enabled_DelegatesToNext(t *testing.T) {
	h := newTraceHandler(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("expected Info to be disabled when next handler's level is Warn")
	}
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("expected Warn to be enabled")
	}
}

func TestNewLogger_JSONFormat_WritesServiceAttr(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("my-service", "json", &buf)
	logger.Info("hi")
	if !strings.Contains(buf.String(), `"service":"my-service"`) {
		t.Fatalf("expected service attribute, got %q", buf.String())
	}
}

func TestNewLogger_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("my-service", "text", &buf)
	logger.Info("hi")
	if !strings.Contains(buf.String(), "service=my-service") {
		t.Fatalf("expected service attribute, got %q", buf.String())
	}
}

package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

var tracer = otel.Tracer("usecase/outbox")

type DispatchOutbox struct {
	outboxRepo      domain.OutboxRepository
	publisher       domain.Publisher
	batchSize       int
	maxRetries      int
	interval        time.Duration
	pruneAfter      time.Duration
	publishedTotal  metric.Int64Counter
	pendingCount    metric.Int64Gauge
	deadLetterCount metric.Int64Gauge
}

func New(
	outboxRepo domain.OutboxRepository,
	publisher domain.Publisher,
	batchSize, maxRetries int,
	interval, pruneAfter time.Duration,
) *DispatchOutbox {
	meter := otel.GetMeterProvider().Meter("usecase/outbox")
	publishedTotal, err := meter.Int64Counter("outbox.published_total")
	if err != nil {
		slog.ErrorContext(context.Background(), "create outbox.published_total counter failed", "err", err.Error())
	}
	pendingCount, err := meter.Int64Gauge("outbox.pending_count")
	if err != nil {
		slog.ErrorContext(context.Background(), "create outbox.pending_count gauge failed", "err", err.Error())
	}
	deadLetterCount, err := meter.Int64Gauge("outbox.dead_letter_count")
	if err != nil {
		slog.ErrorContext(context.Background(), "create outbox.dead_letter_count gauge failed", "err", err.Error())
	}
	return &DispatchOutbox{
		outboxRepo:      outboxRepo,
		publisher:       publisher,
		batchSize:       batchSize,
		maxRetries:      maxRetries,
		interval:        interval,
		pruneAfter:      pruneAfter,
		publishedTotal:  publishedTotal,
		pendingCount:    pendingCount,
		deadLetterCount: deadLetterCount,
	}
}

// notifyDebounce is how long Run waits after the first trigger signal in a
// burst before dispatching, so a flurry of NOTIFYs (e.g. many concurrent
// POSTs landing in the same instant) coalesces into a single dispatch pass
// instead of one per notification.
const notifyDebounce = 50 * time.Millisecond

// Run drives DispatchOutbox: it dispatches on every tick of the poll
// interval (the correctness fallback — always runs, regardless of trigger),
// prunes old published rows hourly, and — Phase 5 Track 3.A — also
// dispatches shortly after a NOTIFY arrives on trigger, so payments publish
// in single-digit ms on an idle system instead of waiting out the full poll
// interval.
//
// trigger may be nil (e.g. in tests, or if the caller chooses not to wire up
// a Listener) — Run works correctly with NOTIFY entirely absent, it just
// falls back to polling only. This is the dependency-rule seam: this
// use-case receives a plain `<-chan struct{}` and never imports pgx/lib/pq;
// the channel's producer (internal/infrastructure/database.Listener) is
// wired in by cmd/ingestion-api/main.go.
func (d *DispatchOutbox) Run(ctx context.Context, trigger <-chan struct{}) {
	ticker := time.NewTicker(d.interval)
	pruneTicker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	defer pruneTicker.Stop()

	// debounce is non-nil only while a trigger signal is pending, so the
	// select below only fires it once per debounce window even under a
	// burst of NOTIFYs.
	var debounce *time.Timer
	var debounceC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if debounce != nil {
				debounce.Stop()
			}
			return
		case <-ticker.C:
			d.dispatch(ctx)
		case <-pruneTicker.C:
			if err := d.outboxRepo.DeleteOldPublished(ctx, d.pruneAfter); err != nil {
				slog.ErrorContext(ctx, "outbox prune error", "err", err.Error())
			}
		case <-trigger:
			if debounce == nil {
				debounce = time.NewTimer(notifyDebounce)
				debounceC = debounce.C
			}
		case <-debounceC:
			debounce = nil
			debounceC = nil
			d.dispatch(ctx)
		}
	}
}

func (d *DispatchOutbox) dispatch(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "outbox.dispatch")
	defer span.End()

	msgs, err := d.outboxRepo.FetchPending(ctx, d.batchSize)
	if err != nil {
		slog.ErrorContext(ctx, "outbox fetch error", "err", pii.Redact(err.Error()))
		span.RecordError(err)
		span.SetStatus(codes.Error, pii.Redact(err.Error()))
		return
	}

	var publishedCount, retryingCount, deadLetterCount int

	for _, msg := range msgs {
		if err := d.publisher.Publish(ctx, msg); err != nil {
			reason := pii.Redact(err.Error())
			slog.ErrorContext(ctx, "outbox publish error", "idempotency_key", msg.IdempotencyKey, "err", reason)
			if msg.RetryCount+1 >= d.maxRetries {
				deadLetterCount++
				if markErr := d.outboxRepo.MarkDeadLetter(ctx, msg.ID, reason); markErr != nil {
					slog.ErrorContext(ctx, "outbox mark dead-letter error", "err", markErr.Error())
				}
			} else {
				retryingCount++
				if retryErr := d.outboxRepo.MarkRetrying(ctx, msg.ID, reason); retryErr != nil {
					slog.ErrorContext(ctx, "outbox mark retrying error", "err", retryErr.Error())
				}
			}
			continue
		}
		publishedCount++
		if markErr := d.outboxRepo.MarkPublished(ctx, msg.ID, time.Now().UTC()); markErr != nil {
			slog.ErrorContext(ctx, "outbox mark published error", "err", markErr.Error())
		}
		// Per-message, not batched, so the counter carries a `method`
		// dimension — a Grafana panel can show publish rate per method,
		// feeding capacity planning for the per-method KEDA limits.
		if d.publishedTotal != nil {
			d.publishedTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("method", msg.PaymentMethod)))
		}
	}

	span.SetAttributes(
		attribute.Int("batch_size", len(msgs)),
		attribute.Int("published_count", publishedCount),
		attribute.Int("retrying_count", retryingCount),
		attribute.Int("dead_letter_count", deadLetterCount),
	)
	// Record the TRUE backlog (Phase 5 Track 2.B) — len(msgs) is capped at
	// batchSize, so a 10,000-row backlog would otherwise read as a flat 50.
	if d.pendingCount != nil {
		if count, err := d.outboxRepo.CountPending(ctx); err != nil {
			slog.ErrorContext(ctx, "outbox count pending error", "err", err.Error())
		} else {
			d.pendingCount.Record(ctx, count)
		}
	}
	if d.deadLetterCount != nil {
		if count, err := d.outboxRepo.CountDeadLetter(ctx); err != nil {
			slog.ErrorContext(ctx, "outbox count dead-letter error", "err", err.Error())
		} else {
			d.deadLetterCount.Record(ctx, count)
		}
	}
}

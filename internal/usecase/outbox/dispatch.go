package outbox

import (
	"context"
	"log"
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
	outboxRepo     domain.OutboxRepository
	publisher      domain.Publisher
	batchSize      int
	maxRetries     int
	interval       time.Duration
	pruneAfter     time.Duration
	publishedTotal metric.Int64Counter
	pendingCount   metric.Int64Gauge
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
		log.Printf("create outbox.published_total counter: %v", err)
	}
	pendingCount, err := meter.Int64Gauge("outbox.pending_count")
	if err != nil {
		log.Printf("create outbox.pending_count gauge: %v", err)
	}
	return &DispatchOutbox{
		outboxRepo:     outboxRepo,
		publisher:      publisher,
		batchSize:      batchSize,
		maxRetries:     maxRetries,
		interval:       interval,
		pruneAfter:     pruneAfter,
		publishedTotal: publishedTotal,
		pendingCount:   pendingCount,
	}
}

func (d *DispatchOutbox) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	pruneTicker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	defer pruneTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatch(ctx)
		case <-pruneTicker.C:
			if err := d.outboxRepo.DeleteOldPublished(ctx, d.pruneAfter); err != nil {
				log.Printf("outbox prune error: %v", err)
			}
		}
	}
}

func (d *DispatchOutbox) dispatch(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "outbox.dispatch")
	defer span.End()

	msgs, err := d.outboxRepo.FetchPending(ctx, d.batchSize)
	if err != nil {
		log.Printf("outbox fetch error: %s", pii.Redact(err.Error()))
		span.RecordError(err)
		span.SetStatus(codes.Error, pii.Redact(err.Error()))
		return
	}

	var publishedCount, retryingCount, deadLetterCount int

	for _, msg := range msgs {
		if err := d.publisher.Publish(ctx, msg); err != nil {
			reason := pii.Redact(err.Error())
			log.Printf("outbox publish error for %s: %s", msg.IdempotencyKey, reason)
			if msg.RetryCount+1 >= d.maxRetries {
				deadLetterCount++
				if markErr := d.outboxRepo.MarkDeadLetter(ctx, msg.ID, reason); markErr != nil {
					log.Printf("outbox mark dead-letter error: %v", markErr)
				}
			} else {
				retryingCount++
				if retryErr := d.outboxRepo.MarkRetrying(ctx, msg.ID, reason); retryErr != nil {
					log.Printf("outbox mark retrying error: %v", retryErr)
				}
			}
			continue
		}
		publishedCount++
		if markErr := d.outboxRepo.MarkPublished(ctx, msg.ID, time.Now().UTC()); markErr != nil {
			log.Printf("outbox mark published error: %v", markErr)
		}
	}

	span.SetAttributes(
		attribute.Int("batch_size", len(msgs)),
		attribute.Int("published_count", publishedCount),
		attribute.Int("retrying_count", retryingCount),
		attribute.Int("dead_letter_count", deadLetterCount),
	)
	if d.publishedTotal != nil && publishedCount > 0 {
		d.publishedTotal.Add(ctx, int64(publishedCount))
	}
	if d.pendingCount != nil {
		d.pendingCount.Record(ctx, int64(len(msgs)))
	}
}

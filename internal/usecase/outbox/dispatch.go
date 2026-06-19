package outbox

import (
	"context"
	"log"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

type DispatchOutbox struct {
	outboxRepo domain.OutboxRepository
	publisher  domain.Publisher
	batchSize  int
	maxRetries int
	interval   time.Duration
	pruneAfter time.Duration
}

func New(
	outboxRepo domain.OutboxRepository,
	publisher domain.Publisher,
	batchSize, maxRetries int,
	interval, pruneAfter time.Duration,
) *DispatchOutbox {
	return &DispatchOutbox{
		outboxRepo: outboxRepo,
		publisher:  publisher,
		batchSize:  batchSize,
		maxRetries: maxRetries,
		interval:   interval,
		pruneAfter: pruneAfter,
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
	msgs, err := d.outboxRepo.FetchPending(ctx, d.batchSize)
	if err != nil {
		log.Printf("outbox fetch error: %v", err)
		return
	}

	for _, msg := range msgs {
		if err := d.publisher.Publish(ctx, msg); err != nil {
			log.Printf("outbox publish error for %s: %v", msg.IdempotencyKey, err)
			if msg.RetryCount+1 >= d.maxRetries {
				if markErr := d.outboxRepo.MarkFailed(ctx, msg.ID, err.Error()); markErr != nil {
					log.Printf("outbox mark failed error: %v", markErr)
				}
			} else {
				if retryErr := d.outboxRepo.IncrementRetry(ctx, msg.ID, err.Error()); retryErr != nil {
					log.Printf("outbox increment retry error: %v", retryErr)
				}
			}
			continue
		}
		if markErr := d.outboxRepo.MarkPublished(ctx, msg.ID, time.Now().UTC()); markErr != nil {
			log.Printf("outbox mark published error: %v", markErr)
		}
	}
}

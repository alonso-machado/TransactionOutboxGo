package database

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Listener is a dedicated Postgres connection that LISTENs on the
// "outbox_new" channel (Phase 5 Track 3.A) and pushes a value onto Notify
// for every NOTIFY it receives, so DispatchOutbox can wake up immediately on
// enqueue instead of waiting out the poll interval.
//
// GORM's pool doesn't expose LISTEN/WaitForNotification, so this uses pgx
// directly on its own connection — separate from the GORM-managed pool used
// for everything else. This is infrastructure: internal/usecase imports no
// pgx, it only ever receives from the Notify channel handed to it by
// cmd/ingestion-api/main.go.
//
// Strictly an optimization. If this connection drops, Run keeps retrying in
// the background; DispatchOutbox's existing poll ticker still drains
// everything on its own — correctness never depends on NOTIFY arriving.
type Listener struct {
	dsn    string
	Notify chan struct{}
}

// NewListener creates a Listener bound to dsn. Call Run in a goroutine to
// start listening; Notify receives a value (non-blocking best-effort) for
// every "outbox_new" notification.
func NewListener(dsn string) *Listener {
	return &Listener{
		dsn: dsn,
		// Buffered 1: dispatch.go debounces bursts itself, so the channel
		// only needs to carry "something happened," never queue up events.
		Notify: make(chan struct{}, 1),
	}
}

// Run connects, issues LISTEN outbox_new, and blocks relaying notifications
// onto l.Notify until ctx is cancelled. On any connection error it logs,
// waits a backoff, and reconnects — it never gives up and never panics; the
// caller's poll ticker is the correctness fallback while this is down.
func (l *Listener) Run(ctx context.Context) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}
		if err := l.listenOnce(ctx); err != nil {
			slog.ErrorContext(ctx, "outbox notify listener error", "err", err.Error(), "retry_in", backoff.String())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (l *Listener) listenOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, l.dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(ctx, "LISTEN outbox_new"); err != nil {
		return err
	}

	// Reset backoff implicitly: a successful connect+LISTEN means the next
	// failure (if any) starts fresh via the caller's loop variable — simplest
	// to just let Run's backoff keep growing across the process lifetime,
	// since reconnect storms are rare and capped at maxBackoff anyway.
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if notification == nil {
			continue
		}
		select {
		case l.Notify <- struct{}{}:
		default:
			// A notify is already pending; the dispatcher will pick up
			// every NEW/RETRYING row on that pass anyway, so coalescing
			// here is correct, not lossy.
		}
	}
}

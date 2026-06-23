//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/stretchr/testify/require"
)

// Phase 5 Track 3.A: the Listener LISTENs on the "outbox_new" channel on its
// own pgx connection and relays each NOTIFY onto its Notify channel, so
// DispatchOutbox can wake immediately on enqueue. This drives it against the
// real testcontainer Postgres: start the listener, fire pg_notify, and assert
// the channel wakes. NOTIFY only reaches an already-established LISTEN, so we
// fire repeatedly until the listener has connected.
func TestListener_RelaysNotifyOntoChannel(t *testing.T) {
	l := database.NewListener(suite.pgURI)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		require.NoError(t, suite.db.Exec("SELECT pg_notify('outbox_new', '')").Error)
		select {
		case <-l.Notify:
			return // success: the listener connected, LISTENed, and relayed
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("expected the listener to relay a NOTIFY onto its channel within the timeout")
}

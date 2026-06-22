package persistence

import (
	"strings"
)

// tableFor returns the per-method hypertable name an insert/query for method
// targets, e.g. "PIX" -> "payments_pix". Matches the queue-name convention
// (rmq.QueueFor) so the two stay obviously paired.
//
// Schema migrations (outbox_messages, the TimescaleDB hypertables, and the
// next_retry_at backoff column) now live as versioned golang-migrate
// .up/.down.sql pairs under migrations/ — see Phase 5 Track 1. They are
// applied by `make migrate` / the migrate/migrate compose service, never by
// the app at boot (AutoMigrate/MigrateTimescale removed).
func tableFor(method string) string {
	return "payments_" + strings.ToLower(method)
}

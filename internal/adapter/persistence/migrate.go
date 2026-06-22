package persistence

import (
	"fmt"
	"strings"

	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
	"gorm.io/gorm"
)

// AutoMigrate covers outbox_messages only — payments moved to TimescaleDB
// hypertables (MigrateTimescale) that GORM's AutoMigrate cannot create
// (extensions, hypertables, compression/retention policies are all raw SQL).
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&OutboxMessageModel{})
}

// tableFor returns the per-method hypertable name an insert/query for method
// targets, e.g. "PIX" -> "payments_pix". Matches the queue-name convention
// (rmq.QueueFor) so the two stay obviously paired.
func tableFor(method string) string {
	return "payments_" + strings.ToLower(method)
}

// MigrateTimescale creates one TimescaleDB hypertable per payment method
// (looping over rmq.Methods, the same canonical list the RabbitMQ topology
// is declared from, so a 6th method only ever needs one new list entry) plus
// a payments VIEW = UNION ALL of all of them, for callers/dashboards that
// want "all payments" without caring which method.
//
// Partitioned on occurred_at (the provider's event time), NOT created_at
// (insert time) — see .claude/plan-phase4.md Track 2: a RabbitMQ redelivery
// of the same message gets a new created_at but carries the same
// occurredAt, so dedup on (source_message_id, occurred_at) survives
// redelivery while dedup on (source_message_id, created_at) would not.
// TimescaleDB requires every UNIQUE index on a hypertable to include the
// partitioning column, which is what forces this dedup key to be two columns
// instead of the pre-Timescale single-column UNIQUE(source_message_id).
//
// Idempotent — every statement is safe to re-run on every boot
// (CREATE ... IF NOT EXISTS / if_not_exists => TRUE).
func MigrateTimescale(db *gorm.DB) error {
	if err := db.Exec(`CREATE EXTENSION IF NOT EXISTS timescaledb`).Error; err != nil {
		return fmt.Errorf("create timescaledb extension: %w", err)
	}

	for _, method := range rmq.Methods {
		table := tableFor(method)

		ddl := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s (
			  id                  uuid        NOT NULL,
			  source_message_id   text        NOT NULL,
			  event_id            text,
			  provider_name       text,
			  provider_payment_id text,
			  external_payment_id text,
			  payer_id            uuid,
			  recipient_id        uuid,
			  amount              bigint,
			  currency            text,
			  method              text        NOT NULL,
			  method_details      jsonb,
			  occurred_at         timestamptz NOT NULL,
			  created_at          timestamptz NOT NULL DEFAULT now(),
			  updated_at          timestamptz NOT NULL DEFAULT now(),
			  UNIQUE (source_message_id, occurred_at)
			)`, table)
		if err := db.Exec(ddl).Error; err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}

		if err := db.Exec(fmt.Sprintf(
			`SELECT create_hypertable('%s', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE)`,
			table,
		)).Error; err != nil {
			return fmt.Errorf("create hypertable %s: %w", table, err)
		}

		if err := db.Exec(fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %[1]s_event_id_idx ON %[1]s (event_id)`, table,
		)).Error; err != nil {
			return fmt.Errorf("create event_id index on %s: %w", table, err)
		}
	}

	unionSQL := make([]string, 0, len(rmq.Methods))
	for _, method := range rmq.Methods {
		unionSQL = append(unionSQL, "SELECT * FROM "+tableFor(method))
	}
	view := fmt.Sprintf(`CREATE OR REPLACE VIEW payments AS %s`, strings.Join(unionSQL, " UNION ALL "))
	if err := db.Exec(view).Error; err != nil {
		return fmt.Errorf("create payments view: %w", err)
	}

	return nil
}

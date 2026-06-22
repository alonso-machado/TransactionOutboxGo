package database

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// withSSLMode appends `sslmode=<mode>` to dsn unless the caller already set
// one explicitly. PCI-DSS encryption-in-transit posture (Phase 5 Track 5.B):
// local/compose defaults to "disable" (DB_SSL_MODE's own default), cloud
// (Pulumi's RDS DATABASE_URL — see infra/pulumi/data.go) sets "require" so
// the connection is TLS-enforced.
func withSSLMode(dsn, sslMode string) string {
	if sslMode == "" || strings.Contains(dsn, "sslmode=") {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "sslmode=" + sslMode
}

// Connect opens the GORM/Postgres connection pool. sslMode is the PCI-DSS
// Track 5.B toggle (config.Config.DBSSLMode) — pass "" to leave dsn
// untouched (e.g. when the caller already encoded sslmode itself).
func Connect(dsn string, sslMode string) (*gorm.DB, error) {
	dsn = withSSLMode(dsn, sslMode)
	// ParameterizedQueries: true keeps GORM's own slow-query/error logger from
	// printing bound values (CPF, barcodes, PIX ids, amounts, payer/recipient
	// UUIDs) to stdout — it substitutes '?' for every parameter instead.
	gormLogger := logger.New(log.New(os.Stdout, "", log.LstdFlags), logger.Config{
		SlowThreshold:        200 * time.Millisecond,
		LogLevel:             logger.Warn,
		ParameterizedQueries: true,
	})

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormLogger,
		// Phase 5 Track 3.B: PgBouncer in transaction-pooling mode hands out
		// a different server connection per transaction, so a server-side
		// prepared statement from one transaction may not exist (or may
		// collide with another client's statement of the same name) on the
		// next. Disabling GORM's own prepared-statement cache keeps the app
		// pooler-safe behind PgBouncer; see docker-compose.yml's pgbouncer
		// service and DATABASE_URL now pointing at it instead of postgres
		// directly.
		PrepareStmt: false,
	})
	if err != nil {
		return nil, err
	}
	// WithoutQueryVariables: bound query parameters include PII (CPF in
	// payerDocument, bank barcodes, PIX endToEndId, payer/recipient UUIDs,
	// amounts) — never send literal values to the trace backend.
	if err := db.Use(otelgorm.NewPlugin(otelgorm.WithoutQueryVariables())); err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(5)
	return db, nil
}

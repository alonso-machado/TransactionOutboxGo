// Package main is the composition root for tickets-api — new in Phase 8.
// tickets-api is a synchronous REST service that reads/writes the events
// DB directly: GET /api/v1/orders/{id} (order status + checkout URL,
// polled by the client after ingestion-api's POST /orders 201),
// POST /api/v1/checkin (staff-authenticated ticket check-in), and
// PATCH /api/v1/tickets/{id}/holder (buyer-name correction, rate-limited).
// It is the mirror-image of ingestion-api: ingestion-api touches the
// outbox DB and RabbitMQ but never events; tickets-api touches events but
// never the outbox DB or RabbitMQ at all.
//
//	@title			Transaction Outbox — Event Ticket System Tickets API
//	@version		1.0
//	@description	Order-status lookup, staff-authenticated ticket check-in, and ticket-holder
//	@description	name correction, reading/writing the events database directly.
//	@BasePath		/
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	handler "github.com/alonsomachado/transaction-outbox-go/internal/adapter/http"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/persistence"
	clerkauth "github.com/alonsomachado/transaction-outbox-go/internal/adapter/staffauth/clerk"
	fakeauth "github.com/alonsomachado/transaction-outbox-go/internal/adapter/staffauth/fake"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/config"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/database"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/logging"
	"github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/telemetry"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/checkin"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ticketholder"
)

func main() {
	ctx := context.Background()
	startupLog := logging.NewLogger("startup", "json", os.Stdout)

	cfg, err := config.Load()
	if err != nil {
		startupLog.ErrorContext(ctx, "config load failed", "err", err.Error())
		os.Exit(1)
	}

	telemetryShutdown, err := telemetry.Setup(ctx, cfg.OtelServiceName, cfg.OtelEndpoint, cfg.MetricsPort, cfg.LogFormat)
	if err != nil {
		startupLog.ErrorContext(ctx, "telemetry setup failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := telemetryShutdown(shutdownCtx); err != nil {
			slog.ErrorContext(shutdownCtx, "telemetry shutdown error", "err", err.Error())
		}
	}()

	// DATABASE_URL must point at the events DB here, not the outbox DB —
	// tickets-api is deployed with EVENTS_DATABASE_URL the same way
	// fulfillment-consumer-worker is (see helmcharts/.../tickets-api/deployment.yaml).
	db, err := database.Connect(cfg.DatabaseURL, cfg.DBSSLMode)
	if err != nil {
		slog.ErrorContext(ctx, "database connect failed", "err", err.Error())
		os.Exit(1)
	}
	// Schema migrations are applied by the migrate-events one-shot before
	// this starts, never here. No RabbitMQ connection at all — tickets-api
	// is purely synchronous REST against events.

	orderRepo := persistence.NewOrderRepository(db)
	chargeRepo := persistence.NewChargeRepository(db)
	ticketRepo := persistence.NewTicketRepository(db)
	eventRepo := persistence.NewEventRepository(db)
	staffUserRepo := persistence.NewStaffUserRepository(db)

	staffAuthenticator, err := newStaffAuthenticator(cfg)
	if err != nil {
		slog.ErrorContext(ctx, "staff authenticator init failed", "err", err.Error())
		os.Exit(1)
	}

	checkInUC := checkin.New(ticketRepo, eventRepo, cfg.TicketSigningSecret)
	updateHolderUC := ticketholder.New(ticketRepo)

	orderStatusHandler := handler.NewOrderStatusHandler(orderRepo, chargeRepo)
	checkinHandler := handler.NewCheckinHandler(checkInUC)
	ticketHolderHandler := handler.NewTicketHolderHandler(updateHolderUC)

	rateLimitStore := ratelimit.NewInMemoryStore(10 * time.Minute)

	router := handler.NewTicketsRouter(orderStatusHandler, checkinHandler, ticketHolderHandler, staffAuthenticator, staffUserRepo, cfg.OtelServiceName, cfg.SwaggerEnabled, handler.RouterConfig{
		TrustedProxies:   cfg.TrustedProxies,
		RateLimitEnabled: cfg.RateLimitEnabled,
		RateLimitStore:   rateLimitStore,
		RateLimitRate:    cfg.RateLimitRate,
		RateLimitBurst:   cfg.RateLimitBurst,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.RateLimitEnabled {
		janitorStop := make(chan struct{})
		go rateLimitStore.Janitor(janitorStop, time.Minute)
		go func() {
			<-ctx.Done()
			close(janitorStop)
		}()
	}

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: router,
	}

	go func() {
		slog.InfoContext(ctx, "tickets-api listening", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(ctx, "http server error", "err", err.Error())
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.InfoContext(ctx, "shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.ErrorContext(shutdownCtx, "graceful shutdown error", "err", err.Error())
	}
}

// newStaffAuthenticator selects the domain.StaffAuthenticator adapter by
// config.StaffAuthProvider: "fake" (default — a fixed test token, no
// network, for local dev/tests) or "clerk" (real).
func newStaffAuthenticator(cfg *config.Config) (domain.StaffAuthenticator, error) {
	switch cfg.StaffAuthProvider {
	case "clerk":
		return clerkauth.New(cfg.ClerkSecretKey), nil
	case "fake", "":
		return fakeauth.New(cfg.StaffAuthFakeToken, cfg.StaffAuthFakeClerkUserID), nil
	default:
		return nil, fmt.Errorf("unknown STAFF_AUTH_PROVIDER %q", cfg.StaffAuthProvider)
	}
}

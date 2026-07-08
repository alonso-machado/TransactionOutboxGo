package handler

import (
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/staffauth"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// NewTicketsRouter builds tickets-api's router — deliberately separate
// from NewRouter (ingestion-api's), since the route set and rate-limit
// topology differ entirely: GET /orders/:id and POST /checkin are
// unlimited (order-status is legitimately polled by the client itself;
// check-in is staff-operated from a small set of door devices, gated by
// auth rather than throttling), while PATCH /tickets/:id/holder is
// rate-limited and unauthenticated (confirmed with the user).
func NewTicketsRouter(
	orderStatusHandler *OrderStatusHandler,
	checkinHandler *CheckinHandler,
	ticketHolderHandler *TicketHolderHandler,
	staffAuthenticator domain.StaffAuthenticator,
	staffUserRepo domain.StaffUserRepository,
	serviceName string,
	swaggerEnabled bool,
	rl RouterConfig,
) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())
	r.Use(otelgin.Middleware(serviceName))

	if err := r.SetTrustedProxies(rl.TrustedProxies); err != nil {
		panic(err)
	}

	r.GET("/healthz", healthz)

	v1 := r.Group("/api/v1")
	{
		v1.GET("/orders/:id", orderStatusHandler.Handle)
		v1.POST("/checkin", staffauth.Middleware(staffAuthenticator, staffUserRepo), checkinHandler.Handle)
	}

	holderGroup := r.Group("/api/v1")
	if rl.RateLimitEnabled {
		holderGroup.Use(ratelimit.Middleware(rl.RateLimitStore, rl.RateLimitRate, rl.RateLimitBurst))
	}
	holderGroup.PATCH("/tickets/:id/holder", ticketHolderHandler.Handle)

	return r
}

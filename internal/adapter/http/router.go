package handler

import (
	"net/http"

	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	docs "github.com/alonsomachado/transaction-outbox-go/docs/ingestion-api"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// RouterConfig carries the optional cross-cutting middleware settings for
// NewRouter, keeping the function signature stable as Phase 4 adds more.
type RouterConfig struct {
	TrustedProxies   []string
	RateLimitEnabled bool
	RateLimitStore   ratelimit.BucketStore
	RateLimitRate    float64
	RateLimitBurst   int
}

// healthz reports liveness/readiness for K8s probes.
//
//	@Summary	Health check
//	@Tags		health
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Router		/healthz [get]
func healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func NewRouter(orderHandler *OrderHandler, webhookHandler *WebhookHandler, serviceName string, swaggerEnabled bool, rl RouterConfig) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())
	r.Use(otelgin.Middleware(serviceName))

	if err := r.SetTrustedProxies(rl.TrustedProxies); err != nil {
		panic(err)
	}

	r.GET("/healthz", healthz)

	v1 := r.Group("/api/v1")
	if rl.RateLimitEnabled {
		v1.Use(ratelimit.Middleware(rl.RateLimitStore, rl.RateLimitRate, rl.RateLimitBurst))
	}
	{
		v1.POST("/orders", orderHandler.Handle)
	}

	// The gateway webhook route sits OUTSIDE the rate-limited v1 group: a
	// payment gateway calls back from its own infrastructure, not a single
	// client IP, and must never be throttled by the leaky-bucket limiter.
	r.POST("/api/v1/webhooks/payments/:provider", webhookHandler.Handle)

	if swaggerEnabled {
		docs.SwaggerInfo.Title = serviceName
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	return r
}

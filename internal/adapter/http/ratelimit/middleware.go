package ratelimit

import (
	"fmt"
	"net/http"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/observability"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
)

// Middleware throttles requests per client IP (c.ClientIP()) using a
// leaky-bucket meter backed by store. It does not read the request body.
func Middleware(store BucketStore, rate float64, burst int) gin.HandlerFunc {
	meter := otel.GetMeterProvider().Meter("adapter/http/ratelimit")
	rejectedTotal := observability.Int64Counter(meter, "ingestion.ratelimit_rejected_total")

	return func(c *gin.Context) {
		key := c.ClientIP()
		allowed, retryAfter := store.Allow(key, rate, burst, time.Now())

		c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", burst))

		if !allowed {
			c.Header("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds())))
			c.Header("X-RateLimit-Remaining", "0")
			rejectedTotal.Add(c.Request.Context(), 1)
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}

		c.Next()
	}
}

package handler

import (
	"net/http"

	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/alonsomachado/transaction-outbox-go/docs"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

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

func NewRouter(paymentHandler *PaymentHandler, serviceName string, swaggerEnabled bool) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())
	r.Use(otelgin.Middleware(serviceName))

	r.GET("/healthz", healthz)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/payments", paymentHandler.Handle)
		v1.PUT("/payments/:id", paymentHandler.Handle)
		v1.PATCH("/payments/:id", paymentHandler.Handle)
	}

	if swaggerEnabled {
		docs.SwaggerInfo.Title = serviceName
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	return r
}

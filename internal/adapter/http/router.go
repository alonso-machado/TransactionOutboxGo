package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func NewRouter(paymentHandler *PaymentHandler) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/api/v1")
	{
		v1.POST("/payments", paymentHandler.Handle)
		v1.PUT("/payments/:id", paymentHandler.Handle)
		v1.PATCH("/payments/:id", paymentHandler.Handle)
	}

	return r
}

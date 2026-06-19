package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func NewRouter(recordHandler *RecordHandler) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/api/v1")
	{
		v1.POST("/records", recordHandler.Handle)
		v1.PUT("/records/:id", recordHandler.Handle)
		v1.PATCH("/records/:id", recordHandler.Handle)
	}

	return r
}

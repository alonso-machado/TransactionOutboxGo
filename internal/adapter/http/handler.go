package handler

import (
	"io"
	"net/http"

	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ingest"
	"github.com/gin-gonic/gin"
)

type RecordHandler struct {
	ingest *ingest.IngestRecord
}

func NewRecordHandler(ingest *ingest.IngestRecord) *RecordHandler {
	return &RecordHandler{ingest: ingest}
}

func (h *RecordHandler) Handle(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}
	if len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "empty body"})
		return
	}

	headers := make(map[string]string, len(c.Request.Header))
	for k := range c.Request.Header {
		headers[k] = c.GetHeader(k)
	}

	resp, err := h.ingest.Execute(c.Request.Context(), ingest.Request{
		HTTPMethod:     c.Request.Method,
		Route:          c.FullPath(),
		Payload:        body,
		Headers:        headers,
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, RecordResponseDTO{
		MessageID:      resp.MessageID.String(),
		IdempotencyKey: resp.IdempotencyKey,
		Status:         "accepted",
	})
}

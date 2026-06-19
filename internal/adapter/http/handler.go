package handler

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ingest"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type PaymentHandler struct {
	ingest *ingest.IngestPayment
}

func NewPaymentHandler(ingest *ingest.IngestPayment) *PaymentHandler {
	return &PaymentHandler{ingest: ingest}
}

func (h *PaymentHandler) Handle(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	var dto PaymentEventRequestDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := dto.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// The payment method's details live in a top-level sibling object named
	// after the lowercase method (e.g. "pix": {...}) — extract it generically
	// so adding a new method never requires a schema change here.
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(body, &raw)

	if err := dto.ValidateMethod(raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	payerID, err := parseOptionalUUID(dto.Payment.PayerID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payment.payerId"})
		return
	}
	recipientID, err := parseOptionalUUID(dto.Payment.RecipientID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payment.recipientId"})
		return
	}

	methodDetails := raw[strings.ToLower(dto.Payment.Method)]

	headers := make(map[string]string, len(c.Request.Header))
	for k := range c.Request.Header {
		headers[k] = c.GetHeader(k)
	}

	resp, err := h.ingest.Execute(c.Request.Context(), ingest.Request{
		HTTPMethod:        c.Request.Method,
		Route:             c.FullPath(),
		EventID:           dto.EventID,
		ProviderName:      dto.Provider.Name,
		ProviderPaymentID: dto.Provider.ProviderPaymentID,
		ExternalPaymentID: dto.Payment.PaymentID,
		PayerID:           payerID,
		RecipientID:       recipientID,
		Amount:            toMinorUnits(dto.Payment.Amount),
		Currency:          dto.Payment.Currency,
		Method:            dto.Payment.Method,
		MethodDetails:     methodDetails,
		OccurredAt:        dto.OccurredAt,
		Headers:           headers,
		IdempotencyKey:    c.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	status := "accepted"
	if !resp.Created {
		status = "duplicate"
	}

	c.JSON(http.StatusCreated, PaymentResponseDTO{
		PaymentID:      resp.PaymentID.String(),
		IdempotencyKey: resp.IdempotencyKey,
		Status:         status,
	})
}

func parseOptionalUUID(s *string) (*uuid.UUID, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	id, err := uuid.Parse(*s)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// toMinorUnits converts a decimal currency amount (e.g. 100.50) to integer
// minor units (e.g. 10050 cents), rounding to the nearest cent.
func toMinorUnits(amount float64) int64 {
	return int64(math.Round(amount * 100))
}

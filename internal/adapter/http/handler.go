package handler

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ingest"
	"github.com/gin-gonic/gin"
)

// propagatedHeaders is the allowlist of inbound request headers copied onto
// the outbox row and the RabbitMQ message. Copying every header would persist
// and publish credentials (Authorization, Cookie, ...) — only correlation and
// idempotency metadata is allowed to travel with the message.
var propagatedHeaders = []string{
	"Idempotency-Key",
	"X-Request-Id",
	"Traceparent",
	"Tracestate",
	"Baggage",
}

type PaymentHandler struct {
	ingest *ingest.IngestPayment
}

func NewPaymentHandler(ingest *ingest.IngestPayment) *PaymentHandler {
	return &PaymentHandler{ingest: ingest}
}

// Handle ingests a payment event.
//
//	@Summary		Ingest a payment event
//	@Description	Accepts a payment-provider webhook-shaped event (PIX, BOLETO, TRANSFER, or any other method)
//	@Description	and durably stores it in the outbox for asynchronous relay to RabbitMQ. Idempotent on the
//	@Description	derived idempotency key (provider.name + eventId + optional Idempotency-Key header).
//	@Tags			payments
//	@Accept			json
//	@Produce		json
//	@Param			Idempotency-Key	header		string				false	"Optional client-supplied idempotency key"
//	@Param			id				path		string				false	"Payment ID (PUT/PATCH only)"
//	@Param			payment			body		PaymentRequest		true	"Payment event payload"
//	@Success		201				{object}	PaymentResponse
//	@Failure		400				{object}	ErrorResponse
//	@Failure		500				{object}	ErrorResponse
//	@Router			/api/v1/payments [post]
//	@Router			/api/v1/payments/{id} [put]
//	@Router			/api/v1/payments/{id} [patch]
func (h *PaymentHandler) Handle(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	// Parse the body once into a raw map: the typed envelope is decoded from
	// it, and the method-specific details live in a top-level sibling object
	// named after the lowercase method (e.g. "pix": {...}), extracted
	// generically so adding a new method never requires a schema change here.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	dto, err := decodeEnvelope(raw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	if err := dto.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	if err := dto.ValidateMethod(raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	payerID, err := domain.ParseOptionalUUID(dto.Payment.PayerID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payment.payerId"})
		return
	}
	recipientID, err := domain.ParseOptionalUUID(dto.Payment.RecipientID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payment.recipientId"})
		return
	}

	methodDetails := raw[strings.ToLower(dto.Payment.Method)]
	switch strings.ToUpper(dto.Payment.Method) {
	case "CARTAO_CREDITO", "CARTAO_DEBITO":
		masked, err := maskPAN(methodDetails)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
			return
		}
		methodDetails = masked
	}

	headers := make(map[string]string, len(propagatedHeaders))
	for _, name := range propagatedHeaders {
		if v := c.GetHeader(name); v != "" {
			headers[name] = v
		}
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
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

// toMinorUnits converts a decimal currency amount (e.g. 100.50) to integer
// minor units (e.g. 10050 cents), rounding to the nearest cent.
func toMinorUnits(amount float64) int64 {
	return int64(math.Round(amount * 100))
}

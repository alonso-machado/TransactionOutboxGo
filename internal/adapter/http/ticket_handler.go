package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ticket"
	"github.com/gin-gonic/gin"
)

type TicketHandler struct {
	ingest *ticket.IngestTicket
}

func NewTicketHandler(ingest *ticket.IngestTicket) *TicketHandler {
	return &TicketHandler{ingest: ingest}
}

// Handle ingests a ticket order event.
//
//	@Summary		Ingest a ticket order
//	@Description	Accepts a ticket-order event and durably stores its "order" object in the ticket_outbox
//	@Description	table for later processing by a dedicated ticket microservice. Idempotent on the order's
//	@Description	event_id (plus an optional Idempotency-Key header).
//	@Tags			tickets
//	@Accept			json
//	@Produce		json
//	@Param			Idempotency-Key	header		string				false	"Optional client-supplied idempotency key"
//	@Param			ticket			body		TicketRequestDTO	true	"Ticket order payload"
//	@Success		201				{object}	TicketResponseDTO
//	@Failure		400				{object}	map[string]string
//	@Failure		500				{object}	map[string]string
//	@Router			/api/v1/ticket [post]
func (h *TicketHandler) Handle(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	// Pull the raw "order" object out once so it can be stored opaquely as the
	// payload, independent of the typed DTO used only for validation — same
	// raw-message approach as the payments handler.
	var envelope struct {
		Order json.RawMessage `json:"order"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	if len(envelope.Order) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "order is required"})
		return
	}

	var dto TicketRequestDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	if err := dto.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	resp, err := h.ingest.Execute(c.Request.Context(), ticket.Request{
		EventID:        dto.Order.EventID,
		Payload:        envelope.Order,
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	status := "accepted"
	if !resp.Created {
		status = "duplicate"
	}

	c.JSON(http.StatusCreated, TicketResponseDTO{
		TicketID:       resp.TicketID.String(),
		IdempotencyKey: resp.IdempotencyKey,
		Status:         status,
	})
}

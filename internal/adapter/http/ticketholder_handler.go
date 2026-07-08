package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/ticketholder"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type TicketHolderHandler struct {
	updateHolder *ticketholder.UpdateHolder
}

func NewTicketHolderHandler(updateHolder *ticketholder.UpdateHolder) *TicketHolderHandler {
	return &TicketHolderHandler{updateHolder: updateHolder}
}

// Handle corrects a ticket's buyer name — no staff auth (rate-limited only,
// see tickets_router.go's routing).
//
//	@Summary		Correct a ticket holder's name
//	@Description	Updates the buyer name on a VALID or CHECKED_IN ticket. Rate-limited per source IP.
//	@Tags			tickets
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string					true	"Ticket ID"
//	@Param			holder	body		UpdateHolderRequestDTO	true	"New holder name"
//	@Success		200		{object}	UpdateHolderResponseDTO
//	@Failure		400		{object}	map[string]string
//	@Failure		404		{object}	map[string]string
//	@Failure		409		{object}	map[string]string
//	@Failure		429		{object}	map[string]string
//	@Router			/api/v1/tickets/{id}/holder [patch]
func (h *TicketHolderHandler) Handle(c *gin.Context) {
	ticketID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ticket id"})
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	var dto UpdateHolderRequestDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	if err := dto.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	if err := h.updateHolder.Execute(c.Request.Context(), ticketholder.Request{TicketID: ticketID, Name: dto.Name}); err != nil {
		switch {
		case errors.Is(err, ticketholder.ErrTicketNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "ticket not found"})
		case errors.Is(err, ticketholder.ErrNotEditable):
			c.JSON(http.StatusConflict, gin.H{"error": "ticket is not in an editable state"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
		}
		return
	}

	c.JSON(http.StatusOK, UpdateHolderResponseDTO{TicketID: ticketID.String(), BuyerName: dto.Name})
}

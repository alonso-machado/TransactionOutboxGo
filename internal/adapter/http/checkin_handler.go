package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/staffauth"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/checkin"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type CheckinHandler struct {
	checkIn *checkin.CheckIn
}

func NewCheckinHandler(checkIn *checkin.CheckIn) *CheckinHandler {
	return &CheckinHandler{checkIn: checkIn}
}

// Handle verifies a scanned ticket's QR signature and checks it in.
// Requires a staff Bearer token (staffauth.Middleware, wired on this route
// only) — the authenticated staff member's venue scope is enforced inside
// the use-case.
//
//	@Summary		Check in a ticket
//	@Description	Verifies the ticket's HMAC signature and flips it VALID -> CHECKED_IN. Requires a
//	@Description	staff Bearer token. Idempotent: checking in an already-checked-in ticket returns
//	@Description	ALREADY_CHECKED_IN, not an error.
//	@Tags			checkin
//	@Accept			json
//	@Produce		json
//	@Security		BearerAuth
//	@Param			checkin	body		CheckinRequestDTO	true	"Scanned ticket QR fields"
//	@Success		200		{object}	CheckinResponseDTO
//	@Failure		400		{object}	map[string]string
//	@Failure		401		{object}	map[string]string
//	@Failure		403		{object}	map[string]string
//	@Failure		404		{object}	map[string]string
//	@Failure		409		{object}	map[string]string
//	@Router			/api/v1/checkin [post]
func (h *CheckinHandler) Handle(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	var dto CheckinRequestDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	if err := dto.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	ticketID, err := uuid.Parse(dto.TicketID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ticketId"})
		return
	}

	var staffLocationID *uuid.UUID
	if staff := staffauth.StaffUserFromContext(c); staff != nil {
		staffLocationID = staff.LocationID
	}

	resp, err := h.checkIn.Execute(c.Request.Context(), checkin.Request{
		TicketID:        ticketID,
		ValidationCode:  dto.ValidationCode,
		Signature:       dto.Signature,
		StaffLocationID: staffLocationID,
	})
	if err != nil {
		if errors.Is(err, checkin.ErrTicketNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "ticket not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	respBody := CheckinResponseDTO{Outcome: string(resp.Outcome)}
	if resp.Ticket != nil {
		respBody.Ticket = &CheckinTicketDTO{
			BuyerName: resp.Ticket.BuyerName,
			Section:   resp.Ticket.Section,
			Row:       resp.Ticket.Row,
			Seat:      resp.Ticket.Seat,
		}
	}
	c.JSON(outcomeStatus(resp.Outcome), respBody)
}

func outcomeStatus(o checkin.Outcome) int {
	switch o {
	case checkin.OutcomeCheckedIn, checkin.OutcomeAlreadyCheckedIn:
		return http.StatusOK
	case checkin.OutcomeInvalidSignature:
		return http.StatusBadRequest
	case checkin.OutcomeNotIssued:
		return http.StatusConflict
	case checkin.OutcomeWrongVenue:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

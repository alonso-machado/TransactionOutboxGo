package handler

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/order"
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

type OrderHandler struct {
	place *order.PlaceOrder
}

func NewOrderHandler(place *order.PlaceOrder) *OrderHandler {
	return &OrderHandler{place: place}
}

// Handle ingests a ticket order.
//
//	@Summary		Place a ticket order
//	@Description	Accepts a request for tickets to an Event, durably stores it in the order outbox
//	@Description	for asynchronous relay to RabbitMQ (routed by eventType/eventSubtype). Idempotent on
//	@Description	the derived idempotency key (sourceOrderId + optional Idempotency-Key header).
//	@Tags			orders
//	@Accept			json
//	@Produce		json
//	@Param			Idempotency-Key	header		string			false	"Optional client-supplied idempotency key"
//	@Param			order			body		OrderRequestDTO	true	"Ticket order payload"
//	@Success		201				{object}	OrderResponseDTO
//	@Failure		400				{object}	map[string]string
//	@Failure		500				{object}	map[string]string
//	@Router			/api/v1/orders [post]
func (h *OrderHandler) Handle(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	var dto OrderRequestDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	if err := dto.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}
	dto.EventType = strings.ToUpper(dto.EventType)
	dto.EventSubtype = strings.ToUpper(dto.EventSubtype)
	if err := dto.ValidateEventType(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	items := make([]order.ItemRequest, len(dto.Tickets))
	for i, t := range dto.Tickets {
		items[i] = order.ItemRequest{
			SourceTicketID: t.ID,
			Section:        t.Section,
			Row:            t.Row,
			Seat:           t.Seat,
			Price:          toMinorUnits(t.Price),
			Currency:       t.Currency,
		}
	}

	headers := make(map[string]string, len(propagatedHeaders))
	for _, name := range propagatedHeaders {
		if v := c.GetHeader(name); v != "" {
			headers[name] = v
		}
	}

	resp, err := h.place.Execute(c.Request.Context(), order.Request{
		SourceOrderID:    dto.SourceOrderID,
		EventType:        dto.EventType,
		EventSubtype:     dto.EventSubtype,
		SourceEventID:    dto.EventID,
		EventName:        dto.EventName,
		SourceVenueID:    dto.Venue.ID,
		VenueName:        dto.Venue.Name,
		VenueCity:        dto.Venue.City,
		Items:            items,
		CustomerName:     dto.Customer.Name,
		CustomerEmail:    dto.Customer.Email,
		CustomerDocument: dto.Customer.Document,
		Currency:         dto.Tickets[0].Currency,
		Headers:          headers,
		IdempotencyKey:   c.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	status := "accepted"
	if !resp.Created {
		status = "duplicate"
	}

	c.JSON(http.StatusCreated, OrderResponseDTO{
		OrderID:        resp.OrderID.String(),
		IdempotencyKey: resp.IdempotencyKey,
		Status:         status,
	})
}

// toMinorUnits converts a decimal currency amount (e.g. 100.50) to integer
// minor units (e.g. 10050 cents), rounding to the nearest cent.
func toMinorUnits(amount float64) int64 {
	return int64(math.Round(amount * 100))
}

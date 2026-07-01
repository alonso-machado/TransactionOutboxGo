package handler

import (
	"errors"
	"fmt"
)

// TicketRequestDTO is the inbound body for POST /api/v1/ticket — a single
// "order" envelope. Only the fields the endpoint validates are typed here;
// the full order object (venue, tickets, payment_details, customer, ...) is
// stored opaquely as the ticket_outbox payload, so adding order fields never
// requires a DTO change.
type TicketRequestDTO struct {
	Order TicketOrderDTO `json:"order"`
}

type TicketOrderDTO struct {
	EventID        string                  `json:"event_id"`
	Tickets        []TicketItemDTO         `json:"tickets"`
	PaymentDetails TicketPaymentDetailsDTO `json:"payment_details"`
}

type TicketItemDTO struct {
	ID       string  `json:"id"`
	Section  string  `json:"section"`
	Row      string  `json:"row"`
	Seat     string  `json:"seat"`
	Price    float64 `json:"price"`
	Currency string  `json:"currency"`
}

type TicketPaymentDetailsDTO struct {
	Method      string  `json:"method"`
	TotalAmount float64 `json:"total_amount"`
	Status      string  `json:"status"`
}

// TicketResponseDTO is the 201 body for POST /api/v1/ticket.
type TicketResponseDTO struct {
	TicketID       string `json:"ticketId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Status         string `json:"status"` // "accepted" or "duplicate"
}

// Validate checks the minimum an order must carry to be processable later:
// an event_id (the dedup boundary), at least one ticket, and a payment
// method. Everything else is stored as-is for the future ticket microservice.
func (d *TicketRequestDTO) Validate() error {
	if d.Order.EventID == "" {
		return errors.New("order.event_id is required")
	}
	if len(d.Order.Tickets) == 0 {
		return errors.New("order.tickets must contain at least one ticket")
	}
	if d.Order.PaymentDetails.Method == "" {
		return errors.New("order.payment_details.method is required")
	}
	for i, t := range d.Order.Tickets {
		if t.ID == "" {
			return fmt.Errorf("order.tickets[%d].id is required", i)
		}
	}
	return nil
}
